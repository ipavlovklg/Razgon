import os
import sqlite3
import time
import signal
from datetime import datetime
from typing import Optional, List
import logging

# Import get_volumes and VolumeInfo from the external module
from get_volumes import get_volumes, VolumeInfo

# --- Stop control ---
stop_event = False

def signal_handler(signum, frame):
    global stop_event
    print("\nStop signal received. Stopping gracefully...")
    stop_event = True

def should_stop():
    global stop_event
    return stop_event

def stop_requested():
    global stop_event
    stop_event = True

# --- DB Initialization ---
def init_db_schema(db_path: str):
    """
    Creates the database file (or opens an existing one) and checks/creates/migrates its schema.
    """
    import pathlib
    path = pathlib.Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    current_version = get_current_schema_version(cursor)

    if current_version < 1:
        apply_migration_v1_to_v2(cursor)

    cursor.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (1);")

    conn.commit()
    conn.close()

def get_current_schema_version(cursor) -> int:
    try:
        cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1;")
        row = cursor.fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0

def apply_migration_v1_to_v2(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS volumes (
            id INTEGER PRIMARY KEY,
            device_guid TEXT UNIQUE NOT NULL,
            drive_letter TEXT NOT NULL,
            label TEXT,
            filesystem TEXT,
            last_scanned_at TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS directories (
            id INTEGER PRIMARY KEY,
            volume_id INTEGER NOT NULL,
            relative_path TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            modified_at TIMESTAMP NOT NULL,
            indexed_at TIMESTAMP,
            FOREIGN KEY (volume_id) REFERENCES volumes(id),
            UNIQUE(volume_id, relative_path)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            directory_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            modified_at TIMESTAMP NOT NULL,
            hash TEXT NULL,
            indexed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (directory_id) REFERENCES directories(id),
            UNIQUE(directory_id, filename)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unique_files (
            id INTEGER PRIMARY KEY,
            source_file_id INTEGER NOT NULL,
            is_duplicate_of INTEGER,
            copy_status TEXT DEFAULT 'pending',
            copied_at TIMESTAMP,
            FOREIGN KEY (source_file_id) REFERENCES files(id),
            FOREIGN KEY (is_duplicate_of) REFERENCES unique_files(id)
        );
    """)

# --- DB Functions for Functionality 1 ---
def get_volume_by_guid(db_conn, device_guid: str) -> Optional[int]:
    """Fetches the volume ID by its GUID."""
    cursor = db_conn.execute("SELECT id FROM volumes WHERE device_guid = ?", (device_guid,))
    row = cursor.fetchone()
    return row[0] if row else None

def create_or_update_volume(db_conn, volume_info: VolumeInfo) -> int:
    """Creates or updates a volume record. Returns the volume ID."""
    # Используем курсор для выполнения запроса
    cursor = db_conn.cursor()
    
    volume_id = get_volume_by_guid(db_conn, volume_info.device_id)
    if volume_id:
        cursor.execute(
            "UPDATE volumes SET drive_letter = ?, label = ?, filesystem = ? WHERE id = ?",
            (volume_info.drive_letter, volume_info.label, volume_info.fs, volume_id)
        )
        return volume_id
    else:
        cursor.execute(
            "INSERT INTO volumes (device_guid, drive_letter, label, filesystem) VALUES (?, ?, ?, ?)",
            (volume_info.device_id, volume_info.drive_letter, volume_info.label, volume_info.fs)
        )
        # Возвращаем lastrowid от курсора, а не от соединения
        return cursor.lastrowid


def is_directory_fully_indexed(db_conn, volume_id: int, relative_path: str) -> bool:
    """Checks if a directory has been fully indexed (has an indexed_at timestamp)."""
    cursor = db_conn.execute(
        "SELECT indexed_at FROM directories WHERE volume_id = ? AND relative_path = ?",
        (volume_id, relative_path)
    )
    row = cursor.fetchone()
    return row is not None and row[0] is not None

def upsert_directory_record(db_conn, volume_id: int, relative_path: str, stat_result, indexed_at: Optional[float]) -> int:
    """
    Inserts or updates a directory record. Returns the directory ID.
    """
    # Используем курсор
    cursor = db_conn.cursor()
    
    existing_cursor = db_conn.execute("SELECT id FROM directories WHERE volume_id = ? AND relative_path = ?", (volume_id, relative_path))
    existing_row = existing_cursor.fetchone()
    existing_id = existing_row[0] if existing_row else None

    if existing_id:
        db_conn.execute(
            "UPDATE directories SET created_at=?, modified_at=?, indexed_at=? WHERE id=?",
            (datetime.fromtimestamp(stat_result.st_ctime), datetime.fromtimestamp(stat_result.st_mtime), indexed_at, existing_id)
        )
        return existing_id
    else:
        cursor.execute( # <- cursor, а не db_conn
            "INSERT INTO directories (volume_id, relative_path, created_at, modified_at, indexed_at) VALUES (?, ?, ?, ?, ?)",
            (volume_id, relative_path, datetime.fromtimestamp(stat_result.st_ctime), datetime.fromtimestamp(stat_result.st_mtime), indexed_at)
        )
        # <- cursor, а не db_conn
        return cursor.lastrowid 


def mark_directory_as_indexed(db_conn, dir_id: int, timestamp: float):
    """Marks a directory as fully indexed by setting its indexed_at timestamp."""
    db_conn.execute(
        "UPDATE directories SET indexed_at = ? WHERE id = ?",
        (datetime.fromtimestamp(timestamp), dir_id)
    )

def insert_file_record(db_conn, dir_id: int, filename: str, stat_result):
    """Inserts a file record into the database."""
    db_conn.execute(
        "INSERT OR IGNORE INTO files (directory_id, filename, size_bytes, created_at, modified_at) VALUES (?, ?, ?, ?, ?)",
        (dir_id, filename, stat_result.st_size, datetime.fromtimestamp(stat_result.st_ctime), datetime.fromtimestamp(stat_result.st_mtime))
    )

# --- Recursive Scanning Logic ---
def scan_single_volume_recursive(db_conn, volume_id: int, current_path: str, root_drive_path: str, progress_counter):
    relative_path = os.path.relpath(current_path, root_drive_path).replace("/", "\\")
    if relative_path == ".":
        relative_path = ""

    dir_already_indexed = is_directory_fully_indexed(db_conn, volume_id, relative_path)

    if not dir_already_indexed:
        try:
            items = os.listdir(current_path)
        except PermissionError:
            print(f"Permission denied: {current_path}")
            return

        files_in_dir = [item for item in items if os.path.isfile(os.path.join(current_path, item))]
        subdirs_in_dir = [item for item in items if os.path.isdir(os.path.join(current_path, item))]

        dir_stat = os.stat(current_path)
        dir_id = upsert_directory_record(db_conn, volume_id, relative_path, dir_stat, indexed_at=None)

        for file_name in files_in_dir:
            file_path = os.path.join(current_path, file_name)
            try:
                file_stat = os.stat(file_path)
                insert_file_record(db_conn, dir_id, file_name, file_stat)
                progress_counter['processed_files'] += 1
                progress_counter['processed_size'] += file_stat.st_size
                if should_stop():
                    return
            except (OSError, PermissionError) as e:
                print(f"Error accessing file {file_path}: {e}")

        for subdir_name in subdirs_in_dir:
            subdir_abs_path = os.path.join(current_path, subdir_name)
            scan_single_volume_recursive(db_conn, volume_id, subdir_abs_path, root_drive_path, progress_counter)
            if should_stop():
                return

        mark_directory_as_indexed(db_conn, dir_id, time.time())
        progress_counter['processed_dirs'] += 1

    else:
        print(f"Skipping already indexed directory: {relative_path}")

def scan_single_volume(db_conn: sqlite3.Connection, volume_info: VolumeInfo, progress_counter):
    volume_id = create_or_update_volume(db_conn, volume_info)
    root_path = volume_info.drive_letter + "\\"
    scan_single_volume_recursive(db_conn, volume_id, root_path, root_path, progress_counter)

def format_bytes(bytes_value: int) -> str:
    bytes_value_f = float(bytes_value)
    """Converts bytes to a human-readable string (KB, MB, GB)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value_f < 1024.0:
            return f"{bytes_value_f:.2f} {unit}"
        bytes_value_f /= 1024
    return f"{bytes_value_f:.2f} PB" # Should not happen for typical disks

def print_progress(current_files, current_size, current_dirs):
    """Prints the current progress in terms of processed files and size."""
    size_str = format_bytes(current_size)
    print(f"\rIndexed: {current_files} files, {size_str}", end='', flush=True)

def scan_and_index_volumes(db_path: str, target_drive_letters: List[str]):
    """Main function to scan and index specified volumes."""
    global stop_event
    stop_event = False

    init_db_schema(db_path)
    db_conn = sqlite3.connect(db_path)

    volumes = get_volumes()
    target_volumes = [v for v in volumes if v.drive_letter in target_drive_letters]

    if not target_volumes:
        print("No matching volumes found for the provided drive letters.")
        return

    progress_counter = {'processed_files': 0, 'processed_size': 0, 'processed_dirs': 0}

    signal.signal(signal.SIGINT, signal_handler)

    for vol_info in target_volumes:
        print(f"\nScanning volume: {vol_info.drive_letter} ({vol_info.label})")
        scan_single_volume(db_conn, vol_info, progress_counter)
        if should_stop():
            break
        print_progress(progress_counter['processed_files'], progress_counter['processed_size'], progress_counter['processed_dirs'])

    db_conn.commit()
    db_conn.close()
    print("\nIndexing completed or stopped.")


if __name__ == "__main__":
    DB_PATH = "disk_index.db"
    print("Available volumes:")
    available_vols = get_volumes()
    for v in available_vols:
        print(f"  {v.drive_letter} - {v.label} ({v.fs})")

    input_letters_str = input("Enter drive letters to index (e.g., E F G): ")
    target_drives = [f"{letter.strip()}:" for letter in input_letters_str.split() if letter.strip()]

    scan_and_index_volumes(DB_PATH, target_drives)