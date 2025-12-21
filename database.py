
import os
import sqlite3
from typing import Optional

from get_volumes import VolumeInfo
from utils import to_iso


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
            volume_guid TEXT UNIQUE NOT NULL,
            letter TEXT NOT NULL,
            label TEXT,
            filesystem TEXT,
            drive_name TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS directories (
            id INTEGER PRIMARY KEY,
            volume_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            modified_at TIMESTAMP NULL,
            indexed_at TIMESTAMP,
            FOREIGN KEY (volume_id) REFERENCES volumes(id),
            UNIQUE(volume_id, path)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            directory_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            modified_at TIMESTAMP NOT NULL,
            indexed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (directory_id) REFERENCES directories(id),
            UNIQUE(directory_id, name)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unique_files (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            copied_at TIMESTAMP,
            hash TEXT,
            FOREIGN KEY (file_id) REFERENCES files(id)
        );
    """)

def get_volume_id_by_guid(db_conn, volume_guid: str) -> Optional[int]:
    """Fetches the volume ID by its GUID."""
    cursor = db_conn.execute("SELECT id FROM volumes WHERE volume_guid = ?", (volume_guid,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_volume_drive_name(db_path: str, volume_guid: str) -> str|None:
    """Returns the volume drive name if assigned."""
    db_conn = sqlite3.connect(db_path)
    cursor = db_conn.execute(
        "SELECT drive_name FROM volumes WHERE volume_guid = ?",
        (volume_guid,)
    )
    row = cursor.fetchone()
    
    return row[0] if row is not None else None 

def ensure_volume_exists(db_conn, volume_info: VolumeInfo, drive_name: str|None) -> int:
    """Returns a new or existing volume record ID."""
    cursor = db_conn.cursor()
    volume_id = get_volume_id_by_guid(db_conn, volume_info.volume_guid)
    if not volume_id:
        cursor.execute(
            "INSERT INTO volumes (volume_guid, letter, label, filesystem, drive_name) VALUES (?, ?, ?, ?, ?)",
            (volume_info.volume_guid, volume_info.letter, volume_info.label, volume_info.filesystem, drive_name)
        )
        volume_id = cursor.lastrowid

    return volume_id

def is_directory_fully_indexed(db_conn, volume_id: int, path: str) -> bool:
    """Checks if a directory has been fully indexed (has an indexed_at timestamp)."""
    cursor = db_conn.execute(
        "SELECT indexed_at FROM directories WHERE volume_id = ? AND path = ?",
        (volume_id, path)
    )
    row = cursor.fetchone()
    return row is not None and row[0] is not None

def ensure_directory_exists(db_conn, volume_id: int, path: str, stat_result: os.stat_result, indexed_at: Optional[float]) -> int:
    """
    Inserts or updates a directory record. Returns the directory ID.
    """
    cursor = db_conn.cursor()
    existing_cursor = db_conn.execute("SELECT id FROM directories WHERE volume_id = ? AND path = ?", (volume_id, path))
    existing_row = existing_cursor.fetchone()
    existing_id = existing_row[0] if existing_row else None
    indexed_at_str = to_iso(indexed_at)

    if not existing_id:
        cursor.execute(
            "INSERT INTO directories (volume_id, path, created_at, modified_at, indexed_at) VALUES (?, ?, ?, ?, ?)",
            (volume_id, path, to_iso(stat_result.st_birthtime), to_iso(stat_result.st_mtime), indexed_at_str)
        )
        existing_id = cursor.lastrowid

    return existing_id

def mark_directory_as_indexed(db_conn, dir_id: int, timestamp: float):
    """Marks a directory as fully indexed by setting its indexed_at timestamp."""
    indexed_at_str = to_iso(timestamp)
    db_conn.execute(
        "UPDATE directories SET indexed_at = ? WHERE id = ?",
        (indexed_at_str, dir_id)
    )

def insert_file_record(db_conn: sqlite3.Connection, dir_id: int, name: str, stat_result: os.stat_result):
    """Inserts a file record into the database."""
    db_conn.execute(
        "INSERT OR IGNORE INTO files (directory_id, name, size, created_at, modified_at) VALUES (?, ?, ?, ?, ?)",
        (dir_id, name, stat_result.st_size, to_iso(stat_result.st_birthtime), to_iso(stat_result.st_mtime))
    )