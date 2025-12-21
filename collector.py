"""
Индексирует файлы на разделах жесткого диска
и собирает БД index.db для дальнейшего анализа.
"""

import os
import sqlite3
import time
import signal
from typing import List
from get_volumes import get_volumes, VolumeInfo
from utils import format_bytes
from ignored import ignored_files, ignored_folders
from console import clear, write, write_line
from database import (ensure_directory_exists, 
    ensure_volume_exists, get_volume_drive_name, 
    init_db_schema, insert_file_record, is_directory_fully_indexed, 
    mark_directory_as_indexed)


stop_event: bool
is_empty_line: bool

def signal_handler(signum, frame):
    global stop_event
    write_line("Stop signal received. Stopping gracefully...")
    stop_event = True

def should_stop():
    global stop_event
    return stop_event

def stop_requested():
    global stop_event
    stop_event = True

def scan_single_volume_recursive(db_conn: sqlite3.Connection, volume_id: int, current_path: str, root_drive_path: str, progress_counter, current_volume_letter):
    global is_empty_line
    path = os.path.relpath(current_path, root_drive_path).replace("/", "\\")
    if path == ".":
        path = ""

    if path.upper() in [folder.upper() for folder in ignored_folders]:
        write_line(f"Found ignored folder: {path}")
        return

    dir_already_indexed = is_directory_fully_indexed(db_conn, volume_id, path)
    if not dir_already_indexed:
        try:
            items = os.listdir(current_path)
        except PermissionError:
            write_line(f"Permission denied: {current_path}")
            return

        files_in_dir = [item for item in items if os.path.isfile(os.path.join(current_path, item))]
        subdirs_in_dir = [item for item in items if os.path.isdir(os.path.join(current_path, item))]

        dir_stat = os.stat(current_path)
        dir_id = ensure_directory_exists(db_conn, volume_id, path, dir_stat, indexed_at=None)

        for file_name in files_in_dir:
            file_path = os.path.join(current_path, file_name)
            
            _, file_path_wo_drive = os.path.splitdrive(file_path)
            file_path_wo_drive = file_path_wo_drive.lstrip(os.sep)
            
            if file_path_wo_drive.upper() in [file.upper() for file in ignored_files]:
                write_line(f"Found ignored file: {file_path_wo_drive}")
                break

            try:
                file_stat = os.stat(file_path)
                insert_file_record(db_conn, dir_id, file_name, file_stat)
                
                progress_counter['processed_files'] += 1
                progress_counter['processed_size'] += file_stat.st_size
                size_str = format_bytes(progress_counter['processed_size'])
                write(f"\rIndexed {progress_counter['processed_files']} files, {size_str}")

                if should_stop():
                    return
            except (OSError, PermissionError) as e:
                write_line(f"Error accessing file {file_path}: {e}")

        for subdir_name in subdirs_in_dir:
            subdir_abs_path = os.path.join(current_path, subdir_name)
            scan_single_volume_recursive(db_conn, volume_id, subdir_abs_path, root_drive_path, progress_counter, current_volume_letter)
            if should_stop():
                return

        mark_directory_as_indexed(db_conn, dir_id, time.time())
    
    pass

def scan_single_volume(db_conn: sqlite3.Connection, volume_info: VolumeInfo, progress_counter, 
                       drive_name: str|None):
    volume_id = ensure_volume_exists(db_conn, volume_info, drive_name)
    root_path = volume_info.letter + ":\\"
    scan_single_volume_recursive(db_conn, volume_id, root_path, 
                                 root_drive_path=root_path, 
                                 progress_counter=progress_counter, 
                                 current_volume_letter=volume_info.letter)

def scan_and_index_volumes(db_path: str, target_letters: List[str], drive_name: str|None):
    """Main function to scan and index specified volumes."""
    global stop_event
    stop_event = False

    db_conn = sqlite3.connect(db_path)

    volumes = get_volumes()
    target_volumes = [v for v in volumes if v.letter in target_letters]

    if not target_volumes:
        write_line("No matching volumes found for the provided drive letters")
        return

    progress_counter = {'processed_files': 0, 'processed_size': 0, 'processed_dirs': 0}

    signal.signal(signal.SIGINT, signal_handler)

    for vol_info in target_volumes:
        write_line(f"Scanning volume {vol_info.letter} ({vol_info.label})")
        scan_single_volume(db_conn, vol_info, progress_counter, drive_name)
        if should_stop():
            break

    db_conn.commit()
    db_conn.close()
    
    if stop_event:
        write_line("Scanning stopped by user request")
    else:
        write_line("Scanning completed successfully")


if __name__ == "__main__":
    DB_PATH = "index.db"
    stop_event = False
    is_empty_line = True
    init_db_schema(DB_PATH)
    
    clear()
    write_line("-== FILES SCANER ==-")
    write_line("Available volumes:")
    available_volumes = get_volumes()
    if not available_volumes:
        write_line("No volumes are available")
        raise KeyboardInterrupt()

    for index, volume in enumerate(available_volumes):
        write_line(f"  {volume.letter} - {volume.label} ({volume.filesystem})")

    try:
        input_letters_str = input(f"Enter volume letters to index ({", ".join(volume.letter for volume in available_volumes)}): ")
        if not input_letters_str:
            write_line("Nothing is selected")
            raise KeyboardInterrupt()

        input_letters_str = input_letters_str.upper()

        letters = []
        drive_names = []
        letters_without_drive_name = []
        for letter in input_letters_str.split():
            volumes = list(filter(lambda vol: vol.letter == letter, available_volumes))
            if len(volumes) != 1:
                write_line(f"Letter `{letter}` is unknown")
                raise KeyboardInterrupt()
            volume = volumes[0]

            drive_name = get_volume_drive_name(DB_PATH, volume.volume_guid)
            if drive_name:
                drive_names.append(drive_name)
            else:
                letters_without_drive_name.append(volume.letter)
            
            letters.append(volume.letter)

            pass

        input_drive_name = None
        if len(drive_names) != 1:
            input_drive_name = input(f"Enter a drive name to identify volumes ({", ".join(letters_without_drive_name)}): ")
        else:
            input_drive_name = drive_names[0]

        scan_and_index_volumes(DB_PATH, letters, input_drive_name)
    except KeyboardInterrupt:
        write_line("Canceled by user")