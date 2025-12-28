# combinator.py

import sqlite3
import logging
from pathlib import PurePosixPath

DB_PATH = "index.db"
LOG_PATH = "combinator.log"
TOP_N = 100


def normalize_path(path: str) -> str:
    """Converts Windows-style path to universal POSIX-style."""
    return path.replace("\\", "/").rstrip("/")


def get_file_extension(name: str) -> str:
    """Returns file extension including the dot, or empty string if none."""
    base = PurePosixPath(name)
    return base.suffix if base.suffix else ""


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row

    # 0. Очистка output_files
    conn.execute("DELETE FROM output_files")

    # Подготовка SQL-запроса для группировки и сортировки
    query = """
    WITH grouped AS (
        SELECT
            f.id AS file_id,
            d.path AS dir_path,
            f.name AS file_name,
            f.size,
            f.created_at,
            f.modified_at,
            LOWER(d.path) AS norm_dir,
            LOWER(f.name) AS norm_name
        FROM files f
        JOIN directories d ON f.directory_id = d.id
    ),
    ranked AS (
        SELECT
            file_id,
            dir_path,
            file_name,
            size,
            norm_dir,
            norm_name,
            ROW_NUMBER() OVER (
                PARTITION BY norm_dir, norm_name
                ORDER BY
                    CASE WHEN modified_at IS NULL THEN 1 ELSE 0 END,
                    modified_at DESC,
                    CASE WHEN created_at IS NULL THEN 1 ELSE 0 END,
                    created_at DESC,
                    CASE WHEN size IS NULL THEN 1 ELSE 0 END,
                    size DESC
            ) AS rn,
            COUNT(*) OVER (PARTITION BY norm_dir, norm_name) AS group_size
        FROM grouped
    )
    SELECT
        file_id,
        dir_path,
        file_name,
        size,
        group_size,
        rn
    FROM ranked
    ORDER BY norm_dir, norm_name, rn;
    """

    total_size = 0
    duplicate_file_count = 0
    group_stats = {}  # (dir, name) -> count

    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # Сначала пройдёмся, чтобы собрать статистику
    groups = {}
    for row in rows:
        key = (normalize_path(row["dir_path"]), row["file_name"].lower())
        if key not in groups:
            groups[key] = []
        groups[key].append(row)

    # Собираем дубли и общий объём
    insert_batch = []
    for key, group in groups.items():
        group_size = len(group)
        if group_size >= 2:
            duplicate_file_count += group_size
            group_stats[key] = group_size

        dir_path, file_name = key
        base_path = f"{dir_path}/{file_name}".lstrip("/")
        ext = get_file_extension(file_name)

        for idx, row in enumerate(group):
            total_size += row["size"] if row["size"] else 0
            if row["rn"] == 1:
                out_path = base_path
            else:
                conflict_dir = base_path
                out_path = f"{conflict_dir}/{idx}{ext}"
            insert_batch.append((row["file_id"], out_path))

    # 1. Вставка в output_files
    conn.executemany(
        "INSERT INTO output_files (file_id, out_path) VALUES (?, ?)",
        insert_batch
    )

    # 2. Общий объём
    from utils import format_bytes
    total_human = format_bytes(total_size)

    # 3. Статистика
    sorted_groups = sorted(group_stats.items(), key=lambda x: x[1], reverse=True)
    top_groups = sorted_groups[:TOP_N]

    # Настройка логгера
    logging.basicConfig(
        filename=LOG_PATH,
        filemode='w',
        level=logging.INFO,
        format='%(message)s'
    )

    # Логируем всё
    for (dir_path, name), cnt in sorted_groups:
        logging.info(f"{dir_path}/{name} -> {cnt} copies")

    # Вывод в консоль
    print(f"Total output size: {total_human}")
    print(f"Files with duplicates: {duplicate_file_count}")
    print(f"Top {TOP_N} duplicated paths:")
    for (dir_path, name), cnt in top_groups:
        print(f"  {dir_path}/{name}: {cnt}")

    conn.commit()
    conn.close()
    print(f"\nLog written to {LOG_PATH}")


if __name__ == "__main__":
    main()