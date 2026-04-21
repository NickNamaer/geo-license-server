import os
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

DB_PATH = os.environ.get("GEOTIVITY_DB_PATH", "/var/data/licenses.db")
BACKUP_DIR = os.environ.get("GEOTIVITY_BACKUP_DIR", "/var/data/backups")
KEEP_LATEST = int(os.environ.get("GEOTIVITY_BACKUP_KEEP", "14"))


def ensure_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def backup_sqlite(db_path: str, backup_dir: str) -> str:
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(f"DB not found: {db_file}")

    backup_root = ensure_dir(backup_dir)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_root / f"licenses_{timestamp}.db"

    src = sqlite3.connect(str(db_file))
    dst = sqlite3.connect(str(backup_file))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    return str(backup_file)


def prune_old_backups(backup_dir: str, keep_latest: int) -> None:
    backup_root = Path(backup_dir)
    files = sorted(
        backup_root.glob("licenses_*.db"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    for old_file in files[keep_latest:]:
        old_file.unlink(missing_ok=True)


def copy_latest_alias(latest_backup_path: str, backup_dir: str) -> str:
    latest_alias = Path(backup_dir) / "licenses_latest.db"
    shutil.copy2(latest_backup_path, latest_alias)
    return str(latest_alias)


def main() -> None:
    backup_path = backup_sqlite(DB_PATH, BACKUP_DIR)
    latest_alias = copy_latest_alias(backup_path, BACKUP_DIR)
    prune_old_backups(BACKUP_DIR, KEEP_LATEST)

    print(f"[backup] ok")
    print(f"[backup] db={DB_PATH}")
    print(f"[backup] saved={backup_path}")
    print(f"[backup] latest={latest_alias}")
    print(f"[backup] keep={KEEP_LATEST}")


if __name__ == "__main__":
    main()