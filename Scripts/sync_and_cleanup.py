#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_and_cleanup.py
- Copy all files/subfolders from SOURCE to DEST (overwriting existing files)
- Then delete files in SOURCE older than RETENTION_DAYS
- Designed to be called by Windows Task Scheduler multiple times per day

USAGE (examples):
  python sync_and_cleanup.py
  python sync_and_cleanup.py --source "C:\\Local\\Folder" --dest "\\\\server\\share\\Target" --days 10
  python sync_and_cleanup.py --log "C:\\Logs\\sync.log" --dry-run

NOTE:
- For UNC shares that require credentials, store them once:
    cmdkey /add:server-or-ip /user:DOMAIN\\username /pass:********
- Make sure the Task Scheduler user has read/write on SOURCE and DEST.

"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# =========================
# DEFAULT CONFIG (override via CLI args)
# =========================
DEFAULT_SOURCE = r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo"                     # Local folder to copy FROM
DEFAULT_DEST   = r"\\192.168.1.237\Accounts\Automation_Reports"      # Network folder to copy TO
DEFAULT_DAYS   = 10                                             # Delete source files older than this (days)
DEFAULT_LOG    = r"C:\ProgramData\ReportRunner\logs\sync.log"   # Log file
OVERWRITE_DEST = True                                           # Always overwrite at destination
RETRIES        = 3                                              # Copy retries per file
RETRY_SLEEP_S  = 1.0                                            # Seconds to wait between retries
SKIP_IF_SAME   = True                                           # Optimization: skip copy if same size+mtime
DRY_RUN        = False                                          # If True, only log actions (no copy/delete)

# =========================
# Logging setup
# =========================
def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

# =========================
# Helpers
# =========================
def is_same_file(src: Path, dst: Path) -> bool:
    """Return True if size and modified time are effectively the same."""
    try:
        if not dst.exists():
            return False
        s_stat = src.stat()
        d_stat = dst.stat()
        # Match size and mtime within 2 seconds tolerance (Windows can round mtimes)
        if s_stat.st_size != d_stat.st_size:
            return False
        return abs(s_stat.st_mtime - d_stat.st_mtime) < 2.0
    except OSError:
        return False

def copy_file_with_retries(src: Path, dst: Path, retries: int = 3, sleep_s: float = 1.0, overwrite: bool = True, skip_if_same: bool = True, dry_run: bool = False) -> bool:
    """
    Copy one file from src to dst. Returns True if success/skip, False if failed.
    - Overwrites if overwrite=True
    - Skips if file is identical and skip_if_same=True
    """
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to create destination folder {dst.parent}: {e}")
        return False

    if skip_if_same and dst.exists() and is_same_file(src, dst):
        logging.info(f"SKIP (same): {src} -> {dst}")
        return True

    if dry_run:
        logging.info(f"DRYRUN COPY: {src} -> {dst}")
        return True

    for attempt in range(1, retries + 1):
        try:
            if overwrite and dst.exists():
                # Remove first to avoid permission quirks on some shares
                try:
                    os.remove(dst)
                except Exception:
                    pass
            shutil.copy2(src, dst)  # preserves timestamps
            logging.info(f"COPIED: {src} -> {dst}")
            return True
        except Exception as e:
            logging.warning(f"Copy failed (attempt {attempt}/{retries}): {src} -> {dst} | {e}")
            time.sleep(sleep_s)
    logging.error(f"FAILED COPY: {src} -> {dst}")
    return False

def copy_tree_overwrite(src_root: Path, dst_root: Path, overwrite: bool, skip_if_same: bool, dry_run: bool) -> tuple[int, int]:
    """
    Walk SRC and copy files to DEST, preserving tree. Overwrites existing files.
    Returns (copied_or_skipped_count, failed_count).
    """
    total_ok = 0
    total_fail = 0

    for dirpath, dirnames, filenames in os.walk(src_root):
        rel = os.path.relpath(dirpath, src_root)
        rel = "" if rel == "." else rel
        dst_dir = dst_root / rel

        # Ensure directory exists at destination
        try:
            if not dry_run:
                dst_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to ensure dest dir {dst_dir}: {e}")
            # continue anyway; file copy will try to mkdir again

        # Copy files in this directory
        for name in filenames:
            s = Path(dirpath) / name
            d = dst_dir / name
            ok = copy_file_with_retries(
                s, d,
                retries=RETRIES,
                sleep_s=RETRY_SLEEP_S,
                overwrite=overwrite,
                skip_if_same=skip_if_same,
                dry_run=dry_run
            )
            if ok:
                total_ok += 1
            else:
                total_fail += 1

    return total_ok, total_fail

def delete_old_files(root: Path, retention_days: int, dry_run: bool) -> tuple[int, int]:
    """
    Delete files in ROOT older than retention_days (based on last modified).
    Returns (deleted_count, failed_count). Leaves folders in place.
    """
    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    failed = 0

    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            p = Path(dirpath) / name
            try:
                mtime = datetime.fromtimestamp(p.stat().st_mtime)
            except OSError:
                # If we can't stat, skip delete; log and continue
                logging.warning(f"Could not stat file for age check: {p}")
                continue

            if mtime < cutoff:
                if dry_run:
                    logging.info(f"DRYRUN DELETE (>{retention_days}d): {p}")
                    continue
                try:
                    p.unlink()
                    logging.info(f"DELETED (>{retention_days}d): {p}")
                    deleted += 1
                except Exception as e:
                    logging.error(f"Failed to delete {p}: {e}")
                    failed += 1

    return deleted, failed

# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser(description="Copy a local folder to a network folder (overwriting), then delete old files from source.")
    parser.add_argument("--source", "-s", default=DEFAULT_SOURCE, help="Source folder (local)")
    parser.add_argument("--dest", "-d", default=DEFAULT_DEST, help="Destination folder (network)")
    parser.add_argument("--days", "-n", type=int, default=DEFAULT_DAYS, help="Delete source files older than N days (default: 10)")
    parser.add_argument("--log", "-l", default=DEFAULT_LOG, help="Log file path")
    parser.add_argument("--no-skip-if-same", action="store_true", help="Do not skip copying files that look identical (size/mtime)")
    parser.add_argument("--dry-run", action="store_true", help="Log actions only; do not copy or delete")
    args = parser.parse_args()

    source = Path(args.source)
    dest   = Path(args.dest)
    days   = int(args.days)
    log_p  = Path(args.log)
    dry    = bool(args.dry_run)
    skip_same = not args.no_skip_if_same

    setup_logging(log_p)

    logging.info("=== SYNC & CLEANUP START ===")
    logging.info(f"Source: {source}")
    logging.info(f"Dest:   {dest}")
    logging.info(f"Overwrite: {OVERWRITE_DEST} | SkipIfSame: {skip_same} | RetentionDays: {days} | DryRun: {dry}")

    if not source.exists():
        logging.error(f"Source does not exist: {source}")
        return 2

    # 1) COPY SOURCE → DEST (overwrite)
    try:
        copied, failed = copy_tree_overwrite(source, dest, OVERWRITE_DEST, skip_same, dry)
        logging.info(f"COPY SUMMARY: ok_or_skipped={copied}, failed={failed}")
        if failed > 0:
            logging.warning("Some files failed to copy.")
    except Exception as e:
        logging.exception(f"Fatal during copy: {e}")
        return 3

    # 2) DELETE OLD FILES IN SOURCE
    try:
        deleted, del_failed = delete_old_files(source, days, dry)
        logging.info(f"DELETE SUMMARY (>={days}d old): deleted={deleted}, failed={del_failed}")
    except Exception as e:
        logging.exception(f"Fatal during delete: {e}")
        return 4

    logging.info("=== SYNC & CLEANUP DONE ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
