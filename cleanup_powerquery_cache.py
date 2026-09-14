from pathlib import Path
from datetime import datetime, timedelta
import logging

# ============================================================
# Configuration
# ============================================================

CACHE_FOLDER = Path(
    r"C:\Users\kapl\AppData\Local\Microsoft\Office\16.0\PowerQuery\Cache\Caches"
)

# Log folder
LOG_FOLDER = Path(
    r"C:\Users\kapl\Desktop\Project-Reporting-Automation\logs"
)

# Delete files older than 2 days
DAYS_TO_KEEP = 2


# ============================================================
# Logging setup
# ============================================================

LOG_FOLDER.mkdir(parents=True, exist_ok=True)

log_file = LOG_FOLDER / f"cleanup_{datetime.now():%Y-%m-%d}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logging.info("=" * 70)
logging.info("PowerQuery Cache Cleanup - Started")
logging.info(f"Cache folder: {CACHE_FOLDER}")
logging.info(f"Delete files older than: {DAYS_TO_KEEP} days")


# ============================================================
# Check folder
# ============================================================

if not CACHE_FOLDER.exists():
    logging.error(f"Cache folder does not exist: {CACHE_FOLDER}")
    logging.info("Cleanup failed.")
    logging.info("=" * 70)
    exit(1)


# ============================================================
# Delete old files
# ============================================================

cutoff_time = datetime.now() - timedelta(days=DAYS_TO_KEEP)

deleted_count = 0
skipped_count = 0
error_count = 0

for file in CACHE_FOLDER.iterdir():

    if not file.is_file():
        continue

    try:
        modified_time = datetime.fromtimestamp(file.stat().st_mtime)

        if modified_time < cutoff_time:

            file_size_mb = file.stat().st_size / (1024 * 1024)

            file.unlink()

            deleted_count += 1

            logging.info(
                f"DELETED | File: {file.name} | "
                f"Modified: {modified_time:%Y-%m-%d %H:%M:%S} | "
                f"Size: {file_size_mb:.2f} MB"
            )

        else:
            skipped_count += 1

    except PermissionError:
        error_count += 1
        logging.error(f"PERMISSION ERROR | Could not delete: {file}")

    except Exception as e:
        error_count += 1
        logging.error(f"ERROR | Could not delete: {file} | {e}")


# ============================================================
# Summary
# ============================================================

logging.info("-" * 70)
logging.info(f"Cleanup completed")
logging.info(f"Files deleted : {deleted_count}")
logging.info(f"Files skipped : {skipped_count}")
logging.info(f"Errors        : {error_count}")
logging.info("=" * 70)
