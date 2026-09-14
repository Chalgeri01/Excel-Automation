# Version : 1.0.0
# Date : 9th Feb 2026

import os
from ftplib import FTP
from datetime import datetime

# CONFIG

CONFIG = {
"LOG_DIR": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo",
"LOCAL_ROOT" : r"\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\KOTHARI ONE\KothariOne",
"FTP_HOST" : "sma.kotharione.in",
"FTP_USER" : "kothari",
"FTP_PASS" : "Kothari@10jtzts",
"FTP_ROOT" : "/docs"
}

#
LOG_FILE_NAME = "ftp-runner.log"
LOG_FILE_PATH = os.path.join(CONFIG["LOG_DIR"], LOG_FILE_NAME)
def write_log(message: str):
    """
    Writes a timestamped message to both the console and a log file, 
    using the configured LOG_DIR.

    Args:
        message (str): The message content to be logged.
    """
    try:
        # 1. Ensure the log directory exists
        # 'exist_ok=True' prevents an error if the directory is already there.
        os.makedirs(CONFIG["LOG_DIR"], exist_ok=True)
        
        # 2. Get the current timestamp
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 3. Construct the full log line
        # We also include the configuration's BATCH value if available (assuming a default of "ScriptRun")
        batch_tag = CONFIG.get("BATCH", "ScriptRun")
        log_line = f"[{ts}][{batch_tag}] {message}"

        # 4. Write the line to the console (Write-Host equivalent)
        print(log_line)

        # 5. Append the line to the log file (Out-File -Append equivalent)
        # Using 'a' for append mode.
        with open(LOG_FILE_PATH, 'a', encoding='utf-8') as f:
            f.write(log_line + '\n')

    except IOError as e:
        # Handle cases where the file cannot be written (e.g., permissions issue)
        print(f"FATAL ERROR: Could not write to log file {LOG_FILE_PATH}. Check directory permissions. Details: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"FATAL ERROR: An unexpected error occurred during logging: {e}")

# Funcation block
def ftp_mkdirs(path,ftp):
    parts = path.strip("/").split("/")
    cur = ""
    for p in parts:
        cur += "/" + p
        try:
            ftp.mkd(cur)
        except:
            pass

def ftp_mtime(path,ftp):
    try:
        r = ftp.sendcmd(f"MDTM {path}")
        return datetime.strptime(r[4:], "%Y%m%d%H%M%S")
    except:
        return None

def main():
    try:
        write_log("Connecting to FTP...")
        ftp = FTP()
        ftp.connect(CONFIG["FTP_HOST"], 21, timeout=180)
        ftp.login(CONFIG["FTP_USER"], CONFIG["FTP_PASS"])

        # FORCE ACTIVE MODE — NO PASV WILL EVER BE USED
        ftp.set_pasv(False)
        write_log("Connected (ACTIVE MODE)")

        write_log("Starting sync...")

        
        for root, dirs, files in os.walk(CONFIG["LOCAL_ROOT"]):
            rel = os.path.relpath(root, CONFIG["LOCAL_ROOT"])
            if rel == ".":
                rel = ""

            ftp_dir = f"{CONFIG["FTP_ROOT"]}/{rel}".replace("\\", "/")
            ftp_mkdirs(ftp_dir,ftp)

            for name in files:
                local_file = os.path.join(root, name)
                ftp_file = f"{ftp_dir}/{name}"

                lm = datetime.fromtimestamp(os.path.getmtime(local_file))
                rm = ftp_mtime(ftp_file,ftp)

                if rm is None or lm > rm:
                    write_log(f"Uploading: {ftp_file}")
                    with open(local_file, "rb") as f:
                        ftp.storbinary(f"STOR {ftp_file}", f)
        ftp.quit()
        write_log("DONE")
    except Exception as e:
        write_log(f"Error while task running FTP : {e}")


if __name__ == "__main__":
    main()












