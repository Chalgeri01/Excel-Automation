#!/usr/bin/env python3
"""
send_reports_configured.py

Updated to handle new column structure in email master file.
Update on :- 20/09/2025
Version :- 1.1
"""

# ============================== CONFIG ===============================

CONFIG = {
    # --- Paths (now relative or with placeholders) ---
    "LOG_DIR": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo",
    # Email list path will be passed as parameter
    # --- Logging metadata ---
    "BATCH": "EmailRun",  # This will be overridden by parameter
    "MASTER_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Master-sheet\03.00 PM Udyam Stock Report.xlsb",
    # --- Gmail sender credentials (use an App Password) ---
    "FROM_USER": "report@kotharigroupindia.com",
    "APP_PASSWORD": "ijzg vrgz qswn asjk",  # Example: abcd efgh ijkl mnop (no spaces)
    # --- Behavior switches ---
    "REQUIRE_METHOD_EMAIL": False,  # set True if you want to require Method="Email" on todays refresh rows
    "DRY_RUN": False,  # set True to test without sending (logs Email/SKIP with Error=DRYRUN)
    # --- SMTP transport (Gmail defaults) ---
    "USE_SSL": True,  # True -> SSL/465 ; False -> STARTTLS/587
    "SMTP_SERVER": "smtp.gmail.com",
    "SMTP_PORT_SSL": 465,
    "SMTP_PORT_STARTTLS": 587,
}

# ============================ END CONFIG =============================

import csv
from datetime import datetime, date, timedelta
from email.message import EmailMessage
import mimetypes
import smtplib
import ssl
from pathlib import Path
import argparse  # NEW: For command line arguments
import sys

import pandas as pd

# Default email body used (no longer reads from "Body" sheet)
DEFAULT_BODY = """{GREETING},

Please find attached today's report.

This is an automated email. Please do not reply to this message.
 
With regards,
Report Automation Team
Kothari Agritech Pvt. Ltd.

-------------------------------------------------------------
Confidentiality Notice: The information contained in this message is confidential.
If you are not the intended recipient, please notify us immediately and delete this message.

🌱 Please consider the environment before printing this email.
"""



# --------------------------- Helper funcs ---------------------------


def today_str_local() -> str:
    return date.today().strftime("%Y-%m-%d")

def load_run_log(log_path: Path) -> pd.DataFrame:
    if not log_path.exists():
        # Create empty DataFrame with required columns
        cols = [
            "Timestamp","RunDate","Batch","Stage","Master","FilePath","Method",
            "Status","Error","DurationS","RecipientsTo","Subject"
        ]
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(log_path, dtype=str, keep_default_na=False, na_values=[])

    # Ensure expected columns exist
    for col in [
        "Timestamp","RunDate","Batch","Stage","Master","FilePath","Method",
        "Status","Error","DurationS","RecipientsTo","Subject"
    ]:
        if col not in df.columns:
            df[col] = ""

    return df

def normalize_addr_list(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    value = value.replace(";", ",")
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return ", ".join(parts)

def split_attachments(value: str) -> list[Path]:
    raw = (value or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(";")]
    return [Path(p) for p in parts if p]

def load_email_list(xlsx_path: Path) -> pd.DataFrame:
    """Load only the List sheet - no longer reads Body sheet"""
    xl = pd.ExcelFile(xlsx_path)

    # "List" sheet only
    df_list = pd.read_excel(xl, "List")
    df_list.columns = [str(c).strip() for c in df_list.columns]

    return df_list

def most_recent_refresh_ok_today(runlog: pd.DataFrame, filepath: Path, email_run_date: date) -> bool:
    """Check if file was refreshed on email_run_date OR the previous day"""
    target = str(filepath)
    
    # Check for email_run_date (e.g., 2025-09-20)
    mask1 = (
        (runlog["Stage"].str.lower() == "refresh")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"] == email_run_date.strftime("%Y-%m-%d"))
        & (runlog["FilePath"].str.casefold() == target.casefold())
    )
    
    # Also check for previous day (e.g., 2025-09-19)
    prev_day = email_run_date - timedelta(days=1)
    mask2 = (
        (runlog["Stage"].str.lower() == "refresh")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"] == prev_day.strftime("%Y-%m-%d"))
        & (runlog["FilePath"].str.casefold() == target.casefold())
    )
    
    return mask1.any() or mask2.any()

def refresh_method_is_email_today(runlog: pd.DataFrame, filepath: Path, email_run_date: date) -> bool:
    """Check if file was refreshed with Email method on email_run_date OR previous day"""
    target = str(filepath)
    
    # Check both today and yesterday
    dates_to_check = [
        email_run_date.strftime("%Y-%m-%d"),
        (email_run_date - timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    
    mask = (
        (runlog["Stage"].str.lower() == "refresh")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"].isin(dates_to_check))
        & (runlog["FilePath"].str.casefold() == target.casefold())
    )
    
    day_rows = runlog.loc[mask]
    if day_rows.empty:
        return False
    return any(
        (m or "").strip().lower() == "email" for m in day_rows["Method"].tolist()
    )

def already_emailed_today(runlog: pd.DataFrame, to_addrs: str, subject: str, email_run_date: date) -> bool:
    """Check if email was already sent on email_run_date OR previous day"""
    to_norm = normalize_addr_list(to_addrs)
    subj = (subject or "").strip()
    
    dates_to_check = [
        email_run_date.strftime("%Y-%m-%d"),
        (email_run_date - timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    
    mask = (
        (runlog["Stage"].str.lower() == "email")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"].isin(dates_to_check))
        & (
            runlog["RecipientsTo"].fillna("").apply(normalize_addr_list).str.casefold()
            == to_norm.casefold()
        )
        & (runlog["Subject"].fillna("").str.casefold() == subj.casefold())
    )
    return mask.any()

def infer_mime(path: Path) -> tuple[str, str]:
    typ, enc = mimetypes.guess_type(str(path))
    if typ is None:
        return ("application", "octet-stream")
    major, minor = typ.split("/", 1)
    return (major, minor)

def send_via_gmail(
    user: str,
    app_password: str,
    msg: EmailMessage,
    use_ssl: bool,
    server: str,
    port_ssl: int,
    port_starttls: int,
):
    """Send EmailMessage via Gmail SMTP using App Password."""
    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(server, port_ssl, context=context) as s:
            s.login(user, app_password)
            s.send_message(msg)
    else:
        with smtplib.SMTP(server, port_starttls) as s:
            s.starttls()
            s.login(user, app_password)
            s.send_message(msg)

def append_log_row(log_path: Path, row: dict):
    file_exists = log_path.exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Timestamp",
                "RunDate",
                "Batch",
                "Stage",
                "Master",
                "FilePath",
                "Method",
                "Status",
                "Error",
                "DurationS",
                "RecipientsTo",
                "Subject",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def cell_str(row, col_name: str) -> str:
    """Return clean string from a dataframe cell; empty if missing/NaN."""
    if col_name not in row or pd.isna(row[col_name]):
        return ""
    return str(row[col_name]).strip()

def get_greeting(to_addrs: str) -> str:
    """Determine greeting based on number of recipients in To field"""
    recipients = [addr.strip() for addr in to_addrs.split(",") if addr.strip()]
    if len(recipients) == 1:
        return "Dear Sir"
    else:
        return "Dear Team"

# NEW: Function to find the correct log file for a batch and date
def find_batch_log_file(log_dir: Path, batch_number: int, email_run_date: date) -> Path:
    """
    Find the log file for a specific batch on the target date.
    Email runs on day N, but looks for logs with date N (since batch ran on night of N-1)
    """
    expected_filename = f"run-log_{email_run_date.strftime('%Y-%m-%d')}_Batch-{batch_number}.csv"
    log_path = log_dir / expected_filename
    
    if log_path.exists():
        return log_path
    
    # If not found, try previous day (fallback)
    prev_day = email_run_date - timedelta(days=1)
    fallback_filename = f"run-log_{prev_day.strftime('%Y-%m-%d')}_Batch-{batch_number}.csv"
    fallback_path = log_dir / fallback_filename
    
    if fallback_path.exists():
        return fallback_path
    
    # If still not found, return the expected path (will create empty log)
    return log_path

# NEW: Parse command line arguments with optional date
def parse_arguments():
    parser = argparse.ArgumentParser(description='Send email reports for specific batch')
    parser.add_argument('--batch', type=int, required=True, 
                       help='Batch number (1, 2, etc.)')
    parser.add_argument('--email-list', type=str, required=True,
                       help='Path to Email_List.xlsx file for this batch')
    parser.add_argument('--email-date', type=str, 
                       default=None,
                       help='Optional: Date for email run (YYYY-MM-DD). If not provided, uses today')
    return parser.parse_args()


# ----------------------------- Main flow ----------------------------


def main():
    # Parse command line arguments
    args = parse_arguments()
    
    BATCH_NUMBER = args.batch
    EMAIL_LIST_PATH = Path(args.email_list)
    
    # Handle date - use provided date or today
    if args.email_date:
        try:
            EMAIL_RUN_DATE = date.fromisoformat(args.email_date)
        except ValueError:
            print(f"ERROR: Invalid date format '{args.email_date}'. Use YYYY-MM-DD.")
            return 2
    else:
        EMAIL_RUN_DATE = date.today()
    
    print(f"Processing Batch {BATCH_NUMBER} for date {EMAIL_RUN_DATE}")
    print(f"Using email list: {EMAIL_LIST_PATH}")
    
    # Update config with batch-specific values
    LOG_DIR = Path(CONFIG["LOG_DIR"])
    BATCH = f"EmailBatch{BATCH_NUMBER}"  # Dynamic batch name
    MASTER_PATH = CONFIG["MASTER_PATH"]
    FROM_USER = CONFIG["FROM_USER"]
    APP_PASSWORD = CONFIG["APP_PASSWORD"]
    REQUIRE_METHOD_EMAIL = bool(CONFIG["REQUIRE_METHOD_EMAIL"])
    DRY_RUN = bool(CONFIG["DRY_RUN"])
    USE_SSL = bool(CONFIG["USE_SSL"])
    SMTP_SERVER = CONFIG["SMTP_SERVER"]
    SMTP_PORT_SSL = int(CONFIG["SMTP_PORT_SSL"])
    SMTP_PORT_STARTTLS = int(CONFIG["SMTP_PORT_STARTTLS"])

    # Find the correct log file for this batch
    LOG_PATH = find_batch_log_file(LOG_DIR, BATCH_NUMBER, EMAIL_RUN_DATE)
    today_str = EMAIL_RUN_DATE.strftime("%Y-%m-%d")

    print(f"Looking for log file: {LOG_PATH}")
    
    if not LOG_PATH.exists():
        print(f"WARNING: Log file not found: {LOG_PATH}")
        print("Will create empty log for email tracking")

    if not FROM_USER or not APP_PASSWORD:
        print(
            "ERROR: Please set FROM_USER and APP_PASSWORD in CONFIG at the top of this file."
        )
        return 2

    # Load data
    runlog = load_run_log(LOG_PATH)
    df_list = load_email_list(EMAIL_LIST_PATH)  # No longer returns body_template

    # Validate essential columns (updated for new structure)
    required_columns = ["Receiver", "CC", "BCC", "Subject", "Attachement Path"]
    for col in required_columns:
        if col not in df_list.columns:
            print(f"ERROR: Column '{col}' missing in List sheet.", flush=True)
            return 2

    # Iterate ALL rows (no longer checking for "Send" column)
    for _, row in df_list.iterrows():
        to_addrs = normalize_addr_list(cell_str(row, "Receiver"))
        cc_addrs = normalize_addr_list(cell_str(row, "CC"))
        bcc_addrs = normalize_addr_list(cell_str(row, "BCC"))
        subject = cell_str(row, "Subject")
        atts = split_attachments(cell_str(row, "Attachement Path"))
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Determine greeting based on number of recipients
        greeting = get_greeting(to_addrs)
        body_text = DEFAULT_BODY.replace("{GREETING}", greeting)

        # Validate basic fields
        if not to_addrs:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": "",
                    "Method": "Email",
                    "Status": "FAIL",
                    "Error": "Missing Receiver",
                    "DurationS": "",
                    "RecipientsTo": "",
                    "Subject": subject,
                },
            )
            continue

        if not subject:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": "",
                    "Method": "Email",
                    "Status": "FAIL",
                    "Error": "Missing Subject",
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": "",
                },
            )
            continue

        # Idempotence: To + Subject already emailed OK today?
        if already_emailed_today(runlog, to_addrs, subject, EMAIL_RUN_DATE):
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": "",
                    "Method": "Email",
                    "Status": "SKIP",
                    "Error": "Already emailed today (To+Subject)",
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": subject,
                },
            )
            continue

        # Freshness checks for attachments
        problems = []
        for p in atts:
            if not p.exists():
                problems.append(f"Missing file: {p}")
                continue
            if not most_recent_refresh_ok_today(runlog, p, EMAIL_RUN_DATE):
                problems.append(f"No successful refresh today: {p}")
                continue
            if REQUIRE_METHOD_EMAIL and not refresh_method_is_email_today(runlog, p, EMAIL_RUN_DATE):
                problems.append(f"Method not 'Email' today: {p}")

        if problems:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": ";".join(str(p) for p in atts),
                    "Method": "Email",
                    "Status": "FAIL",
                    "Error": "; ".join(problems),
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": subject,
                },
            )
            continue

        # Build message
        msg = EmailMessage()
        msg["From"] = FROM_USER
        msg["To"] = to_addrs
        if cc_addrs:
            msg["Cc"] = cc_addrs
        # Do not set "Bcc" header; we add Bcc to the transport recipient list instead.
        msg["Subject"] = subject
        # Text fallback
        msg.set_content(body_text)
        # HTML part (simple)
        msg.add_alternative(
            f"<pre style='font-family: inherit; white-space: pre-wrap'>{body_text}</pre>",
            subtype="html",
        )

        # Attach files
        for p in atts:
            maintype, subtype = infer_mime(p)
            with open(p, "rb") as fh:
                data = fh.read()
            msg.add_attachment(
                data, maintype=maintype, subtype=subtype, filename=p.name
            )

        # Final recipients for transport (To + Cc + Bcc)
        all_recipients = []
        for hdr in ["To", "Cc"]:
            val = msg.get(hdr)
            if val:
                all_recipients += [a.strip() for a in val.split(",") if a.strip()]
        if bcc_addrs:
            all_recipients += [a.strip() for a in bcc_addrs.split(",") if a.strip()]

        # Dry run?
        if DRY_RUN:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": ";".join(str(p) for p in atts),
                    "Method": "Email",
                    "Status": "SKIP",
                    "Error": "DRYRUN",
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": subject,
                },
            )
            continue

        # Send
        try:
            if USE_SSL:
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(
                    SMTP_SERVER, int(CONFIG["SMTP_PORT_SSL"]), context=context
                ) as s:
                    s.login(FROM_USER, APP_PASSWORD)
                    s.send_message(
                        msg, from_addr=FROM_USER, to_addrs=all_recipients or [FROM_USER]
                    )
            else:
                with smtplib.SMTP(SMTP_SERVER, int(CONFIG["SMTP_PORT_STARTTLS"])) as s:
                    s.starttls()
                    s.login(FROM_USER, APP_PASSWORD)
                    s.send_message(
                        msg, from_addr=FROM_USER, to_addrs=all_recipients or [FROM_USER]
                    )

            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": ";".join(str(p) for p in atts),
                    "Method": "Email",
                    "Status": "OK",
                    "Error": "",
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": subject,
                },
            )

        except Exception as e:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "RunDate": today_str,
                    "Batch": BATCH,
                    "Stage": "Email",
                    "Master": MASTER_PATH,
                    "FilePath": ";".join(str(p) for p in atts),
                    "Method": "Email",
                    "Status": "FAIL",
                    "Error": str(e),
                    "DurationS": "",
                    "RecipientsTo": to_addrs,
                    "Subject": subject,
                },
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())