#!/usr/bin/env python3
"""
send_reports_configured.py

Same behavior as your previous version, but:
- Validates "refreshed OK" via MySQL (events table) instead of CSV
- Logs Email OK/FAIL/SKIP into DB (events.stage='Email')
- Exports a per-run email CSV from DB at the end
- Added parallel execution to reduce processing time
- Added force resend option to bypass all validations
"""

# ============================== CONFIG ===============================
CONFIG = {
    "LOG_DIR": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo",

    # Will be overridden by --batch at runtime (only used for log rows)
    "BATCH": "EmailRun",
    "MASTER_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Master-sheet\03.00 PM Udyam Stock Report.xlsb",

    # Gmail
    "FROM_USER": "report@kotharigroupindia.com",
    "APP_PASSWORD": "ijzg vrgz qswn asjk",

    # Behavior
    "REQUIRE_METHOD_EMAIL": False,
    "DRY_RUN": False,

    # SMTP
    "USE_SSL": True,
    "SMTP_SERVER": "smtp.gmail.com",
    "SMTP_PORT_SSL": 465,
    "SMTP_PORT_STARTTLS": 587,

    # Parallel execution
    "MAX_PARALLEL": 1,  # Default max parallel processes
    
    # Fallback email timing (hours)
    "FALLBACK_HOURS": 18,  # Consider emails sent in last 18 hours as "already sent"
    
    # Force resend
    "FORCE_RESEND": False,  # Bypass all validations and resend all emails
}
# ============================ END CONFIG =============================

import os
import csv
from datetime import datetime, date, timedelta, timezone
from email.message import EmailMessage
import mimetypes
import smtplib
import ssl
from pathlib import Path
import argparse
import sys
import time

import pandas as pd
import pymysql
from concurrent.futures import ProcessPoolExecutor, as_completed

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

#
LOG_FILE_NAME = "email-runner.log"
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

# --------------------------- Helpers (unchanged where possible) -----

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
    xl = pd.ExcelFile(xlsx_path)
    df_list = pd.read_excel(xl, "List")
    df_list.columns = [str(c).strip() for c in df_list.columns]
    return df_list

def cell_str(row, col_name: str) -> str:
    if col_name not in row or pd.isna(row[col_name]):
        return ""
    return str(row[col_name]).strip()

def get_greeting(to_addrs: str) -> str:
    recipients = [addr.strip() for addr in to_addrs.split(",") if addr.strip()]
    return "Dear Sir" if len(recipients) == 1 else "Dear Team"

def infer_mime(path: Path) -> tuple[str, str]:
    typ, _ = mimetypes.guess_type(str(path))
    if typ is None:
        return ("application", "octet-stream")
    major, minor = typ.split("/", 1)
    return (major, minor)

def send_via_gmail(user, app_password, msg, use_ssl, server, port_ssl, port_starttls, all_recipients):
    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(server, port_ssl, context=context) as s:
            s.login(user, app_password)
            s.send_message(msg, from_addr=user, to_addrs=all_recipients or [user])
    else:
        with smtplib.SMTP(server, port_starttls) as s:
            s.starttls()
            s.login(user, app_password)
            s.send_message(msg, from_addr=user, to_addrs=all_recipients or [user])

# --------------------------- DB helpers -----------------------------

def parse_conn_env():
    """
    REPORTLOGS_CONN example:
    Server=127.0.0.1;Port=3306;Database=reportlogs;Uid=root;Pwd=****;AllowPublicKeyRetrieval=True;SslMode=None
    """
    raw = os.environ.get("REPORTLOGS_CONN", "")
    parts = [p for p in raw.split(";") if p.strip()]
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()
    host = kv.get("server", "127.0.0.1")
    port = int(kv.get("port", 3306) or 3306)
    db   = kv.get("database", "reportlogs")
    user = kv.get("uid") or kv.get("user") or "root"
    pwd  = kv.get("pwd") or kv.get("password") or ""
    # For PyMySQL: allow public key retrieval and SSL off if SslMode=None
    ssl_mode = (kv.get("sslmode") or "").lower()
    ssl = None
    if ssl_mode and ssl_mode not in ("none", "disabled"):
        ssl = {"ssl": {}}  # use default SSL; adjust if you actually want TLS
    return dict(host=host, port=port, user=user, password=pwd, database=db, charset="utf8mb4", autocommit=True, **({} if ssl is None else ssl))

def db_connect():
    params = parse_conn_env()
    return pymysql.connect(**params)

def db_get_latest_refresh_date(conn, file_path: str) -> date | None:
    """
    Get the most recent rundate when this file was successfully refreshed.
    Returns None if no successful refresh found.
    """
    sql = """
        SELECT rundate
        FROM events
        WHERE LOWER(file_path) COLLATE utf8mb4_unicode_ci = LOWER(%s) COLLATE utf8mb4_unicode_ci
          AND stage='Refresh' AND status='OK'
        ORDER BY rundate DESC, timestamp_utc DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_path,))
        result = cur.fetchone()
        return result[0] if result else None

def db_refresh_ok_for_date(conn, file_path: str, check_date: date) -> bool:
    """
    Check if file was successfully refreshed on a specific date.
    """
    sql = """
        SELECT 1
        FROM events
        WHERE LOWER(file_path) COLLATE utf8mb4_unicode_ci = LOWER(%s) COLLATE utf8mb4_unicode_ci
          AND stage='Refresh' AND status='OK'
          AND rundate = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_path, check_date))
        return cur.fetchone() is not None

def db_refresh_method_email_for_date(conn, file_path: str, check_date: date) -> bool:
    """
    Check if file was refreshed with method 'email' on a specific date.
    """
    sql = """
        SELECT 1
        FROM events
        WHERE LOWER(file_path) COLLATE utf8mb4_unicode_ci = LOWER(%s) COLLATE utf8mb4_unicode_ci
          AND stage='Refresh' AND status='OK'
          AND rundate = %s
          AND LOWER(COALESCE(method,'')) = 'email'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_path, check_date))
        return cur.fetchone() is not None

def db_already_emailed_ok(conn, to_norm: str, subject: str, run_date: date, batch: str, file_path: str) -> bool:
    """
    Check if the same email (To+Subject) was already sent in the last X hours (fallback period).
    This prevents sending duplicate emails within the fallback window.
    """
    #print(f'File path is :- {file_path}')
    sql = """
        SELECT 1
        FROM events
        WHERE stage='Email' AND status='OK'
          AND rundate = %s
          AND batch   = %s
          AND LOWER(recipients_to) = LOWER(%s)
          AND LOWER(subject)       = LOWER(%s)
          AND LOWER(REPLACE(file_path, '\\\\', '/')) = LOWER(REPLACE(%s, '\\\\', '/'))
        LIMIT 1
    """
    #print(f"SQL query :- {sql}")
    with conn.cursor() as cur:
        cur.execute(sql, (str(run_date), batch, to_norm, subject, file_path))
        return cur.fetchone() is not None

def db_write_email_event(conn, run_id: str, batch: str, rundate: date,
                         master_path: str, file_paths: list[Path],
                         to_norm: str, subject: str,
                         status: str, error_text: str = "", duration_s: int | None = None):
    sql = """
        INSERT INTO events
        (run_id,batch,stage,timestamp_utc,rundate,master_path,file_path,method,status,error_text,duration_s,recipients_to,subject)
        VALUES
        (%s,%s,'Email',%s,%s,%s,%s,'Email',%s,%s,%s,%s,%s)
    """
    # store UTC in DB
    ts_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    fp = ";".join(str(p) for p in file_paths) if file_paths else ""
    with conn.cursor() as cur:
        cur.execute(sql, (
            run_id, batch, ts_utc, rundate,
            master_path, fp, status, error_text or "",
            duration_s if duration_s is not None else None,
            to_norm, subject
        ))

def export_email_csv(conn, run_id: str, out_csv_path: Path):
    sql = """
        SELECT
          DATE_FORMAT(CONVERT_TZ(timestamp_utc,'+00:00','+05:30'), '%%Y-%%m-%%d %%H:%%i:%%s') AS Timestamp,
          DATE_FORMAT(rundate, '%%Y-%%m-%%d') AS RunDate,
          batch        AS Batch,
          stage        AS Stage,
          master_path  AS Master,
          file_path    AS FilePath,
          method       AS Method,
          status       AS Status,
          error_text   AS Error,
          duration_s   AS DurationS,
          recipients_to AS RecipientsTo,
          subject      AS Subject
        FROM events
        WHERE run_id = %s
        ORDER BY timestamp_utc ASC, id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Timestamp","RunDate","Batch","Stage","Master","FilePath","Method","Status","Error","DurationS","RecipientsTo","Subject"])
        for r in rows:
            # keep output column order stable
            rec = dict(zip(cols, r))
            w.writerow([rec.get(c,"") for c in ["Timestamp","RunDate","Batch","Stage","Master","FilePath","Method","Status","Error","DurationS","RecipientsTo","Subject"]])

# --------------------------- Parallel Processing Functions ----------

def process_single_email(row_data, config, email_run_id, batch, email_run_date, master_path, require_method_email, dry_run, fallback_hours, force_resend):
    """
    Process a single email row - this function runs in parallel
    """
    time.sleep(2)
    row, index = row_data
    
    # Create a new DB connection for this process
    try:
        conn = db_connect()
    except Exception as e:
        return {
            'index': index,
            'status': 'FAIL',
            'error': f"DB connection failed: {e}",
            'to_norm': "",
            'subject': "",
            'atts': []
        }
    
    try:
        to_addrs = normalize_addr_list(cell_str(row, "Receiver"))
        cc_addrs = normalize_addr_list(cell_str(row, "CC"))
        bcc_addrs = normalize_addr_list(cell_str(row, "BCC"))
        subject = cell_str(row, "Subject")
        atts = split_attachments(cell_str(row, "Attachement Path"))

        # Build body
        greeting = get_greeting(to_addrs)
        body_text = DEFAULT_BODY.replace("{GREETING}", greeting)

        # Validate fields
        if not to_addrs:
            db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, "", subject, "FAIL", "Missing Receiver")
            return {
                'index': index,
                'status': 'FAIL',
                'error': "Missing Receiver",
                'to_norm': "",
                'subject': subject,
                'atts': atts
            }

        if not subject:
            db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_addrs, "", "FAIL", "Missing Subject")
            return {
                'index': index,
                'status': 'FAIL',
                'error': "Missing Subject",
                'to_norm': to_addrs,
                'subject': "",
                'atts': atts
            }

        # Skip all validations if force resend is enabled
        if not force_resend:
            # FIRST: Check if files are refreshed (this should take priority)
            problems = []
            for p in atts:
                if not p.exists():
                    problems.append(f"Missing file: {p}")
                    continue
                
                fp_str = str(p)
                
                # Get the latest refresh date for this file from DB
                latest_refresh_date = db_get_latest_refresh_date(conn, fp_str)
                
                if not latest_refresh_date:
                    problems.append(f"No successful refresh found in database: {p}")
                    continue
                
                # Check if the latest refresh is for today's email run date
                if latest_refresh_date != email_run_date:
                    problems.append(f"File not refreshed for today ({email_run_date}). Last refresh: {latest_refresh_date}: {p}")
                    continue
                
                # If we require method email, check that too
                if require_method_email and not db_refresh_method_email_for_date(conn, fp_str, email_run_date):
                    problems.append(f"Method not 'Email' for today: {p}")

            if problems:
                db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_addrs, subject, "FAIL", "; ".join(problems))
                return {
                    'index': index,
                    'status': 'FAIL',
                    'error': "; ".join(problems),
                    'to_norm': to_addrs,
                    'subject': subject,
                    'atts': atts
                }

            # SECOND: Check if already emailed in last X hours (only if files are fresh)
            to_norm = to_addrs
            if db_already_emailed_ok(conn, to_norm, subject, email_run_date, batch, fp_str):
                db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_norm, subject, "SKIP", f"Already emailed for this run (rundate={email_run_date}, batch={batch})")
                return {
                    'index': index,
                    'status': 'SKIP',
                    'error': f"Already emailed for this run",
                    'to_norm': to_norm,
                    'subject': subject,
                    'atts': atts
                }
        else:
            # Force resend mode - skip all validations
            to_norm = to_addrs
            write_log(f"Email {index+1}: FORCE RESEND - Bypassing all validations")

        # Build message + recipients
        msg = EmailMessage()
        msg["From"] = config["FROM_USER"]
        msg["To"] = to_addrs
        if cc_addrs:
            msg["Cc"] = cc_addrs
        msg["Subject"] = subject
        msg.set_content(body_text)
        msg.add_alternative(f"<pre style='font-family: inherit; white-space: pre-wrap'>{body_text}</pre>", subtype="html")
        for p in atts:
            maintype, subtype = infer_mime(p)
            with open(p, "rb") as fh:
                data = fh.read()
            msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=p.name)
        all_recipients = []
        for hdr in ["To","Cc"]:
            val = msg.get(hdr)
            if val:
                all_recipients += [a.strip() for a in val.split(",") if a.strip()]
        if bcc_addrs:
            all_recipients += [a.strip() for a in bcc_addrs.split(",") if a.strip()]

        if dry_run:
            db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_norm, subject, "SKIP", "DRYRUN")
            return {
                'index': index,
                'status': 'SKIP',
                'error': "DRYRUN",
                'to_norm': to_norm,
                'subject': subject,
                'atts': atts
            }

        # Send email
        try:
            send_via_gmail(
                config["FROM_USER"], config["APP_PASSWORD"], msg, 
                config["USE_SSL"], config["SMTP_SERVER"], 
                config["SMTP_PORT_SSL"], config["SMTP_PORT_STARTTLS"], 
                all_recipients
            )
            status_msg = "OK (FORCED)" if force_resend else "OK"
            db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_norm, subject, "OK", status_msg)
            return {
                'index': index,
                'status': 'OK',
                'error': status_msg,
                'to_norm': to_norm,
                'subject': subject,
                'atts': atts
            }
        except Exception as e:
            db_write_email_event(conn, email_run_id, batch, email_run_date, master_path, atts, to_norm, subject, "FAIL", str(e))
            return {
                'index': index,
                'status': 'FAIL',
                'error': str(e),
                'to_norm': to_norm,
                'subject': subject,
                'atts': atts
            }
    
    finally:
        try:
            conn.close()
        except:
            pass

# --------------------------- Args -----------------------------------

def parse_arguments():
    p = argparse.ArgumentParser(description="Send email reports for a batch (DB-backed).")
    p.add_argument('--batch', type=int, required=True, help='Batch number (1..6 etc.)')
    p.add_argument('--email-list', type=str, required=True, help='Path to Email_List.xlsx')
    p.add_argument('--email-date', type=str, default=None, help='YYYY-MM-DD for email run; default=TODAY')
    p.add_argument('--max-parallel', type=int, default=None, help='Max parallel processes (default: 3)')
    p.add_argument('--fallback-hours', type=int, default=None, help='Hours to check for already sent emails (default: 18)')
    p.add_argument('--force-resend', action='store_true', help='Force resend all emails regardless of refresh status or previous sends')
    return p.parse_args()

# --------------------------- Main -----------------------------------

def main():
    args = parse_arguments()

    BATCH_NUMBER = args.batch
    EMAIL_LIST_PATH = Path(args.email_list)

    # Run date
    if args.email_date:
        try:
            EMAIL_RUN_DATE = date.fromisoformat(args.email_date)
        except ValueError:
            write_log(f"ERROR: Invalid date '{args.email_date}'. Use YYYY-MM-DD.")
            return 2
    else:
        EMAIL_RUN_DATE = date.today()

    LOG_DIR = Path(CONFIG["LOG_DIR"])
    BATCH = f"EmailBatch{BATCH_NUMBER}"
    MASTER_PATH = CONFIG["MASTER_PATH"]
    FROM_USER = CONFIG["FROM_USER"]
    APP_PASSWORD = CONFIG["APP_PASSWORD"]
    REQUIRE_METHOD_EMAIL = bool(CONFIG["REQUIRE_METHOD_EMAIL"])
    DRY_RUN = bool(CONFIG["DRY_RUN"])
    USE_SSL = bool(CONFIG["USE_SSL"])
    SMTP_SERVER = CONFIG["SMTP_SERVER"]
    SMTP_PORT_SSL = int(CONFIG["SMTP_PORT_SSL"])
    SMTP_PORT_STARTTLS = int(CONFIG["SMTP_PORT_STARTTLS"])
    
    # Parallel execution config
    MAX_PARALLEL = args.max_parallel if args.max_parallel is not None else CONFIG["MAX_PARALLEL"]
    
    # Fallback hours config
    FALLBACK_HOURS = args.fallback_hours if args.fallback_hours is not None else CONFIG["FALLBACK_HOURS"]
    
    # Force resend config
    FORCE_RESEND = args.force_resend if args.force_resend is not None else CONFIG["FORCE_RESEND"]

    # New: run_id + output CSV for emails
    email_run_id = f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}"
    email_csv_out = LOG_DIR / f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}.csv"

    if not FROM_USER or not APP_PASSWORD:
        write_log("ERROR: Please set FROM_USER and APP_PASSWORD in CONFIG.")
        return 2

    # Load email list
    df_list = load_email_list(EMAIL_LIST_PATH)
    for col in ["Receiver","CC","BCC","Subject","Attachement Path"]:
        if col not in df_list.columns:
            write_log(f"ERROR: Column '{col}' missing in List sheet.")
            return 2

    write_log(f"Processing Batch {BATCH_NUMBER} for {EMAIL_RUN_DATE} | email_run_id={email_run_id}")
    write_log(f"Email list: {EMAIL_LIST_PATH}")
    write_log(f"Parallel execution: {MAX_PARALLEL} processes")
    write_log(f"Fallback hours: {FALLBACK_HOURS} hours")
    write_log(f"Force resend: {FORCE_RESEND}")

    # Prepare data for parallel processing
    email_rows = [(row, idx) for idx, row in df_list.iterrows()]
    
    total_ok = total_fail = total_skip = 0

    # Process emails in parallel
    with ProcessPoolExecutor(max_workers=MAX_PARALLEL) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(
                process_single_email, 
                row_data, 
                CONFIG, 
                email_run_id, 
                BATCH, 
                EMAIL_RUN_DATE, 
                MASTER_PATH, 
                REQUIRE_METHOD_EMAIL, 
                DRY_RUN,
                FALLBACK_HOURS,
                FORCE_RESEND
            ): row_data[1] 
            for row_data in email_rows
        }
        
        # Process results as they complete
        for future in as_completed(future_to_index):
            try:
                result = future.result()
                if result['status'] == 'OK':
                    total_ok += 1
                    status_indicator = "Force Resend is set to Ture" if FORCE_RESEND else "Force Resend is set to False"
                    write_log(f"Email {result['index']+1}: OK - To: {result['to_norm']}")
                elif result['status'] == 'FAIL':
                    total_fail += 1
                    write_log(f"X Email {result['index']+1}: FAIL - {result['error']}")
                elif result['status'] == 'SKIP':
                    total_skip += 1
                    write_log(f"- Email {result['index']+1}: SKIP - {result['error']}")
                    
            except Exception as e:
                total_fail += 1
                write_log(f"X Email {future_to_index[future]+1}: EXCEPTION - {str(e)}")

    # Export this email run to CSV (IST timestamps)
    try:
        # Reconnect to export CSV (main process connection)
        conn = db_connect()
        export_email_csv(conn, email_run_id, email_csv_out)
        write_log(f"Email CSV exported: {email_csv_out}")
        conn.close()
    except Exception as e:
        write_log(f"WARNING: CSV export failed: {e}")

    write_log(f"Summary: OK={total_ok} FAIL={total_fail} SKIP={total_skip}")
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())