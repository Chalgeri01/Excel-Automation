#!/usr/bin/env python3
"""
send_reports_configured.py

Same behavior as your previous version, but:
- Validates "refreshed OK" via MySQL (events table) instead of CSV
- Logs Email OK/FAIL/SKIP into DB (events.stage='Email')
- Exports a per-run email CSV from DB at the end
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

import pandas as pd
import pymysql

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

def db_refresh_ok(conn, file_path: str, email_run_date: date) -> bool:
    """
    Accept if there's a Refresh/OK for the file on RunDate or RunDate-1.
    Compare case-insensitively with a safe collation.
    """
    d1 = email_run_date
    d0 = d1 - timedelta(days=1)
    sql = """
        SELECT 1
        FROM events
        WHERE LOWER(file_path) COLLATE utf8mb4_unicode_ci = LOWER(%s) COLLATE utf8mb4_unicode_ci
          AND stage='Refresh' AND status='OK'
          AND (rundate = %s OR rundate = %s)
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_path, d1, d0))
        return cur.fetchone() is not None

def db_refresh_method_email(conn, file_path: str, email_run_date: date) -> bool:
    d1 = email_run_date
    d0 = d1 - timedelta(days=1)
    sql = """
        SELECT 1
        FROM events
        WHERE LOWER(file_path) COLLATE utf8mb4_unicode_ci = LOWER(%s) COLLATE utf8mb4_unicode_ci
          AND stage='Refresh' AND status='OK'
          AND (rundate = %s OR rundate = %s)
          AND LOWER(COALESCE(method,'')) = 'email'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_path, d1, d0))
        return cur.fetchone() is not None

def db_already_emailed_ok(conn, to_norm: str, subject: str, email_run_date: date) -> bool:
    d1 = email_run_date
    d0 = d1 - timedelta(days=1)
    sql = """
        SELECT 1
        FROM events
        WHERE stage='Email' AND status='OK'
          AND (rundate = %s OR rundate = %s)
          AND LOWER(recipients_to) = LOWER(%s)
          AND LOWER(subject)       = LOWER(%s)
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (d1, d0, to_norm, subject))
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

# --------------------------- Args -----------------------------------

def parse_arguments():
    p = argparse.ArgumentParser(description="Send email reports for a batch (DB-backed).")
    p.add_argument('--batch', type=int, required=True, help='Batch number (1..6 etc.)')
    p.add_argument('--email-list', type=str, required=True, help='Path to Email_List.xlsx')
    p.add_argument('--email-date', type=str, default=None, help='YYYY-MM-DD for email run; default=TODAY')
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
            print(f"ERROR: Invalid date '{args.email_date}'. Use YYYY-MM-DD.")
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

    # New: run_id + output CSV for emails
    email_run_id = f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}"
    email_csv_out = LOG_DIR / f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}.csv"

    if not FROM_USER or not APP_PASSWORD:
        print("ERROR: Please set FROM_USER and APP_PASSWORD in CONFIG.")
        return 2

    # Load email list (unchanged)
    df_list = load_email_list(EMAIL_LIST_PATH)
    for col in ["Receiver","CC","BCC","Subject","Attachement Path"]:
        if col not in df_list.columns:
            print(f"ERROR: Column '{col}' missing in List sheet.")
            return 2

    # Connect DB once
    try:
        conn = db_connect()
    except Exception as e:
        print(f"ERROR: DB connection failed: {e}")
        return 2

    print(f"Processing Batch {BATCH_NUMBER} for {EMAIL_RUN_DATE} | email_run_id={email_run_id}")
    print(f"Email list: {EMAIL_LIST_PATH}")

    total_ok = total_fail = total_skip = 0

    for _, row in df_list.iterrows():
        to_addrs = normalize_addr_list(cell_str(row, "Receiver"))
        cc_addrs = normalize_addr_list(cell_str(row, "CC"))
        bcc_addrs = normalize_addr_list(cell_str(row, "BCC"))
        subject = cell_str(row, "Subject")
        atts = split_attachments(cell_str(row, "Attachement Path"))

        # Build body (unchanged)
        greeting = get_greeting(to_addrs)
        body_text = DEFAULT_BODY.replace("{GREETING}", greeting)

        # Validate fields
        if not to_addrs:
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, "", subject, "FAIL", "Missing Receiver")
            total_fail += 1
            continue

        if not subject:
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_addrs, "", "FAIL", "Missing Subject")
            total_fail += 1
            continue

        # Idempotence: already emailed OK (To+Subject) today or yesterday?
        to_norm = to_addrs  # we store normalized already
        if db_already_emailed_ok(conn, to_norm, subject, EMAIL_RUN_DATE):
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_norm, subject, "SKIP", "Already emailed (To+Subject)")
            total_skip += 1
            continue

        # Freshness checks for each attachment (same logic as before, but DB-backed)
        problems = []
        for p in atts:
            if not p.exists():
                problems.append(f"Missing file: {p}")
                continue
            # IMPORTANT: compare exact path string as stored; parameterized SQL handles backslashes safely
            fp_str = str(p)
            if not db_refresh_ok(conn, fp_str, EMAIL_RUN_DATE):
                problems.append(f"No successful refresh today: {p}")
                continue
            if REQUIRE_METHOD_EMAIL and not db_refresh_method_email(conn, fp_str, EMAIL_RUN_DATE):
                problems.append(f"Method not 'Email' today: {p}")

        if problems:
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_norm, subject, "FAIL", "; ".join(problems))
            total_fail += 1
            continue

        # Build message + recipients (unchanged)
        msg = EmailMessage()
        msg["From"] = FROM_USER
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

        if CONFIG["DRY_RUN"]:
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_norm, subject, "SKIP", "DRYRUN")
            total_skip += 1
            continue

        # Send + log to DB
        try:
            send_via_gmail(FROM_USER, APP_PASSWORD, msg, USE_SSL, SMTP_SERVER, SMTP_PORT_SSL, SMTP_PORT_STARTTLS, all_recipients)
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_norm, subject, "OK", "")
            total_ok += 1
        except Exception as e:
            db_write_email_event(conn, email_run_id, BATCH, EMAIL_RUN_DATE, MASTER_PATH, atts, to_norm, subject, "FAIL", str(e))
            total_fail += 1

    # Export this email run to CSV (IST timestamps)
    try:
        export_email_csv(conn, email_run_id, email_csv_out)
        print(f"Email CSV exported: {email_csv_out}")
    except Exception as e:
        print(f"WARNING: CSV export failed: {e}")

    try:
        conn.close()
    except:  # noqa
        pass

    print(f"Summary: OK={total_ok} FAIL={total_fail} SKIP={total_skip}")
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
