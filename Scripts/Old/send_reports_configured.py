#!/usr/bin/env python3
"""
send_reports_configured.py

Self-contained email sender for your reporting pipeline.

What it does
------------
- Reads your unified log CSV (Stage="Refresh" + "Email") and the Email_List.xlsx.
- For each row in Email_List.xlsx with Send="X":
  * Splits attachments on ';'
  * Verifies each attachment had a successful Refresh "today" in run-log.csv
  * (Optional) Only sends if the refreshed file’s Method was "Email"
  * Substitutes {VARIABLE1} in the body
  * Sends via Gmail (App Password)
  * Appends Stage="Email" rows to the SAME run-log.csv with OK/FAIL/SKIP

How to run
----------
1) Edit the CONFIG block below.
2) Run:  python send_reports_configured.py
"""

# ============================== CONFIG ===============================

CONFIG = {
    # --- Paths ---
    "LOG_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\run-log.csv",
    "EMAIL_LIST_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Email-Master\Email_List.xlsx",
    # --- Logging metadata ---
    "BATCH": "EmailRun",  # Label you want to see in the log for this email run
    "MASTER_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Master-sheet\03.00 PM Udyam Stock Report.xlsb",
    # --- Gmail sender credentials (use an App Password) ---
    "FROM_USER": "it@kotharigroupindia.com",
    "APP_PASSWORD": "euia zzuy tidn dydk",  # Example: abcd efgh ijkl mnop (no spaces)
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
from datetime import datetime, date
from email.message import EmailMessage
import mimetypes
import smtplib
import ssl
from pathlib import Path

import pandas as pd

# Default email body used if "Body" sheet is missing:
DEFAULT_BODY_FALLBACK = """{VARIABLE1},

Please find attached today’s report.

If you encounter any issues with the report or have suggestions for improvement, kindly submit your feedback using the following link:
https://forms.gle/CvwEkVLpvCuZ2A2J9

Your feedback will help us continuously improve the reporting system.

---

⚠️ This is an automated email. Please do not reply to this message.

With regards,
Report Automation Team
IT Department
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

def load_email_list(xlsx_path: Path) -> tuple[pd.DataFrame, str]:
    xl = pd.ExcelFile(xlsx_path)

    # "List" sheet
    df_list = pd.read_excel(xl, "List")
    df_list.columns = [str(c).strip() for c in df_list.columns]

    # Optional "Body" sheet
    body_text = DEFAULT_BODY_FALLBACK
    if "Body" in xl.sheet_names:
        db = pd.read_excel(xl, "Body", header=None)
        # Use non-empty lines from column 0 as the body
        lines = [
            str(v) for v in db.iloc[:, 0].tolist() if isinstance(v, str) and v.strip()
        ]
        if lines:
            # If the first line is a label like "Email Body:", drop it
            if lines[0].strip().lower().startswith("email body"):
                lines = lines[1:]
            body_text = "\n".join(lines).strip()
    return df_list, body_text

def most_recent_refresh_ok_today(runlog: pd.DataFrame, filepath: Path) -> bool:
    today = today_str_local()
    target = str(filepath)
    mask = (
        (runlog["Stage"].str.lower() == "refresh")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"] == today)
        & (runlog["FilePath"].str.casefold() == target.casefold())
    )
    return mask.any()

def refresh_method_is_email_today(runlog: pd.DataFrame, filepath: Path) -> bool:
    today = today_str_local()
    target = str(filepath)
    mask = (
        (runlog["Stage"].str.lower() == "refresh")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"] == today)
        & (runlog["FilePath"].str.casefold() == target.casefold())
    )
    day_rows = runlog.loc[mask]
    if day_rows.empty:
        return False
    return any(
        (m or "").strip().lower() == "email" for m in day_rows["Method"].tolist()
    )

def already_emailed_today(runlog: pd.DataFrame, to_addrs: str, subject: str) -> bool:
    today = today_str_local()
    to_norm = normalize_addr_list(to_addrs)
    subj = (subject or "").strip()
    mask = (
        (runlog["Stage"].str.lower() == "email")
        & (runlog["Status"].str.upper() == "OK")
        & (runlog["RunDate"] == today)
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


# ----------------------------- Main flow ----------------------------


def main():
    LOG_PATH = Path(CONFIG["LOG_PATH"])
    EMAIL_LIST_PATH = Path(CONFIG["EMAIL_LIST_PATH"])
    BATCH = CONFIG["BATCH"]
    MASTER_PATH = CONFIG["MASTER_PATH"]
    FROM_USER = CONFIG["FROM_USER"]
    APP_PASSWORD = CONFIG["APP_PASSWORD"]
    REQUIRE_METHOD_EMAIL = bool(CONFIG["REQUIRE_METHOD_EMAIL"])
    DRY_RUN = bool(CONFIG["DRY_RUN"])
    USE_SSL = bool(CONFIG["USE_SSL"])
    SMTP_SERVER = CONFIG["SMTP_SERVER"]
    SMTP_PORT_SSL = int(CONFIG["SMTP_PORT_SSL"])
    SMTP_PORT_STARTTLS = int(CONFIG["SMTP_PORT_STARTTLS"])

    if not FROM_USER or not APP_PASSWORD:
        print(
            "ERROR: Please set FROM_USER and APP_PASSWORD in CONFIG at the top of this file."
        )
        return 2

    # Load data
    runlog = load_run_log(LOG_PATH)
    df_list, body_template = load_email_list(EMAIL_LIST_PATH)
    today = today_str_local()

    # Validate essential columns
    for col in ["Send", "Receiver", "Subject", "Attachment", "{VARIABLE1}"]:
        if col not in df_list.columns:
            print(f"ERROR: Column '{col}' missing in List sheet.", flush=True)
            return 2

    # Iterate rows to send
    for _, row in df_list.iterrows():
        if str(row.get("Send", "")).strip().upper() != "X":
            continue

        to_addrs = normalize_addr_list(cell_str(row, "Receiver"))
        cc_addrs = normalize_addr_list(cell_str(row, "CC"))
        bcc_addrs = normalize_addr_list(cell_str(row, "BCC"))
        subject = cell_str(row, "Subject")
        var1 = cell_str(row, "{VARIABLE1}")
        atts = split_attachments(cell_str(row, "Attachment"))
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Validate basic fields
        if not to_addrs:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today,
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
                    "RunDate": today,
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
        if already_emailed_today(runlog, to_addrs, subject):
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today,
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
            if not most_recent_refresh_ok_today(runlog, p):
                problems.append(f"No successful refresh today: {p}")
                continue
            if REQUIRE_METHOD_EMAIL and not refresh_method_is_email_today(runlog, p):
                problems.append(f"Method not 'Email' today: {p}")

        if problems:
            append_log_row(
                LOG_PATH,
                {
                    "Timestamp": now_iso,
                    "RunDate": today,
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
        body_text = body_template.replace("{VARIABLE1}", var1 or "")
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
                    "RunDate": today,
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
                    "RunDate": today,
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
                    "RunDate": today,
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
