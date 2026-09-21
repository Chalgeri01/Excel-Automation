#!/usr/bin/env python3
"""
send_reports_configured.py
Version :- 1.5 Update on 25th Aug 2026

Same behavior as v1.4, plus focused SMTP large-message improvements:
- Validates "refreshed OK" via MySQL (events table) instead of CSV
- Logs Email OK/FAIL/SKIP into DB (events.stage='Email')
- Exports a per-run email CSV from DB at the end
- Uses Google Workspace SMTP Relay (smtp-relay.gmail.com:587) with STARTTLS
- Uses ThreadPoolExecutor for I/O-bound parallel sending
- Keeps one SMTP connection per worker thread and verifies it with NOOP before reuse
- Immediately drops broken sockets instead of calling blocking smtp.quit()
- Recycles SMTP connections after a configured message count or connection age
- Retries temporary SMTP/network failures with bounded exponential backoff
- Separates SMTP connect timeout from SMTP message-send timeout
- Allows a longer timeout only during the actual SMTP message transmission
- Measures both raw attachment size and final MIME message size
- Adds a configurable raw-attachment safety limit before SMTP sending
- Fixes CONFIG FORCE_RESEND handling when --force-resend is not supplied
- Keeps per-email timing/attachment-size metrics in the log
"""

# ============================== CONFIG ===============================
CONFIG = {
    "LOG_DIR": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo",

    # Will be overridden by --batch at runtime (only used for log rows)
    "BATCH": "EmailRun",
    "MASTER_PATH": r"C:\Users\kapl\Desktop\Project-Reporting-Automation\Master-sheet\03.00 PM Udyam Stock Report.xlsb",

    # Sender
    "FROM_USER": "report@kotharigroupindia.com",

    # Behavior
    "REQUIRE_METHOD_EMAIL": False,
    "DRY_RUN": False,

    # Google Workspace SMTP Relay
    # Authentication is by the public IP configured in Google Workspace Admin.
    # Do NOT add an App Password and do NOT call smtp.login().
    "SMTP_SERVER": "smtp-relay.gmail.com",
    "SMTP_PORT": 587,

    # v1.5: separate connection and actual message-send timeouts.
    # Connect timeout applies to SMTP connect/EHLO/STARTTLS and is restored
    # after each successful send.
    "SMTP_CONNECT_TIMEOUT": 30,

    # NOOP uses a short timeout only while checking whether a cached
    # persistent SMTP connection is still alive.
    "SMTP_HEALTH_CHECK_TIMEOUT": 5,

    # Actual DATA/message send is allowed longer because some reports are large.
    # Socket timeout is an inactivity timeout, not an absolute total duration.
    "SMTP_SEND_TIMEOUT": 180,

    # 2 retries = maximum 3 total attempts for one email.
    "SMTP_MAX_RETRIES": 2,
    "SMTP_RETRY_BASE_SECONDS": 2,

    # Proactively recycle connections before NAT/firewall/relay idle state becomes stale.
    "SMTP_MAX_MESSAGES_PER_CONNECTION": 25,
    "SMTP_MAX_CONNECTION_AGE_SECONDS": 300,  # 5 minutes

    # Parallel sending
    # Keep at 2 while validating v1.5 large-message behavior.
    "MAX_PARALLEL": 3,

    # Safety guard on total RAW attachment bytes for one email.
    # This is an application safety threshold, not a claim about Google's exact
    # encoded-message limit. Set to 0 to disable the guard.
    "MAX_RAW_ATTACHMENT_MB": 24,

    # Performance diagnostics in email-runner.log
    "LOG_PERFORMANCE_DETAILS": True,

    # Fallback email timing (hours)
    "FALLBACK_HOURS": 18,  # Consider matching emails sent in last 18 hours as already sent

    # Force resend
    "FORCE_RESEND": False,  # Bypass refresh/duplicate validations and resend all emails
}
# ============================ END CONFIG =============================

import os
import csv
from datetime import datetime, date, timezone
from email.message import EmailMessage
import mimetypes
import smtplib
import ssl
from pathlib import Path
import argparse
import time
import threading

import pandas as pd
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed

# Thread-safe logging and per-thread SMTP connection management
_LOG_LOCK = threading.Lock()
_SMTP_LOCAL = threading.local()
_SMTP_CONNECTIONS = set()
_SMTP_CONNECTIONS_LOCK = threading.Lock()

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

LOG_FILE_NAME = "email-runner.log"
LOG_FILE_PATH = os.path.join(CONFIG["LOG_DIR"], LOG_FILE_NAME)


def write_log(message: str):
    """
    Writes a timestamped message to both the console and a log file,
    using the configured LOG_DIR.
    """
    try:
        os.makedirs(CONFIG["LOG_DIR"], exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        batch_tag = CONFIG.get("BATCH", "ScriptRun")
        log_line = f"[{ts}][{batch_tag}] {message}"

        with _LOG_LOCK:
            print(log_line)
            with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")

    except IOError as e:
        print(
            f"FATAL ERROR: Could not write to log file {LOG_FILE_PATH}. "
            f"Check directory permissions. Details: {e}"
        )
    except Exception as e:
        print(f"FATAL ERROR: An unexpected error occurred during logging: {e}")


# --------------------------- Helpers --------------------------------

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


def _register_smtp_connection(smtp):
    """Track open SMTP objects so the main thread can force-close them at shutdown."""
    with _SMTP_CONNECTIONS_LOCK:
        _SMTP_CONNECTIONS.add(smtp)


def _unregister_smtp_connection(smtp):
    with _SMTP_CONNECTIONS_LOCK:
        _SMTP_CONNECTIONS.discard(smtp)


def _reset_thread_smtp_state():
    """Forget all SMTP state cached by the current worker thread."""
    _SMTP_LOCAL.smtp = None
    _SMTP_LOCAL.smtp_created_at = None
    _SMTP_LOCAL.smtp_message_count = 0


def _raw_close_smtp(smtp):
    """
    Close an SMTP socket immediately.

    Important: intentionally do NOT call smtp.quit() here. A half-dead TCP/TLS
    session can block while QUIT waits for a server response; close() simply
    tears down the local socket.
    """
    if smtp is None:
        return
    try:
        smtp.close()
    except Exception:
        pass


def close_thread_smtp():
    """Immediately close and clear the current worker thread's SMTP connection."""
    smtp = getattr(_SMTP_LOCAL, "smtp", None)
    _reset_thread_smtp_state()

    if smtp is None:
        return

    _unregister_smtp_connection(smtp)
    _raw_close_smtp(smtp)


def close_all_smtp_connections():
    """
    Force-close every remaining SMTP socket after worker completion.

    We deliberately use close(), not quit(), so shutdown cannot hang on a stale
    relay connection.
    """
    with _SMTP_CONNECTIONS_LOCK:
        connections = list(_SMTP_CONNECTIONS)
        _SMTP_CONNECTIONS.clear()

    for smtp in connections:
        _raw_close_smtp(smtp)


def _smtp_connection_age_seconds() -> float:
    created_at = getattr(_SMTP_LOCAL, "smtp_created_at", None)
    if created_at is None:
        return 0.0
    return max(0.0, time.monotonic() - created_at)


def _smtp_health_check(smtp, config) -> tuple[bool, str]:
    """
    Verify a cached SMTP connection using NOOP.

    The health check temporarily uses a short socket timeout so a dead session
    is detected quickly instead of consuming the send timeout.
    """
    health_timeout = max(1, int(config.get("SMTP_HEALTH_CHECK_TIMEOUT", 5)))
    old_timeout = None

    try:
        if smtp.sock is None:
            return False, "socket is already closed"

        old_timeout = smtp.sock.gettimeout()
        smtp.sock.settimeout(health_timeout)

        code, response = smtp.noop()
        if int(code) == 250:
            return True, "250"

        if isinstance(response, bytes):
            response = response.decode(errors="replace")
        return False, f"NOOP returned {code}: {response}"

    except Exception as e:
        return False, str(e)

    finally:
        try:
            if smtp.sock is not None and old_timeout is not None:
                smtp.sock.settimeout(old_timeout)
        except Exception:
            pass


def _create_smtp_connection(config):
    """Create a new STARTTLS connection to the Google Workspace SMTP relay."""
    thread_name = threading.current_thread().name
    connect_started = time.perf_counter()
    context = ssl.create_default_context()
    connect_timeout = max(1, int(config.get("SMTP_CONNECT_TIMEOUT", 30)))

    smtp = smtplib.SMTP(
        config["SMTP_SERVER"],
        int(config["SMTP_PORT"]),
        timeout=connect_timeout,
    )

    try:
        smtp.ehlo()
        smtp.starttls(context=context)
        smtp.ehlo()

        # Ensure the persistent socket remains on the normal connection timeout
        # after TLS negotiation.
        if smtp.sock is not None:
            smtp.sock.settimeout(connect_timeout)

    except Exception:
        _raw_close_smtp(smtp)
        raise

    _SMTP_LOCAL.smtp = smtp
    _SMTP_LOCAL.smtp_created_at = time.monotonic()
    _SMTP_LOCAL.smtp_message_count = 0
    _register_smtp_connection(smtp)

    connect_s = time.perf_counter() - connect_started
    write_log(f"SMTP [{thread_name}] connected/reconnected in {connect_s:.2f}s")
    return smtp


def get_smtp_connection(config):
    """
    Return a healthy SMTP connection dedicated to the current worker thread.

    Before a cached connection is reused we:
      1. recycle it when it is too old or has sent too many messages, and
      2. issue SMTP NOOP to make sure Google/NAT/firewall has not closed it.

    Google Workspace authenticates this relay by the configured public IP,
    therefore smtp.login() is intentionally NOT used.
    """
    smtp = getattr(_SMTP_LOCAL, "smtp", None)

    if smtp is not None:
        max_messages = max(
            1, int(config.get("SMTP_MAX_MESSAGES_PER_CONNECTION", 25))
        )
        max_age = max(
            1, int(config.get("SMTP_MAX_CONNECTION_AGE_SECONDS", 300))
        )
        sent_count = int(
            getattr(_SMTP_LOCAL, "smtp_message_count", 0) or 0
        )
        age_s = _smtp_connection_age_seconds()

        if sent_count >= max_messages or age_s >= max_age:
            write_log(
                f"SMTP [{threading.current_thread().name}] proactive recycle "
                f"(messages={sent_count}, age={age_s:.0f}s)"
            )
            close_thread_smtp()
            smtp = None

    if smtp is not None:
        healthy, detail = _smtp_health_check(smtp, config)
        if healthy:
            return smtp

        write_log(
            f"SMTP [{threading.current_thread().name}] cached connection "
            f"health check failed ('{detail}'); reconnecting"
        )
        close_thread_smtp()

    return _create_smtp_connection(config)


def _is_temporary_smtp_code(code: int) -> bool:
    return 400 <= int(code) < 500


def _smtp_error_text(error) -> str:
    if isinstance(error, bytes):
        return error.decode(errors="replace")
    return str(error)


def _set_socket_timeout(smtp, timeout_seconds: int):
    """
    Set the timeout on the currently active SMTP socket if available.
    Returns the previous socket timeout so the caller can restore it.
    """
    if smtp is None or smtp.sock is None:
        return None

    old_timeout = smtp.sock.gettimeout()
    smtp.sock.settimeout(max(1, int(timeout_seconds)))
    return old_timeout


def _restore_socket_timeout(smtp, old_timeout, fallback_timeout: int):
    """
    Restore a socket timeout after send_message() completes.

    If the original value is unavailable, restore the configured connect timeout.
    This function is deliberately best-effort because the socket may have died.
    """
    try:
        if smtp is not None and smtp.sock is not None:
            timeout_to_restore = (
                old_timeout
                if old_timeout is not None
                else max(1, int(fallback_timeout))
            )
            smtp.sock.settimeout(timeout_to_restore)
    except Exception:
        pass


def send_via_google_relay(config, msg, all_recipients) -> int:
    """
    Send one message through Google Workspace SMTP Relay.

    Returns:
        int: number of send attempts used for this email.

    v1.5 behavior:
    - Reuses one healthy SMTP/STARTTLS connection per worker thread.
    - Verifies cached connections with NOOP before reuse.
    - No Gmail App Password and no SMTP AUTH login.
    - Drops a broken socket immediately (no blocking QUIT).
    - Uses a short connect timeout for connection/health operations.
    - Uses SMTP_SEND_TIMEOUT only during the actual message transmission.
    - Restores the normal connect timeout after a successful send.
    - Retries temporary 4xx SMTP responses and transient network disconnects.
    - Permanent 5xx SMTP errors are returned immediately to the caller.
    """
    max_retries = max(0, int(config.get("SMTP_MAX_RETRIES", 2)))
    retry_base = max(1, int(config.get("SMTP_RETRY_BASE_SECONDS", 2)))
    total_attempts = max_retries + 1

    connect_timeout = max(
        1, int(config.get("SMTP_CONNECT_TIMEOUT", 30))
    )
    send_timeout = max(
        connect_timeout, int(config.get("SMTP_SEND_TIMEOUT", 180))
    )

    for attempt in range(1, total_attempts + 1):
        smtp = None
        old_timeout = None

        try:
            smtp = get_smtp_connection(config)

            # v1.5: only the actual message transfer gets the longer timeout.
            old_timeout = _set_socket_timeout(smtp, send_timeout)

            try:
                smtp.send_message(
                    msg,
                    from_addr=config["FROM_USER"],
                    to_addrs=all_recipients or [config["FROM_USER"]],
                )
            finally:
                # Best effort. If send_message() killed the socket, the outer
                # exception handler will close/reset the connection anyway.
                _restore_socket_timeout(
                    smtp, old_timeout, connect_timeout
                )

            _SMTP_LOCAL.smtp_message_count = int(
                getattr(_SMTP_LOCAL, "smtp_message_count", 0) or 0
            ) + 1
            return attempt

        except smtplib.SMTPResponseException as e:
            code = int(e.smtp_code)
            detail = _smtp_error_text(e.smtp_error)

            # Never trust/reuse the same session after a send exception.
            close_thread_smtp()

            if _is_temporary_smtp_code(code) and attempt < total_attempts:
                delay = retry_base * (2 ** (attempt - 1))
                write_log(
                    f"SMTP temporary error {code} '{detail}'; retrying in {delay}s "
                    f"(attempt {attempt + 1}/{total_attempts})"
                )
                time.sleep(delay)
                continue

            try:
                e.smtp_attempts = attempt
            except Exception:
                pass
            raise

        except (
            smtplib.SMTPServerDisconnected,
            smtplib.SMTPConnectError,
            ConnectionError,
            TimeoutError,
            OSError,
            ssl.SSLError,
        ) as e:
            close_thread_smtp()

            if attempt >= total_attempts:
                try:
                    e.smtp_attempts = attempt
                except Exception:
                    pass
                raise

            delay = retry_base * (2 ** (attempt - 1))
            write_log(
                f"SMTP/network error '{e}'; reconnecting and retrying in {delay}s "
                f"(attempt {attempt + 1}/{total_attempts})"
            )
            time.sleep(delay)

    raise RuntimeError("SMTP send exhausted all attempts unexpectedly")


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
    db = kv.get("database", "reportlogs")
    user = kv.get("uid") or kv.get("user") or "root"
    pwd = kv.get("pwd") or kv.get("password") or ""

    ssl_mode = (kv.get("sslmode") or "").lower()
    ssl_config = None
    if ssl_mode and ssl_mode not in ("none", "disabled"):
        ssl_config = {"ssl": {}}

    return dict(
        host=host,
        port=port,
        user=user,
        password=pwd,
        database=db,
        charset="utf8mb4",
        autocommit=True,
        **({} if ssl_config is None else ssl_config),
    )


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


def db_already_emailed_ok(
    conn,
    to_norm: str,
    subject: str,
    run_date: date,
    batch: str,
    file_path: str,
    fallback_hours: int,
) -> bool:
    """
    Check if the same email was already sent in the fallback period.
    """
    sql = """
        SELECT 1
        FROM events
        WHERE stage='Email' AND status='OK'
          AND rundate = %s
          AND batch   = %s
          AND LOWER(recipients_to) = LOWER(%s)
          AND LOWER(subject)       = LOWER(%s)
          AND LOWER(REPLACE(file_path, '\\\\', '/')) = LOWER(REPLACE(%s, '\\\\', '/'))
          AND timestamp_utc >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s HOUR)
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                str(run_date),
                batch,
                to_norm,
                subject,
                file_path,
                int(fallback_hours),
            ),
        )
        return cur.fetchone() is not None


def db_write_email_event(
    conn,
    run_id: str,
    batch: str,
    rundate: date,
    master_path: str,
    file_paths: list[Path],
    to_norm: str,
    subject: str,
    status: str,
    error_text: str = "",
    duration_s: int | None = None,
):
    sql = """
        INSERT INTO events
        (run_id,batch,stage,timestamp_utc,rundate,master_path,file_path,method,status,error_text,duration_s,recipients_to,subject)
        VALUES
        (%s,%s,'Email',%s,%s,%s,%s,'Email',%s,%s,%s,%s,%s)
    """
    ts_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    fp = ";".join(str(p) for p in file_paths) if file_paths else ""

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                run_id,
                batch,
                ts_utc,
                rundate,
                master_path,
                fp,
                status,
                error_text or "",
                duration_s if duration_s is not None else None,
                to_norm,
                subject,
            ),
        )


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
        output_cols = [
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
        ]
        w.writerow(output_cols)

        for r in rows:
            rec = dict(zip(cols, r))
            w.writerow([rec.get(c, "") for c in output_cols])


# --------------------------- Parallel Processing --------------------

def _empty_metrics() -> dict:
    return {
        "db_connect_s": 0.0,
        "validation_s": 0.0,
        "file_read_s": 0.0,
        "smtp_s": 0.0,
        "total_s": 0.0,
        "attachment_bytes": 0,
        "message_bytes": 0,
        "smtp_attempts": 0,
    }


def _finalize_metrics(metrics: dict, total_started: float) -> dict:
    metrics["total_s"] = max(0.0, time.perf_counter() - total_started)
    return metrics


def _make_result(index, status, error, to_norm, subject, atts, metrics):
    return {
        "index": index,
        "status": status,
        "error": error,
        "to_norm": to_norm,
        "subject": subject,
        "atts": atts,
        "metrics": metrics,
    }


def _max_raw_attachment_bytes(config) -> int:
    """
    Convert MAX_RAW_ATTACHMENT_MB into bytes.
    A value <= 0 disables the raw-size guard.
    """
    try:
        mb = float(config.get("MAX_RAW_ATTACHMENT_MB", 0) or 0)
    except (TypeError, ValueError):
        mb = 0.0

    if mb <= 0:
        return 0

    return int(mb * 1024 * 1024)


def _get_total_raw_attachment_size(atts: list[Path]) -> int:
    """
    Return the total raw size of all attachments using filesystem metadata.
    This lets us enforce the safety guard before reading the full files.
    """
    total = 0
    for p in atts:
        total += int(p.stat().st_size)
    return total


def process_single_email(
    row_data,
    config,
    email_run_id,
    batch,
    email_run_date,
    master_path,
    require_method_email,
    dry_run,
    fallback_hours,
    force_resend,
):
    """
    Process a single email row in a worker thread.

    SMTP is persistent per thread with health checks/recycling.
    DB remains per-email to preserve v1.4 database behavior.
    """
    total_started = time.perf_counter()
    metrics = _empty_metrics()
    row, index = row_data

    db_started = time.perf_counter()
    try:
        conn = db_connect()
        metrics["db_connect_s"] = time.perf_counter() - db_started
    except Exception as e:
        metrics["db_connect_s"] = time.perf_counter() - db_started
        _finalize_metrics(metrics, total_started)
        return _make_result(
            index,
            "FAIL",
            f"DB connection failed: {e}",
            "",
            "",
            [],
            metrics,
        )

    try:
        to_addrs = normalize_addr_list(cell_str(row, "Receiver"))
        cc_addrs = normalize_addr_list(cell_str(row, "CC"))
        bcc_addrs = normalize_addr_list(cell_str(row, "BCC"))
        subject = cell_str(row, "Subject")
        atts = split_attachments(cell_str(row, "Attachement Path"))
        attachment_key = ";".join(str(p) for p in atts) if atts else ""

        greeting = get_greeting(to_addrs)
        body_text = DEFAULT_BODY.replace("{GREETING}", greeting)

        validation_started = time.perf_counter()

        # Validate required fields.
        if not to_addrs:
            metrics["validation_s"] = (
                time.perf_counter() - validation_started
            )
            _finalize_metrics(metrics, total_started)
            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                "",
                subject,
                "FAIL",
                "Missing Receiver",
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index, "FAIL", "Missing Receiver", "", subject, atts, metrics
            )

        if not subject:
            metrics["validation_s"] = (
                time.perf_counter() - validation_started
            )
            _finalize_metrics(metrics, total_started)
            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_addrs,
                "",
                "FAIL",
                "Missing Subject",
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index, "FAIL", "Missing Subject", to_addrs, "", atts, metrics
            )

        to_norm = to_addrs

        if not force_resend:
            problems = []

            for p in atts:
                if not p.exists():
                    problems.append(f"Missing file: {p}")
                    continue

                fp_str = str(p)
                latest_refresh_date = db_get_latest_refresh_date(conn, fp_str)

                if not latest_refresh_date:
                    problems.append(
                        f"No successful refresh found in database: {p}"
                    )
                    continue

                if latest_refresh_date != email_run_date:
                    problems.append(
                        f"File not refreshed for today ({email_run_date}). "
                        f"Last refresh: {latest_refresh_date}: {p}"
                    )
                    continue

                if (
                    require_method_email
                    and not db_refresh_method_email_for_date(
                        conn, fp_str, email_run_date
                    )
                ):
                    problems.append(f"Method not 'Email' for today: {p}")

            if problems:
                metrics["validation_s"] = (
                    time.perf_counter() - validation_started
                )
                _finalize_metrics(metrics, total_started)
                error_text = "; ".join(problems)

                db_write_email_event(
                    conn,
                    email_run_id,
                    batch,
                    email_run_date,
                    master_path,
                    atts,
                    to_addrs,
                    subject,
                    "FAIL",
                    error_text,
                    duration_s=int(round(metrics["total_s"])),
                )
                return _make_result(
                    index,
                    "FAIL",
                    error_text,
                    to_addrs,
                    subject,
                    atts,
                    metrics,
                )

            if db_already_emailed_ok(
                conn,
                to_norm,
                subject,
                email_run_date,
                batch,
                attachment_key,
                fallback_hours,
            ):
                metrics["validation_s"] = (
                    time.perf_counter() - validation_started
                )
                _finalize_metrics(metrics, total_started)
                error_text = (
                    f"Already emailed within {fallback_hours} hours"
                )

                db_write_email_event(
                    conn,
                    email_run_id,
                    batch,
                    email_run_date,
                    master_path,
                    atts,
                    to_norm,
                    subject,
                    "SKIP",
                    f"{error_text} "
                    f"(rundate={email_run_date}, batch={batch})",
                    duration_s=int(round(metrics["total_s"])),
                )
                return _make_result(
                    index,
                    "SKIP",
                    error_text,
                    to_norm,
                    subject,
                    atts,
                    metrics,
                )

        else:
            write_log(
                f"Email {index + 1}: FORCE RESEND - "
                f"Bypassing refresh/duplicate validations"
            )

            # Even in force-resend mode, the attachment still must physically
            # exist because it must be opened and attached.
            missing = [str(p) for p in atts if not p.exists()]
            if missing:
                metrics["validation_s"] = (
                    time.perf_counter() - validation_started
                )
                _finalize_metrics(metrics, total_started)
                error_text = "Missing file: " + "; ".join(missing)

                db_write_email_event(
                    conn,
                    email_run_id,
                    batch,
                    email_run_date,
                    master_path,
                    atts,
                    to_norm,
                    subject,
                    "FAIL",
                    error_text,
                    duration_s=int(round(metrics["total_s"])),
                )
                return _make_result(
                    index,
                    "FAIL",
                    error_text,
                    to_norm,
                    subject,
                    atts,
                    metrics,
                )

        # v1.5 raw attachment safety check BEFORE reading full files into memory.
        try:
            raw_attachment_bytes = _get_total_raw_attachment_size(atts)
            metrics["attachment_bytes"] = raw_attachment_bytes
        except Exception as e:
            metrics["validation_s"] = (
                time.perf_counter() - validation_started
            )
            _finalize_metrics(metrics, total_started)
            error_text = f"Unable to read attachment size: {e}"

            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_norm,
                subject,
                "FAIL",
                error_text,
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index,
                "FAIL",
                error_text,
                to_norm,
                subject,
                atts,
                metrics,
            )

        raw_limit_bytes = _max_raw_attachment_bytes(config)
        if raw_limit_bytes and raw_attachment_bytes > raw_limit_bytes:
            metrics["validation_s"] = (
                time.perf_counter() - validation_started
            )
            _finalize_metrics(metrics, total_started)

            configured_mb = float(
                config.get("MAX_RAW_ATTACHMENT_MB", 0) or 0
            )
            error_text = (
                f"Raw attachment data {_format_size(raw_attachment_bytes)} "
                f"exceeds configured safety limit of "
                f"{configured_mb:.1f}MB"
            )

            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_norm,
                subject,
                "FAIL",
                error_text,
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index,
                "FAIL",
                error_text,
                to_norm,
                subject,
                atts,
                metrics,
            )

        metrics["validation_s"] = (
            time.perf_counter() - validation_started
        )

        # Build MIME message and read attachments from disk/network share.
        msg = EmailMessage()
        msg["From"] = config["FROM_USER"]
        msg["To"] = to_addrs

        if cc_addrs:
            msg["Cc"] = cc_addrs

        msg["Subject"] = subject
        msg.set_content(body_text)
        msg.add_alternative(
            f"<pre style='font-family: inherit; "
            f"white-space: pre-wrap'>{body_text}</pre>",
            subtype="html",
        )

        file_read_started = time.perf_counter()
        attachment_bytes = 0

        for p in atts:
            maintype, subtype = infer_mime(p)
            with open(p, "rb") as fh:
                data = fh.read()

            attachment_bytes += len(data)
            msg.add_attachment(
                data,
                maintype=maintype,
                subtype=subtype,
                filename=p.name,
            )

        metrics["file_read_s"] = (
            time.perf_counter() - file_read_started
        )
        metrics["attachment_bytes"] = attachment_bytes

        # v1.5: record actual serialized MIME size.
        # This is diagnostic only; it does not automatically reject the email.
        try:
            metrics["message_bytes"] = len(msg.as_bytes())
        except Exception as e:
            metrics["message_bytes"] = 0
            write_log(
                f"Email {index + 1}: WARNING - "
                f"Could not calculate MIME message size: {e}"
            )

        all_recipients = []
        for hdr in ["To", "Cc"]:
            val = msg.get(hdr)
            if val:
                all_recipients += [
                    a.strip() for a in val.split(",") if a.strip()
                ]

        if bcc_addrs:
            all_recipients += [
                a.strip()
                for a in bcc_addrs.split(",")
                if a.strip()
            ]

        if dry_run:
            _finalize_metrics(metrics, total_started)
            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_norm,
                subject,
                "SKIP",
                "DRYRUN",
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index,
                "SKIP",
                "DRYRUN",
                to_norm,
                subject,
                atts,
                metrics,
            )

        # Send through relay and time only the SMTP phase.
        smtp_started = time.perf_counter()

        try:
            smtp_attempts = send_via_google_relay(
                config, msg, all_recipients
            )
            metrics["smtp_s"] = (
                time.perf_counter() - smtp_started
            )
            metrics["smtp_attempts"] = smtp_attempts
            _finalize_metrics(metrics, total_started)

            status_msg = "OK (FORCED)" if force_resend else "OK"

            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_norm,
                subject,
                "OK",
                status_msg,
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index,
                "OK",
                status_msg,
                to_norm,
                subject,
                atts,
                metrics,
            )

        except Exception as e:
            metrics["smtp_s"] = (
                time.perf_counter() - smtp_started
            )
            metrics["smtp_attempts"] = int(
                getattr(e, "smtp_attempts", 0) or 0
            )
            _finalize_metrics(metrics, total_started)

            db_write_email_event(
                conn,
                email_run_id,
                batch,
                email_run_date,
                master_path,
                atts,
                to_norm,
                subject,
                "FAIL",
                str(e),
                duration_s=int(round(metrics["total_s"])),
            )
            return _make_result(
                index,
                "FAIL",
                str(e),
                to_norm,
                subject,
                atts,
                metrics,
            )

    finally:
        try:
            conn.close()
        except Exception:
            pass


def _format_size(num_bytes: int) -> str:
    size = max(0, int(num_bytes or 0))
    if size < 1024:
        return f"{size}B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f}KB"
    return f"{size / (1024 * 1024):.2f}MB"


def _format_performance(result: dict) -> str:
    """Compact timing/details appended to each main email result log."""
    if not CONFIG.get("LOG_PERFORMANCE_DETAILS", True):
        return ""

    m = result.get("metrics") or {}

    return (
        f" | Size={_format_size(m.get('attachment_bytes', 0))}"
        f" | MIME={_format_size(m.get('message_bytes', 0))}"
        f" | DBConnect={m.get('db_connect_s', 0.0):.2f}s"
        f" | Validate={m.get('validation_s', 0.0):.2f}s"
        f" | FileRead={m.get('file_read_s', 0.0):.2f}s"
        f" | SMTP={m.get('smtp_s', 0.0):.2f}s"
        f" | Attempts={int(m.get('smtp_attempts', 0) or 0)}"
        f" | Total={m.get('total_s', 0.0):.2f}s"
    )


# --------------------------- Args -----------------------------------

def parse_arguments():
    p = argparse.ArgumentParser(
        description="Send email reports for a batch (DB-backed)."
    )
    p.add_argument(
        "--batch",
        type=str,
        required=True,
        help="Batch identifier (for example 1, 1.1, or 1.2)",
    )
    p.add_argument(
        "--email-list",
        type=str,
        required=True,
        help="Path to Email_List.xlsx",
    )
    p.add_argument(
        "--email-date",
        type=str,
        default=None,
        help="YYYY-MM-DD for email run; default=TODAY",
    )
    p.add_argument(
        "--max-parallel",
        type=int,
        default=None,
        help="Max parallel email worker threads (default: CONFIG value)",
    )
    p.add_argument(
        "--fallback-hours",
        type=int,
        default=None,
        help="Hours to check for already sent emails (default: CONFIG value)",
    )
    p.add_argument(
        "--force-resend",
        action="store_true",
        help=(
            "Force resend all emails regardless of refresh status "
            "or previous sends"
        ),
    )
    return p.parse_args()


# --------------------------- Main -----------------------------------

def main():
    args = parse_arguments()
    run_started = time.perf_counter()

    BATCH_NUMBER = args.batch
    EMAIL_LIST_PATH = Path(args.email_list)

    if args.email_date:
        try:
            EMAIL_RUN_DATE = date.fromisoformat(args.email_date)
        except ValueError:
            write_log(
                f"ERROR: Invalid date '{args.email_date}'. "
                f"Use YYYY-MM-DD."
            )
            return 2
    else:
        EMAIL_RUN_DATE = date.today()

    LOG_DIR = Path(CONFIG["LOG_DIR"])
    BATCH = f"EmailBatch{BATCH_NUMBER}"
    CONFIG["BATCH"] = BATCH

    MASTER_PATH = CONFIG["MASTER_PATH"]
    FROM_USER = CONFIG["FROM_USER"]
    REQUIRE_METHOD_EMAIL = bool(CONFIG["REQUIRE_METHOD_EMAIL"])
    DRY_RUN = bool(CONFIG["DRY_RUN"])
    SMTP_SERVER = CONFIG["SMTP_SERVER"]
    SMTP_PORT = int(CONFIG["SMTP_PORT"])

    MAX_PARALLEL = (
        args.max_parallel
        if args.max_parallel is not None
        else CONFIG["MAX_PARALLEL"]
    )
    MAX_PARALLEL = max(1, int(MAX_PARALLEL))

    FALLBACK_HOURS = (
        args.fallback_hours
        if args.fallback_hours is not None
        else CONFIG["FALLBACK_HOURS"]
    )

    # v1.5 fix: CONFIG=True remains effective even when the CLI flag is absent.
    # CLI --force-resend can only turn it on, never accidentally turn CONFIG off.
    FORCE_RESEND = bool(CONFIG["FORCE_RESEND"]) or bool(
        args.force_resend
    )

    email_run_id = (
        f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}"
    )
    email_csv_out = (
        LOG_DIR
        / f"email-log_{EMAIL_RUN_DATE:%Y-%m-%d}_Batch-{BATCH_NUMBER}.csv"
    )

    if not FROM_USER or not SMTP_SERVER or not SMTP_PORT:
        write_log(
            "ERROR: Please set FROM_USER, SMTP_SERVER, "
            "and SMTP_PORT in CONFIG."
        )
        return 2

    # Load email list
    df_list = load_email_list(EMAIL_LIST_PATH)

    for col in [
        "Receiver",
        "CC",
        "BCC",
        "Subject",
        "Attachement Path",
    ]:
        if col not in df_list.columns:
            write_log(f"ERROR: Column '{col}' missing in List sheet.")
            return 2

    write_log(
        f"Processing Batch {BATCH_NUMBER} for {EMAIL_RUN_DATE} "
        f"| email_run_id={email_run_id}"
    )
    write_log(f"Email list: {EMAIL_LIST_PATH}")
    write_log(f"Email rows: {len(df_list)}")
    write_log(
        f"Parallel execution: {MAX_PARALLEL} worker threads"
    )
    write_log(
        f"SMTP relay: {SMTP_SERVER}:{SMTP_PORT} "
        f"(STARTTLS, IP-authenticated)"
    )
    write_log(
        f"SMTP reliability: "
        f"connect-timeout={CONFIG['SMTP_CONNECT_TIMEOUT']}s, "
        f"send-timeout={CONFIG['SMTP_SEND_TIMEOUT']}s, "
        f"health-check={CONFIG['SMTP_HEALTH_CHECK_TIMEOUT']}s, "
        f"retries={CONFIG['SMTP_MAX_RETRIES']}, "
        f"recycle-after="
        f"{CONFIG['SMTP_MAX_MESSAGES_PER_CONNECTION']} messages/"
        f"{CONFIG['SMTP_MAX_CONNECTION_AGE_SECONDS']}s"
    )
    write_log(
        f"Raw attachment safety limit: "
        f"{CONFIG['MAX_RAW_ATTACHMENT_MB']}MB "
        f"(0 disables)"
    )
    write_log(f"Fallback hours: {FALLBACK_HOURS} hours")
    write_log(f"Force resend: {FORCE_RESEND}")

    email_rows = [
        (row, idx) for idx, row in df_list.iterrows()
    ]

    total_ok = 0
    total_fail = 0
    total_skip = 0
    total_attachment_bytes = 0
    total_message_bytes = 0
    total_smtp_s = 0.0

    try:
        with ThreadPoolExecutor(
            max_workers=MAX_PARALLEL,
            thread_name_prefix="EmailWorker",
        ) as executor:

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
                    FORCE_RESEND,
                ): row_data[1]
                for row_data in email_rows
            }

            for future in as_completed(future_to_index):
                try:
                    result = future.result()
                    perf = _format_performance(result)
                    metrics = result.get("metrics") or {}

                    total_attachment_bytes += int(
                        metrics.get("attachment_bytes", 0) or 0
                    )
                    total_message_bytes += int(
                        metrics.get("message_bytes", 0) or 0
                    )
                    total_smtp_s += float(
                        metrics.get("smtp_s", 0.0) or 0.0
                    )

                    if result["status"] == "OK":
                        total_ok += 1
                        write_log(
                            f"Email {result['index'] + 1}: OK - "
                            f"To: {result['to_norm']}{perf}"
                        )

                    elif result["status"] == "FAIL":
                        total_fail += 1
                        write_log(
                            f"X Email {result['index'] + 1}: FAIL - "
                            f"{result['error']}{perf}"
                        )

                    elif result["status"] == "SKIP":
                        total_skip += 1
                        write_log(
                            f"- Email {result['index'] + 1}: SKIP - "
                            f"{result['error']}{perf}"
                        )

                except Exception as e:
                    total_fail += 1
                    write_log(
                        f"X Email {future_to_index[future] + 1}: "
                        f"EXCEPTION - {str(e)}"
                    )

    finally:
        close_all_smtp_connections()

    # Export this email run to CSV (IST timestamps)
    try:
        conn = db_connect()
        export_email_csv(conn, email_run_id, email_csv_out)
        write_log(f"Email CSV exported: {email_csv_out}")
        conn.close()

    except Exception as e:
        write_log(f"WARNING: CSV export failed: {e}")

    elapsed_s = max(
        0.001, time.perf_counter() - run_started
    )
    processed = total_ok + total_fail + total_skip
    emails_per_minute = (
        (processed / elapsed_s) * 60.0
        if processed
        else 0.0
    )

    write_log(
        f"Summary: OK={total_ok} FAIL={total_fail} "
        f"SKIP={total_skip}"
    )
    write_log(
        f"Run performance: "
        f"Elapsed={elapsed_s:.1f}s ({elapsed_s / 60:.2f} min) | "
        f"Processed={processed} | "
        f"Throughput={emails_per_minute:.2f} rows/min | "
        f"AttachmentData={_format_size(total_attachment_bytes)} | "
        f"MIMEData={_format_size(total_message_bytes)} | "
        f"SMTPTime(sum)={total_smtp_s:.1f}s"
    )

    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
