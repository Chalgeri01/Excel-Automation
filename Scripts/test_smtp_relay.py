import smtplib
import ssl
from email.message import EmailMessage

SMTP_SERVER = "smtp-relay.gmail.com"
SMTP_PORT = 587

FROM_EMAIL = "report@kotharigroupindia.com"
TO_EMAIL = "chalgeri.prakash@gmail.com"

msg = EmailMessage()
msg["From"] = FROM_EMAIL
msg["To"] = TO_EMAIL
msg["Subject"] = "Google Workspace SMTP Relay Test"

msg.set_content("""
Hello,

This is a test email sent through Google Workspace SMTP Relay.

Regards,
Report Automation
""")

context = ssl.create_default_context()

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as smtp:

    smtp.ehlo()

    smtp.starttls(context=context)

    smtp.ehlo()

    smtp.send_message(
        msg,
        from_addr=FROM_EMAIL,
        to_addrs=[TO_EMAIL]
    )

print("Email successfully submitted to Google SMTP Relay.")