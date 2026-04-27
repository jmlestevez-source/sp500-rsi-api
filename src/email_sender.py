# src/email_sender.py
"""
Envío del email report via Gmail SMTP.
Usa EMAIL_USERNAME y EMAIL_PASSWORD de los secrets.
"""

import os
import json
import smtplib
from pathlib import Path
from email.mime.text      import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime             import datetime


REPORT_PATH = Path("data/email_report.json")


def load_report() -> dict:
    """Carga el report generado."""
    if not REPORT_PATH.exists():
        raise FileNotFoundError(
            f"No existe {REPORT_PATH}"
        )
    return json.load(open(REPORT_PATH, encoding="utf-8"))


def send_email(
    subject:   str | None = None,
    body_html: str | None = None,
    to_email:  str | None = None,
) -> bool:
    """
    Envía el email report via Gmail SMTP.
    
    Variables de entorno requeridas:
      EMAIL_USERNAME  → tu cuenta Gmail
      EMAIL_PASSWORD  → App Password de Gmail
    """
    username = os.getenv("EMAIL_USERNAME")
    password = os.getenv("EMAIL_PASSWORD")

    if not username or not password:
        print(
            "  ⚠ EMAIL_USERNAME o EMAIL_PASSWORD "
            "no configurados."
        )
        print(
            "  Añádelos en GitHub Secrets para "
            "recibir el report por email."
        )
        return False

    # Cargar report si no se pasan directamente
    if subject is None or body_html is None:
        report    = load_report()
        subject   = report["subject"]
        body_html = report["body"]

    # Destinatario: usar EMAIL_TO si existe,
    # sino enviar al mismo remitente
    if to_email is None:
        to_email = os.getenv("EMAIL_TO", username)

    print(f"  De: {username}")
    print(f"  Para: {to_email}")
    print(f"  Asunto: {subject[:60]}...")

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = username
        msg["To"]      = to_email

        # Parte texto plano (fallback)
        text_plain = (
            "Este email requiere un cliente "
            "que soporte HTML.\n\n"
            "Puedes ver el report en: "
            "data/email_report.json"
        )
        msg.attach(MIMEText(text_plain, "plain", "utf-8"))
        msg.attach(MIMEText(body_html,  "html",  "utf-8"))

        # Conectar y enviar
        with smtplib.SMTP_SSL(
            "smtp.gmail.com", 465, timeout=30
        ) as server:
            server.login(username, password)
            server.sendmail(
                username,
                to_email,
                msg.as_string(),
            )

        print(f"  ✓ Email enviado correctamente")
        _log_send(to_email, subject)
        return True

    except smtplib.SMTPAuthenticationError as e:
        print(f"  ✗ Error de autenticación Gmail: {e}")
        print(
            "  Verifica que EMAIL_PASSWORD sea un "
            "App Password, no tu contraseña normal."
        )
        return False

    except smtplib.SMTPException as e:
        print(f"  ✗ Error SMTP: {e}")
        return False

    except Exception as e:
        print(f"  ✗ Error enviando email: {e}")
        return False


def _log_send(to_email: str, subject: str) -> None:
    """Registra el envío en data/email_log.json"""
    log_path = Path("data/email_log.json")
    log      = []

    if log_path.exists():
        try:
            log = json.load(open(log_path))
        except Exception:
            log = []

    log.append({
        "timestamp": datetime.now().isoformat(),
        "to":        to_email,
        "subject":   subject,
        "status":    "sent",
    })

    log = log[-50:]  # Mantener últimos 50

    with open(log_path, "w") as f:
        json.dump(log, f, indent=2)


# Alias para compatibilidad
send_report = send_email
