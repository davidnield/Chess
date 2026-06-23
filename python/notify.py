"""
Pipeline email notifications via SMTP — stdlib-only, no extra deps.

Reads credentials from OUTSIDE the repo so they never end up in git:
  • Default config path: %USERPROFILE%\\.chess_pipeline_notify.json   (~/...)
  • Override path: env var CHESS_NOTIFY_CONFIG=C:/path/to/config.json
  • Or skip the file entirely and set env vars directly:
        CHESS_NOTIFY_USERNAME    SMTP login (e.g. me@gmail.com)
        CHESS_NOTIFY_PASSWORD    SMTP app password
        CHESS_NOTIFY_TO          recipient address
        CHESS_NOTIFY_FROM        optional, defaults to USERNAME
        CHESS_NOTIFY_SMTP_HOST   optional, defaults to smtp.gmail.com
        CHESS_NOTIFY_SMTP_PORT   optional, defaults to 587

Config file format (JSON):
    {
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "username":  "you@gmail.com",
      "password":  "xxxx xxxx xxxx xxxx",      // Gmail App Password (16 chars)
      "to_email":  "you@gmail.com",
      "from_email":"you@gmail.com"             // optional
    }

Gmail setup (one-time):
  1. Enable 2-factor auth on the Google account
  2. Visit https://myaccount.google.com/apppasswords
  3. Generate an App Password named "chess pipeline"
  4. Paste the 16-char password into the config file under "password"

Usage:
    from notify import send
    send("Stage 1 complete: 2024 Mar", "Files: 48\\nSize: 45.2 GB")

If no config is found, send() silently returns False — the pipeline keeps
running. Failures during send (network, auth) are caught, logged, and don't
crash the caller. Notifications are best-effort, not load-bearing.
"""

from __future__ import annotations

import json
import os
import smtplib
import ssl
import sys
import time
import traceback
from email.message import EmailMessage
from pathlib import Path

# Match project convention: ensure UTF-8 stdout so unicode in logs/errors
# doesn't crash on Windows cp1252.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_CONFIG_PATH = Path.home() / ".chess_pipeline_notify.json"

# Module-level cache so we don't re-parse JSON on every call
_CACHED_CONFIG: dict | None = None
_CONFIG_LOADED = False


def _load_config() -> dict | None:
    """Resolve config from env vars or JSON file. None if unconfigured."""
    global _CACHED_CONFIG, _CONFIG_LOADED
    if _CONFIG_LOADED:
        return _CACHED_CONFIG
    _CONFIG_LOADED = True

    # Env vars take precedence
    env_user = os.environ.get("CHESS_NOTIFY_USERNAME")
    env_pwd  = os.environ.get("CHESS_NOTIFY_PASSWORD")
    env_to   = os.environ.get("CHESS_NOTIFY_TO")
    if env_user and env_pwd and env_to:
        _CACHED_CONFIG = {
            "smtp_host":  os.environ.get("CHESS_NOTIFY_SMTP_HOST", "smtp.gmail.com"),
            "smtp_port":  int(os.environ.get("CHESS_NOTIFY_SMTP_PORT", "587")),
            "username":   env_user,
            "password":   env_pwd,
            "to_email":   env_to,
            "from_email": os.environ.get("CHESS_NOTIFY_FROM", env_user),
        }
        return _CACHED_CONFIG

    # Then JSON file
    cfg_path = Path(os.environ.get("CHESS_NOTIFY_CONFIG", str(DEFAULT_CONFIG_PATH)))
    if not cfg_path.exists():
        return None
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[notify] Failed to parse {cfg_path}: {e}", file=sys.stderr)
        return None

    required = ("smtp_host", "smtp_port", "username", "password", "to_email")
    missing = [k for k in required if k not in data]
    if missing:
        print(f"[notify] Config {cfg_path} missing keys: {missing}", file=sys.stderr)
        return None

    data.setdefault("from_email", data["username"])
    _CACHED_CONFIG = data
    return _CACHED_CONFIG


def _redact_user(addr: str) -> str:
    """Show 'd***@gmail.com' instead of 'david@gmail.com' in logs."""
    if "@" not in addr:
        return "***"
    local, domain = addr.split("@", 1)
    if not local:
        return f"@{domain}"
    return f"{local[0]}***@{domain}"


def send(subject: str, body: str, *, max_retries: int = 2) -> bool:
    """
    Send a plain-text email. Returns True on success, False otherwise.
    Never raises — notification failures must not break the pipeline.
    """
    cfg = _load_config()
    if cfg is None:
        # Unconfigured; this is a deliberate soft-fail so the pipeline keeps going.
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from_email"]
    msg["To"] = cfg["to_email"]
    msg.set_content(body, charset="utf-8")

    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        try:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=30) as s:
                s.ehlo()
                s.starttls(context=ctx)
                s.ehlo()
                s.login(cfg["username"], cfg["password"])
                s.send_message(msg)
            return True
        except (smtplib.SMTPException, OSError) as e:
            print(f"[notify] Attempt {attempt} failed: {e!r}", file=sys.stderr)
            if attempt > max_retries:
                traceback.print_exc(file=sys.stderr)
                return False
            time.sleep(5 * attempt)
        except Exception as e:
            # Anything else: log and bail (don't retry)
            print(f"[notify] Unexpected error: {e!r}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            return False

    return False


def is_configured() -> bool:
    """True if credentials are available and look complete."""
    return _load_config() is not None


def status_string() -> str:
    """Short string summarizing whether notifications will work (for startup logs)."""
    cfg = _load_config()
    if cfg is None:
        return "notifications: DISABLED (no config found)"
    return (f"notifications: enabled ({_redact_user(cfg['from_email'])} → "
            f"{_redact_user(cfg['to_email'])} via {cfg['smtp_host']})")


# CLI entrypoint for testing: python -m notify "subject" "body"
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python notify.py <subject> <body>")
        print()
        print(status_string())
        sys.exit(1)
    print(status_string())
    ok = send(sys.argv[1], sys.argv[2])
    print(f"send() → {ok}")
    sys.exit(0 if ok else 2)
