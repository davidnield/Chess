"""Access control for using the trainer from other devices on the home network (2026-10-24).

The server binds to 127.0.0.1 unless started with --remote. With --remote it listens on the LAN,
and every request that does not come from this PC itself must carry a session cookie obtained by
entering the access key once on /login (trainer_data/access_key.txt; delete it, and
session_secret.bin, to rotate the key and sign every device out).

Always on, local or remote:
  * Host header must name this machine (localhost, a private-range IP or this PC's hostname):
    blocks DNS-rebinding pages from reaching the server through a browser.
  * State-changing requests (POST/PUT/PATCH/DELETE) carrying an Origin/Referer must come from the
    trainer's own origin: blocks other websites from driving it (cross-site request forgery).
  * Security headers (no framing, no MIME sniffing, no referrer).
Remote requests additionally:
  * need the session cookie (HttpOnly, SameSite=Strict, an HMAC of the key, never the key);
    10 wrong keys from one address lock it out for 10 minutes;
  * cannot change settings or file paths, load the explorer data, or read the access key
    (ADMIN_ONLY) -- those stay on this PC.
The Lichess token is write-only for everyone (routes.py never returns it).
"""
from __future__ import annotations

import hashlib
import hmac
import ipaddress
import secrets
import socket
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

COOKIE = "trainer_auth"
ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"       # no 0/o, 1/l/i: easy to type on a phone
ADMIN_ONLY = {("PUT", "/api/settings"), ("POST", "/api/explorer/load"), ("GET", "/api/access")}
LOCKOUT_TRIES, LOCKOUT_SECS = 10, 600
LOGIN_PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Opening Trainer</title>
<style>body{{font-family:system-ui,sans-serif;max-width:22rem;margin:4rem auto;padding:0 1rem}}
input,button{{font-size:1.1rem;padding:.5rem;width:100%;box-sizing:border-box;margin-top:.5rem}}
.err{{color:#b00}}</style></head><body><h2>Opening Trainer</h2>
<p>Enter the access key shown on the PC (Settings &rarr; Remote access).</p>
<form method="post" action="/login"><input name="key" autocomplete="off" autocapitalize="none"
autofocus><button type="submit">Sign in</button></form><p class="err">{msg}</p></body></html>"""


def is_loopback(host: str | None) -> bool:
    try:
        return host is not None and ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class Access:
    def __init__(self, data_dir: Path, remote: bool):
        self.remote = remote
        kf, sf = data_dir / "access_key.txt", data_dir / "session_secret.bin"
        if not kf.exists():
            kf.write_text("".join(secrets.choice(ALPHABET) for _ in range(12)), encoding="utf-8")
        if not sf.exists():
            sf.write_bytes(secrets.token_bytes(32))
        self.key = kf.read_text(encoding="utf-8").strip().lower()
        self.token = hmac.new(sf.read_bytes(), b"trainer-session:" + self.key.encode(),
                              hashlib.sha256).hexdigest()
        self.fails: dict[str, list] = {}
        self.port = 8321
        self.names = {"localhost", socket.gethostname().lower(), socket.gethostname().lower() + ".local"}

    # -- checks ------------------------------------------------------------------------------
    def host_ok(self, host_header: str | None) -> bool:
        if not host_header:
            return False
        h = host_header.rsplit(":", 1)[0] if not host_header.endswith("]") else host_header
        h = h.strip("[]").lower()
        if h in self.names:
            return True
        try:
            ip = ipaddress.ip_address(h)
            return ip.is_loopback or ip.is_private
        except ValueError:
            return False

    @staticmethod
    def same_origin(request: Request) -> bool:
        src = request.headers.get("origin") or request.headers.get("referer")
        if not src or src == "null":
            return src != "null"
        return urlparse(src).netloc.lower() == (request.headers.get("host") or "").lower()

    def cookie_ok(self, request: Request) -> bool:
        return hmac.compare_digest(request.cookies.get(COOKIE, ""), self.token)

    def try_key(self, ip: str, key: str) -> str | None:
        """None on success, else the error message."""
        n, since = self.fails.get(ip, [0, time.time()])
        if time.time() - since > LOCKOUT_SECS:
            n, since = 0, time.time()
        if n >= LOCKOUT_TRIES:
            return "Too many attempts; try again in 10 minutes."
        if hmac.compare_digest(key.strip().lower().encode(), self.key.encode()):
            self.fails.pop(ip, None)
            return None
        self.fails[ip] = [n + 1, since]
        return "Wrong access key."

    # -- wiring ------------------------------------------------------------------------------
    def install(self, app) -> None:
        access = self

        @app.middleware("http")
        async def guard(request: Request, call_next):
            if not access.host_ok(request.headers.get("host")):
                return JSONResponse({"detail": "unexpected Host header"}, status_code=400)
            if request.method in ("POST", "PUT", "PATCH", "DELETE") and not access.same_origin(request):
                return JSONResponse({"detail": "cross-origin request refused"}, status_code=403)
            local = is_loopback(request.client.host if request.client else None)
            path = request.url.path
            if not local and path != "/login":
                if not access.cookie_ok(request):
                    if path.startswith("/api/"):
                        return JSONResponse({"detail": "sign in first"}, status_code=401)
                    return RedirectResponse("/login", status_code=303)
                if (request.method, path) in ADMIN_ONLY:
                    return JSONResponse({"detail": "only available on the PC running the trainer"},
                                        status_code=403)
            resp = await call_next(request)
            resp.headers["X-Frame-Options"] = "DENY"
            resp.headers["X-Content-Type-Options"] = "nosniff"
            resp.headers["Referrer-Policy"] = "no-referrer"
            return resp

        @app.get("/login")
        def login_page():
            return HTMLResponse(LOGIN_PAGE.format(msg=""))

        @app.post("/login")
        async def login(request: Request):
            form = parse_qs((await request.body()).decode("utf-8", "replace"))
            ip = request.client.host if request.client else "?"
            err = access.try_key(ip, (form.get("key") or [""])[0])
            if err:
                return HTMLResponse(LOGIN_PAGE.format(msg=err), status_code=401)
            resp = RedirectResponse("/", status_code=303)
            resp.set_cookie(COOKIE, access.token, max_age=365 * 86400, httponly=True,
                            samesite="strict", path="/")
            return resp

        @app.get("/api/access")
        def access_info():
            return {"remote": access.remote, "key": access.key, "urls": lan_urls(access.port)}


def lan_urls(port: int = 8321) -> list[str]:
    """http://<private IPv4>:port for this PC's network interfaces."""
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ipaddress.ip_address(ip).is_private and not ipaddress.ip_address(ip).is_loopback:
                ips.add(ip)
    except OSError:
        pass
    return sorted(f"http://{ip}:{port}" for ip in ips)
