"""Trainer access control (trainer_app.security, routes settings), 2026-10-24. In-process HTTP
through Starlette's TestClient with a temp data dir: no server, no network.

  A  this PC (loopback): everything works without signing in, settings included
  B  another device: no cookie -> pages redirect to /login, API calls 401
  C  wrong key -> refused; 10 wrong tries -> locked out even with the right key
  D  right key -> HttpOnly SameSite=Strict cookie (an HMAC, never the key) -> drills work
  E  signed in remotely: settings changes, explorer loading and the access key stay PC-only (403)
  F  the Lichess token is write-only: GET never returns it; a blank PUT keeps it; "-" clears it
  G  a foreign Host header (DNS rebinding) -> 400; a cross-origin POST -> 403, even from loopback
  H  security headers on every response
"""
from __future__ import annotations

import sys
import tempfile
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore")
from starlette.testclient import TestClient  # noqa: E402

from trainer_app.routes import create_app  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        data = Path(td)
        (data / "pack").mkdir()
        app = create_app(data, remote=True)
        key = (data / "access_key.txt").read_text().strip()
        H = {"host": "127.0.0.1:8321"}
        local = TestClient(app, client=("127.0.0.1", 50000), headers=H)
        check("A: local settings GET", local.get("/api/settings").status_code == 200)
        check("A: local settings PUT", local.put("/api/settings", json={"new_per_day": 12}).status_code == 200)
        check("A: local can read the access key", local.get("/api/access").json().get("key") == key)

        RH = {"host": "192.168.1.20:8321"}
        phone = TestClient(app, client=("192.168.1.50", 50001), headers=RH, follow_redirects=False)
        r = phone.get("/")
        check("B: remote page without cookie -> /login", r.status_code == 303 and r.headers["location"] == "/login")
        check("B: remote API without cookie -> 401", phone.get("/api/settings").status_code == 401)
        check("B: the login page itself is reachable", phone.get("/login").status_code == 200)

        r = phone.post("/login", data={"key": "wrongkey1234"})
        check("C: wrong key refused", r.status_code == 401 and "trainer_auth" not in r.cookies)
        attacker = TestClient(app, client=("192.168.1.66", 50002), headers=RH, follow_redirects=False)
        for _ in range(10):
            attacker.post("/login", data={"key": "nope"})
        r = attacker.post("/login", data={"key": key})
        check("C: locked out after 10 wrong tries (even with the right key)",
              r.status_code == 401 and "Too many" in r.text)

        r = phone.post("/login", data={"key": key.upper()})
        sc = r.headers.get("set-cookie", "")
        check("D: right key (case-insensitive) -> cookie", r.status_code == 303 and "trainer_auth=" in sc)
        check("D: cookie is HttpOnly + SameSite=Strict", "httponly" in sc.lower() and "samesite=strict" in sc.lower())
        check("D: cookie is not the key", key not in sc)
        check("D: signed in -> API works", phone.get("/api/pack/info").status_code == 200)
        check("D: signed in -> settings readable", phone.get("/api/settings").status_code == 200)

        check("E: remote settings PUT -> 403", phone.put("/api/settings", json={"new_per_day": 99}).status_code == 403)
        check("E: remote explorer load -> 403", phone.post("/api/explorer/load").status_code == 403)
        check("E: remote cannot read the access key", phone.get("/api/access").status_code == 403)

        local.put("/api/settings", json={"lichess_token": "lip_secret123"})
        s = local.get("/api/settings").json()
        check("F: token never returned", s["lichess_token"] == "" and s["lichess_token_set"] is True)
        check("F: token not in the remote view either", "lip_secret" not in phone.get("/api/settings").text)
        local.put("/api/settings", json={"lichess_token": "", "new_per_day": 11})
        check("F: blank PUT keeps the token", local.get("/api/settings").json()["lichess_token_set"] is True)
        local.put("/api/settings", json={"lichess_token": "-"})
        check("F: '-' clears it", local.get("/api/settings").json()["lichess_token_set"] is False)

        evil = TestClient(app, client=("127.0.0.1", 50003), headers={"host": "evil.example.com:8321"})
        check("G: foreign Host header -> 400", evil.get("/api/settings").status_code == 400)
        r = local.put("/api/settings", json={"new_per_day": 13}, headers={"origin": "http://evil.example.com"})
        check("G: cross-origin PUT from loopback -> 403", r.status_code == 403)
        r = local.put("/api/settings", json={"new_per_day": 13}, headers={"origin": "http://127.0.0.1:8321"})
        check("G: same-origin PUT allowed", r.status_code == 200)

        r = local.get("/api/settings")
        check("H: security headers", r.headers.get("x-frame-options") == "DENY"
              and r.headers.get("x-content-type-options") == "nosniff"
              and r.headers.get("referrer-policy") == "no-referrer")
        check("H: API docs disabled", local.get("/docs").status_code == 404)
        local.close()
        phone.close()
        attacker.close()
        evil.close()
        try:
            app.state.con.close()
        except Exception:
            pass
    print("\nALL PASS" if not FAIL else "\nFAILURES")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
