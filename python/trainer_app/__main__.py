"""Entry point: python -m trainer_app [--data DIR] [--port N] [--host H] [--remote] [--no-browser]

--remote listens on the home network (0.0.0.0) for phone/laptop access, behind the access key
(trainer_app.security). Without it the server is reachable from this PC only."""

from __future__ import annotations

import argparse
import threading
import webbrowser

import uvicorn

from .config import resolve_data_dir
from .routes import create_app


def main() -> None:
    ap = argparse.ArgumentParser(description="Local opening-training app")
    ap.add_argument("--data", default=None, help="Data dir (default: TRAINER_DATA env "
                                                 "or <repo>/trainer_data)")
    ap.add_argument("--port", type=int, default=8321)
    ap.add_argument("--host", default=None, help="default 127.0.0.1; 0.0.0.0 with --remote")
    ap.add_argument("--remote", action="store_true",
                    help="serve the home network too (access key required off this PC)")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    host = args.host or ("0.0.0.0" if args.remote else "127.0.0.1")
    app = create_app(data_dir, remote=args.remote, port=args.port)

    url = f"http://127.0.0.1:{args.port}"
    print(f"Opening Trainer | data: {data_dir} | {url}")
    if args.remote:
        from .security import lan_urls
        key = (data_dir / "access_key.txt").read_text(encoding="utf-8").strip()
        print("Remote access ON (home network). On your phone/laptop open:")
        for u in lan_urls(args.port) or ["http://<this PC's IP>:%d" % args.port]:
            print(f"   {u}")
        print(f"Access key: {key}")
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    uvicorn.run(app, host=host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
