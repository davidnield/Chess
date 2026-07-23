"""Entry point: python -m trainer_app [--data DIR] [--port N] [--host H] [--no-browser]"""

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
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    app = create_app(data_dir)

    url = f"http://{args.host}:{args.port}"
    print(f"Opening Trainer | data: {data_dir} | {url}")
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
