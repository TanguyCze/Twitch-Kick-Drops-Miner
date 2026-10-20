#!/usr/bin/env python3
"""
Twitch & Kick Drops Miner
Unified headless drops miner with Auto-Claim, Auto-Switch, Priority Queue, and Web UI.
"""

from __future__ import annotations

import argparse
import logging
import os
import site
import sys
from pathlib import Path

# Ensure local user site-packages (used by Pterodactyl Python eggs in /home/container/.local) are on sys.path
local_site = Path(__file__).resolve().parent / ".local" / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
if local_site.exists() and str(local_site) not in sys.path:
    site.addsitedir(str(local_site))

import uvicorn


def get_default_port() -> int:
    # Support Pterodactyl's SERVER_PORT environment variable automatically, defaulting to 8920
    for env_key in ("MINER_PORT", "SERVER_PORT", "PORT"):
        val = os.environ.get(env_key)
        if val and val.isdigit():
            return int(val)
    return 8920


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified Twitch & Kick Drops Miner with Web Dashboard"
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MINER_HOST", "0.0.0.0"),
        help="Host interface to bind the web interface (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=get_default_port(),
        help="Port for the web interface (default: 8920 or SERVER_PORT)",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("MINER_DATA_DIR", "data"),
        help="Directory to store configuration, credentials, and claim history (default: ./data)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("MINER_LOG_LEVEL", os.environ.get("LOG_LEVEL", "info")).lower(),
        choices=["debug", "info", "warning", "error"],
        help="Logging level (default: info)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    from web.server import create_app

    data_dir = Path(args.data_dir).resolve()
    app = create_app(data_dir=data_dir)

    print(
        f"\n"
        f"============================================================\n"
        f"  Twitch & Kick Drops Miner\n"
        f"  Web Interface : http://{args.host if args.host != '0.0.0.0' else 'localhost'}:{args.port}\n"
        f"  Bind Address  : {args.host}:{args.port}\n"
        f"  Data Directory: {data_dir}\n"
        f"============================================================\n",
        flush=True,
    )

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
