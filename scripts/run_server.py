#!/usr/bin/env python3
"""Run the Kite auth web server."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kite_wrapper.web_app import run_server


def main():
    parser = argparse.ArgumentParser(description="Run Kite auth web server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind (default: 5000)")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    print(f"Starting Kite auth server on http://{args.host}:{args.port}")
    print("Register this as redirect URL in Kite developer console:")
    print(f"  http://YOUR_SERVER_IP:{args.port}/callback")
    print()
    run_server(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
