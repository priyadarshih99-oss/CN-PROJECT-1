"""
Discovery Tracker Runner.

Starts the central discovery node that allows peers to register, heartbeat,
and query for available peers in the swarm.

Usage:
    python run_tracker.py --port 8000
"""

import argparse
import sys
import time
from src.discovery import DiscoveryTracker
from src.utils import (
    DEFAULT_HOST,
    DEFAULT_TRACKER_PORT,
    DEFAULT_PEER_TIMEOUT,
    validate_address,
)


def main():
    # CLI Argument Configuration (Member 1 Manual Scaffolding)
    parser = argparse.ArgumentParser(description="P2P Discovery Tracker Node")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help=f"Host IP to bind (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_TRACKER_PORT, help=f"Port to listen on (default: {DEFAULT_TRACKER_PORT})")
    parser.add_argument("--timeout", type=float, default=DEFAULT_PEER_TIMEOUT, help=f"Peer timeout in seconds (default: {DEFAULT_PEER_TIMEOUT})")

    args = parser.parse_args()

    # Address validation sanity check
    if not validate_address(args.host, args.port):
        print(f"[ERROR] Invalid bind address specified: {args.host}:{args.port}")
        sys.exit(1)

    tracker = DiscoveryTracker(host=args.host, port=args.port, peer_timeout=args.timeout)
    tracker.start()

    print(f"\n=======================================================")
    print(f" P2P Discovery Tracker Node Active on {args.host}:{args.port}")
    print(f" Peer Timeout Threshold: {args.timeout}s")
    print(f" Press Ctrl+C to shut down.")
    print(f"=======================================================\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping tracker...")
        tracker.stop()


if __name__ == "__main__":
    main()
