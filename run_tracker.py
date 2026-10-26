"""
Discovery Tracker Runner.

Starts the central discovery node that allows peers to register, heartbeat,
and query for available peers in the swarm.

Usage:
    python run_tracker.py --port 8000
"""

import argparse
import time
from src.discovery import DiscoveryTracker


def main():
    parser = argparse.ArgumentParser(description="P2P Discovery Tracker Node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host IP to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Peer timeout in seconds (default: 30.0)")

    args = parser.parse_args()

    tracker = DiscoveryTracker(host=args.host, port=args.port, peer_timeout=args.timeout)
    tracker.start()

    print(f"\n=======================================================")
    print(f" P2P Discovery Tracker Node Active on {args.host}:{args.port}")
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
