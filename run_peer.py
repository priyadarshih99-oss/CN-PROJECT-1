"""
Interactive Peer Node Runner.

Starts a P2P node with TCP socket communication, tracker discovery,
and an interactive command prompt to test network messaging locally.

Usage:
    python run_peer.py --id Peer-1 --port 5001 --tracker-port 8000
    python run_peer.py --id Peer-2 --port 5002 --tracker-port 8000
"""

import argparse
import sys
import uuid
from src.peer import Peer
from src.protocol import MessageType


def print_help():
    print("""
Available Commands:
  discover                  Query tracker for active peers in the network
  autoconnect               Query tracker and automatically connect to all discovered peers
  connect <host> <port>     Directly connect to another peer over TCP socket
  peers                     List all currently active TCP peer connections
  msg <peer_id> <text>      Send a direct TCP message to a connected peer
  broadcast <text>          Broadcast a message to all connected peers
  ping <peer_id>            Send a ping to measure round-trip latency
  status                    Display local peer ID and listening socket info
  help                      Show this help menu
  exit / quit               Gracefully disconnect and exit
""")


def main():
    parser = argparse.ArgumentParser(description="P2P File Distribution Peer Node")
    parser.add_argument("--id", type=str, default=None, help="Unique Peer ID (default: Peer-<short_uuid>)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host IP to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, required=True, help="Port to listen for peer connections (e.g., 5001)")
    parser.add_argument("--tracker-host", type=str, default="127.0.0.1", help="Tracker host IP (default: 127.0.0.1)")
    parser.add_argument("--tracker-port", type=int, default=8000, help="Tracker port (default: 8000, 0 to disable)")
    parser.add_argument("--connect", type=str, default=None, help="Comma-separated host:port list to connect directly")

    args = parser.parse_args()

    peer_id = args.id if args.id else f"Peer-{args.port}"
    tracker_port = args.tracker_port if args.tracker_port > 0 else None
    tracker_host = args.tracker_host if tracker_port else None

    peer = Peer(
        peer_id=peer_id,
        host=args.host,
        port=args.port,
        tracker_host=tracker_host,
        tracker_port=tracker_port,
    )

    peer.start()

    # If direct seed connections specified
    if args.connect:
        for target in args.connect.split(","):
            target = target.strip()
            if ":" in target:
                h, p = target.split(":")
                peer.connect_to_peer(h, int(p))

    print(f"\n=======================================================")
    print(f" P2P Node [{peer_id}] Online on TCP {args.host}:{args.port}")
    print(f" Tracker: {tracker_host}:{tracker_port if tracker_port else 'Disabled'}")
    print(f" Type 'help' for available commands, or 'exit' to quit.")
    print(f"=======================================================\n")

    try:
        while True:
            try:
                line = input(f"[{peer_id}]> ").strip()
            except EOFError:
                break

            if not line:
                continue

            parts = line.split(maxsplit=2)
            cmd = parts[0].lower()

            if cmd in ("exit", "quit"):
                break

            elif cmd == "help":
                print_help()

            elif cmd == "status":
                print(f"Peer ID: {peer.peer_id}")
                print(f"Listening: {peer.host}:{peer.port}")
                print(f"Connected Peers: {len(peer.connections)}")

            elif cmd == "discover":
                peers = peer.discover_peers()
                if not peers:
                    print("No other peers discovered on tracker.")
                else:
                    print(f"Discovered {len(peers)} peer(s):")
                    for p in peers:
                        print(f" - {p['peer_id']} at {p['host']}:{p['port']}")

            elif cmd == "autoconnect":
                count = peer.connect_to_discovered_peers()
                print(f"Connected to {count} newly discovered peer(s).")

            elif cmd == "connect":
                if len(parts) < 3:
                    print("Usage: connect <host> <port>")
                    continue
                chost = parts[1]
                cport = int(parts[2])
                conn = peer.connect_to_peer(chost, cport)
                if conn:
                    print(f"Connected to peer at {chost}:{cport} (Peer ID: {conn.peer_id})")
                else:
                    print(f"Failed to connect to {chost}:{cport}")

            elif cmd == "peers":
                if not peer.connections:
                    print("No active TCP peer connections.")
                else:
                    print(f"Active TCP Peer Connections ({len(peer.connections)}):")
                    for pid, conn in peer.connections.items():
                        status = "Active" if conn.is_alive else "Dead"
                        print(f" - [{pid}] at {conn.host}:{conn.port} ({status})")

            elif cmd == "msg":
                if len(parts) < 3:
                    print("Usage: msg <peer_id> <text>")
                    continue
                target_id = parts[1]
                text = parts[2]
                sent = peer.send_message(target_id, MessageType.CHAT_MSG, {"text": text})
                if sent:
                    print(f"Message sent to [{target_id}].")
                else:
                    print(f"Failed to send message to [{target_id}]. Check if connected.")

            elif cmd == "broadcast":
                if len(parts) < 2:
                    print("Usage: broadcast <text>")
                    continue
                text = parts[1] if len(parts) == 2 else f"{parts[1]} {parts[2]}"
                sent_count = peer.broadcast_message(MessageType.CHAT_MSG, {"text": text})
                print(f"Broadcasted to {sent_count} peer(s).")

            elif cmd == "ping":
                if len(parts) < 2:
                    print("Usage: ping <peer_id>")
                    continue
                target_id = parts[1]
                if peer.ping_peer(target_id):
                    print(f"Ping sent to [{target_id}]. Awaiting PONG response...")
                else:
                    print(f"Failed to ping [{target_id}]. Peer not connected.")

            else:
                print(f"Unknown command: '{cmd}'. Type 'help' for commands.")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        peer.stop()
        print("Goodbye!")


if __name__ == "__main__":
    main()
