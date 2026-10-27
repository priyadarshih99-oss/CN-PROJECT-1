"""
Interactive Peer Node Runner.

Starts a P2P node with TCP socket communication, tracker discovery,
and an interactive command prompt to test network messaging locally.

Usage:
    python run_peer.py --id Peer-1 --port 5001 --tracker-port 8000
    python run_peer.py --id Peer-2 --port 5002 --tracker-port 8000
"""

import argparse
import os
import sys
import uuid
from src.peer import Peer
from src.protocol import MessageType
from src.transfer import TransferManager
from src.verification import PieceVerifier


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

  --- File Transfer & Verification Commands ---
  share <filepath>          Register and seed a file (splits pieces & computes SHA-256)
  download <file_id> <out_path> <h0,h1,...>
                            Download file pieces in parallel from connected peers
  cancel <file_id>          Cancel an ongoing download
  stats                     View upload & download transfer statistics
  verify <filepath> <hash>  Cryptographically verify file integrity with SHA-256

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
    transfer_mgr = TransferManager(peer)

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

            parts = line.split(maxsplit=3)
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
                text = " ".join(parts[1:])
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

            # --- File Transfer & Verification Commands ---
            elif cmd == "share":
                if len(parts) < 2:
                    print("Usage: share <filepath>")
                    continue
                file_path = parts[1]
                try:
                    shared = transfer_mgr.share_file(file_path)
                    print(f"Successfully shared file [{shared.file_id}]:")
                    print(f"  - Size: {shared.file_size} bytes")
                    print(f"  - Pieces: {shared.total_pieces} (Piece size: {shared.piece_size} bytes)")
                    print("  - Piece Hashes (SHA-256):")
                    for i, h in enumerate(shared.piece_hashes):
                        print(f"      Piece {i}: {h}")
                    print(f"\n  To download this on another peer, use:")
                    print(f"  download {shared.file_id} output_{os.path.basename(file_path)} {','.join(shared.piece_hashes)}")
                except Exception as e:
                    print(f"Error sharing file: {e}")

            elif cmd == "download":
                if len(parts) < 4:
                    print("Usage: download <file_id> <output_path> <hash0,hash1,...>")
                    continue
                file_id = parts[1]
                out_path = parts[2]
                hashes = [h.strip() for h in parts[3].split(",") if h.strip()]

                if not hashes:
                    print("Error: At least one piece hash must be provided.")
                    continue

                print(f"Starting parallel download for [{file_id}] ({len(hashes)} piece(s))...")

                def progress_cb(verified, total):
                    pct = (verified / total) * 100
                    print(f"  -> Progress: {verified}/{total} verified ({pct:.1f}%)")

                result = transfer_mgr.download(
                    file_id=file_id,
                    piece_hashes=hashes,
                    output_path=out_path,
                    progress_callback=progress_cb,
                )

                if result.success:
                    print(f"\n Download Complete!")
                    print(f"  - Destination: {result.output_path}")
                    print(f"  - Total Bytes: {result.total_bytes}")
                    print(f"  - Elapsed: {result.elapsed_time:.2f}s")
                    print(f"  - Speed: {result.throughput_kbps:.2f} KB/s")
                    print(f"  - Retries: {result.retry_count}")
                else:
                    print(f"\n Download Failed: {result.error_message}")
                    print(f"  - Verified pieces: {result.verified_pieces}/{result.total_pieces}")
                    print(f"  - Retries attempted: {result.retry_count}")

            elif cmd == "cancel":
                if len(parts) < 2:
                    print("Usage: cancel <file_id>")
                    continue
                transfer_mgr.cancel_download(parts[1])
                print(f"Cancelled transfer for [{parts[1]}].")

            elif cmd == "stats":
                stats = transfer_mgr.get_stats()
                print("Transfer Statistics:")
                print(f"  - Shared Files: {stats['shared_files']}")
                print(f"  - Pieces Served: {stats['total_pieces_served']}")
                print(f"  - Total Bytes Uploaded: {stats['total_bytes_uploaded']}")

            elif cmd == "verify":
                if len(parts) < 3:
                    print("Usage: verify <filepath> <expected_sha256_hash>")
                    continue
                filepath = parts[1]
                expected_h = parts[2]
                valid = PieceVerifier.verify_file(filepath, expected_h)
                if valid:
                    print(f"[VALID] File integrity verified successfully!")
                else:
                    print(f"[CORRUPT/MISMATCH] File does not match expected hash!")

            else:
                print(f"Unknown command: '{cmd}'. Type 'help' for commands.")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        transfer_mgr.stop()
        peer.stop()
        print("Goodbye!")



if __name__ == "__main__":
    main()
