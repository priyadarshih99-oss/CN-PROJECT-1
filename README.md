# Peer-to-Peer (P2P) File Distribution System
**Computer Networks · Coding Assignment 1**  
**Section:** CSE-14 · **Path:** A (Catalogue Project)

---

## Team & Work Split
- **Member 1 (Harsh Priyadarshi - 241551011):** Peer initialization, peer discovery, and TCP socket communication. *(Completed)*
- **Member 2 (Sanket Panda - 2405833):** File metadata management, file splitting, and piece-request logic. *(Completed)*
- **Member 3 (Arnav Raj - 24156006):** Parallel upload/download, piece verification, and failure handling.
- **Member 4 (Udbhav Singh - 241551102):** Performance measurement, visualization, experiment analysis, integration, and documentation.

---

## Member 1 Deliverables Overview

Member 1 establishes the core distributed networking foundation upon which the remaining file transfer and evaluation logic is built:

1. **Peer Initialization (`src/peer.py`):**
   - Configures each peer with a unique `peer_id`, listening IP, and listening TCP port.
   - Multithreaded TCP server accepting simultaneous incoming peer connections.
   - Clean shutdown handler that notifies peers and unregisters gracefully.

2. **Peer Discovery Mechanism (`src/discovery.py`):**
   - **Central Discovery Tracker (`DiscoveryTracker`):** A lightweight rendezvous/tracker service where peers register on startup (`REGISTER`), query active peers (`GET_PEERS`), send periodic keepalive heartbeats (`HEARTBEAT`), and unregister on exit (`UNREGISTER`).
   - **Liveness & Failure Detection:** Stale/disconnected peers that fail to send heartbeats within the timeout window are automatically pruned by the Tracker's background reaper thread.
   - **Direct Connection Fallback:** Peers can also connect directly to known seed IPs (`--connect host:port`) without relying on the tracker.

3. **TCP Socket Communication & Protocol (`src/protocol.py`, `src/connection.py`):**
   - **Length-Prefixed Framing:** Every message is prefixed by a 4-byte big-endian length integer followed by a UTF-8 JSON payload. This eliminates TCP stream fragmentation and packet boundary issues.
   - **Two-Way TCP Handshake:** Mutual identification (`HANDSHAKE` / `HANDSHAKE_ACK`) establishing symmetric bi-directional communication channels.
   - **Thread-Safe PeerConnection:** Atomic thread-safe sending with socket exception and disconnection handling.
   - **Pluggable Event-Driven Dispatch:** Extensible handler registration (`peer.register_handler(msg_type, handler)`) designed so Member 2 and Member 3 can cleanly plug in file metadata and chunk transfer handlers.
   - **Diagnostic Utilities:** Ping/Pong round-trip time (RTT) measurement, broadcast, and unicast messaging.

---

## Member 2 Deliverables Overview

Member 2 implements file chunking, cryptographic metadata representation, and swarm-aware piece scheduling:

1. **File Metadata Management (`src/metadata.py`):**
   - **Cryptographic File Manifests (`FileMetadata`):** Calculates per-chunk SHA-256 hashes and whole-file SHA-256 hashes for integrity verification.
   - **Network Serialization:** Full serialization to and from JSON for seamless TCP exchange with peers (`to_json()`, `from_json()`).
   - **File Manifest Persistence:** Saving and loading `.meta` files on disk.

2. **File Splitting & Reassembly (`src/file_splitter.py`):**
   - **Chunk Splitting:** Breaks files into configurable fixed-size chunks (default 64 KB).
   - **Zero-Copy Disk Reads (`read_piece_from_file`):** Directly reads chunks from disk using file offset seeking, allowing seeders to serve requests without duplicating file storage.
   - **Chunk Storage Cache:** Stores incoming chunks into local peer piece directories.
   - **Bit-for-Bit Reassembly (`reassemble_file`):** Sequentially joins received chunks and cryptographically verifies the completed file against the metadata's whole-file SHA-256 digest.

3. **Piece-Request Logic & Swarm Management (`src/piece_manager.py`):**
   - **Bitfield Tracking:** Real-time state tracking of locally owned chunks (`owned_pieces`) and remote peer inventories (`peer_pieces`).
   - **Pluggable Piece Selection Strategies:**
     - **Rarest-First (`STRATEGY_RAREST_FIRST`):** Prioritizes chunks with the lowest availability count across the swarm to maximize piece diversity and prevent bottlenecks.
     - **Sequential (`STRATEGY_SEQUENTIAL`):** In-order piece downloading.
     - **Random (`STRATEGY_RANDOM`):** Random candidate selection.
   - **In-Flight Request Protection:** Prevents duplicate in-flight requests for the same piece across concurrent peer connections.
   - **Fault Tolerance & Timeout Recovery (Risk 2 Fallback):** Automatically releases in-flight pieces if a peer disconnects or fails to respond within the timeout window, allowing another peer to be scheduled.

---

## Project Structure

```
CN PROJECT/
├── src/
│   ├── __init__.py           # Package exports (Network & Member 2 modules)
│   ├── protocol.py           # Message types, schema definitions, and TCP length-framing
│   ├── connection.py         # Thread-safe PeerConnection socket wrapper
│   ├── discovery.py          # DiscoveryTracker server and DiscoveryClient
│   ├── peer.py               # Peer node server, connection manager, and routing
│   ├── metadata.py           # Member 2: FileMetadata management & SHA-256 verification
│   ├── file_splitter.py      # Member 2: File splitting, piece storage, and reassembly
│   ├── piece_manager.py      # Member 2: Bitfields, Rarest-First scheduling & failure handling
│   └── utils.py              # Timestamped logger and utilities
├── tests/
│   ├── __init__.py
│   ├── test_protocol.py      # Unit tests for protocol framing and serialization
│   ├── test_peer.py          # Integration tests for discovery and TCP peer communication
│   └── test_member2.py       # Unit tests for Member 2 metadata, splitting, and piece logic
├── run_tracker.py            # CLI entry point to launch Discovery Tracker
├── run_peer.py               # Interactive CLI entry point to run peer nodes
└── README.md                 # Project documentation and test guide
```

---

## Local Testing Guide

### 1. Run Automated Unit & Integration Tests

Execute the automated test suite verifying protocol framing, tracker registration, discovery, TCP handshakes, messaging, and teardown:

```bash
python -m unittest discover tests
```

---

### 2. Manual Multi-Peer Interactive Testing

Open **3 separate terminal windows** to simulate a distributed P2P network on your local machine:

#### Terminal 1: Start Discovery Tracker
```bash
python run_tracker.py --port 8000
```

#### Terminal 2: Start Peer 1
```bash
python run_peer.py --id Peer-1 --port 5001 --tracker-port 8000
```

#### Terminal 3: Start Peer 2
```bash
python run_peer.py --id Peer-2 --port 5002 --tracker-port 8000
```

---

### 3. Interactive CLI Commands (Inside Peer Terminals)

Once both peers are running, try the following commands in either terminal:

| Command | Action | Example |
| :--- | :--- | :--- |
| `status` | View local peer ID, listening port, and connection count | `status` |
| `discover` | Query Tracker for all active peers in the swarm | `discover` |
| `autoconnect` | Automatically connect over TCP to all discovered peers | `autoconnect` |
| `connect <host> <port>` | Manually establish TCP connection to a peer | `connect 127.0.0.1 5002` |
| `peers` | List all currently active TCP peer connections | `peers` |
| `ping <peer_id>` | Measure TCP round-trip latency (RTT) to a peer | `ping Peer-2` |
| `msg <peer_id> <text>` | Send a direct TCP message to a specific peer | `msg Peer-2 Hello from Peer 1!` |
| `broadcast <text>` | Broadcast message to all connected peers | `broadcast Hello everyone!` |
| `exit` / `quit` | Gracefully disconnect, notify peers, and unregister | `exit` |

---

## Integration Guide for Teammates

### For Member 2 (File Metadata & Splitting):
Register your custom metadata and piece request handlers on `peer`:
```python
def handle_metadata_req(conn, message):
    file_id = message["payload"]["file_id"]
    # Send metadata back via conn.send(...)
    pass

peer.register_handler(MessageType.METADATA_REQ, handle_metadata_req)
```

### For Member 3 (Piece Transfer & Parallel Download):
Send piece requests or chunk data using `peer.send_message`:
```python
peer.send_message(
    target_peer_id="Peer-2",
    msg_type=MessageType.PIECE_REQ,
    payload={"file_id": "file1", "piece_index": 0}
)
```
