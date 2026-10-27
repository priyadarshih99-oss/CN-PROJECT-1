# Peer-to-Peer (P2P) File Distribution System
**Computer Networks · Coding Assignment 1**  
**Section:** CSE-14 · **Path:** A (Catalogue Project)

---

## Team & Work Split
- **Harsh Priyadarshi (241551011):** Peer initialization, central discovery tracker, keepalive heartbeats, TCP socket communication, protocol framing, and connection management. *(Completed)*
- **Sanket Panda (2405833):** File metadata management, piece splitting & reassembly, cryptographic piece verification (SHA-256), swarm bitfield tracking, rarest-first piece scheduling, and parallel transfer engine with failure recovery. *(Completed)*
- **Udbhav Singh (241551102):** Performance telemetry, microsecond-level benchmarking, speedup & efficiency analysis, data visualization, experiment evaluation, master system integration, and documentation. *(Completed)*

---

## 1. Core Networking & Discovery Overview

Establishes the distributed networking foundation upon which the file transfer and evaluation logic is built:

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
   - **Pluggable Event-Driven Dispatch:** Extensible handler registration (`peer.register_handler(msg_type, handler)`) designed so file metadata and chunk transfer handlers can cleanly plug in.
   - **Diagnostic Utilities:** Ping/Pong round-trip time (RTT) measurement, broadcast, and unicast messaging.

---

## 2. File Metadata, Splitting & Swarm Management Overview

Implements file chunking, cryptographic metadata representation, and swarm-aware piece scheduling:

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
   - **Fault Tolerance & Timeout Recovery:** Automatically releases in-flight pieces if a peer disconnects or fails to respond within the timeout window, allowing another peer to be scheduled.

---

## 3. File Transfer, Verification & Resilience Overview

Implements the data plane transfer engine, multi-peer parallelization, integrity checking, and fault tolerance:

1. **Cryptographic Piece Verification (`src/verification.py`):**
   - **SHA-256 Checksums:** Computes 256-bit cryptographic digests for every chunk transferred over TCP.
   - **Data Integrity Verification:** Validates incoming chunk bytes against expected SHA-256 piece hashes before storage.
   - **Corrupt Chunk Detection:** Rejects corrupted, incomplete, or tampered chunks and triggers immediate re-request.
   - **Whole-File Integrity:** Verifies assembled files against original master checksums.
   - **Base64 Serialization:** Safe binary data encoding/decoding for JSON transport over framed TCP sockets.

2. **Parallel Upload & Seeding (`UploadManager` in `src/transfer.py`, `src/transfer_manager.py`):**
   - **Non-Blocking Worker Pool:** Handles incoming `PIECE_REQ` requests asynchronously using thread workers, preventing upload bottlenecks.
   - **On-Demand Slicing:** Serves chunks on-demand from disk or memory buffers.
   - **Transfer Metrics:** Tracks total bytes uploaded and piece service counts.

3. **Parallel Multi-Peer Downloader (`ParallelDownloader` in `src/transfer.py`):**
   - **Concurrent Worker Pool:** Downloads distinct pieces simultaneously across multiple connected peers.
   - **Dynamic Load Distribution:** Distributes requests among active peers to maximize throughput.
   - **Piece Reassembly:** Accurately reassembles verified pieces in exact sequence `0 .. N-1` into the target file on disk.

4. **Failure Handling & Fault Tolerance:**
   - **Per-Piece Timeouts:** Automatically cancels and re-enqueues stalled chunk requests.
   - **Corrupted Piece Re-requesting:** Catches hash mismatches and re-queues pieces up to `max_retries`.
   - **Peer Penalization & Failover:** Automatically deprioritizes or bypasses flaky peers and shifts requests to healthier seeders.
   - **Socket Drop Recovery:** Detects peer disconnects mid-transfer and re-routes missing pieces to alternate peers.
   - **Graceful Cancellation:** `cancel_download()` stops worker threads and broadcasts `TRANSFER_CANCEL`.

---

## 4. Performance Telemetry, Visualization & System Integration Overview

Implements performance telemetry, visualization, experiment analysis, system integration, and documentation:

1. **Performance Telemetry (`src/performance.py`):**
   - **`PerformanceMetricsCollector`**: Captures microsecond-level transfer times, throughput (MB/s), chunk-level RTT, verification duration, and protocol framing efficiency.
   - Exports raw metrics and chunk logs to CSV and JSON formats.

2. **Automated Experiment Harness (`src/performance.py` -> `ExperimentRunner`):**
   - Executes the matrix defined in Section 3 of the proposal across varying peer counts ($N \in \{1, 2, 4, 8, 12, 16\}$) and file sizes ($1, 5, 10, 20\text{ MB}$).
   - Parameter sweeps over chunk sizes ($16\text{ KB}$ to $512\text{ KB}$).
   - Fault resilience and churn tests simulating mid-transfer peer disconnects.

3. **Experiment Analysis (`src/performance.py` -> `ExperimentAnalyzer`):**
   - Computes Mean, Standard Deviation, Median, Speedup Factor $S(N)$, and Parallel Efficiency $E(N)$ using Pandas.
   - Evaluates the core claim: validates speedup and quantifies communication overhead.
   - Generates formatted Markdown and CSV summary comparison tables (`results/summary_tables.md`).

4. **Visualization Suite (`src/performance.py` -> `BenchmarkVisualizer`):**
   - Generates 6 publication-ready figures (300 DPI) saved in `results/figures/`:
     1. `download_time_vs_peers.png`: Download Time vs. Number of Peers across file sizes.
     2. `throughput_vs_peers.png`: Throughput (MB/s) vs. Swarm Size.
     3. `speedup_analysis.png`: Concurrency Speedup $S(N)$ vs. Linear Ideal & Amdahl's Model.
     4. `piece_distribution.png`: Piece transfer load balancing histogram across seeders.
     5. `chunk_size_tradeoff.png`: Dual-axis plot of chunk size vs. download time & throughput.
     6. `peer_failure_impact.png`: Recovery time under mid-transfer peer crashes.

5. **Master System Integration (`integration.py`):**
   - Integrates Discovery Tracker & TCP sockets, chunking & rarest-first selector, and parallel workers into a unified working system.
   - Performs automated end-to-end swarm file transfers and verifies SHA-256 integrity.

---

## Project Structure

```
CN PROJECT/
├── src/
│   ├── __init__.py           # Package exports (Network, Transfer, & Performance)
│   ├── protocol.py           # Message types, schema definitions, and TCP length-framing
│   ├── connection.py         # Thread-safe PeerConnection socket wrapper
│   ├── discovery.py          # DiscoveryTracker server and DiscoveryClient
│   ├── peer.py               # Peer node server, connection manager, and routing
│   ├── metadata.py           # FileMetadata management & SHA-256 verification
│   ├── file_splitter.py      # File splitting, piece storage, and reassembly
│   ├── piece_manager.py      # Bitfields, Rarest-First scheduling & failure handling
│   ├── transfer_manager.py   # Swarm transfer engine & telemetry hooks
│   ├── verification.py       # Cryptographic piece verification & SHA-256 hashing
│   ├── transfer.py           # Parallel upload/download, failure recovery, assembler
│   ├── performance.py        # Telemetry, experiment runner, analyzer & visualizer
│   └── utils.py              # Timestamped logger and utilities
├── tests/
│   ├── __init__.py
│   ├── test_protocol.py      # Unit tests for protocol framing and serialization
│   ├── test_peer.py          # Integration tests for discovery and TCP peer communication
│   ├── test_member2.py       # Unit tests for metadata, splitting, and piece logic
│   ├── test_transfer.py      # Parallel download, verification, and failure recovery tests
│   └── test_member4.py       # Unit tests for telemetry collector and experiment runner
├── docs/
│   ├── EXPERIMENT_REPORT.md  # Detailed empirical analysis report
│   └── PERFORMANCE_DOCS.md   # Benchmark and telemetry documentation
├── results/
│   ├── figures/              # Generated 300-DPI charts
│   ├── raw_metrics/          # CSV/JSON run outputs
│   └── summary_tables.md     # Markdown statistical tables
├── integration.py            # Master system integration orchestrator
├── run_experiments.py        # Benchmark suite & figure generation script
├── run_tracker.py            # CLI entry point to launch Discovery Tracker
├── run_peer.py               # Interactive CLI entry point with transfer commands
└── README.md                 # Complete project documentation and guide
```

---

## Local Testing Guide

### 1. Run Automated Unit & Integration Tests (10 Tests)

Execute the full automated test suite:

```bash
python -m unittest discover tests
```

---

### 2. Run System Integration Swarm Verification

Run the master integration script demonstrating multi-peer swarm discovery, chunking, rarest-first piece scheduling, parallel transfer, and full verification:

```bash
python integration.py
```

---

### 3. Run Benchmark Suite & Generate Visualizations

Execute the performance benchmark matrix and generate the 6 figures:

```bash
python run_experiments.py
```

---

### 4. Manual Multi-Peer Interactive Testing

Open **3 separate terminal windows** to simulate a distributed P2P network:

#### Terminal 1: Start Discovery Tracker
```bash
python run_tracker.py --port 8000
```

#### Terminal 2: Start Peer 1 (Seeder)
```bash
python run_peer.py --id Peer-1 --port 5001 --tracker-port 8000
```

#### Terminal 3: Start Peer 2 (Leecher)
```bash
python run_peer.py --id Peer-2 --port 5002 --tracker-port 8000
```

---

### 5. Interactive CLI Commands

| Command | Category | Action | Example |
| :--- | :--- | :--- | :--- |
| `discover` | Networking | Query Tracker for all active peers in the swarm | `discover` |
| `autoconnect` | Networking | Automatically connect over TCP to all discovered peers | `autoconnect` |
| `connect <host> <port>` | Networking | Establish direct TCP connection to a peer | `connect 127.0.0.1 5002` |
| `peers` | Networking | List all currently active TCP peer connections | `peers` |
| `ping <peer_id>` | Networking | Measure TCP round-trip latency (RTT) to a peer | `ping Peer-2` |
| `msg <peer_id> <text>` | Networking | Send direct TCP message to a peer | `msg Peer-2 Hello!` |
| `share <filepath>` | Transfer | Register and seed a file (splits pieces & computes SHA-256) | `share testfile.bin` |
| `download <id> <out> <hashes>` | Transfer | Download pieces in parallel from connected peers | `download file1 out.bin h0,h1` |
| `cancel <file_id>` | Transfer | Cancel an active download in progress | `cancel file1` |
| `stats` | Transfer | View upload & download transfer statistics | `stats` |
| `verify <file> <hash>` | Verification | Cryptographically verify file integrity with SHA-256 | `verify out.bin 9f86d081...` |
| `status` | General | View local peer ID, listening port, and connection count | `status` |
| `exit` / `quit` | General | Gracefully disconnect, notify peers, and unregister | `exit` |
