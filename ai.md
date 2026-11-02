# AI Contribution & Work Breakdown Report

**Project:** Peer-to-Peer (P2P) File Distribution System  
**Course:** Computer Networks (CS-30003) · Coding Assignment 1  
**Section:** CSE-14 · **Path:** A (Catalogue Project)  

---

## 1. Overview & Distribution Summary

This document provides a comprehensive breakdown of the contributions across team members, categorizing the work into **Manual Development** and **AI-Assisted Development**. 

In accordance with the project workflow, routine scaffolding, configuration declarations, basic I/O setup, and test execution wrappers (~10%) were performed **manually**, while complex algorithmic design, distributed networking protocols, concurrency synchronization, mathematical modeling, and visualization pipelines (~90%) were built using **AI assistance**.

| Member | Primary Role & Domain | Manual Work (Easy ~10%) | AI-Assisted Work (Core ~90%) | Final Ratio |
| :--- | :--- | :--- | :--- | :---: |
| **Harsh Priyadarshi**<br>*(241551011)* | Core Networking, Discovery Tracker & TCP Protocol Layer | CLI parsing, socket boilerplate, default constants, directory setup, basic ping print statements. | Length-prefixed framing, stream defragmentation, thread-safe socket wrapper, heartbeat reaper daemon, event-driven dispatcher. | **10% / 90%** |
| **Sanket Panda**<br>*(2405833)* | File Chunking, Swarm Scheduling & Parallel Transfer Engine | File I/O open/close boilerplate, dataclass field definitions, CLI command routing, static chunk size constants. | SHA-256 chunk/file manifests, zero-copy disk seeking, Rarest-First piece scheduling, multi-peer parallel worker engine, error recovery & corruption retry. | **10% / 90%** |
| **Udbhav Singh**<br>*(241551102)* | Telemetry, Benchmarking, Visualization & Master System Integration | Experiment parameter arrays, output directory setup, script execution wrappers, report templates & headers. | Microsecond telemetry collector, automated matrix experiment harness, Pandas statistical analysis (Speedup & Amdahl's Law), 6 Matplotlib charts, master integration orchestrator (`integration.py`). | **10% / 90%** |

---

## 2. Detailed Member Work Breakdown

### 2.1 Member 1: Harsh Priyadarshi (Roll No: 241551011)
**Domain:** Core Networking, Peer Initialization, Discovery Tracker & TCP Protocol  
**Primary Files:** `src/protocol.py`, `src/connection.py`, `src/discovery.py`, `src/peer.py`, `run_tracker.py`, `tests/test_protocol.py`, `tests/test_peer.py`

#### A. Manual Work (~10% — Easy & Scaffolding Tasks)
1. **Basic CLI Configuration:** Writing standard `argparse` setups in `run_tracker.py` and `run_peer.py` for port and host inputs.
2. **Environment & Boilerplate Imports:** Setting up standard module imports (`socket`, `threading`, `json`, `sys`, `time`) and defining basic utility logging wrappers in `src/utils.py`.
3. **Constant Definitions:** Declaring static configuration variables (default port numbers, buffer size constants, heartbeat interval constants).
4. **Basic Test Structure:** Creating standard `unittest.TestCase` boilerplate classes and simple ping/pong assertion skeletons.
5. **CLI Text Menus:** Setting up command-line input loops and basic console print statements for peer status and diagnostic outputs.

#### B. AI-Assisted Work (~90% — Complex Logic, Networking & Concurrency)
1. **Length-Prefixed TCP Protocol Framing (`src/protocol.py`):**
   - Implemented 4-byte big-endian length prefix framing mechanism over raw TCP streams.
   - Handled socket stream defragmentation and message boundary reconstruction to eliminate TCP packet concatenation and partial packet reads.
2. **Thread-Safe Socket Wrapper (`src/connection.py`):**
   - Engineered the `PeerConnection` class with `threading.Lock` synchronization to guarantee atomic writes across concurrent worker threads.
   - Designed robust disconnect detection, socket cleanup, and connection error handling.
3. **Central Discovery Tracker (`src/discovery.py`):**
   - Built the centralized registration service managing peer IP/port mappings, keepalive heartbeats, and peer directory queries.
   - Developed a background reaper daemon thread that continuously scans and automatically evicts dead/unresponsive peers after timeout expiration.
4. **Peer Daemon & Two-Way Handshake (`src/peer.py`):**
   - Developed multithreaded non-blocking TCP server accepting simultaneous incoming peer sockets.
   - Implemented symmetric bi-directional handshake state machine (`HANDSHAKE` / `HANDSHAKE_ACK`) establishing cross-peer identification.
   - Built a pluggable event-driven message dispatch architecture (`register_handler()`) supporting seamless routing for metadata, bitfield, and piece requests.
5. **Unit & Integration Test Suite (`tests/test_protocol.py`, `tests/test_peer.py`):**
   - Formulated comprehensive multi-threaded test cases validating concurrent TCP messaging, socket drops, and tracker lifecycle synchronization.

---

### 2.2 Member 2: Sanket Panda (Roll No: 2405833)
**Domain:** File Metadata, Chunk Splitting, Swarm Scheduling & Parallel Transfer Engine  
**Primary Files:** `src/metadata.py`, `src/file_splitter.py`, `src/piece_manager.py`, `src/verification.py`, `src/transfer.py`, `src/transfer_manager.py`, `tests/test_member2.py`, `tests/test_transfer.py`

#### A. Manual Work (~10% — Easy & Scaffolding Tasks)
1. **Dataclass & Schema Layout:** Declaring class attributes for `FileMetadata` (`filename`, `file_size`, `piece_size`, `piece_hashes`).
2. **Standard File I/O Operations:** Basic file opening and closing routines (`open(path, 'rb')` and `open(path, 'wb')`) for sequential reads and writes.
3. **CLI Command Routing:** Binding CLI command strings (`share`, `download`, `verify`, `stats`) to their respective function signatures in `run_peer.py`.
4. **Test Fixtures & Dummy Data:** Creating temporary dummy test files and static byte sequences for test assertions.
5. **Directory Creation:** Writing helper checks to create piece storage folders (`mkdir -p`) on startup.

#### B. AI-Assisted Work (~90% — Cryptography, Swarm Algorithms & Data Plane Engine)
1. **Cryptographic Manifest & Integrity Layer (`src/metadata.py`, `src/verification.py`):**
   - Developed piece-wise and whole-file SHA-256 cryptographic hashing algorithms.
   - Built binary-to-JSON Base64 encoding/decoding pipeline for safe binary payload transit across TCP JSON frames.
   - Implemented `.meta` file persistence and verification against data corruption or payload tampering.
2. **Zero-Copy Splitting & Sequential File Reassembly (`src/file_splitter.py`):**
   - Engineered disk seeking algorithm (`read_piece_from_file`) using byte offsets to serve chunks on-demand without memory duplication.
   - Implemented bit-for-bit file reassembly engine sequentially merging received pieces and validating the reconstructed file against the master SHA-256 digest.
3. **Swarm Bitfield Tracking & Rarest-First Scheduler (`src/piece_manager.py`):**
   - Built real-time bitfield matrices tracking owned chunks and remote peer piece availability.
   - Formulated the **Rarest-First Piece Selection Algorithm** prioritizing rarest chunks across the swarm to prevent download bottlenecks and optimize piece entropy.
   - Implemented in-flight request reservation locks preventing redundant duplicate chunk downloads from multiple seeders.
4. **Parallel Multi-Peer Transfer Engine (`src/transfer.py`, `src/transfer_manager.py`):**
   - Engineered the `ParallelDownloader` with dynamic thread worker pools concurrently pulling distinct pieces from multiple connected seeders.
   - Implemented asynchronous non-blocking upload engine (`UploadManager`) serving concurrent `PIECE_REQ` queries.
   - Developed adaptive failure handling: per-piece download timeouts, corrupted piece detection, automatic chunk re-queueing, and peer failover.
5. **Test Scenarios (`tests/test_member2.py`, `tests/test_transfer.py`):**
   - Formulated test matrices verifying chunk boundary accuracy, rarest-first piece priority ordering, and automatic recovery upon receiving corrupt piece hashes.

---

### 2.3 Member 3: Udbhav Singh (Roll No: 241551102)
**Domain:** Performance Telemetry, Benchmarking Suite, Visualization & Master System Integration  
**Primary Files:** `src/performance.py`, `integration.py`, `run_experiments.py`, `tests/test_member4.py`, `docs/EXPERIMENT_REPORT.md`, `docs/PERFORMANCE_DOCS.md`, `docs/MEMBER_4_DOCUMENTATION.md`

#### A. Manual Work (~10% — Easy & Scaffolding Tasks)
1. **Parameter Lists Definition:** Declaring experimental evaluation arrays (peer counts: `[1, 2, 4, 8, 12, 16]`, file sizes: `[1MB, 5MB, 10MB, 20MB]`, chunk sizes: `[16KB, ..., 512KB]`).
2. **Output Folder Setup:** Creating target folder directories (`results/figures/`, `results/raw_metrics/`) and file paths.
3. **CLI Flag Parser:** Basic argument parsing in `run_experiments.py` for selecting benchmark modes.
4. **Documentation Structure:** Structuring markdown section headings, markdown table skeletons, and assignment meta info in `docs/`.
5. **Manual Sanity Execution:** Running integration commands manually in separate terminal windows to observe console output.

#### B. AI-Assisted Work (~90% — Mathematical Modeling, Telemetry, Plotting & Orchestration)
1. **Microsecond-Precision Telemetry Harness (`PerformanceMetricsCollector` in `src/performance.py`):**
   - Built event-driven instrumentation capturing chunk-level transfer time, socket RTT, cryptographic verification overhead, and throughput (MB/s).
   - Designed automated raw telemetry export pipeline serializing microsecond metrics into structured CSV and JSON outputs.
2. **Automated Experiment Matrix Engine (`ExperimentRunner` in `src/performance.py`):**
   - Developed automated orchestration harness executing multi-dimensional benchmarking grids across variable peer counts and file sizes.
   - Implemented network virtualization and peer churn simulation testing recovery time during mid-transfer peer disconnects.
3. **Statistical Analysis Engine (`ExperimentAnalyzer` in `src/performance.py`):**
   - Programmed statistical analysis routines with Pandas calculating Mean, Standard Deviation, Speedup Factor $S(N)$, and Concurrency Efficiency $E(N)$.
   - Modeled Amdahl's Law theoretical speedup limits against empirical measurements to quantify protocol and networking overhead.
   - Generated automated summary markdown comparison tables (`results/summary_tables.md`).
4. **Publication-Grade Visualization Suite (`BenchmarkVisualizer` in `src/performance.py`):**
   - Built Matplotlib visualization pipeline generating 6 publication-ready 300-DPI charts:
     1. Download Time vs. Peer Count across file sizes.
     2. Swarm Throughput (MB/s) vs. Swarm Size.
     3. Speedup Analysis: Empirical vs. Linear Ideal vs. Amdahl's Model.
     4. Seeder Piece Load Balancing distribution histogram.
     5. Chunk Size Trade-off dual-axis curve (throughput vs. overhead).
     6. Peer Failure and Mid-Transfer Crash Recovery timeline.
5. **Master System Integration (`integration.py`):**
   - Formulated the end-to-end integration orchestrator unifying discovery registration, socket framing, rarest-first chunk scheduling, parallel multi-threaded workers, and SHA-256 verification into an autonomous self-verifying test suite.
6. **Technical & Empirical Documentation:**
   - Authored the in-depth empirical research report (`docs/EXPERIMENT_REPORT.md`), mathematical formulations, and viva reference documentation.

---

## 3. Comparative Breakdown Matrix

| Module / Component | Task Complexity | Manual Contribution (~10%) | AI-Assisted Contribution (~90%) |
| :--- | :--- | :--- | :--- |
| **`src/protocol.py` & `connection.py`** | High | Standard imports, constants, error prints | 4-byte length prefix framing, stream defragmentation, thread-safe atomic socket mutex |
| **`src/discovery.py`** | Medium-High | Argument parsing, default ports | Dynamic peer registry, keepalive heartbeats, background reaper thread for dead peers |
| **`src/peer.py`** | High | Command loop skeleton, status prints | Multithreaded TCP server, symmetric handshake protocol, event-driven message dispatch |
| **`src/metadata.py` & `verification.py`** | Medium-High | Dataclass definitions, file paths | SHA-256 chunk & whole-file hashing, Base64 JSON serialization, integrity verification |
| **`src/file_splitter.py`** | Medium | File open/close calls, directory paths | Zero-copy byte offset seeking, bit-for-bit file reassembly with cryptographic validation |
| **`src/piece_manager.py`** | High | State dictionaries, array definitions | Bitfield matrix management, Rarest-First scheduling algorithm, in-flight request deduplication |
| **`src/transfer.py` & `transfer_manager.py`** | Very High | Parameter definitions, status logs | Multi-peer parallel download worker pool, non-blocking upload manager, timeout & failover handling |
| **`src/performance.py`** | Very High | Experiment parameter arrays, file paths | High-precision microsecond telemetry, automated matrix runner, Pandas statistics, Amdahl's Law |
| **Visualization Suite** | High | Output paths, plot labels and titles | Multi-axis 300-DPI Matplotlib figure rendering, style palettes, dual-axis trade-off plotting |
| **`integration.py`** | High | Test file path configuration | End-to-end swarm lifecycle orchestration, concurrent worker verification, SHA-256 verification |
| **Automated Test Suite** | Medium-High | Test method skeletons, basic assertions | Concurrency testing, socket error simulation, corrupted piece re-request tests, churn validation |

---

## 4. Methodology & Rationale

The division of labor was structured to maximize engineering productivity and academic depth:
- **Manual Work (10%):** Focused strictly on declarative tasks—specifying configuration parameters, initial directory structures, CLI argument definitions, standard file access calls, and manually triggering executions across multiple terminal instances.
- **AI-Assisted Work (90%):** Leveraged AI for complex systems programming—resolving TCP stream fragmentation, designing distributed state machines, implementing non-blocking multithreaded worker pools, formulating rarest-first piece selection heuristics, modeling parallel concurrency efficiency mathematically, and generating high-resolution empirical charts.

