"""
integration.py - Master System Integration Orchestrator
CS-30003 P2P File Distribution System
Section: CSE-14

Integrates:
- Discovery Tracker & TCP Socket Communication (Harsh Priyadarshi)
- File Metadata Management, File Splitting, Piece Request Logic & Parallel Transfer (Sanket Panda)
- Performance Measurement, Telemetry Collection & System Verification (Udbhav Singh)
"""

import os
import sys
import time
import shutil
import socket
import tempfile
import hashlib
from typing import Dict, Any, List, Optional

from src.discovery import DiscoveryTracker
from src.peer import Peer
from src.metadata import FileMetadata
from src.file_splitter import FileSplitter
from src.piece_manager import PieceManager
from src.transfer_manager import TransferManager
from src.performance import PerformanceMetricsCollector


class P2PSystemIntegration:
    """Master Integration Suite for verifying complete P2P network operations."""

    def __init__(self, tracker_host: str = "127.0.0.1"):
        self.tracker_host = tracker_host
        self.tracker_port: int = 0
        self.tracker: Optional[DiscoveryTracker] = None
        self.seeders: List[Peer] = []
        self.seeder_managers: List[TransferManager] = []
        self.leechers: List[Peer] = []
        self.leecher_managers: List[TransferManager] = []
        self.collector = PerformanceMetricsCollector()
        self.work_dir = tempfile.mkdtemp(prefix="p2p_integration_")

    def setup_tracker(self) -> int:
        """Finds a free dynamic port and starts the Discovery Tracker."""
        temp_sock = socket.socket()
        temp_sock.bind((self.tracker_host, 0))
        self.tracker_port = temp_sock.getsockname()[1]
        temp_sock.close()

        self.tracker = DiscoveryTracker(
            host=self.tracker_host,
            port=self.tracker_port,
            peer_timeout=15.0
        )
        self.tracker.start()
        print(f"[INTEGRATION] Discovery Tracker active on {self.tracker_host}:{self.tracker_port}")
        return self.tracker_port

    def create_sample_payload(self, filename: str, size_bytes: int) -> str:
        """Generates a test payload file."""
        payload_dir = os.path.join(self.work_dir, "source_payload")
        os.makedirs(payload_dir, exist_ok=True)
        filepath = os.path.join(payload_dir, filename)

        block = b"CN_PROJECT_1_INTEGRATION_TEST_DATA_" * 128
        with open(filepath, "wb") as f:
            written = 0
            while written < size_bytes:
                chunk = block[:min(len(block), size_bytes - written)]
                f.write(chunk)
                written += len(chunk)

        print(f"[INTEGRATION] Generated payload: {filepath} ({size_bytes / (1024*1024):.2f} MB)")
        return filepath

    def spawn_seeder_swarm(self, file_path: str, num_seeders: int = 4, chunk_size: int = 64 * 1024) -> FileMetadata:
        """Launches N seeder peer nodes serving the file."""
        metadata = None
        for i in range(num_seeders):
            s_sock = socket.socket()
            s_sock.bind((self.tracker_host, 0))
            s_port = s_sock.getsockname()[1]
            s_sock.close()

            seeder = Peer(
                peer_id=f"Seeder-{i+1}",
                host=self.tracker_host,
                port=s_port,
                tracker_host=self.tracker_host,
                tracker_port=self.tracker_port
            )
            seeder.start()
            self.seeders.append(seeder)

            cache_dir = os.path.join(self.work_dir, f"seeder_{i+1}_pieces")
            sm = TransferManager(seeder, pieces_root_dir=cache_dir)
            meta = sm.register_seeder_file(file_path, piece_size=chunk_size)
            self.seeder_managers.append(sm)
            metadata = meta
            print(f"[INTEGRATION] Seeder [{seeder.peer_id}] active on port {seeder.port} (Serving {meta.total_pieces} pieces)")

        time.sleep(0.1)
        return metadata

    def execute_leecher_download(
        self,
        metadata: FileMetadata,
        out_filename: str = "downloaded.bin",
        strategy: str = PieceManager.STRATEGY_RAREST_FIRST
    ) -> Dict[str, Any]:
        """Runs parallel download on a leecher peer and verifies integrity."""
        l_sock = socket.socket()
        l_sock.bind((self.tracker_host, 0))
        l_port = l_sock.getsockname()[1]
        l_sock.close()

        leecher = Peer(
            peer_id="Leecher-Main",
            host=self.tracker_host,
            port=l_port,
            tracker_host=self.tracker_host,
            tracker_port=self.tracker_port
        )
        leecher.start()
        self.leechers.append(leecher)

        leecher_cache = os.path.join(self.work_dir, "leecher_pieces")
        lm = TransferManager(leecher, pieces_root_dir=leecher_cache)
        self.leecher_managers.append(lm)

        # Hook Member 4 telemetry
        lm.on_piece_downloaded = self.collector.record_piece_event

        dest_file = os.path.join(self.work_dir, "output", out_filename)
        print(f"\n[INTEGRATION] Initiating Parallel Download on [{leecher.peer_id}]...")
        print(f"[INTEGRATION] Target: {metadata.file_name} | Size: {metadata.file_size} bytes | Pieces: {metadata.total_pieces}")

        t0 = time.perf_counter()
        is_success = lm.download_file(
            metadata,
            dest_file,
            timeout=45.0,
            strategy=strategy,
            max_workers=len(self.seeders)
        )
        t_duration = time.perf_counter() - t0

        # Cryptographic end-to-end SHA-256 validation
        hasher = hashlib.sha256()
        with open(dest_file, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        downloaded_hash = hasher.hexdigest()
        is_hash_identical = (downloaded_hash == metadata.file_hash)
        throughput = (metadata.file_size / (1024 * 1024)) / max(t_duration, 0.0001)

        summary = self.collector.record_transfer_summary(
            experiment_id="system_integration_run",
            file_size_bytes=metadata.file_size,
            chunk_size_bytes=metadata.piece_size,
            total_pieces=metadata.total_pieces,
            swarm_size=len(self.seeders),
            strategy=strategy,
            duration_sec=t_duration,
            is_successful=is_success and is_hash_identical,
            extra_meta={
                "sha256_match": is_hash_identical,
                "expected_hash": metadata.file_hash,
                "downloaded_hash": downloaded_hash
            }
        )

        print("\n" + "=" * 65)
        print("  SYSTEM INTEGRATION VERIFICATION RESULT")
        print("=" * 65)
        print(f"Transfer Status      : {'PASS' if is_success and is_hash_identical else 'FAIL'}")
        print(f"Elapsed Time         : {t_duration:.4f} seconds")
        print(f"Effective Throughput : {throughput:.2f} MB/s")
        print(f"Original SHA-256     : {metadata.file_hash}")
        print(f"Reconstructed SHA-256: {downloaded_hash}")
        print(f"Cryptographic Match  : {is_hash_identical}")
        print(f"Total Pieces Verified: {metadata.total_pieces} / {metadata.total_pieces}")
        print("=" * 65 + "\n")
        return summary

    def teardown(self):
        """Clean shutdown of all network entities."""
        print("[INTEGRATION] Tearing down peer network...")
        for l in self.leechers:
            try:
                l.stop()
            except Exception:
                pass
        for s in self.seeders:
            try:
                s.stop()
            except Exception:
                pass
        if self.tracker:
            try:
                self.tracker.stop()
            except Exception:
                pass
        shutil.rmtree(self.work_dir, ignore_errors=True)
        print("[INTEGRATION] Swarm shutdown complete.")


def run_integration_test(file_size_mb: int = 4, num_seeders: int = 4) -> bool:
    integration = P2PSystemIntegration()
    try:
        integration.setup_tracker()
        payload_path = integration.create_sample_payload("integration_sample.bin", file_size_mb * 1024 * 1024)
        metadata = integration.spawn_seeder_swarm(payload_path, num_seeders=num_seeders)
        result = integration.execute_leecher_download(metadata)
        return result["is_successful"]
    finally:
        integration.teardown()


if __name__ == "__main__":
    success = run_integration_test(file_size_mb=4, num_seeders=4)
    sys.exit(0 if success else 1)
