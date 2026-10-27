"""
Automated Unit and Integration Tests for File Transfer & Piece Verification:
- Parallel Upload/Download
- Cryptographic Piece Verification (SHA-256)
- Failure Handling (Retries, Corrupt Chunks, Disconnected Peers, Timeouts)
- Transfer Cancellation
"""

import os
import shutil
import tempfile
import time
import unittest

from src.discovery import DiscoveryTracker
from src.peer import Peer
from src.protocol import MessageType, create_message
from src.verification import PieceVerifier
from src.transfer import SharedFile, TransferManager, DownloadResult


class TestTransferAndVerification(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Dedicated tracker for transfer tests
        cls.tracker_port = 8995
        cls.tracker = DiscoveryTracker(host="127.0.0.1", port=cls.tracker_port, peer_timeout=15.0)
        cls.tracker.start()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.tracker.stop()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_piece_verifier_integrity(self):
        """Tests SHA-256 piece hashing, integrity check, and corruption detection."""
        original_data = b"Computer Networks - Distributed P2P File Distribution System!"
        computed_hash = PieceVerifier.compute_hash(original_data)

        # 1. Valid data matches
        self.assertTrue(PieceVerifier.verify_piece(original_data, computed_hash))
        self.assertTrue(PieceVerifier.verify_piece(original_data, computed_hash.upper()))

        # 2. Corrupted data fails
        corrupted_data = b"Computer Networks - Distributed P2P File CORRUPT DATA!"
        self.assertFalse(PieceVerifier.verify_piece(corrupted_data, computed_hash))

        # 3. Base64 encoding/decoding round-trip
        b64 = PieceVerifier.encode_data(original_data)
        decoded = PieceVerifier.decode_data(b64)
        self.assertEqual(decoded, original_data)

        # 4. File-level verification
        test_file = os.path.join(self.temp_dir, "test_verify.txt")
        with open(test_file, "wb") as f:
            f.write(original_data)

        self.assertTrue(PieceVerifier.verify_file(test_file, computed_hash))
        self.assertFalse(PieceVerifier.verify_file(test_file, "wrong_hash_12345"))

    def test_02_single_peer_upload_and_download(self):
        """Tests end-to-end piece uploading, downloading, verification, and reassembly."""
        seeder = Peer(peer_id="Seeder-1", host="127.0.0.1", port=7101, tracker_port=self.tracker_port)
        leecher = Peer(peer_id="Leecher-1", host="127.0.0.1", port=7102, tracker_port=self.tracker_port)

        seeder_tm = TransferManager(seeder)
        leecher_tm = TransferManager(leecher)

        try:
            seeder.start()
            leecher.start()
            time.sleep(0.3)

            # Connect Leecher -> Seeder
            conn = leecher.connect_to_peer("127.0.0.1", 7101, remote_peer_id="Seeder-1")
            self.assertIsNotNone(conn)
            time.sleep(0.2)

            # Generate random 100KB test payload split into 16KB pieces (~7 pieces)
            test_bytes = os.urandom(100 * 1024)
            shared = seeder_tm.share_bytes(test_bytes, file_id="doc100k", piece_size=16 * 1024)
            self.assertEqual(shared.total_pieces, 7)

            # Destination path for reassembled file
            out_file = os.path.join(self.temp_dir, "downloaded_doc100k.bin")

            # Execute download
            result = leecher_tm.download(
                file_id="doc100k",
                piece_hashes=shared.piece_hashes,
                piece_size=16 * 1024,
                output_path=out_file,
                max_workers=2,
            )

            # Assertions
            self.assertTrue(result.success, f"Download failed: {result.error_message}")
            self.assertEqual(result.total_pieces, 7)
            self.assertEqual(result.verified_pieces, 7)
            self.assertEqual(result.failed_pieces, 0)
            self.assertEqual(result.total_bytes, len(test_bytes))
            self.assertTrue(os.path.exists(out_file))

            # Verify assembled file matches original byte-for-byte
            with open(out_file, "rb") as f:
                assembled = f.read()
            self.assertEqual(assembled, test_bytes)

            # Check stats on seeder
            stats = seeder_tm.get_stats()
            self.assertEqual(stats["total_pieces_served"], 7)
            self.assertEqual(stats["total_bytes_uploaded"], len(test_bytes))

        finally:
            seeder_tm.stop()
            leecher_tm.stop()
            seeder.stop()
            leecher.stop()

    def test_03_parallel_multi_peer_download(self):
        """Tests concurrent parallel downloading across multiple seeders simultaneously."""
        seeder_a = Peer(peer_id="Seeder-A", host="127.0.0.1", port=7201, tracker_port=self.tracker_port)
        seeder_b = Peer(peer_id="Seeder-B", host="127.0.0.1", port=7202, tracker_port=self.tracker_port)
        leecher = Peer(peer_id="Leecher-Multi", host="127.0.0.1", port=7203, tracker_port=self.tracker_port)

        tm_a = TransferManager(seeder_a)
        tm_b = TransferManager(seeder_b)
        tm_leecher = TransferManager(leecher)

        try:
            seeder_a.start()
            seeder_b.start()
            leecher.start()
            time.sleep(0.3)

            # Leecher connects to both seeders
            leecher.connect_to_peer("127.0.0.1", 7201, remote_peer_id="Seeder-A")
            leecher.connect_to_peer("127.0.0.1", 7202, remote_peer_id="Seeder-B")
            time.sleep(0.2)

            # 128KB payload with 16KB pieces = 8 pieces
            test_data = os.urandom(128 * 1024)
            file_id = "multi_seed_test"
            piece_size = 16 * 1024

            # Both seeders share the same file
            shared_a = tm_a.share_bytes(test_data, file_id=file_id, piece_size=piece_size)
            shared_b = tm_b.share_bytes(test_data, file_id=file_id, piece_size=piece_size)

            out_file = os.path.join(self.temp_dir, "multi_download.bin")

            # Download using 4 parallel workers across both seeders
            result = tm_leecher.download(
                file_id=file_id,
                piece_hashes=shared_a.piece_hashes,
                piece_size=piece_size,
                output_path=out_file,
                max_workers=4,
            )

            self.assertTrue(result.success, f"Parallel download failed: {result.error_message}")
            self.assertEqual(result.verified_pieces, 8)
            self.assertEqual(result.total_bytes, len(test_data))

            # Verify that both seeders served pieces (parallel load distribution)
            stats_a = tm_a.get_stats()
            stats_b = tm_b.get_stats()
            total_served = stats_a["total_pieces_served"] + stats_b["total_pieces_served"]
            self.assertEqual(total_served, 8)
            self.assertGreater(stats_a["total_pieces_served"], 0)
            self.assertGreater(stats_b["total_pieces_served"], 0)

            # Verify assembled data integrity
            with open(out_file, "rb") as f:
                assembled = f.read()
            self.assertEqual(assembled, test_data)

        finally:
            tm_a.stop()
            tm_b.stop()
            tm_leecher.stop()
            seeder_a.stop()
            seeder_b.stop()
            leecher.stop()

    def test_04_failure_handling_and_retry_on_corrupt_chunk(self):
        """
        Tests failure recovery:
        Seeder intentionally corrupts a piece on the first attempt.
        Downloader detects hash mismatch, rejects the corrupt piece,
        and automatically retries, receiving the valid piece and assembling cleanly.
        """
        seeder = Peer(peer_id="Seeder-Flaky", host="127.0.0.1", port=7301, tracker_port=self.tracker_port)
        leecher = Peer(peer_id="Leecher-Retry", host="127.0.0.1", port=7302, tracker_port=self.tracker_port)

        seeder_tm = TransferManager(seeder)
        leecher_tm = TransferManager(leecher)

        try:
            seeder.start()
            leecher.start()
            time.sleep(0.3)

            leecher.connect_to_peer("127.0.0.1", 7301, remote_peer_id="Seeder-Flaky")
            time.sleep(0.2)

            test_data = b"Testing piece corruption and automatic retry recovery mechanisms." * 100
            file_id = "corrupt_test"
            shared = seeder_tm.share_bytes(test_data, file_id=file_id, piece_size=1024)

            # Intercept piece request: corrupt piece 0 on first request only
            original_handler = seeder._handlers[MessageType.PIECE_REQ]
            attempt_count = {"count": 0}

            def faulty_piece_req_handler(conn, message):
                payload = message.get("payload", {})
                if payload.get("piece_index") == 0 and attempt_count["count"] == 0:
                    attempt_count["count"] += 1
                    # Send tampered data with wrong hash
                    corrupt_data = b"THIS IS CORRUPTED DATA BY MITM/NOISE"
                    resp = create_message(
                        MessageType.PIECE_DATA,
                        seeder.peer_id,
                        {
                            "file_id": payload["file_id"],
                            "piece_index": 0,
                            "status": "OK",
                            "data": PieceVerifier.encode_data(corrupt_data),
                            "hash": PieceVerifier.compute_hash(corrupt_data),
                        }
                    )
                    conn.send(resp)
                    return
                # Normal valid response on retry
                original_handler(conn, message)

            seeder.register_handler(MessageType.PIECE_REQ, faulty_piece_req_handler)

            out_file = os.path.join(self.temp_dir, "recovered.bin")

            result = leecher_tm.download(
                file_id=file_id,
                piece_hashes=shared.piece_hashes,
                piece_size=1024,
                output_path=out_file,
                max_retries=3,
                max_workers=1,
            )

            # Assertions
            self.assertTrue(result.success)
            self.assertGreaterEqual(result.retry_count, 1, "Downloader should have recorded retry for corrupt piece")
            self.assertEqual(result.verified_pieces, shared.total_pieces)

            with open(out_file, "rb") as f:
                assembled = f.read()
            self.assertEqual(assembled, test_data)

        finally:
            seeder_tm.stop()
            leecher_tm.stop()
            seeder.stop()
            leecher.stop()

    def test_05_transfer_cancellation(self):
        """Tests graceful cancellation of an active download."""
        seeder = Peer(peer_id="Seeder-Cancel", host="127.0.0.1", port=7401, tracker_port=self.tracker_port)
        leecher = Peer(peer_id="Leecher-Cancel", host="127.0.0.1", port=7402, tracker_port=self.tracker_port)

        seeder_tm = TransferManager(seeder)
        leecher_tm = TransferManager(leecher)

        try:
            seeder.start()
            leecher.start()
            time.sleep(0.3)

            leecher.connect_to_peer("127.0.0.1", 7401, remote_peer_id="Seeder-Cancel")
            time.sleep(0.2)

            # Large payload
            big_data = os.urandom(500 * 1024)
            shared = seeder_tm.share_bytes(big_data, file_id="cancel_file", piece_size=4096)

            # Cancel after a brief delay
            def cancel_delayed():
                time.sleep(0.05)
                leecher_tm.cancel_download("cancel_file")

            import threading
            threading.Thread(target=cancel_delayed, daemon=True).start()

            result = leecher_tm.download(
                file_id="cancel_file",
                piece_hashes=shared.piece_hashes,
                piece_size=4096,
                max_workers=1,
            )

            self.assertFalse(result.success)
            self.assertIn("cancelled", (result.error_message or "").lower())

        finally:
            seeder_tm.stop()
            leecher_tm.stop()
            seeder.stop()
            leecher.stop()


if __name__ == "__main__":
    unittest.main()
