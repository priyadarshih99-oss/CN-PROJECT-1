"""
Unit tests for Member 2 modules:
- FileMetadata (Metadata creation, SHA-256 verification, JSON serialization)
- FileSplitter (Splitting, direct offset chunk extraction, piece storage & reassembly)
- PieceManager (Local bitfield, remote peer bitfields, Rarest-First scheduling, timeout retry)
"""

import os
import shutil
import unittest
import tempfile
import hashlib
from src.metadata import FileMetadata
from src.file_splitter import FileSplitter
from src.piece_manager import PieceManager
from src.protocol import MessageType


class TestMember2Modules(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sample_file = os.path.join(self.test_dir, "sample.bin")
        self.piece_size = 16 * 1024  # 16 KB chunks

        # Generate a test payload of 50 KB (3 chunks: 16KB, 16KB, 18KB)
        self.file_size = 50 * 1024
        pattern = b"MEMBER_2_P2P_FILE_DISTRIBUTION_SYSTEM_TEST_"
        repeats = (self.file_size // len(pattern)) + 1
        self.file_data = (pattern * repeats)[:self.file_size]

        with open(self.sample_file, "wb") as f:
            f.write(self.file_data)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_metadata_creation_and_serialization(self):
        meta = FileMetadata.create_from_file(self.sample_file, piece_size=self.piece_size)
        self.assertEqual(meta.file_name, "sample.bin")
        self.assertEqual(meta.file_size, self.file_size)
        self.assertEqual(meta.piece_size, self.piece_size)
        self.assertEqual(meta.total_pieces, 4)  # 50KB / 16KB = ceil(3.125) = 4
        self.assertEqual(len(meta.piece_hashes), 4)

        # Check whole-file hash
        expected_hash = hashlib.sha256(self.file_data).hexdigest()
        self.assertEqual(meta.file_hash, expected_hash)

        # Check JSON serialization & deserialization
        json_str = meta.to_json()
        restored = FileMetadata.from_json(json_str)
        self.assertEqual(restored.file_hash, meta.file_hash)
        self.assertEqual(restored.total_pieces, meta.total_pieces)

        # Check manifest save and reload
        manifest_path = os.path.join(self.test_dir, "sample.meta")
        meta.save_file(manifest_path)
        loaded = FileMetadata.load_file(manifest_path)
        self.assertEqual(loaded.file_hash, meta.file_hash)

    def test_file_splitter_and_reassembly(self):
        meta = FileMetadata.create_from_file(self.sample_file, piece_size=self.piece_size)
        pieces_dir = os.path.join(self.test_dir, "pieces")

        # 1. Direct offset extraction for seeder upload
        piece_0 = FileSplitter.read_piece_from_file(self.sample_file, 0, self.piece_size)
        self.assertEqual(len(piece_0), self.piece_size)
        self.assertTrue(meta.verify_piece(0, piece_0))

        # 2. Save all pieces to storage
        for i in range(meta.total_pieces):
            chunk = FileSplitter.read_piece_from_file(self.sample_file, i, self.piece_size)
            FileSplitter.save_piece(pieces_dir, meta.file_name, i, chunk)
            self.assertTrue(FileSplitter.has_piece_in_storage(pieces_dir, meta.file_name, i))

        # 3. Reassemble
        output_file = os.path.join(self.test_dir, "reconstructed.bin")
        success = FileSplitter.reassemble_file(meta, pieces_dir, output_file)
        self.assertTrue(success)

        # Verify bit-for-bit equality
        with open(output_file, "rb") as f:
            reconstructed_data = f.read()
        self.assertEqual(self.file_data, reconstructed_data)

    def test_piece_manager_rarest_first_and_failure(self):
        meta = FileMetadata.create_from_file(self.sample_file, piece_size=self.piece_size)
        pm = PieceManager(metadata=meta, request_timeout=1.0)

        # Swarm piece availability:
        # Peer-1 has: [0, 1]
        # Peer-2 has: [0, 2]
        # Peer-3 has: [1, 2, 3] -> Piece 3 is ONLY on Peer-3 (rarest!)
        pm.register_peer("Peer-1", [0, 1])
        pm.register_peer("Peer-2", [0, 2])
        pm.register_peer("Peer-3", [1, 2, 3])

        freq = pm.get_piece_frequencies()
        self.assertEqual(freq[3], 1)
        self.assertEqual(freq[0], 2)

        # Rarest-first for Peer-3 should pick Piece 3 first
        chosen = pm.select_next_piece("Peer-3", strategy=PieceManager.STRATEGY_RAREST_FIRST)
        self.assertEqual(chosen, 3)

        # In-flight check: Piece 3 is currently requested, so Peer-3 should not be asked for Piece 3 again
        next_chosen = pm.select_next_piece("Peer-3", strategy=PieceManager.STRATEGY_RAREST_FIRST)
        self.assertIn(next_chosen, [1, 2])

        # Test failure & retry (Risk 2 fallback)
        pm.mark_piece_failed(3, peer_id="Peer-3", reason="Dropped connection")
        self.assertNotIn(3, pm.in_flight)

        # Test protocol message generators
        req_msg = pm.create_request_message("LocalPeer", 3)
        self.assertEqual(req_msg["type"], MessageType.PIECE_REQ)
        self.assertEqual(req_msg["payload"]["piece_index"], 3)


if __name__ == "__main__":
    unittest.main()
