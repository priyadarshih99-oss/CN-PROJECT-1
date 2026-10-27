"""
Piece Verification and Cryptographic Integrity Layer.

Handles:
- SHA-256 piece-level cryptographic hash computation
- Checksum verification for incoming chunks
- Corrupt chunk detection and rejection
- Safe binary data Base64 encoding/decoding for JSON transport
- Whole-file integrity verification
"""

import base64
import hashlib
import os
from typing import Optional, List


class PieceVerifier:
    """
    Cryptographic verification utilities for individual pieces and full files.
    Ensures data integrity during distributed P2P file transfers.
    """

    @staticmethod
    def compute_hash(data: bytes) -> str:
        """Computes the SHA-256 hexadecimal hash string for a byte buffer."""
        hasher = hashlib.sha256()
        hasher.update(data)
        return hasher.hexdigest().lower()

    @staticmethod
    def verify_piece(data: bytes, expected_hash: str) -> bool:
        """
        Verifies that piece byte data matches the expected SHA-256 checksum.
        Returns True if hash matches, False if data is corrupt or missing.
        """
        if data is None or not expected_hash:
            return False
        actual_hash = PieceVerifier.compute_hash(data)
        return actual_hash == expected_hash.strip().lower()

    @staticmethod
    def compute_file_hash(file_path: str, chunk_size: int = 64 * 1024) -> str:
        """Computes the full SHA-256 hash of a file on disk."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest().lower()

    @staticmethod
    def verify_file(file_path: str, expected_hash: str) -> bool:
        """Verifies if the full file on disk matches expected SHA-256 checksum."""
        try:
            return PieceVerifier.compute_file_hash(file_path) == expected_hash.strip().lower()
        except (FileNotFoundError, OSError):
            return False

    @staticmethod
    def compute_piece_hashes(data: bytes, piece_size: int) -> List[str]:
        """Splits raw byte data into pieces and returns the SHA-256 hash of each piece."""
        hashes = []
        total_len = len(data)
        for offset in range(0, total_len, piece_size):
            chunk = data[offset : offset + piece_size]
            hashes.append(PieceVerifier.compute_hash(chunk))
        return hashes

    @staticmethod
    def encode_data(data: bytes) -> str:
        """Encodes raw binary piece bytes to Base64 ASCII string for JSON serialization."""
        return base64.b64encode(data).decode("ascii")

    @staticmethod
    def decode_data(b64_string: str) -> bytes:
        """Decodes Base64 ASCII string back to raw binary bytes."""
        return base64.b64decode(b64_string.encode("ascii"))
