"""
metadata.py - Member 2: File Metadata Management
Project: CS-30003 P2P File Distribution System

This module handles:
1. Generating metadata for files to be shared (file size, chunk size, piece hashes, whole-file hash).
2. Serializing metadata to/from JSON for TCP network exchange with other peers.
3. Loading and saving `.meta` manifest files.
4. Verifying piece hash integrity against known metadata.
"""

import os
import json
import math
import time
import hashlib
from typing import List, Dict, Any, Optional


class FileMetadata:
    """
    Represents metadata for a file in the P2P distribution network.
    Contains structural information and cryptographic hashes of all pieces.
    """

    DEFAULT_PIECE_SIZE = 64 * 1024  # 64 KB default chunk size

    def __init__(
        self,
        file_name: str,
        file_size: int,
        piece_size: int,
        total_pieces: int,
        file_hash: str,
        piece_hashes: List[str],
        created_at: Optional[float] = None,
    ):
        self.file_name: str = file_name
        self.file_size: int = file_size
        self.piece_size: int = piece_size
        self.total_pieces: int = total_pieces
        self.file_hash: str = file_hash  # SHA-256 hex of the entire file
        self.piece_hashes: List[str] = piece_hashes  # SHA-256 hex for each chunk
        self.created_at: float = created_at if created_at is not None else time.time()

    @classmethod
    def create_from_file(cls, file_path: str, piece_size: int = DEFAULT_PIECE_SIZE) -> "FileMetadata":
        """
        Reads a local file and generates the metadata, including SHA-256 hashes
        for each chunk and the whole file.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        total_pieces = math.ceil(file_size / piece_size) if file_size > 0 else 1

        overall_hasher = hashlib.sha256()
        piece_hashes = []

        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(piece_size)
                if not chunk:
                    # In case of empty file
                    if file_size == 0 and len(piece_hashes) == 0:
                        empty_hash = hashlib.sha256(b"").hexdigest()
                        piece_hashes.append(empty_hash)
                        overall_hasher.update(b"")
                    break
                
                overall_hasher.update(chunk)
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                piece_hashes.append(chunk_hash)

        return cls(
            file_name=file_name,
            file_size=file_size,
            piece_size=piece_size,
            total_pieces=len(piece_hashes),
            file_hash=overall_hasher.hexdigest(),
            piece_hashes=piece_hashes,
        )

    def verify_piece(self, piece_index: int, piece_data: bytes) -> bool:
        """
        Validates if the provided piece data matches the registered SHA-256 hash.
        Used by piece logic and verification layers.
        """
        if piece_index < 0 or piece_index >= self.total_pieces:
            return False
        computed_hash = hashlib.sha256(piece_data).hexdigest()
        return computed_hash == self.piece_hashes[piece_index]

    def verify_file(self, file_path: str) -> bool:
        """
        Verifies if an assembled file matches the full-file hash.
        """
        if not os.path.isfile(file_path):
            return False
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(self.piece_size):
                hasher.update(chunk)
        return hasher.hexdigest() == self.file_hash

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary format for JSON serialization."""
        return {
            "file_name": self.file_name,
            "file_size": self.file_size,
            "piece_size": self.piece_size,
            "total_pieces": self.total_pieces,
            "file_hash": self.file_hash,
            "piece_hashes": self.piece_hashes,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileMetadata":
        """Reconstruct a FileMetadata instance from dictionary data."""
        return cls(
            file_name=data["file_name"],
            file_size=int(data["file_size"]),
            piece_size=int(data["piece_size"]),
            total_pieces=int(data["total_pieces"]),
            file_hash=data["file_hash"],
            piece_hashes=list(data["piece_hashes"]),
            created_at=data.get("created_at", time.time()),
        )

    def to_json(self, indent: Optional[int] = None) -> str:
        """Serializes metadata to JSON string for network transmission."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, json_str: str) -> "FileMetadata":
        """Parses metadata from a JSON string received over TCP."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def save_file(self, meta_path: str) -> None:
        """Saves metadata manifest to a file (e.g. filename.meta)."""
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=4)

    @classmethod
    def load_file(cls, meta_path: str) -> "FileMetadata":
        """Loads metadata manifest from a file."""
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            f"<FileMetadata name='{self.file_name}', size={self.file_size} bytes, "
            f"pieces={self.total_pieces}, piece_size={self.piece_size} bytes>"
        )
