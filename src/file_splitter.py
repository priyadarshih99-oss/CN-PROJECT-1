"""
file_splitter.py - Member 2: File Splitting and Reassembly
Project: CS-30003 P2P File Distribution System

This module handles:
1. Splitting a file into fixed-size chunks for distributed transfer.
2. Direct chunk retrieval by piece index (for uploading/seeding peers).
3. Storing downloaded pieces in temporary piece storage.
4. Reassembling pieces into the complete file with cryptographic integrity checks.
"""

import os
from typing import Tuple, List
from src.metadata import FileMetadata


class FileSplitter:
    """
    Handles file splitting into chunks, direct piece extraction,
    and piece reassembly for the P2P file distribution network.
    """

    @staticmethod
    def get_piece_filename(file_name: str, piece_index: int) -> str:
        """Returns standard filename convention for a piece."""
        return f"{file_name}.part_{piece_index:05d}"

    @classmethod
    def split_file(
        cls, file_path: str, output_dir: str, piece_size: int = FileMetadata.DEFAULT_PIECE_SIZE
    ) -> Tuple[FileMetadata, List[str]]:
        """
        Splits a file into individual chunk files stored in output_dir,
        and generates the corresponding FileMetadata.
        Returns: (FileMetadata, list_of_piece_filepaths)
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        os.makedirs(output_dir, exist_ok=True)
        metadata = FileMetadata.create_from_file(file_path, piece_size=piece_size)
        file_name = metadata.file_name

        piece_paths = []
        with open(file_path, "rb") as f:
            for piece_index in range(metadata.total_pieces):
                chunk_data = f.read(piece_size)
                part_filename = cls.get_piece_filename(file_name, piece_index)
                part_path = os.path.join(output_dir, part_filename)

                with open(part_path, "wb") as part_file:
                    part_file.write(chunk_data)

                piece_paths.append(part_path)

        return metadata, piece_paths

    @classmethod
    def read_piece_from_file(cls, file_path: str, piece_index: int, piece_size: int) -> bytes:
        """
        Reads a piece directly from a full file by seeking to (piece_index * piece_size).
        This allows an uploading peer to serve chunk requests without pre-splitting
        the entire file on disk.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        offset = piece_index * piece_size
        file_size = os.path.getsize(file_path)

        if offset >= file_size and file_size > 0:
            raise IndexError(f"Piece index {piece_index} out of bounds for file of size {file_size}")

        with open(file_path, "rb") as f:
            f.seek(offset)
            return f.read(piece_size)

    @classmethod
    def save_piece(cls, pieces_dir: str, file_name: str, piece_index: int, data: bytes) -> str:
        """
        Saves a received chunk to disk in the peer's pieces cache directory.
        Returns the path of the saved chunk file.
        """
        os.makedirs(pieces_dir, exist_ok=True)
        part_name = cls.get_piece_filename(file_name, piece_index)
        part_path = os.path.join(pieces_dir, part_name)

        with open(part_path, "wb") as f:
            f.write(data)

        return part_path

    @classmethod
    def read_piece_from_storage(cls, pieces_dir: str, file_name: str, piece_index: int) -> bytes:
        """
        Reads a piece previously stored in the pieces cache directory.
        """
        part_name = cls.get_piece_filename(file_name, piece_index)
        part_path = os.path.join(pieces_dir, part_name)
        if not os.path.isfile(part_path):
            raise FileNotFoundError(f"Piece {piece_index} not found at {part_path}")

        with open(part_path, "rb") as f:
            return f.read()

    @classmethod
    def has_piece_in_storage(cls, pieces_dir: str, file_name: str, piece_index: int) -> bool:
        """Checks if a piece is already saved locally."""
        part_name = cls.get_piece_filename(file_name, piece_index)
        part_path = os.path.join(pieces_dir, part_name)
        return os.path.isfile(part_path)

    @classmethod
    def reassemble_file(
        cls,
        metadata: FileMetadata,
        pieces_dir: str,
        output_file_path: str,
        cleanup_pieces: bool = False,
    ) -> bool:
        """
        Reassembles all chunk pieces into the destination file.
        Verifies the SHA-256 integrity of the complete reconstructed file.
        Returns True if successful and valid, False otherwise.
        """
        os.makedirs(os.path.dirname(os.path.abspath(output_file_path)), exist_ok=True)

        # First verify all pieces exist
        for i in range(metadata.total_pieces):
            if not cls.has_piece_in_storage(pieces_dir, metadata.file_name, i):
                raise FileNotFoundError(
                    f"Cannot reassemble '{metadata.file_name}': missing piece {i}/{metadata.total_pieces}"
                )

        # Assemble the file sequentially
        with open(output_file_path, "wb") as out_f:
            for i in range(metadata.total_pieces):
                piece_data = cls.read_piece_from_storage(pieces_dir, metadata.file_name, i)
                out_f.write(piece_data)

        # Validate file hash
        is_valid = metadata.verify_file(output_file_path)
        if not is_valid:
            return False

        # Optional cleanup of piece parts
        if cleanup_pieces:
            for i in range(metadata.total_pieces):
                part_name = cls.get_piece_filename(metadata.file_name, i)
                part_path = os.path.join(pieces_dir, part_name)
                try:
                    os.remove(part_path)
                except OSError:
                    pass

        return True
