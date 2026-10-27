"""
piece_manager.py - Member 2: Piece-Request Logic and Swarm State
Project: CS-30003 P2P File Distribution System

This module handles:
1. Local bitfield tracking (which pieces this peer owns vs needs).
2. Remote peer bitfield tracking (what pieces each connected peer has).
3. Piece-request logic with pluggable strategies:
   - RAREST_FIRST: picks missing pieces with lowest frequency across the swarm
   - SEQUENTIAL: picks missing pieces in numerical order (0, 1, 2, ...)
   - RANDOM: picks randomly among missing pieces available on the peer
4. In-flight request management with timeout and retry handling (resolving Risk 2).
5. Thread-safe operations for concurrent multi-peer downloads (Member 3).
"""

import time
import random
import threading
from typing import List, Dict, Set, Optional, Any
from src.metadata import FileMetadata
from src.protocol import MessageType, create_message


class PieceManager:
    """
    Manages piece state, peer piece availability, and piece request decisions.
    Thread-safe for use with multi-threaded peer transfer workers.
    """

    STRATEGY_RAREST_FIRST = "rarest_first"
    STRATEGY_SEQUENTIAL = "sequential"
    STRATEGY_RANDOM = "random"

    def __init__(self, metadata: FileMetadata, request_timeout: float = 10.0):
        self.metadata: FileMetadata = metadata
        self.total_pieces: int = metadata.total_pieces
        self.file_name: str = metadata.file_name
        self.request_timeout: float = request_timeout

        self._lock = threading.Lock()

        # Local piece ownership: True if piece is downloaded and verified
        self.owned_pieces: List[bool] = [False] * self.total_pieces

        # Map of peer_id -> Set[piece_index] of pieces available at that peer
        self.peer_pieces: Dict[str, Set[int]] = {}

        # In-flight requests: piece_index -> {"peer_id": str, "timestamp": float}
        self.in_flight: Dict[int, Dict[str, Any]] = {}

        # Transfer log for audit / experiment analysis (Member 4)
        self.request_logs: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # Local State & Bitfield
    # -------------------------------------------------------------------------

    def mark_all_owned(self) -> None:
        """Call this if the peer is a seeder who already has the complete file."""
        with self._lock:
            self.owned_pieces = [True] * self.total_pieces

    def mark_piece_received(self, piece_index: int) -> None:
        """Marks a piece as downloaded, verified, and locally owned."""
        with self._lock:
            if 0 <= piece_index < self.total_pieces:
                self.owned_pieces[piece_index] = True
                self.in_flight.pop(piece_index, None)
                self.request_logs.append({
                    "event": "PIECE_RECEIVED",
                    "piece_index": piece_index,
                    "timestamp": time.time(),
                })

    def mark_piece_failed(self, piece_index: int, peer_id: Optional[str] = None, reason: str = "") -> None:
        """
        Releases an in-flight piece back to the pool so it can be re-requested
        from another peer (implements Risk 2 fallback).
        """
        with self._lock:
            self.in_flight.pop(piece_index, None)
            self.request_logs.append({
                "event": "PIECE_FAILED",
                "piece_index": piece_index,
                "peer_id": peer_id,
                "reason": reason,
                "timestamp": time.time(),
            })

    def has_piece(self, piece_index: int) -> bool:
        """Returns True if the piece is already owned."""
        with self._lock:
            return 0 <= piece_index < self.total_pieces and self.owned_pieces[piece_index]

    def is_complete(self) -> bool:
        """Returns True if all pieces have been received."""
        with self._lock:
            return all(self.owned_pieces)

    def get_progress(self) -> float:
        """Returns download completion ratio from 0.0 to 1.0."""
        with self._lock:
            if self.total_pieces == 0:
                return 1.0
            return sum(self.owned_pieces) / self.total_pieces

    def get_bitfield(self) -> List[int]:
        """Returns the list of piece indices owned by this peer."""
        with self._lock:
            return [i for i, owned in enumerate(self.owned_pieces) if owned]

    def get_missing_pieces(self) -> List[int]:
        """Returns indices of all pieces not yet downloaded."""
        with self._lock:
            return [i for i, owned in enumerate(self.owned_pieces) if not owned]

    # -------------------------------------------------------------------------
    # Peer Bitfield Management
    # -------------------------------------------------------------------------

    def register_peer(self, peer_id: str, available_pieces: List[int]) -> None:
        """
        Registers or updates the list of pieces available at a remote peer.
        """
        with self._lock:
            valid_pieces = {p for p in available_pieces if 0 <= p < self.total_pieces}
            self.peer_pieces[peer_id] = valid_pieces

    def update_peer_piece(self, peer_id: str, piece_index: int) -> None:
        """Updates that a peer has acquired a new piece (e.g. via HAVE message)."""
        with self._lock:
            if peer_id not in self.peer_pieces:
                self.peer_pieces[peer_id] = set()
            if 0 <= piece_index < self.total_pieces:
                self.peer_pieces[peer_id].add(piece_index)

    def unregister_peer(self, peer_id: str) -> None:
        """
        Removes a peer upon disconnect. Releases any pieces that were in-flight
        with this peer so other peers can be requested.
        """
        with self._lock:
            self.peer_pieces.pop(peer_id, None)
            timed_out_pieces = [
                idx for idx, info in self.in_flight.items() if info["peer_id"] == peer_id
            ]
            for idx in timed_out_pieces:
                del self.in_flight[idx]
                self.request_logs.append({
                    "event": "PEER_DISCONNECTED_RETRY",
                    "piece_index": idx,
                    "peer_id": peer_id,
                    "timestamp": time.time(),
                })

    # -------------------------------------------------------------------------
    # Piece Request Logic & Selection Strategies
    # -------------------------------------------------------------------------

    def _clean_expired_in_flight(self) -> None:
        """Helper to release pieces whose requests timed out."""
        now = time.time()
        expired = [
            idx
            for idx, info in self.in_flight.items()
            if now - info["timestamp"] > self.request_timeout
        ]
        for idx in expired:
            peer = self.in_flight[idx]["peer_id"]
            del self.in_flight[idx]
            self.request_logs.append({
                "event": "REQUEST_TIMEOUT_RETRY",
                "piece_index": idx,
                "peer_id": peer,
                "timestamp": now,
            })

    def get_piece_frequencies(self) -> Dict[int, int]:
        """
        Calculates how many active peers have each piece.
        Used for Rarest-First scheduling.
        """
        freq: Dict[int, int] = {i: 0 for i in range(self.total_pieces)}
        for piece_set in self.peer_pieces.values():
            for p in piece_set:
                if p in freq:
                    freq[p] += 1
        return freq

    def select_next_piece(
        self, peer_id: str, strategy: str = STRATEGY_RAREST_FIRST
    ) -> Optional[int]:
        """
        Main decision logic for requesting a piece from a specific peer.
        Selects a piece that:
        1. We do NOT already own.
        2. Is NOT currently in-flight (or whose request timed out).
        3. The target peer actually possesses.

        Returns: piece_index (int) or None if no eligible pieces exist for this peer.
        """
        with self._lock:
            self._clean_expired_in_flight()

            peer_has = self.peer_pieces.get(peer_id, set())
            if not peer_has:
                return None

            # Eligible candidates: peer has it, we do not own it, and not currently in-flight
            candidates = [
                i
                for i in peer_has
                if not self.owned_pieces[i] and i not in self.in_flight
            ]

            if not candidates:
                return None

            if strategy == self.STRATEGY_SEQUENTIAL:
                selected_piece = min(candidates)

            elif strategy == self.STRATEGY_RANDOM:
                selected_piece = random.choice(candidates)

            elif strategy == self.STRATEGY_RAREST_FIRST:
                frequencies = self.get_piece_frequencies()
                selected_piece = min(candidates, key=lambda idx: (frequencies.get(idx, 0), idx))

            else:
                raise ValueError(f"Unknown piece selection strategy: {strategy}")

            # Mark in-flight
            self.in_flight[selected_piece] = {
                "peer_id": peer_id,
                "timestamp": time.time(),
            }
            self.request_logs.append({
                "event": "PIECE_REQUESTED",
                "piece_index": selected_piece,
                "peer_id": peer_id,
                "strategy": strategy,
                "timestamp": time.time(),
            })

            return selected_piece

    # -------------------------------------------------------------------------
    # Network Protocol Message Generators
    # -------------------------------------------------------------------------

    def create_request_message(self, sender_id: str, piece_index: int) -> Dict[str, Any]:
        """
        Generates standard protocol dictionary to ask a peer for a specific piece.
        Conforms with MessageType.PIECE_REQ in protocol.py.
        """
        return create_message(
            msg_type=getattr(MessageType, "PIECE_REQ", "PIECE_REQ"),
            sender_id=sender_id,
            payload={
                "file_name": self.file_name,
                "piece_index": piece_index,
            },
        )

    def create_have_message(self, sender_id: str, piece_index: int) -> Dict[str, Any]:
        """
        Generates standard HAVE broadcast message indicating a newly downloaded piece.
        """
        return create_message(
            msg_type=getattr(MessageType, "HAVE_PIECE", "HAVE_PIECE"),
            sender_id=sender_id,
            payload={
                "file_name": self.file_name,
                "piece_index": piece_index,
            },
        )

    def create_bitfield_message(self, sender_id: str) -> Dict[str, Any]:
        """
        Generates initial handshake bitfield message advertising owned pieces.
        """
        return create_message(
            msg_type=getattr(MessageType, "BITFIELD", "BITFIELD"),
            sender_id=sender_id,
            payload={
                "file_name": self.file_name,
                "available_pieces": self.get_bitfield(),
                "total_pieces": self.total_pieces,
            },
        )

    def get_status_summary(self) -> Dict[str, Any]:
        """Returns real-time status summary for logging and UI / Member 4."""
        with self._lock:
            owned = sum(self.owned_pieces)
            return {
                "file_name": self.file_name,
                "total_pieces": self.total_pieces,
                "owned_pieces": owned,
                "missing_pieces": self.total_pieces - owned,
                "in_flight": len(self.in_flight),
                "progress_percent": round((owned / self.total_pieces) * 100, 2)
                if self.total_pieces > 0
                else 100.0,
                "connected_peers": list(self.peer_pieces.keys()),
            }
