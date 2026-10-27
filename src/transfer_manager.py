"""
transfer_manager.py - Parallel Transfer Engine, Verification & Swarm Coordination
CS-30003 P2P File Distribution System

Integrates:
- Member 1: Peer socket routing and message dispatch
- Member 2: Metadata verification, chunk storage, and rarest-first scheduling
- Member 3: Parallel worker threads, SHA-256 verification, and timeout failover
- Member 4: Microsecond performance telemetry hooks
"""

import os
import time
import base64
import hashlib
import threading
from typing import Dict, Any, Optional, Callable, List, Set

from src.protocol import MessageType, create_message
from src.connection import PeerConnection
from src.peer import Peer
from src.metadata import FileMetadata
from src.file_splitter import FileSplitter
from src.piece_manager import PieceManager


class TransferManager:
    """
    Manages parallel uploading, downloading, verification, and piece coordination
    on top of Member 1's Peer node and Member 2's PieceManager.
    """

    def __init__(self, peer: Peer, pieces_root_dir: str = "./pieces_cache"):
        self.peer = peer
        self.pieces_root_dir = pieces_root_dir
        os.makedirs(self.pieces_root_dir, exist_ok=True)

        self._lock = threading.Lock()

        # {file_hash: {"metadata": FileMetadata, "file_path": str, "piece_manager": PieceManager, "is_seeder": bool}}
        self.files: Dict[str, Dict[str, Any]] = {}

        # Pending chunk responses: (file_hash, piece_index) -> threading.Event + data
        self.pending_piece_events: Dict[str, threading.Event] = {}
        self.pending_piece_data: Dict[str, Dict[str, Any]] = {}

        # Telemetry callbacks for Member 4
        self.on_piece_downloaded: Optional[Callable[[Dict[str, Any]], None]] = None
        self.on_piece_uploaded: Optional[Callable[[Dict[str, Any]], None]] = None

        # Register message handlers on Member 1's Peer
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.peer.register_handler(MessageType.METADATA_REQ, self._handle_metadata_req)
        self.peer.register_handler(MessageType.METADATA_RESP, self._handle_metadata_resp)
        self.peer.register_handler(MessageType.BITFIELD, self._handle_bitfield)
        self.peer.register_handler(MessageType.HAVE_PIECE, self._handle_have_piece)
        self.peer.register_handler(MessageType.PIECE_REQ, self._handle_piece_req)
        self.peer.register_handler(MessageType.PIECE_RESP, self._handle_piece_resp)

    def register_seeder_file(self, file_path: str, piece_size: int = FileMetadata.DEFAULT_PIECE_SIZE) -> FileMetadata:
        """Registers a completed file locally as a seeder."""
        metadata = FileMetadata.create_from_file(file_path, piece_size=piece_size)
        pm = PieceManager(metadata)
        pm.mark_all_owned()

        with self._lock:
            self.files[metadata.file_hash] = {
                "metadata": metadata,
                "file_path": file_path,
                "piece_manager": pm,
                "is_seeder": True
            }

        self.peer.logger.info(f"Registered seeder file [{metadata.file_name}] ({metadata.total_pieces} pieces, Hash: {metadata.file_hash[:12]}...)")
        return metadata

    def broadcast_bitfield(self, file_hash: str) -> None:
        """Broadcasts local piece ownership bitfield for a file to all connected peers."""
        with self._lock:
            file_entry = self.files.get(file_hash)
        if not file_entry:
            return

        pm: PieceManager = file_entry["piece_manager"]
        bitfield = pm.get_bitfield()

        self.peer.broadcast_message(
            MessageType.BITFIELD,
            {"file_hash": file_hash, "bitfield": bitfield}
        )

    # ------------------ Message Handlers ------------------ #

    def _handle_metadata_req(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        file_hash = message.get("payload", {}).get("file_hash")
        with self._lock:
            entry = self.files.get(file_hash)
        if entry:
            meta: FileMetadata = entry["metadata"]
            conn.send(create_message(MessageType.METADATA_RESP, self.peer.peer_id, {
                "file_hash": file_hash,
                "metadata": meta.to_dict()
            }))

    def _handle_metadata_resp(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        pass

    def _handle_bitfield(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        sender_id = message.get("sender_id", conn.peer_id)
        payload = message.get("payload", {})
        file_hash = payload.get("file_hash")
        bitfield = payload.get("bitfield", [])

        with self._lock:
            entry = self.files.get(file_hash)
        if entry:
            pm: PieceManager = entry["piece_manager"]
            pm.register_peer(sender_id, bitfield)
            self.peer.logger.debug(f"Updated bitfield for peer [{sender_id}] on file [{file_hash[:8]}]: {len(bitfield)} pieces available.")

    def _handle_have_piece(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        sender_id = message.get("sender_id", conn.peer_id)
        payload = message.get("payload", {})
        file_hash = payload.get("file_hash")
        piece_index = payload.get("piece_index")

        with self._lock:
            entry = self.files.get(file_hash)
        if entry and piece_index is not None:
            pm: PieceManager = entry["piece_manager"]
            pm.update_peer_piece(sender_id, piece_index)

    def _handle_piece_req(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        t_req = time.perf_counter()
        sender_id = message.get("sender_id", conn.peer_id)
        payload = message.get("payload", {})
        file_hash = payload.get("file_hash")
        piece_index = payload.get("piece_index")

        with self._lock:
            entry = self.files.get(file_hash)

        if not entry:
            return

        pm: PieceManager = entry["piece_manager"]
        if not pm.has_piece(piece_index):
            return

        meta: FileMetadata = entry["metadata"]
        file_path = entry["file_path"]

        # Read piece either directly from source file or pieces cache
        try:
            if entry["is_seeder"] and os.path.isfile(file_path):
                chunk_bytes = FileSplitter.read_piece_from_file(file_path, piece_index, meta.piece_size)
            else:
                pieces_dir = os.path.join(self.pieces_root_dir, file_hash)
                chunk_bytes = FileSplitter.read_piece_from_storage(pieces_dir, meta.file_name, piece_index)

            # Send response with base64 payload
            b64_data = base64.b64encode(chunk_bytes).decode("ascii")
            resp_msg = create_message(MessageType.PIECE_RESP, self.peer.peer_id, {
                "file_hash": file_hash,
                "piece_index": piece_index,
                "data": b64_data,
                "sha256": meta.piece_hashes[piece_index]
            })
            conn.send(resp_msg)

            duration = time.perf_counter() - t_req
            if self.on_piece_uploaded:
                self.on_piece_uploaded({
                    "seeder_id": self.peer.peer_id,
                    "target_peer_id": sender_id,
                    "file_hash": file_hash,
                    "piece_index": piece_index,
                    "bytes": len(chunk_bytes),
                    "duration": duration,
                    "timestamp": time.time()
                })
        except Exception as e:
            self.peer.logger.error(f"Error serving piece {piece_index} to [{sender_id}]: {e}")

    def _handle_piece_resp(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        sender_id = message.get("sender_id", conn.peer_id)
        payload = message.get("payload", {})
        file_hash = payload.get("file_hash")
        piece_index = payload.get("piece_index")
        b64_data = payload.get("data")
        expected_hash = payload.get("sha256")

        if piece_index is None or not b64_data:
            return

        event_key = f"{file_hash}:{piece_index}"
        with self._lock:
            self.pending_piece_data[event_key] = {
                "sender_id": sender_id,
                "b64_data": b64_data,
                "expected_hash": expected_hash,
                "received_at": time.time()
            }
            evt = self.pending_piece_events.get(event_key)
            if evt:
                evt.set()

    # ------------------ Client Download Engine ------------------ #

    def download_file(
        self,
        metadata: FileMetadata,
        destination_path: str,
        timeout: float = 60.0,
        strategy: str = PieceManager.STRATEGY_RAREST_FIRST,
        max_workers: int = 8
    ) -> bool:
        """
        Executes parallel piece retrieval across all connected peers.
        Verifies SHA-256 for each piece, stores pieces on disk, and reassembles
        the complete file once all pieces arrive.
        """
        t_start = time.perf_counter()
        file_hash = metadata.file_hash
        pieces_dir = os.path.join(self.pieces_root_dir, file_hash)
        os.makedirs(pieces_dir, exist_ok=True)

        pm = PieceManager(metadata, request_timeout=5.0)
        with self._lock:
            self.files[file_hash] = {
                "metadata": metadata,
                "file_path": destination_path,
                "piece_manager": pm,
                "is_seeder": False
            }

        # Step 1: Discover and connect to swarm peers
        self.peer.connect_to_discovered_peers()
        time.sleep(0.05)

        # Exchange bitfields
        self.broadcast_bitfield(file_hash)

        # Step 2: Parallel Download Worker Loop
        download_finished = threading.Event()
        active_peers = list(self.peer.connections.keys())
        if not active_peers:
            self.peer.logger.error("No active peer connections available for download.")
            return False

        # Populate initial peer pieces assuming seeders have all pieces if bitfield hasn't arrived
        for pid in active_peers:
            if pid not in pm.peer_pieces or not pm.peer_pieces[pid]:
                pm.register_peer(pid, list(range(metadata.total_pieces)))

        def worker_loop(peer_id: str):
            while not download_finished.is_set() and self.peer._is_running:
                # Select candidate piece using piece manager strategy
                piece_idx = pm.select_next_piece(peer_id, strategy=strategy)
                if piece_idx is None:
                    time.sleep(0.01)
                    if pm.is_complete():
                        download_finished.set()
                        break
                    continue

                event_key = f"{file_hash}:{piece_idx}"
                evt = threading.Event()
                with self._lock:
                    self.pending_piece_events[event_key] = evt
                    self.pending_piece_data.pop(event_key, None)

                # Send PIECE_REQ
                t_req = time.perf_counter()
                success = False
                send_ok = self.peer.send_message(
                    peer_id,
                    MessageType.PIECE_REQ,
                    {"file_hash": file_hash, "piece_index": piece_idx}
                )

                if send_ok and evt.wait(timeout=4.0):
                    with self._lock:
                        data_record = self.pending_piece_data.pop(event_key, None)

                    if data_record:
                        raw_bytes = base64.b64decode(data_record["b64_data"])
                        # Cryptographic piece verification (SHA-256)
                        t_verify = time.perf_counter()
                        is_valid = metadata.verify_piece(piece_idx, raw_bytes)
                        v_duration = time.perf_counter() - t_verify

                        if is_valid:
                            # Save chunk
                            FileSplitter.save_piece(pieces_dir, metadata.file_name, piece_idx, raw_bytes)
                            pm.mark_piece_received(piece_idx)
                            success = True

                            # Broadcast HAVE_PIECE to swarm
                            self.peer.broadcast_message(
                                MessageType.HAVE_PIECE,
                                {"file_hash": file_hash, "piece_index": piece_idx}
                            )

                            rtt = time.perf_counter() - t_req
                            if self.on_piece_downloaded:
                                self.on_piece_downloaded({
                                    "receiver_id": self.peer.peer_id,
                                    "source_peer_id": peer_id,
                                    "file_hash": file_hash,
                                    "piece_index": piece_idx,
                                    "bytes": len(raw_bytes),
                                    "duration": rtt,
                                    "verification_time": v_duration,
                                    "timestamp": time.time(),
                                    "status": "SUCCESS"
                                })
                        else:
                            self.peer.logger.warning(f"Piece {piece_idx} received from [{peer_id}] failed SHA-256 integrity check!")
                            pm.mark_piece_failed(piece_idx, peer_id, "CORRUPT_HASH")
                    else:
                        pm.mark_piece_failed(piece_idx, peer_id, "NO_DATA")
                else:
                    pm.mark_piece_failed(piece_idx, peer_id, "TIMEOUT")

                with self._lock:
                    self.pending_piece_events.pop(event_key, None)

                if pm.is_complete():
                    download_finished.set()
                    break

        threads = []
        for pid in active_peers[:max_workers]:
            t = threading.Thread(target=worker_loop, args=(pid,), daemon=True)
            threads.append(t)
            t.start()

        # Monitor deadline
        deadline = time.time() + timeout
        while time.time() < deadline and not download_finished.is_set():
            if pm.is_complete():
                download_finished.set()
                break
            time.sleep(0.02)

        download_finished.set()
        for t in threads:
            t.join(timeout=0.3)

        if not pm.is_complete():
            self.peer.logger.error(f"Download timed out! Progress: {pm.get_progress()*100:.1f}%")
            return False

        # Reassemble the file
        os.makedirs(os.path.dirname(os.path.abspath(destination_path)), exist_ok=True)
        FileSplitter.reassemble_file(metadata, pieces_dir, destination_path)
        is_intact = metadata.verify_file(destination_path)

        t_total = time.perf_counter() - t_start
        mb_rate = (metadata.file_size / (1024 * 1024)) / max(t_total, 0.0001)
        self.peer.logger.info(f"File [{metadata.file_name}] reassembled & verified: Intact={is_intact} in {t_total:.3f}s ({mb_rate:.2f} MB/s)")
        return is_intact
