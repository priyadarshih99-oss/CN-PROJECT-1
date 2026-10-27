"""
Parallel Upload/Download, Piece Verification, and Failure Handling.

This module implements:
1. Parallel piece uploading with non-blocking thread workers.
2. Parallel piece downloading across multiple peer connections.
3. Cryptographic piece verification (SHA-256) and corrupt chunk rejection.
4. Robust failure handling:
   - Request timeouts
   - Peer disconnection / socket drop recovery
   - Corrupt piece detection & automatic re-requesting
   - Peer penalization & dynamic alternate peer selection
   - Graceful transfer cancellation
5. File splitting and reassembly.
"""

import math
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Callable, Tuple

from src.protocol import MessageType, create_message
from src.connection import PeerConnection
from src.verification import PieceVerifier
from src.utils import setup_logger


@dataclass
class DownloadResult:
    """Detailed summary of a completed or failed download session."""
    file_id: str
    success: bool
    total_pieces: int
    verified_pieces: int
    failed_pieces: int
    retry_count: int
    total_bytes: int
    elapsed_time: float
    throughput_kbps: float
    error_message: Optional[str] = None
    output_path: Optional[str] = None
    data: Optional[bytes] = None


class SharedFile:
    """
    Represents a file registered for seeding/uploading to other peers.
    Pieces can be read on-demand from disk or from an in-memory buffer.
    """

    def __init__(
        self,
        file_id: str,
        file_path: Optional[str] = None,
        data: Optional[bytes] = None,
        piece_size: int = 256 * 1024,
    ):
        self.file_id = file_id
        self.file_path = file_path
        self.piece_size = piece_size
        self._lock = threading.Lock()

        if data is not None:
            self._in_memory_data: Optional[bytes] = data
            self.file_size = len(data)
        elif file_path and os.path.exists(file_path):
            self._in_memory_data = None
            self.file_size = os.path.getsize(file_path)
        else:
            raise ValueError("Either valid file_path or data buffer must be provided.")

        self.total_pieces = max(1, math.ceil(self.file_size / self.piece_size)) if self.file_size > 0 else 0
        self.piece_hashes: List[str] = self._calculate_all_hashes()

    def _calculate_all_hashes(self) -> List[str]:
        """Calculates SHA-256 hashes for all pieces."""
        hashes = []
        for idx in range(self.total_pieces):
            chunk = self.read_piece(idx)
            if chunk is not None:
                hashes.append(PieceVerifier.compute_hash(chunk))
            else:
                hashes.append("")
        return hashes

    def read_piece(self, piece_index: int) -> Optional[bytes]:
        """Reads the raw bytes for a specific piece index."""
        if piece_index < 0 or piece_index >= self.total_pieces:
            return None

        if self._in_memory_data is not None:
            start = piece_index * self.piece_size
            end = min(start + self.piece_size, self.file_size)
            return self._in_memory_data[start:end]

        if self.file_path and os.path.exists(self.file_path):
            with self._lock:
                try:
                    with open(self.file_path, "rb") as f:
                        f.seek(piece_index * self.piece_size)
                        return f.read(self.piece_size)
                except OSError:
                    return None

        return None


class UploadManager:
    """
    Manages piece uploading to requesting peers.
    Uses a thread pool to handle concurrent piece upload requests without blocking
    the peer's network socket receiving thread.
    """

    def __init__(self, peer_id: str, max_workers: int = 8):
        self.peer_id = peer_id
        self.logger = setup_logger(f"UploadManager-{peer_id}")
        self.shared_files: Dict[str, SharedFile] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="UploadWorker")

        # Upload Statistics
        self.total_bytes_uploaded = 0
        self.total_pieces_served = 0
        self._stats_lock = threading.Lock()

    def register_file(self, shared_file: SharedFile) -> None:
        """Registers a shared file for uploading."""
        with self._lock:
            self.shared_files[shared_file.file_id] = shared_file
        self.logger.info(
            f"File [{shared_file.file_id}] registered for upload: "
            f"{shared_file.file_size} bytes in {shared_file.total_pieces} piece(s)."
        )

    def unregister_file(self, file_id: str) -> None:
        """Unregisters a shared file."""
        with self._lock:
            if file_id in self.shared_files:
                del self.shared_files[file_id]
                self.logger.info(f"File [{file_id}] unregistered from upload manager.")

    def handle_piece_request(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """
        Pluggable handler for MessageType.PIECE_REQ.
        Dispatches request processing to worker thread pool.
        """
        self._executor.submit(self._process_piece_request, conn, message)

    def _process_piece_request(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Processes a piece request and transmits PIECE_DATA response."""
        payload = message.get("payload", {})
        file_id = payload.get("file_id")
        piece_index = payload.get("piece_index")
        requester_id = message.get("sender_id", "Unknown")

        if file_id is None or piece_index is None:
            self.logger.warning(f"Malformed PIECE_REQ from [{requester_id}]: {payload}")
            return

        with self._lock:
            shared_file = self.shared_files.get(file_id)

        if not shared_file:
            self.logger.warning(f"Piece request for unknown file [{file_id}] from [{requester_id}].")
            err_msg = create_message(
                MessageType.PIECE_DATA,
                self.peer_id,
                {"file_id": file_id, "piece_index": piece_index, "status": "ERROR", "error": "File not found"}
            )
            conn.send(err_msg)
            return

        piece_data = shared_file.read_piece(piece_index)
        if piece_data is None:
            self.logger.warning(f"Piece [{piece_index}] not found for file [{file_id}].")
            err_msg = create_message(
                MessageType.PIECE_DATA,
                self.peer_id,
                {"file_id": file_id, "piece_index": piece_index, "status": "ERROR", "error": "Piece out of bounds"}
            )
            conn.send(err_msg)
            return

        # Encode and compute SHA-256
        b64_data = PieceVerifier.encode_data(piece_data)
        checksum = PieceVerifier.compute_hash(piece_data)

        response = create_message(
            MessageType.PIECE_DATA,
            self.peer_id,
            {
                "file_id": file_id,
                "piece_index": piece_index,
                "status": "OK",
                "data": b64_data,
                "hash": checksum,
                "size": len(piece_data),
            }
        )

        sent = conn.send(response)
        if sent:
            with self._stats_lock:
                self.total_bytes_uploaded += len(piece_data)
                self.total_pieces_served += 1
            self.logger.debug(
                f"Uploaded piece [{piece_index}] of file [{file_id}] to [{requester_id}] ({len(piece_data)} bytes)."
            )
        else:
            self.logger.warning(f"Failed to send piece [{piece_index}] to [{requester_id}]: connection dropped.")

    def stop(self) -> None:
        """Shuts down upload worker pool."""
        self._executor.shutdown(wait=False)


class ParallelDownloader:
    """
    Downloads file pieces concurrently across multiple connected peers.
    Features:
    - Parallel download threads using dynamic piece dispatching
    - SHA-256 piece verification and corruption detection
    - Request timeout management
    - Automatic retries on socket failure or corrupt piece
    - Dynamic peer penalization for failed/slow peers
    - Assembly of verified pieces into destination file or memory buffer
    """

    def __init__(self, peer_instance: Any):
        self.peer = peer_instance
        self.peer_id = peer_instance.peer_id
        self.logger = setup_logger(f"Downloader-{self.peer_id}")

        # Active downloads state: (file_id, piece_index) -> (threading.Event, response_holder)
        self._pending_requests: Dict[Tuple[str, int], Tuple[threading.Event, List[Dict[str, Any]]]] = {}
        self._pending_lock = threading.Lock()

        # Cancellation flags: file_id -> threading.Event
        self._cancel_flags: Dict[str, threading.Event] = {}

    def handle_piece_data(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """
        Pluggable handler for incoming MessageType.PIECE_DATA messages.
        Matches received piece against pending waiting requests and signals completion.
        """
        payload = message.get("payload", {})
        file_id = payload.get("file_id")
        piece_index = payload.get("piece_index")

        if file_id is None or piece_index is None:
            return

        key = (file_id, piece_index)
        with self._pending_lock:
            pending = self._pending_requests.get(key)
            if pending:
                event, resp_list = pending
                resp_list.append(payload)
                event.set()

    def handle_transfer_cancel(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Handles cancellation notices from peers."""
        payload = message.get("payload", {})
        file_id = payload.get("file_id")
        self.logger.info(f"Received TRANSFER_CANCEL for file [{file_id}] from [{message.get('sender_id')}].")

    def cancel_download(self, file_id: str) -> None:
        """Cancels an active download session."""
        with self._pending_lock:
            flag = self._cancel_flags.get(file_id)
            if flag:
                flag.set()
                self.logger.info(f"Cancellation requested for download [{file_id}].")

    def download_file(
        self,
        file_id: str,
        piece_hashes: List[str],
        piece_size: int = 256 * 1024,
        output_path: Optional[str] = None,
        target_peers: Optional[List[str]] = None,
        max_workers: int = 4,
        timeout: float = 4.0,
        max_retries: int = 3,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> DownloadResult:
        """
        Executes parallel download of all pieces of a file across available peers.

        Args:
            file_id: Unique identifier for the file.
            piece_hashes: List of expected SHA-256 hex strings for pieces 0..N-1.
            piece_size: Byte size of standard pieces.
            output_path: Path on disk to assemble the downloaded file (optional).
            target_peers: Specific peer IDs to download from. If None, uses all connected peers.
            max_workers: Number of concurrent worker threads.
            timeout: Maximum seconds to wait for a piece response before retrying.
            max_retries: Maximum download retry attempts per piece before failing.
            progress_callback: Optional callback func(verified_count, total_count).

        Returns:
            DownloadResult with metrics and verification status.
        """
        start_time = time.time()
        total_pieces = len(piece_hashes)

        cancel_event = threading.Event()
        with self._pending_lock:
            self._cancel_flags[file_id] = cancel_event

        # Resolve peer list
        if target_peers is None:
            with self.peer._conn_lock:
                available_peers = [pid for pid, c in self.peer.connections.items() if c.is_alive]
        else:
            available_peers = list(target_peers)

        if not available_peers:
            return DownloadResult(
                file_id=file_id,
                success=False,
                total_pieces=total_pieces,
                verified_pieces=0,
                failed_pieces=total_pieces,
                retry_count=0,
                total_bytes=0,
                elapsed_time=0.0,
                throughput_kbps=0.0,
                error_message="No active peers available for download.",
            )

        self.logger.info(
            f"Initiating parallel download for [{file_id}]: {total_pieces} piece(s) "
            f"using {min(max_workers, total_pieces)} worker(s) across {len(available_peers)} peer(s)."
        )

        # Download state tracking
        piece_queue: queue.Queue = queue.Queue()
        for idx in range(total_pieces):
            piece_queue.put(idx)

        downloaded_pieces: Dict[int, bytes] = {}
        piece_retries: Dict[int, int] = {idx: 0 for idx in range(total_pieces)}
        peer_penalties: Dict[str, int] = {pid: 0 for pid in available_peers}

        state_lock = threading.Lock()
        total_retries_metric = 0
        fatal_error: List[str] = []

        def worker_loop(worker_id: int):
            nonlocal total_retries_metric
            while not cancel_event.is_set() and not fatal_error:
                try:
                    piece_idx = piece_queue.get(timeout=0.2)
                except queue.Empty:
                    # Check if all pieces downloaded
                    with state_lock:
                        if len(downloaded_pieces) >= total_pieces:
                            break
                    continue

                with state_lock:
                    if piece_idx in downloaded_pieces:
                        piece_queue.task_done()
                        continue

                # Select best available peer (least penalized, alive)
                with self.peer._conn_lock:
                    live_peers = [
                        pid for pid in available_peers
                        if pid in self.peer.connections and self.peer.connections[pid].is_alive
                    ]

                if not live_peers:
                    self.logger.error("All peers disconnected during transfer.")
                    fatal_error.append("All peers disconnected.")
                    piece_queue.task_done()
                    break

                # Sort by penalty (lowest penalty first)
                with state_lock:
                    live_peers.sort(key=lambda p: peer_penalties.get(p, 0))
                    selected_peer = live_peers[worker_id % len(live_peers)]

                expected_hash = piece_hashes[piece_idx]
                success, piece_bytes, error_reason = self._request_and_verify_piece(
                    file_id=file_id,
                    piece_index=piece_idx,
                    expected_hash=expected_hash,
                    target_peer=selected_peer,
                    timeout=timeout,
                    cancel_event=cancel_event,
                )

                if success and piece_bytes is not None:
                    # Verified successfully!
                    with state_lock:
                        downloaded_pieces[piece_idx] = piece_bytes
                        # Reward peer by reducing penalty
                        if selected_peer in peer_penalties and peer_penalties[selected_peer] > 0:
                            peer_penalties[selected_peer] -= 1

                    piece_queue.task_done()

                    # Send ACK back to peer
                    self.peer.send_message(
                        selected_peer,
                        MessageType.TRANSFER_ACK,
                        {"file_id": file_id, "piece_index": piece_idx, "status": "VERIFIED"}
                    )

                    if progress_callback:
                        progress_callback(len(downloaded_pieces), total_pieces)

                else:
                    # Failure handling: timeout, corrupted chunk, or socket failure
                    with state_lock:
                        piece_retries[piece_idx] += 1
                        total_retries_metric += 1
                        # Penalize problematic peer
                        peer_penalties[selected_peer] = peer_penalties.get(selected_peer, 0) + 1
                        retries_done = piece_retries[piece_idx]

                    self.logger.warning(
                        f"[Failure Handling] Piece [{piece_idx}] failed from [{selected_peer}] "
                        f"({error_reason}). Attempt {retries_done}/{max_retries}."
                    )

                    if retries_done < max_retries and not cancel_event.is_set():
                        # Re-enqueue for retry with another peer
                        piece_queue.put(piece_idx)
                    else:
                        self.logger.error(f"Piece [{piece_idx}] exceeded maximum retries ({max_retries}). Aborting.")
                        fatal_error.append(f"Piece {piece_idx} failed after {max_retries} retries: {error_reason}")

                    piece_queue.task_done()

        # Spawn parallel workers
        num_workers = min(max_workers, total_pieces)
        threads: List[threading.Thread] = []
        for w_idx in range(num_workers):
            t = threading.Thread(target=worker_loop, args=(w_idx,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # Cleanup pending cancellation flag
        with self._pending_lock:
            if file_id in self._cancel_flags:
                del self._cancel_flags[file_id]

        elapsed = max(0.001, time.time() - start_time)

        if cancel_event.is_set():
            return DownloadResult(
                file_id=file_id,
                success=False,
                total_pieces=total_pieces,
                verified_pieces=len(downloaded_pieces),
                failed_pieces=total_pieces - len(downloaded_pieces),
                retry_count=total_retries_metric,
                total_bytes=sum(len(b) for b in downloaded_pieces.values()),
                elapsed_time=elapsed,
                throughput_kbps=0.0,
                error_message="Download cancelled by user.",
            )

        if fatal_error or len(downloaded_pieces) < total_pieces:
            err_msg = fatal_error[0] if fatal_error else "Incomplete download."
            return DownloadResult(
                file_id=file_id,
                success=False,
                total_pieces=total_pieces,
                verified_pieces=len(downloaded_pieces),
                failed_pieces=total_pieces - len(downloaded_pieces),
                retry_count=total_retries_metric,
                total_bytes=sum(len(b) for b in downloaded_pieces.values()),
                elapsed_time=elapsed,
                throughput_kbps=0.0,
                error_message=err_msg,
            )

        # Assemble file pieces in exact order 0 .. N-1
        full_buffer = bytearray()
        for idx in range(total_pieces):
            full_buffer.extend(downloaded_pieces[idx])
        assembled_data = bytes(full_buffer)
        total_bytes = len(assembled_data)

        # Write to disk if output path provided
        if output_path:
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(assembled_data)
            self.logger.info(f"File [{file_id}] assembled and written to: {output_path}")

        throughput = (total_bytes / 1024.0) / elapsed

        self.logger.info(
            f"Download completed successfully for [{file_id}]: "
            f"{total_bytes} bytes in {elapsed:.2f}s ({throughput:.2f} KB/s), {total_retries_metric} retries."
        )

        return DownloadResult(
            file_id=file_id,
            success=True,
            total_pieces=total_pieces,
            verified_pieces=total_pieces,
            failed_pieces=0,
            retry_count=total_retries_metric,
            total_bytes=total_bytes,
            elapsed_time=elapsed,
            throughput_kbps=throughput,
            output_path=output_path,
            data=assembled_data,
        )

    def _request_and_verify_piece(
        self,
        file_id: str,
        piece_index: int,
        expected_hash: str,
        target_peer: str,
        timeout: float,
        cancel_event: threading.Event,
    ) -> Tuple[bool, Optional[bytes], str]:
        """
        Sends PIECE_REQ, awaits PIECE_DATA, and verifies piece integrity.
        Returns: (success: bool, piece_data: bytes or None, reason: str)
        """
        event = threading.Event()
        resp_holder: List[Dict[str, Any]] = []
        key = (file_id, piece_index)

        with self._pending_lock:
            self._pending_requests[key] = (event, resp_holder)

        try:
            # Send PIECE_REQ message over TCP
            sent = self.peer.send_message(
                target_peer,
                MessageType.PIECE_REQ,
                {"file_id": file_id, "piece_index": piece_index}
            )

            if not sent:
                return False, None, f"Failed to send request to peer [{target_peer}]"

            # Wait for response with timeout
            wait_step = 0.2
            total_waited = 0.0
            while total_waited < timeout:
                if cancel_event.is_set():
                    return False, None, "Download cancelled"
                if event.wait(timeout=wait_step):
                    break
                total_waited += wait_step

            if not event.is_set():
                return False, None, f"Socket request timed out ({timeout}s)"

            if not resp_holder:
                return False, None, "Empty response received"

            response = resp_holder[0]
            if response.get("status") == "ERROR":
                return False, None, f"Remote peer error: {response.get('error', 'unknown')}"

            b64_data = response.get("data")
            if not b64_data:
                return False, None, "No data field in response"

            # Decode piece bytes
            try:
                piece_bytes = PieceVerifier.decode_data(b64_data)
            except Exception as e:
                return False, None, f"Base64 decoding failed: {e}"

            # ---------------- Piece Cryptographic Verification ---------------- #
            is_valid = PieceVerifier.verify_piece(piece_bytes, expected_hash)
            if not is_valid:
                actual_hash = PieceVerifier.compute_hash(piece_bytes)
                return (
                    False,
                    None,
                    f"Integrity check failed: Expected {expected_hash[:8]}..., got {actual_hash[:8]}..."
                )

            return True, piece_bytes, "OK"

        finally:
            with self._pending_lock:
                if key in self._pending_requests:
                    del self._pending_requests[key]


class TransferManager:
    """
    High-level Coordinator connecting the Peer network layer with
    Parallel Upload/Download, Piece Verification, and Failure Handling.
    """

    def __init__(self, peer_instance: Any, upload_workers: int = 8):
        self.peer = peer_instance
        self.upload_manager = UploadManager(peer_instance.peer_id, max_workers=upload_workers)
        self.downloader = ParallelDownloader(peer_instance)
        self.logger = setup_logger(f"TransferManager-{peer_instance.peer_id}")

        # Bind event-driven protocol handlers on Peer
        self._register_protocol_handlers()

    def _register_protocol_handlers(self) -> None:
        """Hooks transfer and piece verification handlers into Peer message dispatch pipeline."""
        self.peer.register_handler(MessageType.PIECE_REQ, self.upload_manager.handle_piece_request)
        self.peer.register_handler(MessageType.PIECE_DATA, self.downloader.handle_piece_data)
        self.peer.register_handler(MessageType.TRANSFER_CANCEL, self.downloader.handle_transfer_cancel)
        self.peer.register_handler(MessageType.TRANSFER_ACK, self._handle_transfer_ack)

    def _handle_transfer_ack(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Handles confirmation ACK from remote peers that received verified pieces."""
        payload = message.get("payload", {})
        self.logger.debug(
            f"Remote peer [{message.get('sender_id')}] confirmed piece [{payload.get('piece_index')}] "
            f"for file [{payload.get('file_id')}]."
        )

    def share_file(self, file_path: str, piece_size: int = 256 * 1024, file_id: Optional[str] = None) -> SharedFile:
        """Shares a local file from disk with piece splitting and SHA-256 computation."""
        fid = file_id if file_id else os.path.basename(file_path)
        shared = SharedFile(file_id=fid, file_path=file_path, piece_size=piece_size)
        self.upload_manager.register_file(shared)
        return shared

    def share_bytes(self, data: bytes, file_id: str, piece_size: int = 64 * 1024) -> SharedFile:
        """Shares in-memory byte buffer (ideal for programmatic transfers and testing)."""
        shared = SharedFile(file_id=file_id, data=data, piece_size=piece_size)
        self.upload_manager.register_file(shared)
        return shared

    def download(
        self,
        file_id: str,
        piece_hashes: List[str],
        piece_size: int = 256 * 1024,
        output_path: Optional[str] = None,
        target_peers: Optional[List[str]] = None,
        max_workers: int = 4,
        timeout: float = 4.0,
        max_retries: int = 3,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> DownloadResult:
        """Initiates parallel download across connected peers."""
        return self.downloader.download_file(
            file_id=file_id,
            piece_hashes=piece_hashes,
            piece_size=piece_size,
            output_path=output_path,
            target_peers=target_peers,
            max_workers=max_workers,
            timeout=timeout,
            max_retries=max_retries,
            progress_callback=progress_callback,
        )

    def cancel_download(self, file_id: str) -> None:
        """Cancels download and notifies connected peers."""
        self.downloader.cancel_download(file_id)
        self.peer.broadcast_message(MessageType.TRANSFER_CANCEL, {"file_id": file_id})

    def get_stats(self) -> Dict[str, Any]:
        """Returns statistics for active uploads and downloads."""
        return {
            "shared_files": list(self.upload_manager.shared_files.keys()),
            "total_bytes_uploaded": self.upload_manager.total_bytes_uploaded,
            "total_pieces_served": self.upload_manager.total_pieces_served,
        }

    def stop(self) -> None:
        """Stops upload and download workers."""
        self.upload_manager.stop()
