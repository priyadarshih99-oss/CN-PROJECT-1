"""
TCP Connection Wrapper for Peer-to-Peer Communication.

Handles thread-safe message transmission, socket lifecycle, and cleanup.
"""

import socket
import threading
from typing import Dict, Any, Optional
from src.protocol import send_msg, recv_msg


class PeerConnection:
    """
    Manages an active TCP connection to a specific peer.
    Provides thread-safe sending and robust message receiving.
    """

    def __init__(self, sock: socket.socket, host: str, port: int, peer_id: Optional[str] = None):
        self.sock = sock
        self.host = host
        self.port = port
        self.peer_id = peer_id
        self._send_lock = threading.Lock()
        self._is_active = True

    @property
    def is_alive(self) -> bool:
        return self._is_active

    def send(self, message: Dict[str, Any]) -> bool:
        """Thread-safe transmission of a protocol message."""
        if not self._is_active:
            return False

        with self._send_lock:
            try:
                send_msg(self.sock, message)
                return True
            except (socket.error, BrokenPipeError, ConnectionResetError):
                self.close()
                return False

    def receive(self) -> Optional[Dict[str, Any]]:
        """Receives the next message from the remote peer."""
        if not self._is_active:
            return None

        try:
            msg = recv_msg(self.sock)
            if msg is None:
                self.close()
            return msg
        except (socket.error, ConnectionResetError, ConnectionAbortedError):
            self.close()
            return None

    def close(self) -> None:
        """Closes the underlying TCP socket and marks connection inactive."""
        if not self._is_active:
            return
        self._is_active = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            try:
                self.sock.close()
            except OSError:
                pass

    def __repr__(self) -> str:
        return f"<PeerConnection peer_id={self.peer_id} remote={self.host}:{self.port} active={self._is_active}>"
