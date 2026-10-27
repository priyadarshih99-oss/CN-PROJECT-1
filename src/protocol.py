"""
P2P Network Protocol Definitions and Message Framing.

This module defines standard message types, message schema creation,
and robust length-prefixed TCP framing to prevent fragmentation issues.
"""

import json
import struct
import socket
from typing import Dict, Any, Optional

# Standard Header: 4-byte unsigned integer (big-endian) specifying payload byte length
HEADER_STRUCT = struct.Struct(">I")
HEADER_SIZE = HEADER_STRUCT.size


class MessageType:
    # Discovery & Registration
    REGISTER = "REGISTER"
    REGISTER_ACK = "REGISTER_ACK"
    UNREGISTER = "UNREGISTER"
    UNREGISTER_ACK = "UNREGISTER_ACK"
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"
    GET_PEERS = "GET_PEERS"
    PEER_LIST = "PEER_LIST"

    # Direct Peer-to-Peer TCP Communication
    HANDSHAKE = "HANDSHAKE"
    HANDSHAKE_ACK = "HANDSHAKE_ACK"
    PING = "PING"
    PONG = "PONG"
    CHAT_MSG = "CHAT_MSG"
    DISCONNECT = "DISCONNECT"

    # Reserved hooks for Member 2 (Metadata & Piece Requests)
    METADATA_REQ = "METADATA_REQ"
    METADATA_RESP = "METADATA_RESP"
    PIECE_REQ = "PIECE_REQ"
    PIECE_RESP = "PIECE_RESP"
    BITFIELD = "BITFIELD"
    HAVE_PIECE = "HAVE_PIECE"

    # Reserved hooks for Member 3 (Data Transfer & Verification)
    PIECE_DATA = "PIECE_DATA"
    TRANSFER_ACK = "TRANSFER_ACK"
    TRANSFER_CANCEL = "TRANSFER_CANCEL"


def create_message(msg_type: str, sender_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Constructs a standard protocol message dictionary."""
    return {
        "type": msg_type,
        "sender_id": sender_id,
        "payload": payload if payload is not None else {}
    }


def send_msg(sock: socket.socket, message: Dict[str, Any]) -> None:
    """
    Serializes a message to JSON and sends it with a 4-byte length prefix.
    Guarantees that full message is sent.
    """
    raw_payload = json.dumps(message).encode("utf-8")
    header = HEADER_STRUCT.pack(len(raw_payload))
    sock.sendall(header + raw_payload)


def recv_msg(sock: socket.socket) -> Optional[Dict[str, Any]]:
    """
    Reads a length-prefixed JSON message from the socket.
    Returns the parsed dictionary, or None if socket was closed.
    """
    # 1. Read header (4 bytes)
    header_data = _recv_all(sock, HEADER_SIZE)
    if not header_data:
        return None

    (payload_length,) = HEADER_STRUCT.unpack(header_data)

    # 2. Read full payload
    payload_data = _recv_all(sock, payload_length)
    if not payload_data:
        return None

    return json.loads(payload_data.decode("utf-8"))


def _recv_all(sock: socket.socket, n_bytes: int) -> Optional[bytes]:
    """Helper to reliably receive exactly n_bytes from a TCP stream."""
    buffer = bytearray()
    while len(buffer) < n_bytes:
        try:
            chunk = sock.recv(n_bytes - len(buffer))
        except (ConnectionResetError, ConnectionAbortedError, socket.timeout):
            return None
        if not chunk:
            # Connection closed by remote peer
            return None
        buffer.extend(chunk)
    return bytes(buffer)
