"""
Core Peer Implementation.

Handles:
- Peer initialization & listening TCP socket
- Peer discovery via DiscoveryTracker
- Direct TCP socket-based peer-to-peer connections and handshakes
- Bi-directional asynchronous message routing with pluggable handlers
- Graceful connection teardown and failure handling
"""

import socket
import threading
import time
from typing import Dict, Any, Optional, Callable, List
from src.protocol import MessageType, create_message
from src.connection import PeerConnection
from src.discovery import DiscoveryClient
from src.utils import setup_logger


class Peer:
    """
    Represents an independent node in the P2P network.
    """

    def __init__(
        self,
        peer_id: str,
        host: str = "127.0.0.1",
        port: int = 5001,
        tracker_host: Optional[str] = "127.0.0.1",
        tracker_port: Optional[int] = 8000,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.logger = setup_logger(f"Peer-{self.peer_id}")

        # Active TCP peer connections: remote_peer_id -> PeerConnection
        self.connections: Dict[str, PeerConnection] = {}
        self._conn_lock = threading.Lock()

        # Pluggable message handlers: msg_type -> callable(conn, message)
        self._handlers: Dict[str, Callable[[PeerConnection, Dict[str, Any]], None]] = {}

        # Sockets and Threads
        self._server_socket: Optional[socket.socket] = None
        self._is_running = False
        self._accept_thread: Optional[threading.Thread] = None

        # Discovery client
        self.discovery_client: Optional[DiscoveryClient] = None
        if self.tracker_host and self.tracker_port:
            self.discovery_client = DiscoveryClient(
                tracker_host=self.tracker_host,
                tracker_port=self.tracker_port,
                peer_id=self.peer_id,
                local_host=self.host,
                local_port=self.port,
            )

        # Register default protocol handlers
        self._setup_default_handlers()

    def _setup_default_handlers(self) -> None:
        """Sets up default handlers for ping/pong, chat messages, and disconnects."""
        self.register_handler(MessageType.PING, self._handle_ping)
        self.register_handler(MessageType.PONG, self._handle_pong)
        self.register_handler(MessageType.CHAT_MSG, self._handle_chat)
        self.register_handler(MessageType.DISCONNECT, self._handle_disconnect)

    def register_handler(self, msg_type: str, handler_func: Callable[[PeerConnection, Dict[str, Any]], None]) -> None:
        """
        Allows other modules (Member 2 - Metadata, Member 3 - Transfer) to attach
        custom handlers for specific message types without altering core networking code.
        """
        self._handlers[msg_type] = handler_func

    def start(self) -> None:
        """
        Initializes the peer:
        1. Binds TCP listening socket.
        2. Starts background accept loop for incoming connections.
        3. Registers with discovery tracker.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(20)
        self._is_running = True

        self.logger.info(f"Peer initialized and listening on TCP {self.host}:{self.port}")

        # Start accepting incoming peer connections
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

        # Register with Discovery Tracker if enabled
        if self.discovery_client:
            registered = self.discovery_client.register()
            if registered:
                self.logger.info(f"Peer [{self.peer_id}] registered with tracker at {self.tracker_host}:{self.tracker_port}")
            else:
                self.logger.warning("Could not register with tracker (tracker may be offline).")

    def _accept_loop(self) -> None:
        """Listens for and accepts incoming TCP connections from other peers."""
        while self._is_running:
            try:
                client_sock, client_addr = self._server_socket.accept()
                threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_sock, client_addr),
                    daemon=True,
                ).start()
            except OSError:
                break

    def _handle_incoming_connection(self, sock: socket.socket, addr: tuple) -> None:
        """Performs handshake with incoming peer and begins listening for messages."""
        try:
            # Expect HANDSHAKE message
            handshake = PeerConnection(sock, addr[0], addr[1]).receive()
            if not handshake or handshake.get("type") != MessageType.HANDSHAKE:
                self.logger.warning(f"Invalid handshake received from {addr}. Closing socket.")
                sock.close()
                return

            remote_peer_id = handshake.get("sender_id")
            if not remote_peer_id:
                sock.close()
                return

            # Acknowledge handshake
            ack = create_message(MessageType.HANDSHAKE_ACK, self.peer_id, {"status": "CONNECTED"})
            conn = PeerConnection(sock, addr[0], addr[1], peer_id=remote_peer_id)
            conn.send(ack)

            with self._conn_lock:
                # If an existing connection to this peer exists, close the old one
                if remote_peer_id in self.connections:
                    self.connections[remote_peer_id].close()
                self.connections[remote_peer_id] = conn

            self.logger.info(f"Incoming TCP connection established with peer [{remote_peer_id}] from {addr}")

            # Start message reception loop for this peer
            self._peer_recv_loop(conn)

        except Exception as e:
            self.logger.error(f"Error establishing incoming connection with {addr}: {e}")
            try:
                sock.close()
            except OSError:
                pass

    def connect_to_peer(self, host: str, port: int, remote_peer_id: Optional[str] = None, timeout: float = 5.0) -> Optional[PeerConnection]:
        """
        Initiates an outgoing TCP connection to a peer and completes the handshake.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            sock.connect((host, port))
            sock.settimeout(None)  # Reset to blocking mode for data transfer

            # Send HANDSHAKE
            hs_msg = create_message(MessageType.HANDSHAKE, self.peer_id, {"host": self.host, "port": self.port})
            conn = PeerConnection(sock, host, port, peer_id=remote_peer_id)
            if not conn.send(hs_msg):
                conn.close()
                return None

            # Receive HANDSHAKE_ACK
            ack = conn.receive()
            if not ack or ack.get("type") != MessageType.HANDSHAKE_ACK:
                self.logger.warning(f"Handshake failed with {host}:{port}")
                conn.close()
                return None

            actual_peer_id = ack.get("sender_id") or remote_peer_id
            conn.peer_id = actual_peer_id

            with self._conn_lock:
                if actual_peer_id in self.connections:
                    self.connections[actual_peer_id].close()
                self.connections[actual_peer_id] = conn

            self.logger.info(f"Successfully connected to peer [{actual_peer_id}] at {host}:{port}")

            # Start background message receiver loop for this connection
            threading.Thread(target=self._peer_recv_loop, args=(conn,), daemon=True).start()
            return conn

        except Exception as e:
            self.logger.error(f"Failed to connect to peer at {host}:{port}: {e}")
            try:
                sock.close()
            except OSError:
                pass
            return None

    def _peer_recv_loop(self, conn: PeerConnection) -> None:
        """Continuously reads messages from a connected peer and routes them."""
        while self._is_running and conn.is_alive:
            msg = conn.receive()
            if msg is None:
                break

            msg_type = msg.get("type")
            sender_id = msg.get("sender_id", conn.peer_id or "UNKNOWN")

            handler = self._handlers.get(msg_type)
            if handler:
                try:
                    handler(conn, msg)
                except Exception as e:
                    self.logger.error(f"Error executing handler for {msg_type} from [{sender_id}]: {e}")
            else:
                self.logger.debug(f"Unhandled message type: {msg_type} from [{sender_id}]")

        # Cleanup connection upon disconnection
        self._remove_connection(conn)

    def _remove_connection(self, conn: PeerConnection) -> None:
        """Removes a dead or disconnected peer from active connections."""
        peer_id_to_remove = conn.peer_id
        with self._conn_lock:
            if peer_id_to_remove and peer_id_to_remove in self.connections:
                if self.connections[peer_id_to_remove] is conn:
                    del self.connections[peer_id_to_remove]
                    self.logger.info(f"Connection with peer [{peer_id_to_remove}] closed and removed.")
        conn.close()

    def discover_peers(self) -> List[Dict[str, Any]]:
        """Queries the discovery tracker for other active peers."""
        if not self.discovery_client:
            self.logger.warning("No discovery tracker configured.")
            return []
        peers = self.discovery_client.get_peers()
        self.logger.info(f"Discovered {len(peers)} active peer(s) from tracker.")
        return peers

    def connect_to_discovered_peers(self) -> int:
        """Queries the tracker and connects to all active peers not currently connected."""
        peers = self.discover_peers()
        connected_count = 0
        for p in peers:
            pid = p["peer_id"]
            if pid == self.peer_id:
                continue
            with self._conn_lock:
                already_connected = pid in self.connections and self.connections[pid].is_alive

            if not already_connected:
                conn = self.connect_to_peer(p["host"], p["port"], remote_peer_id=pid)
                if conn:
                    connected_count += 1
        return connected_count

    def send_message(self, target_peer_id: str, msg_type: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        """Sends a message to a specific connected peer."""
        with self._conn_lock:
            conn = self.connections.get(target_peer_id)

        if not conn or not conn.is_alive:
            self.logger.warning(f"Cannot send {msg_type}: Not connected to peer [{target_peer_id}].")
            return False

        msg = create_message(msg_type, self.peer_id, payload)
        return conn.send(msg)

    def broadcast_message(self, msg_type: str, payload: Optional[Dict[str, Any]] = None) -> int:
        """Broadcasts a message to all currently connected peers."""
        with self._conn_lock:
            active_conns = list(self.connections.values())

        sent_count = 0
        msg = create_message(msg_type, self.peer_id, payload)
        for conn in active_conns:
            if conn.is_alive and conn.send(msg):
                sent_count += 1

        self.logger.info(f"Broadcasted {msg_type} to {sent_count}/{len(active_conns)} peer(s).")
        return sent_count

    def ping_peer(self, target_peer_id: str) -> bool:
        """Sends a PING message with timestamp to measure round-trip time."""
        payload = {"timestamp": time.time()}
        return self.send_message(target_peer_id, MessageType.PING, payload)

    # ---------------- Default Message Handlers ---------------- #

    def _handle_ping(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Responds to PING with PONG containing the original timestamp."""
        payload = message.get("payload", {})
        pong_msg = create_message(MessageType.PONG, self.peer_id, payload)
        conn.send(pong_msg)
        self.logger.debug(f"Replied with PONG to [{conn.peer_id}]")

    def _handle_pong(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Calculates and logs round-trip time (RTT)."""
        payload = message.get("payload", {})
        orig_time = payload.get("timestamp")
        rtt_str = ""
        if orig_time:
            rtt_ms = (time.time() - orig_time) * 1000.0
            rtt_str = f" (RTT: {rtt_ms:.2f} ms)"
        self.logger.info(f"Received PONG from [{conn.peer_id}]{rtt_str}")

    def _handle_chat(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Displays chat/data message from peer."""
        sender = message.get("sender_id", conn.peer_id)
        text = message.get("payload", {}).get("text", "")
        self.logger.info(f"[CHAT from {sender}]: {text}")

    def _handle_disconnect(self, conn: PeerConnection, message: Dict[str, Any]) -> None:
        """Handles graceful disconnect request from peer."""
        self.logger.info(f"Received DISCONNECT notice from [{conn.peer_id}].")
        self._remove_connection(conn)

    def stop(self) -> None:
        """
        Gracefully shuts down the peer:
        - Sends DISCONNECT notices to all peers and closes connections.
        - Deregisters from discovery tracker.
        - Closes TCP listening socket.
        """
        self.logger.info(f"Shutting down peer [{self.peer_id}]...")
        self._is_running = False

        # Notify and close peer connections
        with self._conn_lock:
            for pid, conn in list(self.connections.items()):
                try:
                    conn.send(create_message(MessageType.DISCONNECT, self.peer_id))
                except Exception:
                    pass
                conn.close()
            self.connections.clear()

        # Deregister from discovery tracker
        if self.discovery_client:
            try:
                self.discovery_client.unregister()
            except Exception:
                pass

        # Close listening server socket
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass

        self.logger.info(f"Peer [{self.peer_id}] shutdown complete.")
