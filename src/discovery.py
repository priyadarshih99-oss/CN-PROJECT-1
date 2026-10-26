"""
Peer Discovery Mechanism: Discovery Tracker and Discovery Client.

Enables peers to register their availability, discover active peers in the network,
maintain heartbeat liveness, and handle peer departure/failure.
"""

import socket
import threading
import time
from typing import Dict, Any, List, Optional
from src.protocol import MessageType, create_message, send_msg, recv_msg
from src.utils import setup_logger


class DiscoveryTracker:
    """
    Central Peer Discovery Service / Registry Node.
    Listens for TCP requests from peers to register, heartbeat, query, and deregister.
    Periodically prunes inactive peers whose heartbeats have timed out.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8000, peer_timeout: float = 30.0):
        self.host = host
        self.port = port
        self.peer_timeout = peer_timeout
        self.logger = setup_logger("DiscoveryTracker")

        # In-memory registry: peer_id -> {"host": str, "port": int, "last_seen": float, "registered_at": float}
        self.registered_peers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._is_running = False
        self._server_socket: Optional[socket.socket] = None
        self._reaper_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Starts the discovery server and background reaper."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(10)
        self._is_running = True

        self.logger.info(f"Discovery Tracker running on {self.host}:{self.port}")

        # Start reaper thread for pruning dead peers
        self._reaper_thread = threading.Thread(target=self._prune_stale_peers_loop, daemon=True)
        self._reaper_thread.start()

        # Start accept loop
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()

    def _accept_loop(self) -> None:
        while self._is_running:
            try:
                client_sock, client_addr = self._server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, client_addr),
                    daemon=True
                ).start()
            except OSError:
                break

    def _handle_client(self, sock: socket.socket, addr: tuple) -> None:
        try:
            msg = recv_msg(sock)
            if not msg:
                return

            msg_type = msg.get("type")
            sender_id = msg.get("sender_id", "UNKNOWN")
            payload = msg.get("payload", {})

            response = self._process_request(msg_type, sender_id, payload)
            if response:
                send_msg(sock, response)
        except Exception as e:
            self.logger.error(f"Error handling client {addr}: {e}")
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _process_request(self, msg_type: str, sender_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        now = time.time()

        if msg_type == MessageType.REGISTER:
            host = payload.get("host")
            port = payload.get("port")
            with self._lock:
                self.registered_peers[sender_id] = {
                    "host": host,
                    "port": port,
                    "last_seen": now,
                    "registered_at": now
                }
            self.logger.info(f"Registered peer [{sender_id}] at {host}:{port}. Active count: {len(self.registered_peers)}")
            return create_message(MessageType.REGISTER_ACK, "TRACKER", {"status": "SUCCESS", "peer_id": sender_id})

        elif msg_type == MessageType.HEARTBEAT:
            with self._lock:
                if sender_id in self.registered_peers:
                    self.registered_peers[sender_id]["last_seen"] = now
                    status = "ACKNOWLEDGED"
                else:
                    status = "NOT_FOUND"
            return create_message(MessageType.HEARTBEAT_ACK, "TRACKER", {"status": status})

        elif msg_type == MessageType.GET_PEERS:
            with self._lock:
                peers_list = [
                    {
                        "peer_id": pid,
                        "host": info["host"],
                        "port": info["port"]
                    }
                    for pid, info in self.registered_peers.items()
                    if pid != sender_id
                ]
            self.logger.debug(f"Serving peer list to [{sender_id}], total available: {len(peers_list)}")
            return create_message(MessageType.PEER_LIST, "TRACKER", {"peers": peers_list})

        elif msg_type == MessageType.UNREGISTER:
            with self._lock:
                if sender_id in self.registered_peers:
                    del self.registered_peers[sender_id]
                    self.logger.info(f"Unregistered peer [{sender_id}]. Active count: {len(self.registered_peers)}")
            return create_message(MessageType.UNREGISTER_ACK, "TRACKER", {"status": "SUCCESS"})

        return None

    def _prune_stale_peers_loop(self) -> None:
        """Periodically check for peer timeouts and remove dead peers."""
        while self._is_running:
            time.sleep(5)
            now = time.time()
            to_remove = []
            with self._lock:
                for pid, info in list(self.registered_peers.items()):
                    if now - info["last_seen"] > self.peer_timeout:
                        to_remove.append(pid)

                for pid in to_remove:
                    del self.registered_peers[pid]
                    self.logger.warning(f"Peer [{pid}] timed out and was removed from tracker. Active count: {len(self.registered_peers)}")

    def stop(self) -> None:
        """Stops the discovery tracker."""
        self._is_running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        self.logger.info("Discovery Tracker stopped.")


class DiscoveryClient:
    """
    Client interface for peers to interact with the DiscoveryTracker.
    Manages registration, peer list fetching, and background heartbeats.
    """

    def __init__(self, tracker_host: str, tracker_port: int, peer_id: str, local_host: str, local_port: int):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.peer_id = peer_id
        self.local_host = local_host
        self.local_port = local_port
        self.logger = setup_logger(f"DiscoveryClient-{peer_id}")

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._is_running = False

    def _send_tracker_request(self, message: Dict[str, Any], timeout: float = 3.0) -> Optional[Dict[str, Any]]:
        """Opens a short-lived TCP connection to the tracker, sends message and gets response."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((self.tracker_host, self.tracker_port))
            send_msg(sock, message)
            response = recv_msg(sock)
            return response
        except (socket.error, socket.timeout) as e:
            self.logger.warning(f"Failed to communicate with tracker at {self.tracker_host}:{self.tracker_port}: {e}")
            return None
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def register(self) -> bool:
        """Registers this peer with the discovery tracker."""
        payload = {"host": self.local_host, "port": self.local_port}
        req = create_message(MessageType.REGISTER, self.peer_id, payload)
        res = self._send_tracker_request(req)
        if res and res.get("type") == MessageType.REGISTER_ACK:
            self.logger.info("Successfully registered with discovery tracker.")
            self._start_heartbeat()
            return True
        return False

    def get_peers(self) -> List[Dict[str, Any]]:
        """Queries the tracker for other active peers in the swarm."""
        req = create_message(MessageType.GET_PEERS, self.peer_id)
        res = self._send_tracker_request(req)
        if res and res.get("type") == MessageType.PEER_LIST:
            peers = res.get("payload", {}).get("peers", [])
            return peers
        return []

    def unregister(self) -> bool:
        """Deregisters this peer from the tracker upon shutdown."""
        self._is_running = False
        req = create_message(MessageType.UNREGISTER, self.peer_id)
        res = self._send_tracker_request(req)
        return bool(res and res.get("type") == MessageType.UNREGISTER_ACK)

    def _start_heartbeat(self, interval: float = 10.0) -> None:
        """Starts periodic heartbeat in the background."""
        self._is_running = True

        def heartbeat_loop():
            while self._is_running:
                time.sleep(interval)
                if not self._is_running:
                    break
                req = create_message(MessageType.HEARTBEAT, self.peer_id)
                self._send_tracker_request(req, timeout=2.0)

        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
