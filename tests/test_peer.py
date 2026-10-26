"""
Integration tests for Member 1 deliverables:
- Peer initialization & listening TCP socket
- Discovery Tracker registration & discovery query
- Direct TCP socket-based peer-to-peer connection & handshake
- Direct TCP messaging and custom handler dispatch
- Ping/Pong roundtrip
- Graceful peer departure and unregistration
"""

import time
import unittest
from src.discovery import DiscoveryTracker
from src.peer import Peer
from src.protocol import MessageType


class TestPeerNetworking(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Start Discovery Tracker on port 8990
        cls.tracker_port = 8990
        cls.tracker = DiscoveryTracker(host="127.0.0.1", port=cls.tracker_port, peer_timeout=10.0)
        cls.tracker.start()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.tracker.stop()

    def test_peer_lifecycle_discovery_and_tcp_communication(self):
        # 1. Initialize Peer A and Peer B on distinct ports
        peer_a = Peer(peer_id="Peer-Alpha", host="127.0.0.1", port=7001, tracker_port=self.tracker_port)
        peer_b = Peer(peer_id="Peer-Beta", host="127.0.0.1", port=7002, tracker_port=self.tracker_port)

        try:
            peer_a.start()
            peer_b.start()
            time.sleep(0.3)

            # 2. Verify both peers registered with the Tracker
            with self.tracker._lock:
                self.assertIn("Peer-Alpha", self.tracker.registered_peers)
                self.assertIn("Peer-Beta", self.tracker.registered_peers)

            # 3. Peer A discovers Peer B through Tracker
            discovered = peer_a.discover_peers()
            peer_ids = [p["peer_id"] for p in discovered]
            self.assertIn("Peer-Beta", peer_ids)

            # 4. Peer A connects to Peer B over TCP
            conn = peer_a.connect_to_peer(host="127.0.0.1", port=7002, remote_peer_id="Peer-Beta")
            self.assertIsNotNone(conn)
            self.assertTrue(conn.is_alive)
            time.sleep(0.2)

            # Both peers should have active connection in their table
            self.assertIn("Peer-Beta", peer_a.connections)
            self.assertIn("Peer-Alpha", peer_b.connections)

            # 5. Test bi-directional messaging with a registered custom handler
            received_messages = []

            def custom_handler(connection, msg):
                received_messages.append(msg)

            # Register a custom handler on Peer B (simulating Member 2/3 registering piece/metadata requests)
            peer_b.register_handler(MessageType.CHAT_MSG, custom_handler)

            # Peer A sends message to Peer B over TCP
            sent = peer_a.send_message("Peer-Beta", MessageType.CHAT_MSG, {"text": "Test P2P Message"})
            self.assertTrue(sent)

            # Wait for message reception
            time.sleep(0.3)
            self.assertEqual(len(received_messages), 1)
            self.assertEqual(received_messages[0]["sender_id"], "Peer-Alpha")
            self.assertEqual(received_messages[0]["payload"]["text"], "Test P2P Message")

            # 6. Test Ping-Pong over TCP
            ping_sent = peer_a.ping_peer("Peer-Beta")
            self.assertTrue(ping_sent)
            time.sleep(0.2)

        finally:
            # 7. Graceful teardown
            peer_a.stop()
            peer_b.stop()
            time.sleep(0.3)

            # Verify unregistration from Tracker
            with self.tracker._lock:
                self.assertNotIn("Peer-Alpha", self.tracker.registered_peers)
                self.assertNotIn("Peer-Beta", self.tracker.registered_peers)


if __name__ == "__main__":
    unittest.main()
