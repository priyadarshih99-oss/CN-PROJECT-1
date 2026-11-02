"""
Unit tests for protocol message framing, serialization, and networking utilities.
"""

import socket
import threading
import unittest
from src.protocol import MessageType, create_message, send_msg, recv_msg, HEADER_SIZE
from src.utils import validate_address, format_bytes, DEFAULT_HOST, DEFAULT_TRACKER_PORT


class TestProtocol(unittest.TestCase):

    def test_create_message(self):
        msg = create_message(MessageType.HANDSHAKE, "Peer-A", {"version": 1})
        self.assertEqual(msg["type"], MessageType.HANDSHAKE)
        self.assertEqual(msg["sender_id"], "Peer-A")
        self.assertEqual(msg["payload"]["version"], 1)

    def test_send_and_recv_msg_over_loopback_socket(self):
        # Create a loopback TCP socket pair
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_port = server_sock.getsockname()[1]
        server_sock.listen(1)

        received_msg = []

        def server_thread():
            conn, _ = server_sock.accept()
            msg = recv_msg(conn)
            received_msg.append(msg)
            conn.close()

        t = threading.Thread(target=server_thread)
        t.start()

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(("127.0.0.1", server_port))

        sent_msg = create_message(MessageType.CHAT_MSG, "Peer-Client", {"text": "Hello TCP Stream!"})
        send_msg(client_sock, sent_msg)
        client_sock.close()

        t.join(timeout=2.0)
        server_sock.close()

        self.assertEqual(len(received_msg), 1)
        self.assertEqual(received_msg[0]["type"], MessageType.CHAT_MSG)
        self.assertEqual(received_msg[0]["sender_id"], "Peer-Client")
        self.assertEqual(received_msg[0]["payload"]["text"], "Hello TCP Stream!")

    def test_network_utility_validations(self):
        """Sanity test for address validation and byte formatter utilities."""
        self.assertTrue(validate_address("127.0.0.1", 8000))
        self.assertTrue(validate_address("localhost", 5001))
        self.assertFalse(validate_address("127.0.0.1", 70000))  # Out of port range
        self.assertFalse(validate_address("invalid_ip_host", 8000))

        # Test byte formatting helper
        self.assertEqual(format_bytes(512), "512 B")
        self.assertEqual(format_bytes(1024), "1.00 KB")
        self.assertEqual(format_bytes(1048576), "1.00 MB")


if __name__ == "__main__":
    unittest.main()
