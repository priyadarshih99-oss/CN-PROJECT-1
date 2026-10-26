"""
Unit tests for protocol message framing and serialization.
"""

import socket
import threading
import unittest
from src.protocol import MessageType, create_message, send_msg, recv_msg, HEADER_SIZE


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


if __name__ == "__main__":
    unittest.main()
