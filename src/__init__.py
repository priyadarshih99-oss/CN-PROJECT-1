"""
P2P File Distribution System - Core Network, Transfer & Performance Engine
"""

from src.protocol import MessageType, create_message, send_msg, recv_msg
from src.connection import PeerConnection
from src.discovery import DiscoveryTracker, DiscoveryClient
from src.peer import Peer
from src.metadata import FileMetadata
from src.file_splitter import FileSplitter
from src.piece_manager import PieceManager
from src.transfer_manager import TransferManager
from src.verification import PieceVerifier
from src.transfer import (
    SharedFile,
    UploadManager,
    ParallelDownloader,
    DownloadResult,
)

try:
    from src.performance import (
        PerformanceMetricsCollector,
        ExperimentRunner,
        ExperimentAnalyzer,
        BenchmarkVisualizer,
    )
except ImportError:
    PerformanceMetricsCollector = None
    ExperimentRunner = None
    ExperimentAnalyzer = None
    BenchmarkVisualizer = None

__all__ = [
    "MessageType",
    "create_message",
    "send_msg",
    "recv_msg",
    "PeerConnection",
    "DiscoveryTracker",
    "DiscoveryClient",
    "Peer",
    "FileMetadata",
    "FileSplitter",
    "PieceManager",
    "TransferManager",
    "PerformanceMetricsCollector",
    "ExperimentRunner",
    "ExperimentAnalyzer",
    "BenchmarkVisualizer",
    "PieceVerifier",
    "SharedFile",
    "UploadManager",
    "ParallelDownloader",
    "DownloadResult",
]
