"""
performance.py - Member 4: Performance Measurement, Visualization & Experiment Analysis
CS-30003 P2P File Distribution System
Author: Udbhav Singh (241551102) - Section: CSE-14

Implements:
1. High-resolution telemetry collection (timing, throughput, piece distribution, overhead).
2. Automated multi-variable experiment runner (evaluating the claim from Section 3 of proposal).
3. Statistical analysis and summary table generation (Pandas).
4. Publication-quality Matplotlib visualization suite (6 figures).
"""

import os
import sys
import time
import shutil
import tempfile
import threading
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

# Configure non-interactive Matplotlib backend
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.discovery import DiscoveryTracker
from src.peer import Peer
from src.metadata import FileMetadata
from src.file_splitter import FileSplitter
from src.piece_manager import PieceManager
from src.transfer_manager import TransferManager


class PerformanceMetricsCollector:
    """Thread-safe telemetry collector capturing transfer-level and piece-level metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.transfer_records: List[Dict[str, Any]] = []
        self.piece_events: List[Dict[str, Any]] = []

    def record_piece_event(self, event_data: Dict[str, Any]) -> None:
        with self._lock:
            self.piece_events.append(dict(event_data))

    def record_transfer_summary(
        self,
        experiment_id: str,
        file_size_bytes: int,
        chunk_size_bytes: int,
        total_pieces: int,
        swarm_size: int,
        strategy: str,
        duration_sec: float,
        is_successful: bool,
        failure_rate_simulated: float = 0.0,
        extra_meta: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        throughput_mb_s = (file_size_bytes / (1024 * 1024)) / max(duration_sec, 0.00001) if is_successful else 0.0
        record = {
            "experiment_id": experiment_id,
            "timestamp": time.time(),
            "file_size_bytes": file_size_bytes,
            "file_size_mb": round(file_size_bytes / (1024 * 1024), 2),
            "chunk_size_bytes": chunk_size_bytes,
            "chunk_size_kb": round(chunk_size_bytes / 1024, 2),
            "total_pieces": total_pieces,
            "swarm_size": swarm_size,
            "strategy": strategy,
            "duration_sec": round(duration_sec, 4),
            "throughput_mb_s": round(throughput_mb_s, 3),
            "is_successful": is_successful,
            "failure_rate_simulated": failure_rate_simulated
        }
        if extra_meta:
            record.update(extra_meta)

        with self._lock:
            self.transfer_records.append(record)
        return record

    def get_summary_dataframe(self) -> pd.DataFrame:
        with self._lock:
            return pd.DataFrame(self.transfer_records)

    def get_piece_events_dataframe(self) -> pd.DataFrame:
        with self._lock:
            return pd.DataFrame(self.piece_events)

    def export_csv(self, summary_path: str, piece_path: Optional[str] = None) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(summary_path)), exist_ok=True)
        self.get_summary_dataframe().to_csv(summary_path, index=False)
        if piece_path:
            os.makedirs(os.path.dirname(os.path.abspath(piece_path)), exist_ok=True)
            self.get_piece_events_dataframe().to_csv(piece_path, index=False)


class ExperimentRunner:
    """Automates multi-peer benchmark experiments testing the core hypothesis."""

    def __init__(self, output_dir: str = "./results"):
        self.output_dir = output_dir
        self.figures_dir = os.path.join(output_dir, "figures")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.figures_dir, exist_ok=True)
        self.collector = PerformanceMetricsCollector()

    @staticmethod
    def generate_dummy_file(filepath: str, size_bytes: int) -> str:
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        block = b"CN_PROJECT_1_MEMBER_4_BENCHMARK_" * 2000
        block = block[:65536]
        with open(filepath, "wb") as f:
            written = 0
            while written < size_bytes:
                to_write = min(len(block), size_bytes - written)
                f.write(block[:to_write])
                written += to_write
        return filepath

    def run_single_transfer(
        self,
        file_size_bytes: int,
        num_seeders: int,
        chunk_size_bytes: int = 64 * 1024,
        piece_strategy: str = PieceManager.STRATEGY_RAREST_FIRST,
        kill_peers_midway: int = 0
    ) -> Dict[str, Any]:
        """Runs an isolated end-to-end swarm transfer experiment."""
        temp_dir = tempfile.mkdtemp(prefix="p2p_exp_")
        tracker = None
        seeders: List[Peer] = []
        seeder_managers: List[TransferManager] = []
        leecher = None
        leecher_manager = None

        try:
            # 1. Start Tracker on a dynamic port
            import socket
            temp_sock = socket.socket()
            temp_sock.bind(("127.0.0.1", 0))
            t_port = temp_sock.getsockname()[1]
            temp_sock.close()

            tracker = DiscoveryTracker(host="127.0.0.1", port=t_port, peer_timeout=15.0)
            tracker.start()
            time.sleep(0.05)

            # 2. Create Source File
            source_file = os.path.join(temp_dir, "source", "test_file.bin")
            self.generate_dummy_file(source_file, file_size_bytes)

            # 3. Spin up Seeders
            for i in range(num_seeders):
                s_sock = socket.socket()
                s_sock.bind(("127.0.0.1", 0))
                s_port = s_sock.getsockname()[1]
                s_sock.close()

                seeder = Peer(
                    peer_id=f"seeder-{i+1}",
                    host="127.0.0.1",
                    port=s_port,
                    tracker_host="127.0.0.1",
                    tracker_port=t_port
                )
                seeder.start()
                seeders.append(seeder)

                sm = TransferManager(seeder, pieces_root_dir=os.path.join(temp_dir, f"seeder_{i}_cache"))
                metadata = sm.register_seeder_file(source_file, piece_size=chunk_size_bytes)
                seeder_managers.append(sm)

            time.sleep(0.1)

            # 4. Spin up Leecher
            l_sock = socket.socket()
            l_sock.bind(("127.0.0.1", 0))
            l_port = l_sock.getsockname()[1]
            l_sock.close()

            leecher = Peer(
                peer_id="leecher-node",
                host="127.0.0.1",
                port=l_port,
                tracker_host="127.0.0.1",
                tracker_port=t_port
            )
            leecher.start()

            dest_path = os.path.join(temp_dir, "downloaded", "result.bin")
            leecher_manager = TransferManager(leecher, pieces_root_dir=os.path.join(temp_dir, "leecher_cache"))
            leecher_manager.on_piece_downloaded = self.collector.record_piece_event

            # Mid-transfer fault injection
            if kill_peers_midway > 0 and len(seeders) > kill_peers_midway:
                def kill_worker():
                    time.sleep(0.08)
                    for k in range(kill_peers_midway):
                        try:
                            seeders[k].stop()
                        except Exception:
                            pass
                threading.Thread(target=kill_worker, daemon=True).start()

            # 5. Execute Download
            t0 = time.perf_counter()
            is_success = leecher_manager.download_file(
                metadata,
                dest_path,
                timeout=45.0,
                strategy=piece_strategy,
                max_workers=max(num_seeders, 2)
            )
            duration = time.perf_counter() - t0

            # 6. Record Record
            exp_id = f"exp_s{file_size_bytes}_p{num_seeders}_c{chunk_size_bytes}_k{kill_peers_midway}"
            record = self.collector.record_transfer_summary(
                experiment_id=exp_id,
                file_size_bytes=file_size_bytes,
                chunk_size_bytes=chunk_size_bytes,
                total_pieces=metadata.total_pieces,
                swarm_size=num_seeders,
                strategy=piece_strategy,
                duration_sec=duration,
                is_successful=is_success,
                failure_rate_simulated=kill_peers_midway / max(num_seeders, 1),
                extra_meta={"killed_peers": kill_peers_midway}
            )
            return record

        finally:
            if leecher:
                try:
                    leecher.stop()
                except Exception:
                    pass
            for s in seeders:
                try:
                    s.stop()
                except Exception:
                    pass
            if tracker:
                try:
                    tracker.stop()
                except Exception:
                    pass
            shutil.rmtree(temp_dir, ignore_errors=True)

    def run_matrix_benchmark(
        self,
        peer_counts: Optional[List[int]] = None,
        file_sizes_mb: Optional[List[int]] = None,
        iterations: int = 2
    ) -> pd.DataFrame:
        if peer_counts is None:
            peer_counts = [1, 2, 4, 8, 12, 16]
        if file_sizes_mb is None:
            file_sizes_mb = [1, 5, 10, 20]

        total_runs = len(peer_counts) * len(file_sizes_mb) * iterations
        cur = 0
        print("=" * 70)
        print("MEMBER 4: EXECUTING SCALING CLAIM BENCHMARK MATRIX")
        print(f"Peer Counts: {peer_counts} | File Sizes: {file_sizes_mb} MB")
        print("=" * 70)

        for sz in file_sizes_mb:
            sz_bytes = sz * 1024 * 1024
            for p in peer_counts:
                for rep in range(iterations):
                    cur += 1
                    print(f"[{cur}/{total_runs}] Size: {sz:2d}MB | Peers: {p:2d} | Rep: {rep+1}...", end="", flush=True)
                    res = self.run_single_transfer(sz_bytes, p, chunk_size_bytes=64 * 1024)
                    print(f" -> {'PASS' if res['is_successful'] else 'FAIL'} in {res['duration_sec']:.3f}s ({res['throughput_mb_s']:.2f} MB/s)")

        self.collector.export_csv(
            os.path.join(self.output_dir, "raw_metrics.csv"),
            os.path.join(self.output_dir, "piece_transfer_logs.csv")
        )
        return self.collector.get_summary_dataframe()

    def run_chunk_size_sweep(self, file_size_mb: int = 5, peer_count: int = 4) -> pd.DataFrame:
        chunks_kb = [16, 32, 64, 128, 256, 512]
        sz_bytes = file_size_mb * 1024 * 1024
        print("\n" + "=" * 70)
        print("MEMBER 4: CHUNK SIZE TRADEOFF BENCHMARK")
        print("=" * 70)

        for c_kb in chunks_kb:
            print(f"Testing Chunk Size: {c_kb:3d} KB...", end="", flush=True)
            res = self.run_single_transfer(sz_bytes, peer_count, chunk_size_bytes=c_kb * 1024)
            print(f" -> {res['duration_sec']:.3f}s ({res['throughput_mb_s']:.2f} MB/s)")

        return self.collector.get_summary_dataframe()

    def run_fault_sweep(self, file_size_mb: int = 5, total_peers: int = 6) -> pd.DataFrame:
        scenarios = [("0 Crashes", 0), ("1 Crash", 1), ("2 Crashes", 2), ("3 Crashes", 3)]
        sz_bytes = file_size_mb * 1024 * 1024
        print("\n" + "=" * 70)
        print("MEMBER 4: FAULT TOLERANCE & CHURN RECOVERY BENCHMARK")
        print("=" * 70)

        for label, killed in scenarios:
            print(f"Running scenario: {label}...", end="", flush=True)
            res = self.run_single_transfer(sz_bytes, total_peers, kill_peers_midway=killed)
            print(f" -> Success: {res['is_successful']} in {res['duration_sec']:.3f}s")

        return self.collector.get_summary_dataframe()


class ExperimentAnalyzer:
    """Computes statistical summaries, Amdahl's Law speedups, and Markdown tables."""

    def __init__(self, df_metrics: pd.DataFrame, df_pieces: Optional[pd.DataFrame] = None):
        self.df = df_metrics.copy()
        self.piece_df = df_pieces.copy() if df_pieces is not None else pd.DataFrame()

    def compute_summary_table(self) -> pd.DataFrame:
        valid = self.df[self.df["is_successful"] == True].copy()
        if valid.empty:
            return pd.DataFrame()

        grouped = valid.groupby(["file_size_mb", "swarm_size"]).agg(
            mean_duration=("duration_sec", "mean"),
            std_duration=("duration_sec", "std"),
            mean_throughput=("throughput_mb_s", "mean"),
            trials=("duration_sec", "count")
        ).reset_index()

        baseline_times = {}
        for sz, sub in grouped.groupby("file_size_mb"):
            b = sub[sub["swarm_size"] == 1]
            baseline_times[sz] = b["mean_duration"].values[0] if not b.empty else sub["mean_duration"].values[0]

        speedups, efficiencies = [], []
        for _, row in grouped.iterrows():
            sz = row["file_size_mb"]
            n = row["swarm_size"]
            t_base = baseline_times.get(sz, row["mean_duration"])
            t_curr = row["mean_duration"]
            s = t_base / max(t_curr, 0.0001)
            eff = (s / n) * 100.0
            speedups.append(round(s, 2))
            efficiencies.append(round(eff, 1))

        grouped["speedup"] = speedups
        grouped["efficiency_pct"] = efficiencies
        grouped["mean_duration"] = grouped["mean_duration"].round(4)
        grouped["std_duration"] = grouped["std_duration"].fillna(0.0).round(4)
        grouped["mean_throughput"] = grouped["mean_throughput"].round(2)
        return grouped

    def generate_markdown_tables(self) -> str:
        summary = self.compute_summary_table()
        lines = [
            "### Empirical Performance Comparison Table\n",
            "| File Size (MB) | Swarm Size (Peers) | Mean Time (s) | Std Dev (s) | Mean Throughput (MB/s) | Speedup S(N) | Efficiency (%) |",
            "|:--------------:|:------------------:|:-------------:|:-----------:|:----------------------:|:------------:|:--------------:|"
        ]
        for _, r in summary.iterrows():
            lines.append(f"| {r['file_size_mb']} MB | {int(r['swarm_size'])} peers | {r['mean_duration']:.4f} s | {r['std_duration']:.4f} s | {r['mean_throughput']:.2f} MB/s | {r['speedup']:.2f}x | {r['efficiency_pct']:.1f}% |")
        lines.append("\n")
        return "\n".join(lines)


class BenchmarkVisualizer:
    """Generates 6 publication-ready figures using Matplotlib."""

    def __init__(self, output_dir: str = "./results/figures"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self._setup_style()

    def _setup_style(self):
        plt.rcParams.update({
            "font.size": 11,
            "axes.labelsize": 12,
            "axes.titlesize": 13,
            "grid.alpha": 0.35,
            "lines.linewidth": 2.2,
            "lines.markersize": 7
        })

    def plot_download_time_vs_peers(self, df_summary: pd.DataFrame, filename: str = "download_time_vs_peers.png") -> str:
        fig, ax = plt.subplots(figsize=(8.5, 5.2), dpi=300)
        colors = ["#2b5c8f", "#d95f02", "#7570b3", "#1b9e77"]
        markers = ["o", "s", "^", "D"]

        sizes = sorted(df_summary["file_size_mb"].unique())
        for idx, sz in enumerate(sizes):
            sub = df_summary[df_summary["file_size_mb"] == sz].sort_values("swarm_size")
            ax.errorbar(
                sub["swarm_size"], sub["mean_duration"], yerr=sub["std_duration"],
                fmt=f"-{markers[idx % len(markers)]}", color=colors[idx % len(colors)],
                label=f"File Size: {sz} MB", capsize=4, capthick=1.2, elinewidth=1.2
            )

        ax.set_title("Download Time vs. Number of Available Peers", fontweight="bold", pad=12)
        ax.set_xlabel("Number of Available Peers (Swarm Size)")
        ax.set_ylabel("Download Time (seconds)")
        ax.set_xticks(sorted(df_summary["swarm_size"].unique()))
        ax.grid(True, linestyle="--")
        ax.legend(frameon=True, facecolor="white", shadow=True)

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path

    def plot_throughput_vs_peers(self, df_summary: pd.DataFrame, filename: str = "throughput_vs_peers.png") -> str:
        fig, ax = plt.subplots(figsize=(8.5, 5.2), dpi=300)
        colors = ["#2b5c8f", "#d95f02", "#7570b3", "#1b9e77"]
        markers = ["o", "s", "^", "D"]

        sizes = sorted(df_summary["file_size_mb"].unique())
        for idx, sz in enumerate(sizes):
            sub = df_summary[df_summary["file_size_mb"] == sz].sort_values("swarm_size")
            ax.plot(
                sub["swarm_size"], sub["mean_throughput"],
                f"-{markers[idx % len(markers)]}", color=colors[idx % len(colors)],
                label=f"{sz} MB File"
            )

        ax.set_title("Effective Download Throughput vs. Swarm Size", fontweight="bold", pad=12)
        ax.set_xlabel("Number of Available Peers")
        ax.set_ylabel("Throughput (MB/s)")
        ax.set_xticks(sorted(df_summary["swarm_size"].unique()))
        ax.grid(True, linestyle="--")
        ax.legend(frameon=True, loc="upper left")

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path

    def plot_speedup_analysis(self, df_summary: pd.DataFrame, filename: str = "speedup_analysis.png") -> str:
        fig, ax = plt.subplots(figsize=(8.5, 5.2), dpi=300)
        max_sz = df_summary["file_size_mb"].max()
        sub = df_summary[df_summary["file_size_mb"] == max_sz].sort_values("swarm_size")
        peers = sub["swarm_size"].values
        speedup = sub["speedup"].values

        ax.plot(peers, peers, "--", color="#7f8c8d", label="Ideal Linear Speedup (y = x)", linewidth=1.8)
        p = 0.92
        amdahl_s = [1.0 / ((1.0 - p) + (p / n)) for n in peers]
        ax.plot(peers, amdahl_s, "-.", color="#e74c3c", label="Amdahl's Law Model (P=0.92)", linewidth=2.0)
        ax.plot(peers, speedup, "-o", color="#2980b9", label=f"Measured P2P Speedup ({max_sz}MB File)", linewidth=2.5)

        ax.set_title("Concurrency Speedup S(N) vs. Swarm Scalability", fontweight="bold", pad=12)
        ax.set_xlabel("Available Peers (N)")
        ax.set_ylabel("Speedup Factor S(N) = T(1) / T(N)")
        ax.set_xticks(peers)
        ax.grid(True, linestyle="--")
        ax.legend(frameon=True, loc="upper left")

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path

    def plot_piece_distribution(self, df_pieces: pd.DataFrame, filename: str = "piece_distribution.png") -> str:
        if df_pieces.empty or "source_peer_id" not in df_pieces.columns:
            return ""
        fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=300)
        counts = df_pieces["source_peer_id"].value_counts().sort_index()
        bars = ax.bar(counts.index, counts.values, color="#3498db", edgecolor="#2980b9", width=0.55)

        ax.set_title("Piece Transfer Distribution Across Swarm Peers", fontweight="bold", pad=12)
        ax.set_xlabel("Peer ID")
        ax.set_ylabel("Number of Pieces Supplied")
        ax.grid(axis="y", linestyle="--", alpha=0.4)

        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height}", xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontweight="semibold"
            )

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path

    def plot_chunk_size_tradeoff(self, df_chunks: pd.DataFrame, filename: str = "chunk_size_tradeoff.png") -> str:
        fig, ax1 = plt.subplots(figsize=(8.5, 5.0), dpi=300)
        sub = df_chunks.sort_values("chunk_size_kb")
        x = [f"{int(c)} KB" for c in sub["chunk_size_kb"]]

        color_time = "#2980b9"
        ax1.set_xlabel("Chunk Size (KB)")
        ax1.set_ylabel("Download Time (s)", color=color_time)
        l1 = ax1.plot(x, sub["duration_sec"], "-o", color=color_time, label="Download Time (s)")
        ax1.tick_params(axis="y", labelcolor=color_time)
        ax1.grid(True, linestyle="--")

        ax2 = ax1.twinx()
        color_tp = "#27ae60"
        ax2.set_ylabel("Throughput (MB/s)", color=color_tp)
        l2 = ax2.plot(x, sub["throughput_mb_s"], "-s", color=color_tp, label="Throughput (MB/s)")
        ax2.tick_params(axis="y", labelcolor=color_tp)

        lines = l1 + l2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc="center right", frameon=True)
        plt.title("Chunk Size Tradeoff: Socket Framing vs. Pipeline Concurrency", fontweight="bold", pad=12)

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path

    def plot_fault_resilience(self, df_faults: pd.DataFrame, filename: str = "peer_failure_impact.png") -> str:
        fig, ax = plt.subplots(figsize=(8.0, 4.8), dpi=300)
        labels = [f"{int(r.get('killed_peers', 0))} Peers Failed" for _, r in df_faults.iterrows()]
        times = df_faults["duration_sec"].values

        bars = ax.bar(labels, times, color=["#27ae60", "#f39c12", "#e67e22", "#c0392b"], width=0.5)
        ax.set_title("Fault Tolerance: Impact of Mid-Transfer Peer Disconnection", fontweight="bold", pad=12)
        ax.set_ylabel("Total Download & Recovery Time (seconds)")
        ax.grid(axis="y", linestyle="--", alpha=0.4)

        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height:.2f}s", xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 4), textcoords="offset points", ha="center", va="bottom", fontweight="bold"
            )

        path = os.path.join(self.output_dir, filename)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return path
