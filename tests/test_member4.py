"""
tests/test_member4.py - Unit & Integration Tests for Member 4 Deliverables
Author: Udbhav Singh (241551102) - Section: CSE-14
"""

import os
import shutil
import tempfile
import unittest

try:
    import pandas as pd
except ImportError:
    pd = None

from src.performance import (
    PerformanceMetricsCollector,
    ExperimentAnalyzer,
    BenchmarkVisualizer
)
from integration import run_integration_test


class TestMember4Performance(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="m4_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_metrics_collector(self):
        collector = PerformanceMetricsCollector()
        rec = collector.record_transfer_summary(
            experiment_id="test_exp",
            file_size_bytes=4 * 1024 * 1024,
            chunk_size_bytes=64 * 1024,
            total_pieces=64,
            swarm_size=4,
            strategy="rarest_first",
            duration_sec=2.0,
            is_successful=True
        )
        self.assertEqual(rec["file_size_mb"], 4.0)
        self.assertEqual(rec["throughput_mb_s"], 2.0)

        df = collector.get_summary_dataframe()
        self.assertEqual(len(df), 1)

    @unittest.skipIf(pd is None, "pandas is required for statistical dataframe analysis")
    def test_analyzer_and_markdown_tables(self):
        df_dummy = pd.DataFrame([
            {"file_size_mb": 5.0, "swarm_size": 1, "duration_sec": 4.0, "throughput_mb_s": 1.25, "is_successful": True},
            {"file_size_mb": 5.0, "swarm_size": 2, "duration_sec": 2.2, "throughput_mb_s": 2.27, "is_successful": True},
            {"file_size_mb": 5.0, "swarm_size": 4, "duration_sec": 1.5, "throughput_mb_s": 3.33, "is_successful": True},
        ])
        analyzer = ExperimentAnalyzer(df_dummy)
        summary = analyzer.compute_summary_table()
        self.assertEqual(len(summary), 3)
        self.assertIn("speedup", summary.columns)
        self.assertIn("efficiency_pct", summary.columns)

        # Baseline (N=1) speedup should be 1.0
        self.assertEqual(summary.loc[summary["swarm_size"] == 1, "speedup"].values[0], 1.0)
        # N=2 speedup = 4.0 / 2.2 = 1.82
        self.assertAlmostEqual(summary.loc[summary["swarm_size"] == 2, "speedup"].values[0], 1.82, places=1)

        md = analyzer.generate_markdown_tables()
        self.assertIn("| 5.0 MB | 1 peers |", md)

    @unittest.skipIf(pd is None, "pandas and matplotlib are required for visualization tests")
    def test_visualizer_generates_plots(self):
        df_summary = pd.DataFrame([
            {"file_size_mb": 5.0, "swarm_size": 1, "mean_duration": 4.0, "std_duration": 0.1, "mean_throughput": 1.25, "speedup": 1.0, "efficiency_pct": 100.0},
            {"file_size_mb": 5.0, "swarm_size": 2, "mean_duration": 2.2, "std_duration": 0.1, "mean_throughput": 2.27, "speedup": 1.82, "efficiency_pct": 91.0},
            {"file_size_mb": 5.0, "swarm_size": 4, "mean_duration": 1.5, "std_duration": 0.1, "mean_throughput": 3.33, "speedup": 2.67, "efficiency_pct": 66.8},
        ])
        figures_dir = os.path.join(self.test_dir, "figures")
        viz = BenchmarkVisualizer(output_dir=figures_dir)

        p1 = viz.plot_download_time_vs_peers(df_summary)
        p2 = viz.plot_throughput_vs_peers(df_summary)
        p3 = viz.plot_speedup_analysis(df_summary)

        self.assertTrue(os.path.isfile(p1))
        self.assertTrue(os.path.isfile(p2))
        self.assertTrue(os.path.isfile(p3))

    def test_full_system_integration(self):
        """End-to-end integration test with real loopback TCP sockets."""
        success = run_integration_test(file_size_mb=1, num_seeders=2)
        self.assertTrue(success, "P2P System Integration transfer should complete successfully.")


if __name__ == "__main__":
    unittest.main()
