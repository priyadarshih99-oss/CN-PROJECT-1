"""
run_experiments.py - Master Benchmark & Figure Generation CLI
CS-30003 P2P File Distribution System
Author: Udbhav Singh (241551102) - Section: CSE-14

Executes:
1. Matrix scaling benchmark across peer counts and file sizes.
2. Chunk size tradeoff analysis (16 KB - 512 KB).
3. Fault resilience & churn recovery benchmark.
4. Generates all 6 publication figures and Markdown summary tables.
"""

import os
import sys

base_dir = os.path.dirname(os.path.abspath(__file__))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from src.performance import (
    ExperimentRunner,
    ExperimentAnalyzer,
    BenchmarkVisualizer
)


def main():
    print("=" * 80)
    print("  CS-30003 PEER-TO-PEER FILE DISTRIBUTION SYSTEM")
    print("  MEMBER 4 BENCHMARK SUITE: PERFORMANCE, VISUALIZATION & ANALYSIS")
    print("  Author: Udbhav Singh (241551102) | Section: CSE-14")
    print("=" * 80)

    results_dir = os.path.join(base_dir, "results")
    figures_dir = os.path.join(results_dir, "figures")
    os.makedirs(figures_dir, exist_ok=True)

    runner = ExperimentRunner(output_dir=results_dir)

    # 1. Run Core Scaling Matrix (Proposal Section 3)
    print("\n>>> STEP 1: Running Core Scaling Matrix (Peers & File Sizes)...")
    peer_matrix = [1, 2, 4, 8, 12, 16]
    size_matrix = [1, 5, 10, 20]
    df_raw = runner.run_matrix_benchmark(
        peer_counts=peer_matrix,
        file_sizes_mb=size_matrix,
        iterations=2
    )

    # 2. Run Chunk Size Sweep
    print("\n>>> STEP 2: Running Chunk Size Sweep (16 KB - 512 KB)...")
    runner.run_chunk_size_sweep(file_size_mb=5, peer_count=4)

    # 3. Run Fault Tolerance Sweep
    print("\n>>> STEP 3: Running Fault Tolerance & Churn Recovery Sweep...")
    runner.run_fault_sweep(file_size_mb=5, total_peers=6)

    # 4. Statistical Analysis
    print("\n>>> STEP 4: Generating Statistical Aggregations & Comparison Tables...")
    analyzer = ExperimentAnalyzer(
        df_metrics=runner.collector.get_summary_dataframe(),
        df_pieces=runner.collector.get_piece_events_dataframe()
    )
    summary_df = analyzer.compute_summary_table()
    tables_md = analyzer.generate_markdown_tables()

    summary_file = os.path.join(results_dir, "summary_tables.md")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("# Empirical Performance Benchmark Results\n\n")
        f.write(tables_md)

    print(f"Summary tables written to: {summary_file}")
    print("\n" + tables_md)

    # 5. Visualizations
    print(">>> STEP 5: Generating High-Resolution Figures via Matplotlib...")
    viz = BenchmarkVisualizer(output_dir=figures_dir)

    f1 = viz.plot_download_time_vs_peers(summary_df)
    print(f"  [Figure 1] {f1}")

    f2 = viz.plot_throughput_vs_peers(summary_df)
    print(f"  [Figure 2] {f2}")

    f3 = viz.plot_speedup_analysis(summary_df)
    print(f"  [Figure 3] {f3}")

    f4 = viz.plot_piece_distribution(runner.collector.get_piece_events_dataframe())
    print(f"  [Figure 4] {f4}")

    # Chunk size plot
    raw_df = runner.collector.get_summary_dataframe()
    chunk_df = raw_df[raw_df["swarm_size"] == 4].drop_duplicates(subset=["chunk_size_kb"])
    f5 = viz.plot_chunk_size_tradeoff(chunk_df)
    print(f"  [Figure 5] {f5}")

    # Fault resilience plot
    fault_df = raw_df[raw_df["failure_rate_simulated"] > 0.0]
    baseline_row = raw_df[(raw_df["swarm_size"] == 6) & (raw_df["failure_rate_simulated"] == 0.0)]
    if not baseline_row.empty:
        import pandas as pd
        fault_df = pd.concat([baseline_row.iloc[[0]], fault_df])
    if not fault_df.empty:
        f6 = viz.plot_fault_resilience(fault_df)
        print(f"  [Figure 6] {f6}")

    print("\n" + "=" * 80)
    print("BENCHMARK MATRIX EXECUTION & PLOT GENERATION COMPLETE!")
    print(f"Figures saved in: {figures_dir}")
    print("=" * 80)


if __name__ == "__main__":
    main()
