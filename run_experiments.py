"""
Role: Performance Telemetry, Visualization, Experiment Analysis & Master Integration

Executes:
1. Matrix scaling benchmark across peer counts and file sizes.
2. Chunk size tradeoff analysis (16 KB - 512 KB).
3. Fault resilience & churn recovery benchmark.
4. Statistical aggregation and Amdahl's Law validation.
5. Generates all 6 publication figures and Markdown summary tables.
"""

import argparse
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


def parse_arguments():
    """CLI Argument Configuration (Member 3 Manual Scaffolding: Udbhav Singh)"""
    parser = argparse.ArgumentParser(
        description="P2P Swarm Performance Benchmark & Figure Generator CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["all", "matrix", "chunk", "fault", "viz"],
        default="all",
        help="Benchmark execution mode to run",
    )
    parser.add_argument(
        "--peers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8, 12, 16],
        help="Swarm peer count evaluation matrix",
    )
    parser.add_argument(
        "--file-sizes",
        type=int,
        nargs="+",
        default=[1, 5, 10, 20],
        help="Payload file sizes (in MB) for evaluation matrix",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Number of iterations per matrix cell",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.path.join(base_dir, "results"),
        help="Directory to save raw telemetry CSVs and generated figures",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    print("=" * 80)
    print("  CS-30003 PEER-TO-PEER FILE DISTRIBUTION SYSTEM")
    print("  MEMBER 4 BENCHMARK SUITE: PERFORMANCE, VISUALIZATION & ANALYSIS")
    print("  Author: Udbhav Singh (241551102) | Section: CSE-14")
    print(f"  Mode: {args.mode.upper()} | Output: {args.output_dir}")
    print("=" * 80)

    # Output directory scaffolding (Member 3 manual setup)
    results_dir = os.path.abspath(args.output_dir)
    figures_dir = os.path.join(results_dir, "figures")
    raw_dir = os.path.join(results_dir, "raw_metrics")
    os.makedirs(figures_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    runner = ExperimentRunner(output_dir=results_dir)

    # 1. Run Core Scaling Matrix (Proposal Section 3)
    if args.mode in ("all", "matrix"):
        print(f"\n>>> STEP 1: Running Core Scaling Matrix across peers {args.peers} and sizes {args.file_sizes} MB...")
        runner.run_matrix_benchmark(
            peer_counts=args.peers,
            file_sizes_mb=args.file_sizes,
            iterations=args.iterations,
        )

    # 2. Run Chunk Size Sweep
    if args.mode in ("all", "chunk"):
        print("\n>>> STEP 2: Running Chunk Size Sweep (16 KB - 512 KB)...")
        runner.run_chunk_size_sweep(file_size_mb=5, peer_count=4)

    # 3. Run Fault Tolerance Sweep
    if args.mode in ("all", "fault"):
        print("\n>>> STEP 3: Running Fault Tolerance & Churn Recovery Sweep...")
        runner.run_fault_sweep(file_size_mb=5, total_peers=6)

    # 4. Statistical Analysis
    if args.mode in ("all", "matrix", "chunk", "fault", "viz"):
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
