"""
main.py - Unified Entry Point for MapReduce Word Count Simulation

Command-Line & GUI Interface for INTE 22253 Part C Option 2 Project.

Usage Examples:
    # 1. Run MapReduce on default 50k dataset with 4 mappers:
    python3 main.py

    # 2. Run MapReduce with custom dataset and worker count:
    python3 main.py --file sample_data/performance_500k.txt --mappers 4

    # 3. Run Sequential Baseline:
    python3 main.py --baseline --file sample_data/correctness_50k.txt

    # 4. Verify Correctness (Compare Baseline vs MapReduce):
    python3 main.py --verify

    # 5. Run Full 3-Trial Performance Benchmarking Experiment:
    python3 main.py --benchmark

    # 6. Generate datasets:
    python3 main.py --generate 50k
    python3 main.py --generate 500k

    # 7. Launch Graphical User Interface:
    python3 main.py --gui
"""

import argparse
import os
import sys

from baseline import run_baseline
from generate_data import generate_dataset
from mapreduce_core import run_mapreduce_job


def main():
    parser = argparse.ArgumentParser(
        description="MapReduce-Style Word Count Simulation (INTE 22253 Part C Option 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 main.py                           # Run MapReduce (4 mappers, 2 reducers)
  python3 main.py --baseline                # Run Baseline Word Count
  python3 main.py --verify                  # Run Correctness Verification (PASS check)
  python3 main.py --benchmark               # Run 3-Trial Benchmark Experiment (Generates CSV)
  python3 main.py --gui                     # Launch Tkinter GUI
""",
    )

    parser.add_argument(
        "-f", "--file", type=str, default=None, help="Path to input text dataset file"
    )
    parser.add_argument(
        "-m", "--mappers", type=int, default=4, help="Number of Mapper worker processes (default: 4)"
    )
    parser.add_argument(
        "-r", "--reducers", type=int, default=2, help="Number of Reducer worker processes (default: 2)"
    )
    parser.add_argument(
        "--baseline", action="store_true", help="Run single-process baseline word count"
    )
    parser.add_argument(
        "--verify", action="store_true", help="Run automated correctness verification (Baseline vs MapReduce)"
    )
    parser.add_argument(
        "--benchmark", action="store_true", help="Run full 3-trial performance experiment across configurations"
    )
    parser.add_argument(
        "--generate", type=str, choices=["50k", "500k", "all"], help="Generate synthetic test datasets"
    )
    parser.add_argument(
        "--gui", action="store_true", help="Launch interactive Tkinter Graphical User Interface"
    )

    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    sample_dir = os.path.join(base_dir, "sample_data")
    os.makedirs(sample_dir, exist_ok=True)

    default_50k = os.path.join(sample_dir, "correctness_50k.txt")
    default_500k = os.path.join(sample_dir, "performance_500k.txt")

    # Mode 1: GUI
    if args.gui:
        try:
            from gui import launch_gui
            launch_gui()
        except Exception as e:
            print(f"[ERROR] Failed to launch GUI: {e}")
            print("Note: In headless or non-display terminal environments, use CLI flags.")
        return

    # Mode 2: Dataset Generation
    if args.generate:
        if args.generate == "50k":
            generate_dataset(default_50k, 50000)
        elif args.generate == "500k":
            generate_dataset(default_500k, 500000)
        elif args.generate == "all":
            generate_dataset(default_50k, 50000)
            generate_dataset(default_500k, 500000)
        return

    # Mode 3: Correctness Verification
    if args.verify:
        from benchmark import verify_correctness
        if not os.path.exists(default_50k):
            print(f"[main] Generating 50k correctness dataset at '{default_50k}'...")
            generate_dataset(default_50k, 50000)
        verify_correctness(default_50k)
        return

    # Mode 4: Performance Benchmark
    if args.benchmark:
        from benchmark import run_benchmarks, verify_correctness
        if not os.path.exists(default_50k):
            generate_dataset(default_50k, 50000)
        if not verify_correctness(default_50k):
            print("ERROR: Correctness check failed. Aborting benchmark.")
            sys.exit(1)
        if not os.path.exists(default_500k):
            generate_dataset(default_500k, 500000)
        csv_path = os.path.join(base_dir, "performance_results.csv")
        word_out = os.path.join(sample_dir, "final_word_counts.txt")
        run_benchmarks(default_500k, csv_path, word_out, runs_per_config=3)
        return

    # Resolve target dataset file
    target_file = args.file
    if not target_file:
        target_file = default_50k
        if not os.path.exists(target_file):
            print(f"[main] Generating default dataset at '{target_file}'...")
            generate_dataset(target_file, 50000)

    # Mode 5: Baseline
    if args.baseline:
        print(f"=== Running Baseline Word Count on '{target_file}' ===")
        counts, elapsed = run_baseline(target_file)
        print(f"Execution Time  : {elapsed:.4f} seconds")
        print(f"Total Words     : {sum(counts.values()):,}")
        print(f"Unique Words    : {len(counts):,}")
        top_10 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
        print("\nTop 10 Most Frequent Words:")
        for rank, (word, freq) in enumerate(top_10, start=1):
            print(f"  {rank:2d}. {word:<25} : {freq:,}")
        return

    # Mode 6: Default MapReduce Job
    print(f"=== Running MapReduce Word Count on '{target_file}' ===")
    counts, elapsed = run_mapreduce_job(
        file_path=target_file,
        num_mappers=args.mappers,
        num_reducers=args.reducers,
        log_callback=print,
    )
    top_10 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 Most Frequent Words:")
    for rank, (word, freq) in enumerate(top_10, start=1):
        print(f"  {rank:2d}. {word:<25} : {freq:,}")


if __name__ == "__main__":
    main()
