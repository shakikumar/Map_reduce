"""
benchmark.py - Automated Correctness Testing and Performance Benchmarking Harness

This script performs two critical functions:
1. Automated Correctness Verification:
   - Executes Baseline and MapReduce on the ~50,000-line dataset.
   - Verifies key-by-key and value-by-value equivalence.
   - Displays "PASS: Results match!" on success.
   - Halts immediately if results diverge.

2. Performance Benchmarking:
   - Generates and tests the ~500,000-line dataset.
   - Evaluates 4 configurations:
     a) Baseline (Single Process)
     b) MapReduce (1 Mapper + 2 Reducers)
     c) MapReduce (2 Mappers + 2 Reducers)
     d) MapReduce (4 Mappers + 2 Reducers)
   - Executes 3 trials per configuration and calculates the arithmetic mean.
   - Records system CPU architecture, core count, dataset size, and actual execution times.
   - Saves clean benchmark metrics to `performance_results.csv` and final word counts to `sample_data/final_word_counts.txt`.
"""

import csv
import os
import platform
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple

from baseline import run_baseline
from generate_data import generate_dataset
from mapreduce_core import run_mapreduce_job


def get_cpu_info() -> Tuple[str, int]:
    """Retrieves the host machine's CPU model and physical/logical core count."""
    cores = os.cpu_count() or 1
    cpu_model = platform.processor() or platform.machine() or "Unknown CPU"

    # Try platform-specific query for macOS
    if platform.system() == "Darwin":
        try:
            brand = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"]).decode().strip()
            if brand:
                cpu_model = brand
        except Exception:
            try:
                model = subprocess.check_output(["sysctl", "-n", "hw.model"]).decode().strip()
                if model:
                    cpu_model = model
            except Exception:
                pass
    return cpu_model, cores


def verify_correctness(dataset_path: str) -> bool:
    """
    Executes Baseline and MapReduce (4 mappers, 2 reducers) on `dataset_path`
    and strictly validates output equivalence.

    Returns:
        True if all word counts match 100%, False otherwise.
    """
    print("\n" + "=" * 60)
    print("STAGE 1: CORRECTNESS VERIFICATION")
    print("=" * 60)
    print(f"Testing on dataset: {dataset_path}")

    # 1. Baseline
    print("\n[1/2] Running Sequential Baseline...")
    base_counts, base_time = run_baseline(dataset_path)
    print(f"      Baseline completed in {base_time:.4f}s ({len(base_counts):,} unique words)")

    # 2. MapReduce (4 mappers, 2 reducers)
    print("\n[2/2] Running Multiprocess MapReduce (4 Mappers, 2 Reducers)...")
    mr_counts, mr_time = run_mapreduce_job(dataset_path, num_mappers=4, num_reducers=2)
    print(f"      MapReduce completed in {mr_time:.4f}s ({len(mr_counts):,} unique words)")

    # 3. Key and Value Comparison
    print("\nComparing Baseline vs MapReduce results...")
    if len(base_counts) != len(mr_counts):
        print(f"FAIL: Key count mismatch! Baseline: {len(base_counts)}, MapReduce: {len(mr_counts)}")
        return False

    mismatches = 0
    for word, base_count in base_counts.items():
        mr_count = mr_counts.get(word, None)
        if mr_count is None:
            print(f"FAIL: Word '{word}' missing from MapReduce results!")
            mismatches += 1
            if mismatches >= 5:
                break
        elif mr_count != base_count:
            print(f"FAIL: Word '{word}' count mismatch! Baseline: {base_count}, MapReduce: {mr_count}")
            mismatches += 1
            if mismatches >= 5:
                break

    if mismatches > 0:
        print(f"\nResult: FAILED with {mismatches} mismatch(es).")
        return False

    print("\n" + "*" * 40)
    print("  PASS: Results match!")
    print("*" * 40 + "\n")
    return True


def run_benchmarks(
    dataset_path: str,
    output_csv_path: str,
    word_count_output_path: str,
    runs_per_config: int = 3,
) -> List[Dict]:
    """
    Executes 3 trials for each required configuration and writes results to CSV.
    """
    cpu_model, core_count = get_cpu_info()
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset_lines = sum(1 for _ in f)

    print("=" * 60)
    print("STAGE 2: PERFORMANCE BENCHMARKING EXPERIMENT")
    print("=" * 60)
    print(f"System CPU   : {cpu_model} ({core_count} Cores)")
    print(f"Dataset File : {dataset_path} ({dataset_lines:,} lines)")
    print(f"Trials/Config: {runs_per_config} runs (Averaged)\n")

    configurations = [
        {"name": "Baseline", "mappers": 1, "reducers": 0, "type": "baseline"},
        {"name": "MapReduce (1M + 2R)", "mappers": 1, "reducers": 2, "type": "mapreduce"},
        {"name": "MapReduce (2M + 2R)", "mappers": 2, "reducers": 2, "type": "mapreduce"},
        {"name": "MapReduce (4M + 2R)", "mappers": 4, "reducers": 2, "type": "mapreduce"},
    ]

    benchmark_rows = []
    final_sample_counts: Optional[Dict[str, int]] = None

    for config in configurations:
        cfg_name = config["name"]
        m_count = config["mappers"]
        r_count = config["reducers"]
        cfg_type = config["type"]

        print(f"\n>>> Evaluating Configuration: {cfg_name} <<<")
        run_times = []

        for trial in range(1, runs_per_config + 1):
            print(f"  [Run {trial}/{runs_per_config}] Executing...", end="", flush=True)
            if cfg_type == "baseline":
                counts, duration = run_baseline(dataset_path)
            else:
                counts, duration = run_mapreduce_job(
                    file_path=dataset_path,
                    num_mappers=m_count,
                    num_reducers=r_count,
                    log_callback=None,
                )
            run_times.append(duration)
            print(f" Done ({duration:.4f}s)")
            if final_sample_counts is None:
                final_sample_counts = counts

        avg_time = sum(run_times) / len(run_times)
        print(f"  -> Average Execution Time: {avg_time:.4f}s")

        row = {
            "Configuration": cfg_name,
            "Mappers": m_count,
            "Reducers": r_count,
            "Dataset Lines": dataset_lines,
            "CPU Model": cpu_model,
            "CPU Cores": core_count,
            "Run 1 (s)": round(run_times[0], 4),
            "Run 2 (s)": round(run_times[1], 4),
            "Run 3 (s)": round(run_times[2], 4),
            "Average (s)": round(avg_time, 4),
        }
        benchmark_rows.append(row)

    # Calculate baseline speedup comparison
    base_avg = benchmark_rows[0]["Average (s)"]
    for row in benchmark_rows:
        speedup = base_avg / row["Average (s)"] if row["Average (s)"] > 0 else 1.0
        row["Speedup vs Baseline"] = round(speedup, 2)

    # Save to CSV
    os.makedirs(os.path.dirname(os.path.abspath(output_csv_path)), exist_ok=True)
    fieldnames = [
        "Configuration",
        "Mappers",
        "Reducers",
        "Dataset Lines",
        "CPU Model",
        "CPU Cores",
        "Run 1 (s)",
        "Run 2 (s)",
        "Run 3 (s)",
        "Average (s)",
        "Speedup vs Baseline",
    ]

    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(benchmark_rows)

    print(f"\n[benchmark] Performance metrics successfully saved to '{output_csv_path}'")

    # Save final word count sample to evidence file
    if final_sample_counts:
        os.makedirs(os.path.dirname(os.path.abspath(word_count_output_path)), exist_ok=True)
        sorted_counts = sorted(final_sample_counts.items(), key=lambda x: x[1], reverse=True)
        with open(word_count_output_path, "w", encoding="utf-8") as out_f:
            out_f.write(f"# Final Word Counts Summary ({dataset_lines:,} lines)\n")
            out_f.write(f"# Total Words: {sum(final_sample_counts.values()):,}\n")
            out_f.write(f"# Unique Words: {len(final_sample_counts):,}\n\n")
            out_f.write("Rank\tWord\tFrequency\n")
            out_f.write("-" * 35 + "\n")
            for rank, (word, count) in enumerate(sorted_counts[:100], start=1):
                out_f.write(f"{rank}\t{word}\t{count:,}\n")
        print(f"[benchmark] Top 100 word counts saved to '{word_count_output_path}'")

    # Display clean ASCII performance table
    print("\n" + "=" * 95)
    print("FINAL PERFORMANCE BENCHMARK SUMMARY")
    print("=" * 95)
    print(
        f"{'Configuration':<24} | {'Mappers':<7} | {'Reducers':<8} | {'Run 1':<8} | {'Run 2':<8} | {'Run 3':<8} | {'Average':<8} | {'Speedup'}"
    )
    print("-" * 95)
    for r in benchmark_rows:
        print(
            f"{r['Configuration']:<24} | {r['Mappers']:<7} | {r['Reducers']:<8} | "
            f"{r['Run 1 (s)']:<8.4f} | {r['Run 2 (s)']:<8.4f} | {r['Run 3 (s)']:<8.4f} | "
            f"{r['Average (s)']:<8.4f} | {r['Speedup vs Baseline']}x"
        )
    print("=" * 95 + "\n")

    return benchmark_rows


def main() -> None:
    """Main benchmark entry point."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "sample_data")
    os.makedirs(data_dir, exist_ok=True)

    correctness_path = os.path.join(data_dir, "correctness_50k.txt")
    performance_path = os.path.join(data_dir, "performance_500k.txt")
    csv_output = os.path.join(base_dir, "performance_results.csv")
    word_count_output = os.path.join(data_dir, "final_word_counts.txt")

    # 1. Ensure 50k dataset exists
    if not os.path.exists(correctness_path):
        print(f"[benchmark] Generating 50k dataset at '{correctness_path}'...")
        generate_dataset(correctness_path, 50000)

    # 2. Run Correctness Verification
    is_correct = verify_correctness(correctness_path)
    if not is_correct:
        print("ERROR: Correctness test failed! Halting benchmark execution.")
        sys.exit(1)

    # 3. Ensure 500k dataset exists
    if not os.path.exists(performance_path):
        print(f"[benchmark] Generating 500k dataset at '{performance_path}'...")
        generate_dataset(performance_path, 500000)

    # 4. Run Benchmarks
    run_benchmarks(
        dataset_path=performance_path,
        output_csv_path=csv_output,
        word_count_output_path=word_count_output,
        runs_per_config=3,
    )


if __name__ == "__main__":
    main()
