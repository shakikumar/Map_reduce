"""
baseline.py - Single-Process Baseline Word Count Implementation

This script serves as the ground-truth correctness reference and single-threaded
performance baseline for the MapReduce word-count implementation.

Key Characteristics:
- Single process, sequential execution.
- Uses the identical tokenization and word normalization rule from `normalizer.py`.
- Computes standard word frequencies using a standard dictionary / Counter.
- Provides consistent timing scope for direct, fair comparison with MapReduce.
"""

import os
import sys
import time
from collections import Counter
from typing import Dict, Tuple

from normalizer import tokenize_and_normalize


def run_baseline_from_lines(lines: list) -> Tuple[Dict[str, int], float]:
    """
    Executes sequential word count on pre-loaded lines of text.

    Args:
        lines: List of raw text strings.

    Returns:
        Tuple of (word_counts_dictionary, execution_time_seconds).
    """
    start_time = time.perf_counter()

    counts: Dict[str, int] = Counter()
    for line in lines:
        tokens = tokenize_and_normalize(line)
        for token in tokens:
            counts[token] += 1

    elapsed_time = time.perf_counter() - start_time
    return dict(counts), elapsed_time


def run_baseline(file_path: str) -> Tuple[Dict[str, int], float]:
    """
    Executes end-to-end sequential baseline word count on a target file.
    
    Timing Scope: Includes reading the file, tokenizing, normalizing,
    and aggregating word frequencies sequentially.

    Args:
        file_path: Path to the input text file.

    Returns:
        Tuple of (word_counts_dictionary, execution_time_seconds).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    start_time = time.perf_counter()

    counts: Dict[str, int] = Counter()
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            tokens = tokenize_and_normalize(line)
            for token in tokens:
                counts[token] += 1

    elapsed_time = time.perf_counter() - start_time
    return dict(counts), elapsed_time


def main() -> None:
    """CLI runner for baseline word count."""
    if len(sys.argv) < 2:
        default_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "sample_data", "correctness_50k.txt"
        )
        if not os.path.exists(default_file):
            print(f"[baseline] Generating default 50k dataset at '{default_file}'...")
            import generate_data
            generate_data.generate_dataset(default_file, 50000)
        file_path = default_file
    else:
        file_path = sys.argv[1]

    print(f"=== Running Baseline Word Count on '{file_path}' ===")
    counts, elapsed = run_baseline(file_path)

    total_words = sum(counts.values())
    unique_words = len(counts)
    print(f"Execution Time  : {elapsed:.4f} seconds")
    print(f"Total Words     : {total_words:,}")
    print(f"Unique Words    : {unique_words:,}")
    
    print("\nTop 10 Most Frequent Words:")
    top_10 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for rank, (word, freq) in enumerate(top_10, start=1):
        print(f"  {rank:2d}. {word:<25} : {freq:,}")


if __name__ == "__main__":
    main()
