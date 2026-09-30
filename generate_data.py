"""
generate_data.py - Synthetic Dataset Generator for MapReduce Word Count

Generates reproducible text datasets using a fixed random seed:
1. Correctness Dataset  : ~50,000 lines (~3.5 MB)
2. Performance Dataset  : ~500,000 lines (~35 MB)

Features varied technical vocabulary (cloud computing, distributed systems,
algorithms), common English vocabulary, punctuation, numbers, and contractions
to thoroughly test word normalization, partitioning, combining, shuffling,
and reducing.
"""

import os
import random
import sys

# Fixed random seed for deterministic and reproducible test data
RANDOM_SEED = 42

# Varied vocabulary collections
SUBJECTS = [
    "distributed systems", "cloud computing", "mapreduce framework", "parallel processing",
    "cluster coordinator", "worker node", "mapper task", "reducer task",
    "inter-process communication", "data partitioning", "crc32 hash function",
    "python multiprocessing", "operating system", "concurrent execution",
    "storage node", "network topology", "fault tolerance", "load balancing",
    "cloud infrastructure", "big data analytics", "batch pipeline", "virtual machine"
]

VERBS = [
    "processes", "partitions", "aggregates", "distributes", "scales",
    "shuffles", "optimizes", "computes", "coordinates", "synchronizes",
    "transforms", "executes", "evaluates", "manages", "allocates"
]

OBJECTS = [
    "large datasets efficiently", "intermediate key-value pairs", "combiner buffers",
    "reducer queues safely", "workloads across multiple cores", "data chunks in memory",
    "network packets without deadlock", "word frequency statistics", "performance benchmarks",
    "cloud storage buckets", "concurrent tasks in parallel", "system resource metrics"
]

PHRASES = [
    "it's well known that Dean and Ghemawat introduced MapReduce in 2004",
    "don't forget that stable CRC32 hashing prevents inconsistent routing",
    "can't achieve linear speedup due to Amdahl's law and IPC overhead",
    "cloud2026 architectures prioritize elastic scaling and high availability",
    "combiners perform local aggregation before data reaches the shuffle stage",
    "two reducer processes ensure balanced distribution of partitioned keys",
    "a single-process baseline provides the exact ground-truth correctness reference",
    "proper queue draining prevents process deadlocks during heavy workloads",
    "worker processes avoid Python's Global Interpreter Lock (GIL) limitations",
    "real-time monitoring tracks throughput and execution latency across runs"
]

PUNCTUATION_STYLES = [
    ".", "!", "?", "...", ";", " --", ":", ", and", ", while", "!"
]


def generate_sentence() -> str:
    """Constructs a varied sentence with vocabulary, numbers, and contractions."""
    style = random.random()
    if style < 0.35:
        # Structured technical statement
        subj = random.choice(SUBJECTS)
        verb = random.choice(VERBS)
        obj = random.choice(OBJECTS)
        punct = random.choice(PUNCTUATION_STYLES)
        return f"{subj.capitalize()} {verb} {obj}{punct}"
    elif style < 0.70:
        # Predefined rich phrase with contractions / numbers / names
        phrase = random.choice(PHRASES)
        modifier = random.choice(["Indeed,", "Furthermore,", "Notably,", "In practice,", "As observed,"])
        punct = random.choice([".", "!", ";"])
        return f"{modifier} {phrase}{punct}"
    else:
        # Mixed compound sentence
        s1 = random.choice(SUBJECTS)
        v1 = random.choice(VERBS)
        s2 = random.choice(SUBJECTS)
        v2 = random.choice(VERBS)
        num = random.randint(1, 1000)
        return f"{s1.capitalize()} {v1} item #{num}, whereas {s2} {v2} {random.choice(OBJECTS)}."


def generate_dataset(output_path: str, target_lines: int) -> None:
    """
    Generates a dataset of `target_lines` sentences and writes to `output_path`.
    """
    random.seed(RANDOM_SEED)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    print(f"[generate_data] Generating {target_lines:,} lines to '{output_path}'...")
    
    # Write in batches for high I/O performance
    batch_size = 10000
    lines_written = 0

    with open(output_path, "w", encoding="utf-8") as f:
        while lines_written < target_lines:
            current_batch_size = min(batch_size, target_lines - lines_written)
            batch = [generate_sentence() + "\n" for _ in range(current_batch_size)]
            f.writelines(batch)
            lines_written += current_batch_size

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"[generate_data] Successfully generated {lines_written:,} lines ({file_size_mb:.2f} MB).")


def main() -> None:
    """Command-line interface for dataset generation."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_data")
    os.makedirs(data_dir, exist_ok=True)

    correctness_path = os.path.join(data_dir, "correctness_50k.txt")
    performance_path = os.path.join(data_dir, "performance_500k.txt")

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "50k" or mode == "correctness":
            generate_dataset(correctness_path, 50000)
            return
        elif mode == "500k" or mode == "performance":
            generate_dataset(performance_path, 500000)
            return
        elif mode.isdigit():
            custom_lines = int(mode)
            custom_path = os.path.join(data_dir, f"custom_{custom_lines}.txt")
            generate_dataset(custom_path, custom_lines)
            return

    # Default: generate both
    generate_dataset(correctness_path, 50000)
    generate_dataset(performance_path, 500000)


if __name__ == "__main__":
    main()
