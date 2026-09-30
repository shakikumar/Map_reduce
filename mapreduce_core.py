"""
mapreduce_core.py - Core MapReduce Framework with Mapper-Side Shuffle & Reducer Signalling

This module implements a complete local simulation of the MapReduce programming model
using Python multiprocessing.Processes and inter-process communication Queues.

Architecture Pipeline:
    Input File
        │
        ▼
    Data Partition (Balanced Line Chunks)
        │
    ┌───┴───────────────────────────────┐
    ▼                                   ▼
Mapper 1 (Process)                  Mapper N (Process)
  ├─ Map: (word, 1)                   ├─ Map: (word, 1)
  ├─ Combiner: Local Aggregation      ├─ Combiner: Local Aggregation
  └─ Mapper-Side Shuffle (CRC32):     └─ Mapper-Side Shuffle (CRC32):
     Routes to Reducer Queues 0 & 1      Routes to Reducer Queues 0 & 1
     Sends EOF to Reducers 0 & 1         Sends EOF to Reducers 0 & 1
    │                                   │
    └─────────────────┬─────────────────┘
                      ▼
    ┌─────────────────┴─────────────────┐
    ▼                                   ▼
Reducer 1 (Queue 0)                 Reducer 2 (Queue 1)
  ├─ Collects from all N mappers      ├─ Collects from all N mappers
  ├─ Waits for N completion signals   ├─ Waits for N completion signals
  └─ Computes sum(values) per word    └─ Computes sum(values) per word
    │                                   │
    └─────────────────┬─────────────────┘
                      ▼
             Final Combined Result

Key Features & Guarantees:
1. Pure Multiprocessing: Uses multiprocessing.Process for independent memory spaces
   and bypassing Python's Global Interpreter Lock (GIL).
2. Mapper-Side Shuffle: Mappers hash words directly to reducer queues using stable CRC32.
3. Deterministic Termination: Explicit End-of-Data (EOF) signals sent from each mapper
   to all reducers. Reducers terminate strictly when count(EOF) == N mappers (no timeouts).
4. Queue Safety: Results collected from queues before joining worker processes to
   guarantee zero deadlocks.
"""

import multiprocessing
import os
import sys
import time
import zlib
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Tuple

from normalizer import tokenize_and_normalize

# Special Sentinel tuple used for deterministic reducer termination signalling
# Format: ("__EOF_SIGNAL__", mapper_id)
EOF_TAG = "__EOF_SIGNAL__"


def mapper_worker(
    mapper_id: int,
    lines: List[str],
    reducer_queues: List[multiprocessing.Queue],
    num_reducers: int,
    status_queue: Optional[multiprocessing.Queue] = None,
) -> None:
    """
    Worker process for the MAP, COMBINER, and MAPPER-SIDE SHUFFLE stages.

    Steps executed inside this process:
    1. MAP STAGE:
       - Read assigned text partition.
       - Tokenize and normalize words using identical normalization rules.
       - Generate intermediate (word, 1) pairs.

    2. COMBINER STAGE (Local Aggregation):
       - Group and sum word frequencies locally within this mapper.
       - Drastically reduces data transmitted across inter-process queues.

    3. MAPPER-SIDE SHUFFLE / PARTITION STAGE:
       - For each unique (word, count), compute:
         reducer_id = zlib.crc32(word.encode('utf-8')) % num_reducers
       - Group intermediate pairs into per-reducer buckets.
       - Push partitioned batches into the target reducer's queue.

    4. REDUCER COMPLETION SIGNALLING:
       - Send an explicit EOF signal ("__EOF_SIGNAL__", mapper_id) to EVERY reducer queue.
    """
    try:
        if status_queue:
            status_queue.put(f"[Mapper-{mapper_id}] Started processing {len(lines):,} lines")

        # --- Stage 1 & 2: Map and Combiner (Local Aggregation) ---
        local_combiner: Dict[str, int] = defaultdict(int)
        total_tokens_mapped = 0

        for line in lines:
            tokens = tokenize_and_normalize(line)
            for token in tokens:
                local_combiner[token] += 1
                total_tokens_mapped += 1

        if status_queue:
            status_queue.put(
                f"[Mapper-{mapper_id}] Map & Combiner complete: "
                f"{total_tokens_mapped:,} tokens aggregated into {len(local_combiner):,} unique keys"
            )

        # --- Stage 3: Mapper-Side Shuffle / Partition using CRC32 ---
        # Bucket data by target reducer ID
        buckets: List[List[Tuple[str, int]]] = [[] for _ in range(num_reducers)]

        for word, count in local_combiner.items():
            # CRC32 provides deterministic 32-bit integer hash across all processes and runs
            reducer_id = zlib.crc32(word.encode("utf-8")) % num_reducers
            buckets[reducer_id].append((word, count))

        # Send partitioned batches to respective reducer queues
        for r_id in range(num_reducers):
            if buckets[r_id]:
                # Send bucket as a batch to minimize queue lock contention
                reducer_queues[r_id].put(buckets[r_id])
                if status_queue:
                    status_queue.put(
                        f"[Mapper-{mapper_id}] Shuffled {len(buckets[r_id]):,} keys to Reducer-{r_id}"
                    )

        # --- Stage 4: Send Explicit End-of-Data (EOF) Signal to ALL Reducers ---
        for r_id in range(num_reducers):
            reducer_queues[r_id].put((EOF_TAG, mapper_id))

        if status_queue:
            status_queue.put(f"[Mapper-{mapper_id}] Sent EOF signals to all {num_reducers} reducers. Finished.")

    except Exception as e:
        if status_queue:
            status_queue.put(f"[Mapper-{mapper_id}] ERROR: {str(e)}")
        raise e


def reducer_worker(
    reducer_id: int,
    input_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    num_mappers: int,
    status_queue: Optional[multiprocessing.Queue] = None,
) -> None:
    """
    Worker process for the REDUCE stage.

    Steps executed inside this process:
    1. Read batches from its dedicated input_queue.
    2. If an item is an EOF signal from a mapper, record completion for that mapper.
    3. If an item is a batch of (word, count) pairs, sum values:
       word_counts[word] += count   (equivalent to sum(values))
    4. Terminate strictly when EOF signals have been received from ALL N mappers.
    5. Place the final reduced dictionary (reducer_id, word_counts) onto result_queue.
    """
    try:
        if status_queue:
            status_queue.put(f"[Reducer-{reducer_id}] Started, awaiting data from {num_mappers} mappers...")

        final_counts: Dict[str, int] = defaultdict(int)
        completed_mappers = set()

        while True:
            item = input_queue.get()

            # Check for End-of-Data signal
            if isinstance(item, tuple) and len(item) == 2 and item[0] == EOF_TAG:
                mapper_id = item[1]
                completed_mappers.add(mapper_id)
                if status_queue:
                    status_queue.put(
                        f"[Reducer-{reducer_id}] Received EOF from Mapper-{mapper_id} "
                        f"({len(completed_mappers)}/{num_mappers} mappers complete)"
                    )
                # Terminate only after receiving EOF from ALL N mappers
                if len(completed_mappers) >= num_mappers:
                    if status_queue:
                        status_queue.put(
                            f"[Reducer-{reducer_id}] All {num_mappers} mapper EOF signals received. "
                            f"Finalizing {len(final_counts):,} unique keys."
                        )
                    break
            else:
                # Item is a batch of (word, count) tuples from a mapper
                for word, count in item:
                    final_counts[word] += count  # sum(values)

        # Transmit final reduced partition to coordinator
        result_queue.put((reducer_id, dict(final_counts)))

    except Exception as e:
        if status_queue:
            status_queue.put(f"[Reducer-{reducer_id}] ERROR: {str(e)}")
        raise e


def partition_lines(lines: List[str], num_partitions: int) -> List[List[str]]:
    """
    Splits a list of text lines into `num_partitions` balanced consecutive chunks.

    Example with 10 lines and 3 partitions:
    - Partition 0: lines 0..3 (4 lines)
    - Partition 1: lines 4..6 (3 lines)
    - Partition 2: lines 7..9 (3 lines)
    """
    total_lines = len(lines)
    if num_partitions <= 0:
        raise ValueError("num_partitions must be at least 1")

    base_chunk_size = total_lines // num_partitions
    remainder = total_lines % num_partitions

    partitions: List[List[str]] = []
    start_idx = 0

    for i in range(num_partitions):
        # Distribute remainder lines evenly across the first 'remainder' partitions
        chunk_size = base_chunk_size + (1 if i < remainder else 0)
        end_idx = start_idx + chunk_size
        partitions.append(lines[start_idx:end_idx])
        start_idx = end_idx

    return partitions


def run_mapreduce_job(
    file_path: str,
    num_mappers: int = 4,
    num_reducers: int = 2,
    log_callback: Optional[Callable[[str], None]] = None,
) -> Tuple[Dict[str, int], float]:
    """
    Coordinates and executes a full MapReduce Word Count job.

    Timing Scope:
    Measures the entire end-to-end execution:
    1. Reading dataset file from disk
    2. Partitioning data across mappers
    3. Spawning and executing Mapper and Reducer processes
    4. Mapper-side Shuffle and Inter-Process Communication
    5. Reduce aggregation
    6. Collecting and merging final word count dictionary

    Args:
        file_path: Absolute or relative path to text dataset.
        num_mappers: Number of parallel mapper worker processes (e.g. 1, 2, 4).
        num_reducers: Number of parallel reducer worker processes (default: 2).
        log_callback: Optional callable for streaming status messages to UI/CLI.

    Returns:
        Tuple of (merged_word_counts_dictionary, total_elapsed_seconds).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    log(f"=== Starting MapReduce Job ===")
    log(f"Input File: {file_path}")
    log(f"Configuration: {num_mappers} Mapper(s), {num_reducers} Reducer(s)")

    start_time = time.perf_counter()

    # 1. Read input dataset
    log(f"[Coordinator] Reading dataset file...")
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    total_lines = len(lines)
    log(f"[Coordinator] Read {total_lines:,} lines from disk.")

    # 2. Partition dataset for mappers
    partitions = partition_lines(lines, num_mappers)
    for idx, part in enumerate(partitions, start=1):
        log(f"[Coordinator] Partition {idx}: {len(part):,} lines assigned to Mapper-{idx}")

    # 3. Create Inter-Process Communication Queues
    # One dedicated queue per Reducer
    reducer_queues = [multiprocessing.Queue() for _ in range(num_reducers)]
    # Single queue for Reducers to send final results back to Coordinator
    result_queue = multiprocessing.Queue()
    # Queue for worker processes to stream status messages back to Coordinator
    status_queue = multiprocessing.Queue()

    # 4. Initialize and Spawn Reducer Processes First
    reducers: List[multiprocessing.Process] = []
    for r_id in range(num_reducers):
        p = multiprocessing.Process(
            target=reducer_worker,
            args=(r_id, reducer_queues[r_id], result_queue, num_mappers, status_queue),
            name=f"ReducerProcess-{r_id}",
        )
        reducers.append(p)
        p.start()

    # 5. Initialize and Spawn Mapper Processes
    mappers: List[multiprocessing.Process] = []
    for m_idx in range(num_mappers):
        mapper_id = m_idx + 1  # 1-indexed (Mapper-1 ... Mapper-N)
        p = multiprocessing.Process(
            target=mapper_worker,
            args=(mapper_id, partitions[m_idx], reducer_queues, num_reducers, status_queue),
            name=f"MapperProcess-{mapper_id}",
        )
        mappers.append(p)
        p.start()

    # 6. Monitor Status Messages and Collect Reducer Results Safely
    # Critical Queue Safety Rule:
    # We must retrieve all expected items from result_queue BEFORE joining
    # processes to guarantee processes never deadlock on full queues.
    completed_results: Dict[int, Dict[str, int]] = {}
    
    # We expect results from all `num_reducers`
    while len(completed_results) < num_reducers:
        # Drain any worker status logs
        while not status_queue.empty():
            try:
                msg = status_queue.get_nowait()
                log(msg)
            except Exception:
                break

        # Check for completed reducer result (with a short timeout to allow status draining)
        try:
            r_id, reduced_dict = result_queue.get(timeout=0.05)
            completed_results[r_id] = reduced_dict
            log(f"[Coordinator] Successfully collected result partition from Reducer-{r_id} ({len(reduced_dict):,} keys)")
        except Exception:
            pass

    # Drain any remaining status logs
    while not status_queue.empty():
        try:
            msg = status_queue.get_nowait()
            log(msg)
        except Exception:
            break

    # 7. Join Worker Processes Cleanly
    for p in mappers:
        p.join()
    for p in reducers:
        p.join()

    # 8. Merge Disjoint Reducer Partitions
    # Because CRC32 ensures disjoint key distribution, dictionary merging is direct
    final_word_counts: Dict[str, int] = {}
    for r_id in range(num_reducers):
        final_word_counts.update(completed_results[r_id])

    elapsed_time = time.perf_counter() - start_time
    total_words = sum(final_word_counts.values())
    unique_words = len(final_word_counts)

    log(f"[Coordinator] Job Complete!")
    log(f"Execution Time  : {elapsed_time:.4f} seconds")
    log(f"Total Words     : {total_words:,}")
    log(f"Unique Words    : {unique_words:,}")

    return final_word_counts, elapsed_time


def main() -> None:
    """CLI runner for testing MapReduce core."""
    default_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "sample_data", "correctness_50k.txt"
    )
    if not os.path.exists(default_file):
        print(f"[MapReduce] Generating default dataset at '{default_file}'...")
        import generate_data
        generate_data.generate_dataset(default_file, 50000)

    file_to_run = sys.argv[1] if len(sys.argv) > 1 else default_file
    mappers_count = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    counts, duration = run_mapreduce_job(
        file_path=file_to_run,
        num_mappers=mappers_count,
        num_reducers=2,
        log_callback=print,
    )

    print("\nTop 10 Most Frequent Words:")
    top_10 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for rank, (word, freq) in enumerate(top_10, start=1):
        print(f"  {rank:2d}. {word:<25} : {freq:,}")


if __name__ == "__main__":
    main()
