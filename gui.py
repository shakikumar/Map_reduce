"""
gui.py - MapReduce Word Count Processor GUI

Interactive desktop interface for INTE 22253 Option 2 (Cloud Programming Model).

Displays the end-to-end MapReduce Word Count workflow:
1. Input Dataset Selection (default: sample_data/correctness_50k.txt)
2. Mapper & Reducer Configuration (4 Mappers, 2 Reducers, Combiner Enabled)
3. One-Click Execution & Correctness Verification
4. Real-time Execution Log displaying each pipeline stage
5. Results Summary (Status, Total Words, Unique Words, Time)
6. Word Count Results Table showing final frequencies produced by MapReduce
"""

import os
import queue
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Dict, List, Optional, Tuple

from baseline import run_baseline
from generate_data import generate_dataset
from mapreduce_core import run_mapreduce_job


class MapReduceGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("MapReduce Word Count Processor - INTE 22253")
        self.root.geometry("900x780")
        self.root.minsize(820, 680)

        # Base directories
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.sample_dir = os.path.join(self.base_dir, "sample_data")
        os.makedirs(self.sample_dir, exist_ok=True)

        self.default_50k = os.path.join(self.sample_dir, "correctness_50k.txt")
        if not os.path.exists(self.default_50k):
            generate_dataset(self.default_50k, 50000)

        # Thread-safe queues
        self.log_queue: queue.Queue = queue.Queue()
        self.ui_queue: queue.Queue = queue.Queue()
        self.is_running = False

        # Build UI layout
        self._setup_theme()
        self._create_widgets()

        # Start periodic main-thread queue processors
        self.root.after(50, self._process_ui_queue)
        self.root.after(80, self._process_log_queue)

    def _setup_theme(self):
        """Configures clean, modern UI styling."""
        self.bg_color = "#f4f6f9"
        self.card_bg = "#ffffff"
        self.header_bg = "#1e293b"
        self.border_color = "#e2e8f0"

        self.root.configure(bg=self.bg_color)
        self.style = ttk.Style()
        self.style.theme_use("clam")

        self.style.configure(".", background=self.bg_color, font=("Helvetica", 10))
        self.style.configure("Card.TFrame", background=self.card_bg, relief="solid", borderwidth=1)
        self.style.configure("Treeview.Heading", font=("Helvetica", 9, "bold"), background="#e2e8f0")
        self.style.configure("Treeview", font=("Helvetica", 9), rowheight=22)

    def _create_widgets(self):
        # 1. Header Banner
        header_frame = tk.Frame(self.root, bg=self.header_bg, height=55)
        header_frame.pack(fill=tk.X, side=tk.TOP)
        header_frame.pack_propagate(False)

        title_lbl = tk.Label(
            header_frame,
            text="MapReduce Word Count Processor",
            bg=self.header_bg,
            fg="#ffffff",
            font=("Helvetica", 14, "bold"),
        )
        title_lbl.pack(side=tk.LEFT, padx=18, pady=12)

        subtitle_lbl = tk.Label(
            header_frame,
            text="INTE 22253 Option 2 | Cloud Programming Model",
            bg=self.header_bg,
            fg="#94a3b8",
            font=("Helvetica", 10),
        )
        subtitle_lbl.pack(side=tk.RIGHT, padx=18, pady=15)

        # Main Scrollable / Stacked Container
        main_container = tk.Frame(self.root, bg=self.bg_color)
        main_container.pack(fill=tk.BOTH, expand=True, padx=14, pady=10)

        # 2. Configuration & Execution Card
        self._build_control_section(main_container)

        # 3. Metrics Summary Cards
        self._build_metrics_section(main_container)

        # 4. Lower Split Area: Execution Log (Left/Top) & Word Count Results Table (Right/Bottom)
        content_split = ttk.PanedWindow(main_container, orient=tk.VERTICAL)
        content_split.pack(fill=tk.BOTH, expand=True, pady=(4, 0))

        log_frame = tk.Frame(content_split, bg=self.bg_color)
        results_frame = tk.Frame(content_split, bg=self.bg_color)

        content_split.add(log_frame, weight=1)
        content_split.add(results_frame, weight=1)

        self._build_log_section(log_frame)
        self._build_results_table_section(results_frame)

    def _build_control_section(self, parent: tk.Frame):
        card = tk.LabelFrame(
            parent,
            text=" MapReduce Configuration & Run Controls ",
            bg=self.card_bg,
            font=("Helvetica", 10, "bold"),
            fg="#1e293b",
            relief="solid",
            bd=1,
        )
        card.pack(fill=tk.X, pady=(0, 8))

        # Row 1: Input File Selection
        row_file = tk.Frame(card, bg=self.card_bg)
        row_file.pack(fill=tk.X, padx=12, pady=(8, 6))

        tk.Label(row_file, text="Input File:", bg=self.card_bg, font=("Helvetica", 9, "bold")).pack(side=tk.LEFT)
        self.dataset_var = tk.StringVar(value=self.default_50k)
        self.path_entry = tk.Entry(
            row_file, textvariable=self.dataset_var, font=("Helvetica", 9), relief="solid", bd=1
        )
        self.path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 8), ipady=3)

        self.browse_btn = ttk.Button(row_file, text="Browse...", command=self._browse_file)
        self.browse_btn.pack(side=tk.RIGHT)

        # Row 2: Architecture Parameters
        row_config = tk.Frame(card, bg=self.card_bg)
        row_config.pack(fill=tk.X, padx=12, pady=(0, 8))

        tk.Label(row_config, text="Mapper Workers:", bg=self.card_bg, font=("Helvetica", 9, "bold")).pack(side=tk.LEFT)
        self.mappers_var = tk.IntVar(value=4)
        mappers_spin = ttk.Spinbox(row_config, from_=1, to=16, textvariable=self.mappers_var, width=4)
        mappers_spin.pack(side=tk.LEFT, padx=(4, 18))

        tk.Label(row_config, text="Reducers:", bg=self.card_bg, font=("Helvetica", 9, "bold")).pack(side=tk.LEFT)
        tk.Label(row_config, text="2 (Fixed via CRC32)", bg=self.card_bg, fg="#475569", font=("Helvetica", 9)).pack(
            side=tk.LEFT, padx=(4, 18)
        )

        tk.Label(row_config, text="Combiner:", bg=self.card_bg, font=("Helvetica", 9, "bold")).pack(side=tk.LEFT)
        tk.Label(row_config, text="Enabled (Mapper-Side Local)", bg=self.card_bg, fg="#15803d", font=("Helvetica", 9, "bold")).pack(
            side=tk.LEFT, padx=(4, 0)
        )

        # Row 3: Action Buttons
        row_buttons = tk.Frame(card, bg=self.card_bg)
        row_buttons.pack(fill=tk.X, padx=12, pady=(0, 10))

        self.btn_run_mr = tk.Button(
            row_buttons,
            text="▶  Run MapReduce Job",
            bg="#2563eb",
            fg="#ffffff",
            font=("Helvetica", 10, "bold"),
            relief="flat",
            cursor="hand2",
            padx=14,
            pady=4,
            command=self._start_mapreduce,
        )
        self.btn_run_mr.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_verify = tk.Button(
            row_buttons,
            text="✔  Verify Correctness (PASS Check)",
            bg="#0d9488",
            fg="#ffffff",
            font=("Helvetica", 10, "bold"),
            relief="flat",
            cursor="hand2",
            padx=12,
            pady=4,
            command=self._start_verification,
        )
        self.btn_verify.pack(side=tk.LEFT)

    def _build_metrics_section(self, parent: tk.Frame):
        metrics_container = tk.Frame(parent, bg=self.bg_color)
        metrics_container.pack(fill=tk.X, pady=(0, 8))

        # Status badge card
        status_card = tk.Frame(metrics_container, bg=self.card_bg, relief="solid", bd=1, padx=10, pady=6)
        status_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 6))
        tk.Label(status_card, text="STATUS", bg=self.card_bg, font=("Helvetica", 8, "bold"), fg="#64748b").pack(anchor=tk.W)
        self.status_badge = tk.Label(
            status_card, text="IDLE", bg="#e2e8f0", fg="#334155", font=("Helvetica", 11, "bold"), padx=6, pady=2
        )
        self.status_badge.pack(anchor=tk.W, pady=(2, 0))

        # Total Words card
        words_card = tk.Frame(metrics_container, bg=self.card_bg, relief="solid", bd=1, padx=10, pady=6)
        words_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 6))
        tk.Label(words_card, text="TOTAL WORDS", bg=self.card_bg, font=("Helvetica", 8, "bold"), fg="#64748b").pack(anchor=tk.W)
        self.total_words_lbl = tk.Label(
            words_card, text="--", bg=self.card_bg, font=("Helvetica", 12, "bold"), fg="#1e293b"
        )
        self.total_words_lbl.pack(anchor=tk.W, pady=(2, 0))

        # Unique Words card
        unique_card = tk.Frame(metrics_container, bg=self.card_bg, relief="solid", bd=1, padx=10, pady=6)
        unique_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 6))
        tk.Label(unique_card, text="UNIQUE WORDS", bg=self.card_bg, font=("Helvetica", 8, "bold"), fg="#64748b").pack(anchor=tk.W)
        self.unique_words_lbl = tk.Label(
            unique_card, text="--", bg=self.card_bg, font=("Helvetica", 12, "bold"), fg="#1e293b"
        )
        self.unique_words_lbl.pack(anchor=tk.W, pady=(2, 0))

        # Execution Time card
        time_card = tk.Frame(metrics_container, bg=self.card_bg, relief="solid", bd=1, padx=10, pady=6)
        time_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tk.Label(time_card, text="EXECUTION TIME", bg=self.card_bg, font=("Helvetica", 8, "bold"), fg="#64748b").pack(anchor=tk.W)
        self.time_lbl = tk.Label(
            time_card, text="-- s", bg=self.card_bg, font=("Helvetica", 12, "bold"), fg="#2563eb"
        )
        self.time_lbl.pack(anchor=tk.W, pady=(2, 0))

    def _build_log_section(self, parent: tk.Frame):
        card = tk.LabelFrame(
            parent,
            text=" Execution Log (Pipeline Stages) ",
            bg=self.card_bg,
            font=("Helvetica", 10, "bold"),
            fg="#1e293b",
            relief="solid",
            bd=1,
        )
        card.pack(fill=tk.BOTH, expand=True, pady=(0, 4))

        log_inner = tk.Frame(card, bg=self.card_bg)
        log_inner.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        self.log_text = tk.Text(
            log_inner,
            bg="#0f172a",
            fg="#e2e8f0",
            insertbackground="white",
            font=("Courier", 9),
            wrap=tk.WORD,
            relief="flat",
            bd=4,
            height=8,
        )
        log_scroll = ttk.Scrollbar(log_inner, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def _build_results_table_section(self, parent: tk.Frame):
        card = tk.LabelFrame(
            parent,
            text=" Word Count Results Table ",
            bg=self.card_bg,
            font=("Helvetica", 10, "bold"),
            fg="#1e293b",
            relief="solid",
            bd=1,
        )
        card.pack(fill=tk.BOTH, expand=True, pady=(4, 0))

        table_inner = tk.Frame(card, bg=self.card_bg)
        table_inner.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        columns = ("rank", "word", "count")
        self.results_tree = ttk.Treeview(table_inner, columns=columns, show="headings", height=8)

        self.results_tree.heading("rank", text="#")
        self.results_tree.heading("word", text="Word")
        self.results_tree.heading("count", text="Count / Frequency")

        self.results_tree.column("rank", width=50, anchor=tk.CENTER, stretch=False)
        self.results_tree.column("word", width=380, anchor=tk.W)
        self.results_tree.column("count", width=160, anchor=tk.E)

        table_scroll = ttk.Scrollbar(table_inner, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=table_scroll.set)

        self.results_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def _format_log_message(self, raw_msg: str) -> Optional[str]:
        """Translates raw core messages into clear, student-friendly pipeline stage logs."""
        if "=== Starting MapReduce Job ===" in raw_msg or "Input File:" in raw_msg or "Configuration:" in raw_msg:
            return None
        if "Execution Time" in raw_msg or "Total Words" in raw_msg or "Unique Words" in raw_msg:
            return None
        if "Reading dataset file" in raw_msg:
            return "[Coordinator] Reading input file from disk..."
        if "Read " in raw_msg and " lines from disk" in raw_msg:
            lines_str = raw_msg.split("Read ")[-1].split(" lines")[0]
            return f"[Coordinator] Input loaded: {lines_str} lines read"
        if "Partition " in raw_msg and "assigned to Mapper" in raw_msg:
            part_info = raw_msg.split("[Coordinator] ")[-1]
            return f"[Coordinator] Partitioning input: {part_info}"
        if "Started processing" in raw_msg and "[Mapper-" in raw_msg:
            mapper_id = raw_msg.split("[Mapper-")[1].split("]")[0]
            lines_cnt = raw_msg.split("Started processing ")[-1]
            return f"[Mapper {mapper_id}] Started map phase on {lines_cnt}"
        if "Map & Combiner complete" in raw_msg:
            mapper_id = raw_msg.split("[Mapper-")[1].split("]")[0]
            tokens_info = raw_msg.split("Map & Combiner complete: ")[-1]
            return f"[Mapper {mapper_id}] Combiner completed: {tokens_info}"
        if "Shuffled " in raw_msg and "[Mapper-" in raw_msg:
            mapper_id = raw_msg.split("[Mapper-")[1].split("]")[0]
            if "Reducer-0" in raw_msg:
                return f"[Mapper {mapper_id}] Mapper-side Shuffle completed: CRC32 routed to Reducers"
            return None
        if "Sent EOF signals" in raw_msg and "[Mapper-" in raw_msg:
            mapper_id = raw_msg.split("[Mapper-")[1].split("]")[0]
            return f"[Mapper {mapper_id}] Completed (Sent EOF signals to all Reducers)"
        if "All " in raw_msg and "mapper EOF signals received" in raw_msg:
            reducer_id = raw_msg.split("[Reducer-")[1].split("]")[0]
            return f"[Reducer {reducer_id}] Completed: All Mapper EOF signals received (Aggregation finished)"
        if "Successfully collected result partition" in raw_msg:
            reducer_part = raw_msg.split("[Coordinator] Successfully collected result partition from ")[-1]
            return f"[Coordinator] Collected partition from {reducer_part}"
        if "Job Complete!" in raw_msg:
            return "[Coordinator] Final result merged: Job completed successfully!"
        if "[ERROR]" in raw_msg:
            return raw_msg
        return None

    def _log(self, message: str):
        """Enqueue message for main UI thread display."""
        formatted = self._format_log_message(message)
        if formatted:
            self.log_queue.put(formatted)

    def _log_direct(self, message: str):
        """Enqueue a direct informational log message."""
        self.log_queue.put(message)

    def _process_ui_queue(self):
        """Safely executes background thread UI tasks on the Tkinter main thread."""
        while not self.ui_queue.empty():
            try:
                callback = self.ui_queue.get_nowait()
                callback()
            except queue.Empty:
                break
            except Exception as e:
                print(f"[UI ERROR] {e}", file=sys.stderr)
        self.root.after(50, self._process_ui_queue)

    def _process_log_queue(self):
        """Pumps messages from queue to Tkinter log text widget."""
        while not self.log_queue.empty():
            try:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
            except queue.Empty:
                break
        self.root.after(80, self._process_log_queue)

    def _run_on_ui_thread(self, func):
        """Ensures a function runs on the main Tkinter thread."""
        if threading.current_thread() is threading.main_thread():
            func()
        else:
            self.ui_queue.put(func)

    def _set_status(self, text: str, color: str = "#e2e8f0", fg: str = "#334155"):
        self._run_on_ui_thread(lambda: self.status_badge.config(text=text, bg=color, fg=fg))

    def _toggle_controls(self, running: bool):
        def _apply():
            self.is_running = running
            state = tk.DISABLED if running else tk.NORMAL
            self.btn_run_mr.config(state=state)
            self.btn_verify.config(state=state)
            self.browse_btn.config(state=state)

        self._run_on_ui_thread(_apply)

    def _clear_logs_and_results(self):
        def _apply():
            self.log_text.delete("1.0", tk.END)
            for row in self.results_tree.get_children():
                self.results_tree.delete(row)
            self.time_lbl.config(text="-- s")
            self.total_words_lbl.config(text="--")
            self.unique_words_lbl.config(text="--")

        self._run_on_ui_thread(_apply)

    def _display_results(
        self,
        counts: Dict[str, int],
        elapsed: float,
        status_text: str = "COMPLETED",
        status_bg: str = "#dcfce7",
        status_fg: str = "#15803d",
    ):
        total_w = sum(counts.values())
        uniq_w = len(counts)
        sorted_words = sorted(counts.items(), key=lambda x: x[1], reverse=True)

        def _apply():
            self.time_lbl.config(text=f"{elapsed:.4f} s")
            self.total_words_lbl.config(text=f"{total_w:,}")
            self.unique_words_lbl.config(text=f"{uniq_w:,}")
            self._set_status(status_text, status_bg, status_fg)

            for row in self.results_tree.get_children():
                self.results_tree.delete(row)

            # Insert all word count results into the scrollable table
            for rank, (word, count) in enumerate(sorted_words, start=1):
                self.results_tree.insert("", tk.END, values=(rank, word, f"{count:,}"))

        self._run_on_ui_thread(_apply)

    def _browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Input Text Dataset",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
            initialdir=self.sample_dir,
        )
        if filename:
            self.dataset_var.set(filename)

    def _start_mapreduce(self):
        file_path = self.dataset_var.get().strip()
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid dataset file.")
            return

        mappers = self.mappers_var.get()
        self._toggle_controls(True)
        self._set_status("RUNNING", "#dbeafe", "#1e40af")
        self._clear_logs_and_results()

        self._log_direct("=== Starting MapReduce Word Count Job ===")
        self._log_direct(f"Input File : {file_path}")
        self._log_direct(f"Mappers    : {mappers}")
        self._log_direct("Reducers   : 2 (CRC32 Partitioned)")
        self._log_direct("Combiner   : Enabled (Mapper-Side Local)\n")

        def task():
            try:
                counts, elapsed = run_mapreduce_job(
                    file_path=file_path,
                    num_mappers=mappers,
                    num_reducers=2,
                    log_callback=self._log,
                )
                self._display_results(counts, elapsed, status_text="COMPLETED", status_bg="#dcfce7", status_fg="#15803d")
                self._log_direct(f"\n==========================================")
                self._log_direct(f"  Status        : COMPLETED")
                self._log_direct(f"  Total Words   : {sum(counts.values()):,}")
                self._log_direct(f"  Unique Words  : {len(counts):,}")
                self._log_direct(f"  Execution Time: {elapsed:.4f} seconds")
                self._log_direct(f"  Mappers       : {mappers}")
                self._log_direct(f"  Reducers      : 2")
                self._log_direct(f"==========================================")
            except Exception as e:
                self._log_direct(f"[ERROR] MapReduce failed: {str(e)}")
                self._set_status("ERROR", "#fee2e2", "#b91c1c")
            finally:
                self._toggle_controls(False)

        threading.Thread(target=task, daemon=True).start()

    def _start_verification(self):
        file_path = self.dataset_var.get().strip()
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid dataset file.")
            return

        mappers = self.mappers_var.get()
        self._toggle_controls(True)
        self._set_status("VERIFYING", "#fef3c7", "#92400e")
        self._clear_logs_and_results()

        self._log_direct("=== Correctness Verification (Baseline vs MapReduce) ===")
        self._log_direct(f"Input File: {file_path}\n")

        def task():
            try:
                # Stage 1: Sequential Baseline
                self._log_direct("[Stage 1] Running Sequential Baseline (Single-Thread)...")
                b_counts, b_time = run_baseline(file_path)
                b_total = sum(b_counts.values())
                b_unique = len(b_counts)
                self._log_direct(f"  ✓ Baseline Total Words  : {b_total:,}")
                self._log_direct(f"  ✓ Baseline Unique Words : {b_unique:,}")
                self._log_direct(f"  ✓ Baseline Time         : {b_time:.4f}s\n")

                # Stage 2: Multiprocess MapReduce
                self._log_direct(f"[Stage 2] Running Multiprocess MapReduce ({mappers} Mappers, 2 Reducers)...")
                mr_counts, mr_time = run_mapreduce_job(
                    file_path=file_path,
                    num_mappers=mappers,
                    num_reducers=2,
                    log_callback=self._log,
                )
                mr_total = sum(mr_counts.values())
                mr_unique = len(mr_counts)
                self._log_direct(f"  ✓ MapReduce Total Words : {mr_total:,}")
                self._log_direct(f"  ✓ MapReduce Unique Words: {mr_unique:,}")
                self._log_direct(f"  ✓ MapReduce Time        : {mr_time:.4f}s\n")

                # Stage 3: Verification Comparison
                self._log_direct("[Stage 3] Comparing Word Count Frequencies...")
                self._log_direct(f"  Baseline  : {b_total:,} total words, {b_unique:,} unique words")
                self._log_direct(f"  MapReduce : {mr_total:,} total words, {mr_unique:,} unique words")

                match = (b_counts == mr_counts)
                if match:
                    self._display_results(mr_counts, mr_time, status_text="PASS: MATCH", status_bg="#dcfce7", status_fg="#15803d")
                    self._log_direct("\n" + "=" * 48)
                    self._log_direct("  PASS: Results match!")
                    self._log_direct("  100% equivalence between Baseline and MapReduce.")
                    self._log_direct("=" * 48)
                else:
                    self._set_status("FAIL: MISMATCH", "#fee2e2", "#b91c1c")
                    self._log_direct("\n" + "=" * 48)
                    self._log_direct("  FAIL: Word counts differ between Baseline and MapReduce!")
                    self._log_direct("=" * 48)
            except Exception as e:
                self._log_direct(f"[ERROR] Verification failed: {str(e)}")
                self._set_status("ERROR", "#fee2e2", "#b91c1c")
            finally:
                self._toggle_controls(False)

        threading.Thread(target=task, daemon=True).start()


def launch_gui():
    root = tk.Tk()
    app = MapReduceGUI(root)
    root.mainloop()


if __name__ == "__main__":
    launch_gui()
