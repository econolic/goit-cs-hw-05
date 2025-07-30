#!/usr/bin/env python3
"""
Word-frequency analyser with MapReduce (threads / processes) + CLI.

Usage:
    python task2_word_freq.py <url> [--top N] [--backend threads|processes]
    [--words word1 word2 ...] [--stopwords-file path] [--no-auto-stopwords]
    [--png path] [--csv path]

Features
--------
* Downloads text from a URL.
* Counts word frequencies via streaming MapReduce.
  - `--backend threads` (default) or `--backend processes`.
* Automatic language detection (langdetect) + stop-word removal (stopwords-iso).
* Streaming chunks (20 k words) → low RAM footprint.
* Horizontal bar-chart of the top-N words (`matplotlib`).
  - `--png` to save the plot, otherwise it opens an interactive window.
* `--csv` to save the full frequency table.
* Execution summary with timing + optional RAM/CPU (psutil).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, List

import matplotlib.pyplot as plt
import pandas as pd
import requests

# Optional libraries
try:
    import psutil  # runtime stats
except ImportError:
    psutil = None
from langdetect import detect  # language code
from stopwordsiso import stopwords  # ISO stop-word lists

# Text helpers
# FIXED: Simplified regex and removed deprecated re.UNICODE flag.
_NON_WORD_RE = re.compile(r'[^\w\s]+')
_CHUNK_SIZE = 20_000  # words per streaming chunk


def clean_text(text: str) -> Iterable[str]:
    """Yield lowercase tokens, stripping punctuation and other non-word chars."""
    text = _NON_WORD_RE.sub(" ", text)
    for word in text.split():
        yield word.lower()


def download_text(url: str, timeout: int = 30) -> str:
    """Fetch raw text from URL."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text

# MapReduce core
def _chunk_iterable(it: Iterable[str], size: int):
    chunk: List[str] = []
    for word in it:
        chunk.append(word)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def _count_chunk(chunk: List[str], words_filter: set[str] | None) -> Counter:
    if words_filter is not None:
        chunk = [w for w in chunk if w in words_filter]
    return Counter(chunk)


def map_reduce_stream(
    words_iter: Iterable[str],
    backend: str = "threads",
    words_filter: set[str] | None = None,
) -> Counter:
    """
    Streaming MapReduce.

    Parameters
    ----------
    words_iter : Iterable[str]
        Token generator.
    backend : {"threads", "processes"}
        Parallel execution backend.
    words_filter : set[str] | None
        If provided - count only these words.

    Returns
    -------
    Counter
        word → frequency.
    """
    executor_cls = ThreadPoolExecutor if backend == "threads" else ProcessPoolExecutor
    counters: List[Counter] = []
    with executor_cls() as executor:
        futures = [
            executor.submit(_count_chunk, chunk, words_filter)
            for chunk in _chunk_iterable(words_iter, _CHUNK_SIZE)
        ]
        for fut in futures:
            counters.append(fut.result())

    total = Counter()
    for c in counters:
        total.update(c)
    return total

# Visualisation & persistence
def visualise_top(
    freq: Counter,
    top_n: int,
    save_png: Path | None,
) -> None:
    """Horizontal bar chart of the `top_n` most common words."""
    top_items = list(freq.most_common(top_n))
    if not top_items:
        print("No words to display.")
        return
    words, counts = zip(*top_items)

    plt.figure(figsize=(10, 8)) # Adjusted for more words
    bars = plt.barh(range(len(words)), counts, align="center")
    plt.yticks(range(len(words)), words)
    plt.xlabel("Frequency")
    plt.title(f"Top-{top_n} words")
    plt.gca().invert_yaxis()

    for bar, count in zip(bars, counts):
        plt.text(
            count + max(counts) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            str(count),
            va="center",
        )

    plt.tight_layout()
    if save_png:
        plt.savefig(save_png, dpi=300)
        print(f"[i] PNG saved → {save_png.resolve()}")
    else:
        plt.show()
    plt.close()


def save_csv(freq: Counter, path: Path) -> None:
    """Persist the entire frequency table."""
    with path.open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["word", "count"])
        wr.writerows(sorted(list(freq.items()), key=lambda item: item[1], reverse=True))  # type: ignore
    print(f"[i] CSV saved → {path.resolve()}")

# CLI helpers
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Word-frequency analysis via streaming MapReduce.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("url", help="Text URL to analyse")
    p.add_argument("--top", type=int, default=10, help="Number of words to plot")
    p.add_argument(
        "--backend",
        choices=["threads", "processes"],
        default="threads",
        help="Parallelisation strategy",
    )
    p.add_argument(
        "--words",
        nargs="+",
        help="Analyse only these words (disables stop-word removal)",
    )
    p.add_argument(
        "--stopwords-file",
        type=Path,
        help="File with custom stop-words (one per line)",
    )
    p.add_argument("--png", type=Path, help="Save chart as PNG")
    p.add_argument("--csv", type=Path, help="Save full table as CSV")
    p.add_argument(
        "--no-auto-stopwords",
        action="store_true",
        help="Turn off automatic lang-based stop-words",
    )
    return p.parse_args(argv)


def _load_stopwords(path: Path | None) -> set[str]:
    if not path:
        return set()
    with path.open(encoding="utf-8") as fh:
        return {line.strip().lower() for line in fh if line.strip()}


# Main
def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    user_stopwords = _load_stopwords(args.stopwords_file)
    auto_stopwords: set[str] = set()

    t0 = time.perf_counter()
    text = download_text(args.url)
    t1 = time.perf_counter()

    if (
        not args.no_auto_stopwords
        and args.words is None
    ):
        try:
            lang = detect(text)
            auto_stopwords = set(stopwords(lang))
            print(f"[i] Detected language: {lang}; {len(auto_stopwords)} stop-words loaded.")
        except Exception as e:
            print(f"[!] Language detection failed: {e}")

    words_filter = {w.lower() for w in args.words} if args.words else None

    # Combine all stopwords, but only if no specific words are requested
    all_stopwords = user_stopwords.union(auto_stopwords)

    words_iter = clean_text(text)
    # Filter stopwords before MapReduce if no specific words are being counted
    if words_filter is None:
        words_iter = (word for word in words_iter if word not in all_stopwords)

    freq = map_reduce_stream(
        words_iter,
        backend=args.backend,
        words_filter=words_filter,
    )

    t2 = time.perf_counter()

    if args.csv:
        save_csv(freq, args.csv)
    visualise_top(freq, args.top, args.png)

    t3 = time.perf_counter()

    # Summary Generation
    print("\n--- Execution Summary ---")
    summary_data = {
        "Metric": [
            "Total words (filtered)",
            "Unique words",
            "Download time",
            "Processing time (MapReduce)",
            "Visualisation/Save time",
            "Total execution time",
            "Parallel backend",
        ],
        "Value": [
            f"{sum(freq.values()):,}",
            f"{len(freq):,}",
            f"{t1 - t0:.2f}s",
            f"{t2 - t1:.2f}s",
            f"{t3 - t2:.2f}s",
            f"{t3 - t0:.2f}s",
            args.backend,
        ],
    }

    if psutil:
        proc = psutil.Process()
        mem_info = proc.memory_info()
        summary_data["Metric"].append("RAM usage (peak)")
        summary_data["Value"].append(f"{mem_info.rss / 1024**2:.2f} MB")
        summary_data["Metric"].append("CPU time (user)")
        summary_data["Value"].append(f"{proc.cpu_times().user:.2f}s")
    
    df = pd.DataFrame(summary_data)
    # Use to_string() for clean console output
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()