"""
normalizer.py - Shared Word Normalization Module

This module defines the single, exact word normalization and tokenization
rule used consistently across the entire project:
1. Baseline Word Count
2. MapReduce Mapper Process
3. Correctness Verification

Normalization Rules:
- Convert all text to lowercase.
- Keep letters (a-z), digits (0-9), and apostrophes (').
- Remove all other punctuation (periods, commas, exclamation marks, quotes, etc.).
- Strip isolated apostrophes and ignore empty tokens.
"""

import re
from typing import List

# Regular expression pattern to match tokens containing letters, numbers, and apostrophes
# Example matches: "distributed", "systems", "it's", "cloud2026", "don't"
TOKEN_PATTERN = re.compile(r"[a-z0-9']+")


def tokenize_and_normalize(text: str) -> List[str]:
    """
    Normalizes a line or block of text and returns a list of cleaned words.

    Steps:
    1. Lowercase the input string.
    2. Extract all continuous tokens matching [a-z0-9']+.
    3. Strip leading/trailing standalone apostrophes (e.g. "'hello'" -> "hello", "'" -> ignored).
    4. Filter out any empty tokens.

    Example:
        Input:  "Cloud, CLOUD. It's Cloud2026! 'MapReduce' --distributed?"
        Output: ['cloud', 'cloud', "it's", 'cloud2026', 'mapreduce', 'distributed']
    """
    if not text:
        return []

    # 1. Lowercase
    lowered = text.lower()

    # 2. Find matching tokens
    raw_tokens = TOKEN_PATTERN.findall(lowered)

    # 3. Clean stray apostrophes and ignore empty tokens
    cleaned_tokens: List[str] = []
    for token in raw_tokens:
        clean = token.strip("'")
        if clean:
            cleaned_tokens.append(clean)

    return cleaned_tokens
