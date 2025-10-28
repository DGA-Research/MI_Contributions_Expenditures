from __future__ import annotations

import csv
import io
from typing import Iterable

import pandas as pd


def parse_txt_bytes(data: bytes, *, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Convert a Pennsylvania campaign finance TXT export into a DataFrame.

    The parser reads the file using the csv module to tolerate inconsistent
    row lengths, normalises whitespace, and pads rows so the resulting DataFrame
    has a consistent column count.
    """
    stream = io.StringIO(data.decode(encoding, errors="ignore"))
    reader = csv.reader(stream)

    rows: list[list[str]] = []
    max_cols = 0

    for row in reader:
        cleaned = [cell.strip() for cell in row]
        if not cleaned or not any(cell for cell in cleaned):
            continue
        max_cols = max(max_cols, len(cleaned))
        rows.append(cleaned)

    if not rows:
        return pd.DataFrame()

    for row in rows:
        if len(row) < max_cols:
            row.extend([""] * (max_cols - len(row)))

    columns = [f"Column {idx}" for idx in range(1, max_cols + 1)]
    return pd.DataFrame(rows, columns=columns)


__all__ = ["parse_txt_bytes"]
