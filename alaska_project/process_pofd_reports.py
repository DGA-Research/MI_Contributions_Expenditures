"""Parse POFD PDF filings and export schedule data to an Excel workbook."""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pdfplumber

# Map normalized table headers to a friendly schedule sheet name.
SCHEDULE_HEADER_MAP: Dict[Tuple[str, ...], str] = {
    ("Owner", "Type", "Detail", "Description", "Amount"): "Income",
    ("Owner", "Type", "Detail", "Description / Interest"): "Interests",
    ("Owner", "Type", "Name"): "LoansAndDebts",
    ("Owner", "Type of Lease", "Lease/Contract ID", "Interest", "Status", "Description"): "Leases",
    ("Associated Person", "Description"): "CloseEconomicAssociations",
    ("Name", "Address", "Compensation"): "LobbyistPartnerEmployers",
}

# Preferred column ordering for each schedule (after metadata columns).
SCHEDULE_DISPLAY_COLUMNS: Dict[str, List[str]] = {
    "Income": ["Owner", "Type", "Detail", "Description", "Amount", "Amount Minimum", "Amount Maximum"],
    "Interests": ["Owner", "Type", "Detail", "Description / Interest"],
    "LoansAndDebts": ["Owner", "Type", "Name"],
    "Leases": ["Owner", "Type of Lease", "Lease/Contract ID", "Interest", "Status", "Description"],
    "CloseEconomicAssociations": ["Associated Person", "Description"],
    "LobbyistPartnerEmployers": ["Name", "Address", "Compensation"],
}

SCHEDULE_ORDER: List[str] = list(SCHEDULE_DISPLAY_COLUMNS.keys())

# Metadata columns to copy to every schedule row for context.
SCHEDULE_METADATA_COLUMNS: List[str] = [
    "Source File",
    "Report Year",
    "Report Type",
    "Submission Date",
    "Report Dates",
    "Filing As",
]


def clean_cell(value: str | None) -> str:
    """Collapse whitespace and normalise a table cell into a single-line string."""
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", value.strip())
    return text


def normalised_header(row: Iterable[str | None]) -> Tuple[str, ...]:
    """Return a tuple of cleaned header values for schedule identification."""
    return tuple(clean_cell(cell) for cell in row)


def flatten_values(values: Iterable[str | None]) -> str:
    """Create a comparable lowercase token string from a header or row."""
    parts: List[str] = []
    for value in values:
        cleaned = clean_cell(value)
        if cleaned:
            parts.append(cleaned)
    return " ".join(parts).lower()


AMOUNT_NUMBER_RE = re.compile(r"\$?\s*([0-9][0-9,]*)")
AMOUNT_MIN_COLUMN = "Amount Minimum"
AMOUNT_MAX_COLUMN = "Amount Maximum"


def parse_amount_bounds(amount: str | float | int | None) -> Tuple[Optional[int], Optional[int]]:
    """Return numeric lower/upper bounds parsed from a textual amount range."""
    if amount is None:
        return (None, None)
    if isinstance(amount, (int, float)):
        value = int(amount)
        return (value, value)
    text = str(amount).strip()
    if not text:
        return (None, None)
    lower_text = text.lower()
    numbers = [
        int(match.replace(",", ""))
        for match in AMOUNT_NUMBER_RE.findall(text)
    ]
    if not numbers:
        return (None, None)

    sanitized_lower = lower_text.replace("not more than", "").replace("no more than", "")
    has_range_indicator = "-" in text or "between" in lower_text or (
        "from" in lower_text and ("to" in lower_text or "through" in lower_text)
    )
    has_upper_indicator = any(
        phrase in lower_text
        for phrase in (
            "not more than",
            "no more than",
            "or less",
            "less than or equal",
            "less than",
            "up to",
            "at most",
        )
    )
    has_lower_indicator = any(
        phrase in sanitized_lower
        for phrase in (
            "more than",
            "at least",
            "greater than",
            "minimum",
        )
    )

    min_val: Optional[int] = None
    max_val: Optional[int] = None

    if has_range_indicator or (len(numbers) >= 2 and (" and " in lower_text or has_upper_indicator or has_lower_indicator)):
        min_val = numbers[0]
        if len(numbers) > 1:
            max_val = numbers[-1]

    if has_upper_indicator:
        max_val = numbers[-1]
        if len(numbers) > 1:
            min_val = numbers[0]

    if "or more" in lower_text:
        min_val = numbers[-1]
        max_val = None
    elif has_lower_indicator and min_val is None:
        min_val = numbers[0]

    if len(numbers) == 1 and min_val is None and max_val is None:
        min_val = numbers[0]
        max_val = numbers[0]

    return (min_val, max_val)


def extract_rental_appendix_rows(table: List[List[str | None]]) -> List[Dict[str, str]] | None:
    """Detect and parse supplemental rental tables appended outside the standard income schedule."""
    cleaned_pairs: List[Tuple[str, str]] = []
    for row in table:
        if not row:
            continue
        detail = clean_cell(row[0]) if len(row) > 0 else ""
        amount = clean_cell(row[1]) if len(row) > 1 else ""
        if not detail and not amount:
            continue
        cleaned_pairs.append((detail, amount))

    if not cleaned_pairs:
        return None

    has_marker = any("rental income" in detail.lower() for detail, _ in cleaned_pairs)
    rental_rows = [
        pair
        for pair in cleaned_pairs
        if "total (usd)" in pair[0].lower() and ("more than $" in pair[1].lower() or "not more than $" in pair[1].lower())
    ]

    if not has_marker and len(rental_rows) < 3:
        return None

    extracted: List[Dict[str, str]] = []
    for detail, amount in cleaned_pairs:
        lowered_detail = detail.lower()
        if not detail or not amount:
            continue
        if lowered_detail == "rental income":
            continue
        if lowered_detail.startswith("date range"):
            continue
        extracted.append(
            {
                "Owner": "Filer",
                "Type": "Rental",
                "Detail": detail,
                "Description": "",
                "Amount": amount,
            }
        )

    return extracted or None


def extract_metadata(first_page: pdfplumber.page.Page) -> Dict[str, str]:
    """Build a metadata dictionary from the first page of a filing."""
    metadata: Dict[str, str] = {}
    tables = first_page.extract_tables()
    if not tables or not tables[0] or not tables[0][0]:
        # Fallback: try basic text extraction if no table was detected.
        text = first_page.extract_text() or ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
    else:
        block = tables[0][0][0]
        lines = [line.strip() for line in block.split("\n") if line.strip()]

    for line in lines:
        normalized = re.sub(r"\s+", " ", line)
        if normalized.upper().startswith("OWNER TYPE DETAIL DESCRIPTION AMOUNT"):
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def get_default_header(schedule_name: str) -> Tuple[str, ...]:
    """Return the canonical header tuple for a given schedule."""
    for header, mapped_name in SCHEDULE_HEADER_MAP.items():
        if mapped_name == schedule_name:
            return header
    return tuple(SCHEDULE_DISPLAY_COLUMNS.get(schedule_name, []))


def gather_schedule_rows(
    pdf_path: Path,
    metadata: Dict[str, str],
) -> Dict[str, List[Dict[str, str]]]:
    """Extract all recognised schedule tables from a PDF."""
    schedule_rows: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    last_schedule_name: str | None = None
    last_header_by_schedule: Dict[str, Tuple[str, ...]] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                if not table or not table[0]:
                    continue

                supplemental_rentals = extract_rental_appendix_rows(table)
                if supplemental_rentals:
                    schedule_name = "Income"
                    canonical_header = get_default_header(schedule_name)
                    last_header_by_schedule[schedule_name] = canonical_header
                    last_schedule_name = schedule_name
                    for row_dict in supplemental_rentals:
                        enriched_row = {col: metadata.get(col, "") for col in SCHEDULE_METADATA_COLUMNS}
                        enriched_row.update(row_dict)
                        schedule_rows[schedule_name].append(enriched_row)
                    continue

                header_tuple = normalised_header(table[0])
                schedule_name = SCHEDULE_HEADER_MAP.get(header_tuple)
                canonical_header: Tuple[str, ...] | None = header_tuple if schedule_name else None
                if not schedule_name:
                    flattened_header = flatten_values(header_tuple)
                    for template, candidate in SCHEDULE_HEADER_MAP.items():
                        if not template:
                            continue
                        if flatten_values(template) == flattened_header:
                            schedule_name = candidate
                            canonical_header = template
                            break
                if not schedule_name:
                    if not any(header_tuple) and last_schedule_name:
                        cached_header = last_header_by_schedule.get(last_schedule_name)
                        expected_len = len(cached_header) if cached_header else 0
                        # Count non-empty row lengths to ensure this looks like a continuation table.
                        row_lengths = {len([cell for cell in row if cell is not None]) for row in table if row}
                        if (
                            cached_header
                            and expected_len > 0
                            and row_lengths
                            and max(row_lengths) >= expected_len
                        ):
                            schedule_name = last_schedule_name
                            canonical_header = cached_header
                        else:
                            continue
                    else:
                        continue
                if not canonical_header or not any(canonical_header):
                    cached_header = last_header_by_schedule.get(schedule_name)
                    if cached_header:
                        canonical_header = cached_header
                    else:
                        canonical_header = get_default_header(schedule_name)
                else:
                    last_header_by_schedule[schedule_name] = canonical_header
                header_names = list(canonical_header)
                if not header_names:
                    continue
                canonical_tuple = tuple(header_names)
                last_header_by_schedule[schedule_name] = canonical_tuple
                last_schedule_name = schedule_name
                if not schedule_name:
                    continue
                for raw_row in table[1:]:
                    cleaned_row = [clean_cell(cell) for cell in raw_row]
                    if not any(cleaned_row):
                        continue
                    first_cell = cleaned_row[0].lower()
                    if first_cell.startswith("page ") and " of " in first_cell:
                        continue
                    if tuple(cleaned_row[: len(canonical_tuple)]) == canonical_tuple:
                        continue
                    if flatten_values(cleaned_row) == flatten_values(canonical_tuple):
                        continue
                    # Pad to header length to avoid missing columns.
                    if len(cleaned_row) > len(header_names):
                        cleaned_row = cleaned_row[: len(header_names)]
                    elif len(cleaned_row) < len(header_names):
                        cleaned_row.extend([""] * (len(header_names) - len(cleaned_row)))
                    row_dict = {col: cleaned_row[idx] for idx, col in enumerate(header_names)}
                    enriched_row = {col: metadata.get(col, "") for col in SCHEDULE_METADATA_COLUMNS}
                    enriched_row.update(row_dict)
                    schedule_rows[schedule_name].append(enriched_row)
    return schedule_rows


def build_metadata_summary(records: List[Dict[str, str]]) -> pd.DataFrame:
    """Create a summary DataFrame containing one row per filing."""
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    preferred_order = [
        "Source File",
        "Submission Date",
        "Report Year",
        "Report Dates",
        "Report Type",
        "Filing As",
        "Municipality",
        "Office",
        "Branch",
        "Position",
        "Department",
        "First Name",
        "Last Name",
        "Address",
        "City, State Zip",
        "Contact Phone",
        "Alternate Phone",
        "Email",
        "Partner Type",
        "Spouse/Domestic Partner Name",
        "Dependent Children",
        "Non-Dependent Children",
    ]
    ordered_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in ordered_cols]
    return df[ordered_cols + remaining_cols]


def build_income_summary(df: pd.DataFrame, *, rentals_only: bool = False) -> pd.DataFrame:
    """Aggregate income minimum and maximum totals by year."""
    if df.empty:
        return pd.DataFrame(columns=["Year", "Low", "High"])

    data = df.copy()
    if rentals_only:
        type_series = data["Type"].astype("string").str.strip().str.lower()
        data = data[type_series == "rental"]

    if data.empty:
        return pd.DataFrame(columns=["Year", "Low", "High"])

    data = data.copy()
    data["Report Year"] = pd.to_numeric(data["Report Year"], errors="coerce")
    data = data.dropna(subset=["Report Year"])
    if data.empty:
        return pd.DataFrame(columns=["Year", "Low", "High"])

    data["Report Year"] = data["Report Year"].astype("Int64")

    def sum_min(series: pd.Series) -> Optional[int]:
        non_null = series.dropna()
        if non_null.empty:
            return pd.NA
        return int(non_null.sum())

    def sum_max(series: pd.Series) -> Optional[int]:
        if series.isna().all():
            return pd.NA
        if series.isna().any():
            return pd.NA
        return int(series.dropna().sum())

    summary = (
        data.groupby("Report Year")
        .agg(
            Low=pd.NamedAgg(column=AMOUNT_MIN_COLUMN, aggfunc=sum_min),
            High=pd.NamedAgg(column=AMOUNT_MAX_COLUMN, aggfunc=sum_max),
        )
        .reset_index()
        .rename(columns={"Report Year": "Year"})
        .sort_values("Year")
        .reset_index(drop=True)
    )

    summary["Year"] = summary["Year"].astype("Int64")
    summary["Low"] = pd.array(summary["Low"], dtype="Int64")
    summary["High"] = pd.array(summary["High"], dtype="Int64")

    return summary


def schedules_to_excel(all_schedule_rows: Dict[str, List[Dict[str, str]]], output_path: Path) -> None:
    """Write all schedule data to an Excel workbook."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        income_df: Optional[pd.DataFrame] = None
        for schedule_name in sorted(
            all_schedule_rows.keys(),
            key=lambda name: SCHEDULE_ORDER.index(name) if name in SCHEDULE_ORDER else name,
        ):
            rows = all_schedule_rows[schedule_name]
            if not rows:
                continue
            df = pd.DataFrame(rows)
            if "Amount" in df.columns:
                min_vals: List[Optional[int]] = []
                max_vals: List[Optional[int]] = []
                for value in df["Amount"]:
                    amount_min, amount_max = parse_amount_bounds(value)
                    min_vals.append(amount_min)
                    max_vals.append(amount_max)
                df[AMOUNT_MIN_COLUMN] = pd.array([pd.NA if v is None else v for v in min_vals], dtype="Int64")
                df[AMOUNT_MAX_COLUMN] = pd.array([pd.NA if v is None else v for v in max_vals], dtype="Int64")
            for col in SCHEDULE_METADATA_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            schedule_columns = SCHEDULE_DISPLAY_COLUMNS.get(schedule_name, [])
            for col in schedule_columns:
                if col not in df.columns:
                    df[col] = ""
            ordered_cols = [col for col in SCHEDULE_METADATA_COLUMNS if col in df.columns]
            ordered_cols += [col for col in schedule_columns if col in df.columns and col not in ordered_cols]
            # Append any remaining columns to cover unexpected data.
            ordered_cols += [col for col in df.columns if col not in ordered_cols]
            df = df[ordered_cols]
            # Sort rows for readability.
            sort_cols = [col for col in ("Report Year", "Source File", "Owner", "Name") if col in df.columns]
            if sort_cols:
                df = df.sort_values(sort_cols, ignore_index=True)
            df.to_excel(writer, sheet_name=schedule_name, index=False)
            if schedule_name == "Income":
                income_df = df.copy()

        if income_df is not None and not income_df.empty:
            income_summary = build_income_summary(income_df)
            if not income_summary.empty:
                income_summary.to_excel(writer, sheet_name="Income Summary", index=False)
            rental_summary = build_income_summary(income_df, rentals_only=True)
            if not rental_summary.empty:
                rental_summary.to_excel(writer, sheet_name="Income Summary Rentals", index=False)


def process_filings(input_dir: Path, output_path: Path) -> Path:
    """Parse all PDFs in a directory and emit the consolidated Excel workbook."""
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"No PDF files found in {input_dir}")

    all_schedule_rows: Dict[str, List[Dict[str, str]]] = {name: [] for name in SCHEDULE_DISPLAY_COLUMNS}
    metadata_records: List[Dict[str, str]] = []

    for pdf_path in pdf_files:
        with pdfplumber.open(pdf_path) as pdf:
            metadata = extract_metadata(pdf.pages[0])
        metadata["Source File"] = pdf_path.name
        metadata_records.append(metadata)
        schedule_rows = gather_schedule_rows(pdf_path, metadata)
        for schedule_name, rows in schedule_rows.items():
            all_schedule_rows[schedule_name].extend(rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    schedules_to_excel(all_schedule_rows, output_path)

    # Write a summary sheet if we have metadata.
    summary_df = build_metadata_summary(metadata_records)
    if not summary_df.empty:
        with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

    return output_path


def process_single_pdf(pdf_path: Path, output_path: Path) -> Path:
    """Parse a single POFD PDF and store the schedules in `output_path`."""
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    with pdfplumber.open(pdf_path) as pdf:
        metadata = extract_metadata(pdf.pages[0])
    metadata["Source File"] = pdf_path.name

    schedule_rows = gather_schedule_rows(pdf_path, metadata)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    schedules_to_excel(schedule_rows, output_path)

    summary_df = build_metadata_summary([metadata])
    if not summary_df.empty:
        with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse POFD PDF filings into an Excel workbook.")
    parser.add_argument(
        "--input-dir",
        default="Taylor_POFDs_65/Taylor_POFDs",
        type=Path,
        help="Directory containing POFD PDF filings.",
    )
    parser.add_argument(
        "--output",
        default="Taylor_POFD_schedules.xlsx",
        type=Path,
        help="Path to the output Excel workbook.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = process_filings(args.input_dir, args.output)
    print(f"Wrote schedules to {output_path}")


if __name__ == "__main__":
    main()
