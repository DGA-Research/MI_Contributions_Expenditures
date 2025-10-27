#!/usr/bin/env python3
"""
Parser for Arizona campaign finance quarterly reports.

The Arizona PDFs have a fairly consistent layout for Schedule C2 entries.
This utility extracts the individual contribution records and emits JSON or
CSV data with columns that mirror the supplied sample workbook.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from pypdf import PdfReader

_DATE_AMOUNT_RE = re.compile(r"(?P<date>\d{2}/\d{2}/\d{4}).*?\$(?P<amount>[\(\)\d,\.]+)")
_STATE_ZIP_RE = re.compile(r"\b([A-Z]{2})\s+(\d{3,5}(?:-\d{4})?)\b")


def _clean_line(line: str) -> str:
    """Collapse internal whitespace and replace non-breaking spaces."""
    return " ".join(line.replace("\xa0", " ").strip().split())


def _is_header_line(line: str) -> bool:
    prefixes = (
        "Quarter ",
        "Covers ",
        "Jurisdiction:",
        "Schedule ",
        "Filed on ",
    )
    if any(line.startswith(prefix) for prefix in prefixes):
        return True
    if line.startswith("100") and "for Arizona" in line:
        return True
    if "Secretary of State" in line:
        return True
    if "Cycle To Date" in line:
        return True
    return False


def _is_name_line(line: str) -> bool:
    if ":" in line:
        return False
    if line.count(",") != 1:
        return False
    if any(char.isdigit() for char in line):
        return False
    return True


def _parse_decimal(value: str) -> Optional[Decimal]:
    cleaned = value.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("-"):
        negative = True
        cleaned = cleaned[1:]
    try:
        number = Decimal(cleaned)
    except InvalidOperation:
        logging.debug("Unable to parse decimal from %r", value)
        return None
    if negative:
        number = -number
    return number


def _format_currency(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    magnitude = abs(value).quantize(Decimal("0.01"))
    formatted = format(magnitude, "f")
    if value < 0:
        return f"({formatted})"
    return formatted


def _split_name(value: str) -> tuple[Optional[str], Optional[str]]:
    parts = [part.strip() or None for part in value.split(",", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return value.strip() or None, None


def _split_occupation_employer(value: str) -> tuple[Optional[str], Optional[str]]:
    if not value:
        return None, None
    if "," in value:
        occupation, employer = value.split(",", 1)
        return occupation.strip() or None, employer.strip() or None
    return value.strip() or None, None


@dataclass
class AZContributionEntry:
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    address_line1: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    occupation: Optional[str] = None
    employer: Optional[str] = None
    address_full: Optional[str] = None
    date: Optional[str] = None
    amount: Optional[Decimal] = None
    transaction_type: Optional[str] = None
    total_to_date: Optional[Decimal] = None
    raw_text: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def to_json_dict(self, include_raw: bool = False) -> dict:
        payload = {
            "last_name": self.last_name,
            "first_name": self.first_name,
            "address_line1": self.address_line1,
            "state": self.state,
            "zip_code": self.zip_code,
            "occupation": self.occupation,
            "employer": self.employer,
            "address_full": self.address_full,
            "date": self.date,
            "amount": _format_currency(self.amount),
            "transaction_type": self.transaction_type,
            "total_to_date": _format_currency(self.total_to_date),
        }
        if self.extra:
            payload["extra"] = self.extra
        if include_raw:
            payload["raw_text"] = self.raw_text
        return payload

    def to_csv_row(self) -> dict:
        return {
            "LAST NAME": self.last_name or "",
            "FIRST NAME": self.first_name or "",
            "ADDRESS (Line 1)": self.address_line1 or "",
            "STATE": self.state or "",
            "ZIP": self.zip_code or "",
            "OCCUPATION": self.occupation or "",
            "EMPLOYER": self.employer or "",
            "ADDRESS (Full)": self.address_full or "",
            "DATE": self.date or "",
            "AMOUNT": _format_currency(self.amount) or "",
            "TYPE": self.transaction_type or "",
            "TOTAL TO DATE": _format_currency(self.total_to_date) or "",
            "RAW": self.raw_text or "",
        }


class AZReportParser:
    def __init__(self, pdf_path: Path):
        self.pdf_path = pdf_path
        self.reader = PdfReader(str(pdf_path))

    def parse_contributions(self) -> List[AZContributionEntry]:
        lines: List[str] = []
        for page in self.reader.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                cleaned = _clean_line(raw_line)
                if not cleaned or _is_header_line(cleaned):
                    continue
                lines.append(cleaned)

        entries: List[AZContributionEntry] = []
        current: List[str] = []

        for line in lines:
            if _is_name_line(line) and current:
                entry = self._build_entry(current)
                if entry is not None:
                    entries.append(entry)
                current = []
            current.append(line)

        if current:
            entry = self._build_entry(current)
            if entry is not None:
                entries.append(entry)

        return entries

    def _build_entry(self, lines: Sequence[str]) -> Optional[AZContributionEntry]:
        if not lines:
            return None

        try:
            raw_text = "\n".join(lines)
            name_line = lines[0]
            last_name, first_name = _split_name(name_line)

            date_index = None
            for idx, line in enumerate(lines):
                if _DATE_AMOUNT_RE.search(line):
                    date_index = idx
                    break
            if date_index is None:
                logging.debug("Failed to locate date within entry %r", raw_text)
                return None

            address_lines = list(lines[1:date_index])
            address_full = ", ".join(address_lines) if address_lines else None
            address_line1 = None
            state = None
            zip_code = None
            if address_lines:
                first_address = address_lines[0]
                address_line1 = first_address.split(",", 1)[0].strip()
                state_zip_match = _STATE_ZIP_RE.search(address_lines[-1])
                if state_zip_match:
                    state, zip_code = state_zip_match.groups()
                    if zip_code:
                        if "-" in zip_code:
                            prefix, suffix = zip_code.split("-", 1)
                            if prefix.isdigit() and len(prefix) < 5:
                                zip_code = f"{prefix.zfill(5)}-{suffix}"
                        elif zip_code.isdigit() and len(zip_code) < 5:
                            zip_code = zip_code.zfill(5)

            date_line = lines[date_index]
            date_match = _DATE_AMOUNT_RE.search(date_line)
            date_value = date_match.group("date")
            amount_value = _parse_decimal(date_match.group("amount"))

            total_to_date: Optional[Decimal] = None
            occupation: Optional[str] = None
            employer: Optional[str] = None
            transaction_type: Optional[str] = None
            original_date: Optional[str] = None

            for line in lines[date_index + 1 :]:
                if line.startswith("$") and "Original" not in line:
                    if total_to_date is None:
                        total_to_date = _parse_decimal(line)
                    continue
                if "Occupation:" in line:
                    before_label = line.split("Occupation:", 1)[0].strip()
                    occupation, employer = _split_occupation_employer(before_label)
                    continue
                if "Trans. Type:" in line:
                    transaction_type = line.split("Trans. Type:", 1)[0].strip()
                    continue
                if "Original Date:" in line:
                    original_date = line.split("Original Date:", 1)[1].strip() or None
                    continue

            if transaction_type and original_date:
                transaction_display = f"{transaction_type} {original_date}"
            elif transaction_type:
                transaction_display = transaction_type
            elif amount_value is not None and amount_value < 0:
                transaction_display = "Refunded Contribution"
            else:
                transaction_display = "Contribution"

            return AZContributionEntry(
                last_name=last_name,
                first_name=first_name,
                address_line1=address_line1,
                state=state,
                zip_code=zip_code,
                occupation=occupation,
                employer=employer,
                address_full=address_full,
                date=date_value,
                amount=amount_value,
                transaction_type=transaction_display,
                total_to_date=total_to_date,
                raw_text=raw_text,
            )
        except Exception:  # pylint: disable=broad-except
            logging.exception("Unable to parse entry: %r", lines)
            return None


def _write_json(entries: Iterable[AZContributionEntry], path: Path, include_raw: bool) -> None:
    payload = [entry.to_json_dict(include_raw=include_raw) for entry in entries]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote %s", path)


def _write_csv(entries: Sequence[AZContributionEntry], path: Path) -> None:
    if not entries:
        header = {
            "LAST NAME": "",
            "FIRST NAME": "",
            "ADDRESS (Line 1)": "",
            "STATE": "",
            "ZIP": "",
            "OCCUPATION": "",
            "EMPLOYER": "",
            "ADDRESS (Full)": "",
            "DATE": "",
            "AMOUNT": "",
            "TYPE": "",
            "TOTAL TO DATE": "",
            "RAW": "",
        }
        rows = [header]
    else:
        rows = [entry.to_csv_row() for entry in entries]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logging.info("Wrote %s", path)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract Arizona Schedule C2 contribution data from a campaign finance PDF."
    )
    parser.add_argument("pdf_path", type=Path, help="Path to the Arizona campaign finance PDF.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("parsed_output_az"),
        help="Directory where JSON/CSV files will be written (default: %(default)s).",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=("json", "csv"),
        default=["json", "csv"],
        help="One or more output formats to emit (default: json csv).",
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Include the original text block for each entry in the output.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging during parsing.",
    )
    return parser


def main(args: Optional[Sequence[str]] = None) -> None:
    parser = _build_arg_parser()
    parsed_args = parser.parse_args(args=args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    pdf_path: Path = parsed_args.pdf_path
    if not pdf_path.exists():
        parser.error(f"PDF not found: {pdf_path}")

    report_parser = AZReportParser(pdf_path)
    contributions = report_parser.parse_contributions()

    parsed_args.output_dir.mkdir(parents=True, exist_ok=True)

    if "json" in parsed_args.formats:
        _write_json(contributions, parsed_args.output_dir / "contributions.json", parsed_args.include_raw)
    if "csv" in parsed_args.formats:
        _write_csv(contributions, parsed_args.output_dir / "contributions.csv")


if __name__ == "__main__":
    main()
