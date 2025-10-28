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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pypdf import PdfReader

_DATE_AMOUNT_RE = re.compile(r"(?P<date>\d{2}/\d{2}/\d{4}).*?\$(?P<amount>[\(\)\d,\.]+)")
_STATE_ZIP_RE = re.compile(r"\b([A-Z]{2})\s+(\d{3,5}(?:-\d{4})?)\b")
_DATE_NAME_LINE_RE = re.compile(r"\d{2}/\d{2}/\d{4}\s+\$[\(\)\d,\.]+\s+Name:")
_SCHEDULE_MARKERS = [
    "Schedule C2 - Individual contributions",
    "Schedule In-State Contributions of $100 or Less",
    "Schedule E1 - Operating expenses",
    "Schedule E4 - Aggregate Small Expenses",
    "Schedule R1 - Other receipts, interest & dividends",
]


def _clean_line(line: str) -> str:
    """Collapse internal whitespace and replace non-breaking spaces."""
    return " ".join(line.replace("\xa0", " ").strip().split())


def _is_header_line(line: str) -> bool:
    prefixes = (
        "Quarter ",
        "Covers ",
        "Jurisdiction:",
        "Schedule ",
    )
    if any(line.startswith(prefix) for prefix in prefixes):
        return True
    if "Filed on" in line:
        return True
    if re.match(r"^\d{3,}\s", line) and "," not in line:
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


def _extract_label_value(line: str, label: str) -> Optional[str]:
    if label not in line:
        return None
    before, after = line.split(label, 1)
    before = before.strip()
    after = after.strip()
    if not before and not after:
        return None
    if not before:
        return after or None
    if not after:
        return before or None
    return f"{before} {after}".strip()


def _looks_like_vendor_line(line: str) -> bool:
    if not line:
        return False
    if ":" in line:
        return False
    if line.startswith("$"):
        return False
    if _DATE_NAME_LINE_RE.match(line):
        return False
    return True


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


@dataclass
class AZOperatingExpenseEntry:
    name: Optional[str] = None
    address_lines: List[str] = field(default_factory=list)
    date: Optional[str] = None
    amount: Optional[Decimal] = None
    payment_method: Optional[str] = None
    cycle_to_date: Optional[Decimal] = None
    category: Optional[str] = None
    transaction_type: Optional[str] = None
    memo: Optional[str] = None
    extra: List[str] = field(default_factory=list)
    raw_text: Optional[str] = None

    def to_csv_row(self) -> dict:
        return {
            "NAME": self.name or "",
            "ADDRESS": " | ".join(self.address_lines),
            "DATE": self.date or "",
            "AMOUNT": _format_currency(self.amount) or "",
            "PAYMENT METHOD": self.payment_method or "",
            "CYCLE TO DATE": _format_currency(self.cycle_to_date) or "",
            "CATEGORY": self.category or "",
            "TRANSACTION TYPE": self.transaction_type or "",
            "MEMO": self.memo or "",
            "DETAILS": " | ".join(self.extra),
            "RAW": self.raw_text or "",
        }

    def to_json_dict(self, include_raw: bool = False) -> dict:
        data = {
            "name": self.name,
            "address_lines": self.address_lines,
            "date": self.date,
            "amount": _format_currency(self.amount),
            "payment_method": self.payment_method,
            "cycle_to_date": _format_currency(self.cycle_to_date),
            "category": self.category,
            "transaction_type": self.transaction_type,
            "memo": self.memo,
            "extra": self.extra or None,
        }
        if not data["extra"]:
            del data["extra"]
        if include_raw:
            data["raw_text"] = self.raw_text
        return data


@dataclass
class AZOtherReceiptEntry:
    name: Optional[str] = None
    address_lines: List[str] = field(default_factory=list)
    date: Optional[str] = None
    amount: Optional[Decimal] = None
    payment_method: Optional[str] = None
    cycle_to_date: Optional[Decimal] = None
    transaction_type: Optional[str] = None
    memo: Optional[str] = None
    extra: List[str] = field(default_factory=list)
    raw_text: Optional[str] = None

    def to_csv_row(self) -> dict:
        return {
            "NAME": self.name or "",
            "ADDRESS": " | ".join(self.address_lines),
            "DATE": self.date or "",
            "AMOUNT": _format_currency(self.amount) or "",
            "PAYMENT METHOD": self.payment_method or "",
            "CYCLE TO DATE": _format_currency(self.cycle_to_date) or "",
            "TRANSACTION TYPE": self.transaction_type or "",
            "MEMO": self.memo or "",
            "DETAILS": " | ".join(self.extra),
            "RAW": self.raw_text or "",
        }

    def to_json_dict(self, include_raw: bool = False) -> dict:
        data = {
            "name": self.name,
            "address_lines": self.address_lines,
            "date": self.date,
            "amount": _format_currency(self.amount),
            "payment_method": self.payment_method,
            "cycle_to_date": _format_currency(self.cycle_to_date),
            "transaction_type": self.transaction_type,
            "memo": self.memo,
            "extra": self.extra or None,
        }
        if not data["extra"]:
            del data["extra"]
        if include_raw:
            data["raw_text"] = self.raw_text
        return data


class AZReportParser:
    def __init__(self, pdf_path: Path):
        self.pdf_path = pdf_path
        self.reader = PdfReader(str(pdf_path))

    def _collect_schedule_lines(self, schedule_marker: str) -> List[str]:
        lines: List[str] = []
        in_section = False
        for page in self.reader.pages:
            raw_text = page.extract_text() or ""
            page_lines = [_clean_line(line) for line in raw_text.splitlines()]
            if not in_section and any(schedule_marker in line for line in page_lines):
                in_section = True

            if not in_section:
                continue

            for line in page_lines:
                if not line:
                    continue
                if any(
                    other != schedule_marker and other in line
                    for other in _SCHEDULE_MARKERS
                ):
                    in_section = False
                    break
                if _is_header_line(line):
                    continue
                lines.append(line)

            if not in_section and lines:
                break

        cleaned = [
            line
            for line in lines
            if line
            and "Filed on" not in line
            and not re.match(r"^\d+\s*Filed on", line)
        ]
        return cleaned

    def parse_contributions(self) -> List[AZContributionEntry]:
        lines = self._collect_schedule_lines("Schedule C2 - Individual contributions")
        entries: List[AZContributionEntry] = []
        current: List[str] = []

        for line in lines:
            if any(
                line.startswith(boundary)
                for boundary in ("Total of", "Net Total", "Total Small", "Total of Aggregate")
            ):
                break

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

    def _split_vendor_entries(self, lines: List[str]) -> List[Tuple[str, List[str], str, List[str]]]:
        entries: List[Tuple[str, List[str], str, List[str]]] = []
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            if line.startswith("Total"):
                break
            name = line
            idx += 1

            address_lines: List[str] = []
            while idx < len(lines) and not _DATE_NAME_LINE_RE.match(lines[idx]):
                next_line = lines[idx]
                if next_line.startswith("Total"):
                    break
                if next_line and ":" in next_line and next_line.split(":", 1)[0] in {"Memo", "Category", "Address", "Trans. Type"}:
                    # Prevent misclassifying detail lines as part of address.
                    break
                address_lines.append(next_line)
                idx += 1

            if idx >= len(lines) or not _DATE_NAME_LINE_RE.match(lines[idx]):
                break

            date_line = lines[idx]
            idx += 1

            detail_lines: List[str] = []
            while idx < len(lines):
                lookahead_line = lines[idx]
                if lookahead_line.startswith("Total"):
                    break
                if _looks_like_vendor_line(lookahead_line):
                    lookahead = idx + 1
                    while lookahead < len(lines) and not _DATE_NAME_LINE_RE.match(lines[lookahead]):
                        probe = lines[lookahead]
                        if probe.startswith("Total"):
                            break
                        if ":" in probe:
                            break
                        lookahead += 1
                    if lookahead < len(lines) and _DATE_NAME_LINE_RE.match(lines[lookahead]):
                        break
                detail_lines.append(lookahead_line)
                idx += 1

            entries.append((name, address_lines, date_line, detail_lines))

            if idx < len(lines) and lines[idx].startswith("Total"):
                break

        return entries

    def parse_operating_expenses(self) -> List[AZOperatingExpenseEntry]:
        lines = self._collect_schedule_lines("Schedule E1 - Operating expenses")
        entries: List[AZOperatingExpenseEntry] = []

        for name, address_lines, date_line, detail_lines in self._split_vendor_entries(lines):
            entry = self._build_operating_expense_entry(name, address_lines, date_line, detail_lines)
            if entry:
                entries.append(entry)
        return entries

    def parse_other_receipts(self) -> List[AZOtherReceiptEntry]:
        lines = self._collect_schedule_lines("Schedule R1 - Other receipts, interest & dividends")
        entries: List[AZOtherReceiptEntry] = []
        for name, address_lines, date_line, detail_lines in self._split_vendor_entries(lines):
            entry = self._build_other_receipt_entry(name, address_lines, date_line, detail_lines)
            if entry:
                entries.append(entry)
        return entries

    def parse_in_state_small_contributions(self) -> List[Dict[str, str]]:
        lines = self._collect_schedule_lines("Schedule In-State Contributions of $100 or Less")
        return self._build_summary_rows(lines, cycle_label="Cycle to Date")

    def parse_aggregate_small_expenses(self) -> List[Dict[str, str]]:
        lines = self._collect_schedule_lines("Schedule E4 - Aggregate Small Expenses")
        return self._build_summary_rows(lines, cycle_label="Cycle to Date")

    def _build_operating_expense_entry(
        self,
        name: str,
        address_lines: List[str],
        date_line: str,
        detail_lines: List[str],
    ) -> Optional[AZOperatingExpenseEntry]:
        match = _DATE_AMOUNT_RE.search(date_line)
        if not match:
            logging.debug("Unable to parse operating expense date/amount from %r", date_line)
            return None

        date = match.group("date")
        amount = _parse_decimal(match.group("amount"))
        payment_method: Optional[str] = None
        cycle_to_date: Optional[Decimal] = None
        category: Optional[str] = None
        transaction_type: Optional[str] = None
        memo: Optional[str] = None
        extras: List[str] = []

        for detail in detail_lines:
            if detail.startswith("$"):
                if cycle_to_date is None:
                    cycle_to_date = _parse_decimal(detail)
                else:
                    extras.append(detail)
                continue
            extracted = _extract_label_value(detail, "Address:")
            if extracted is not None:
                payment_method = extracted
                continue
            extracted = _extract_label_value(detail, "Category:")
            if extracted is not None:
                category = extracted
                continue
            extracted = _extract_label_value(detail, "Trans. Type:")
            if extracted is not None:
                transaction_type = extracted
                continue
            extracted = _extract_label_value(detail, "Memo:")
            if extracted is not None:
                memo = extracted
                continue
            extracted = _extract_label_value(detail, "Occupation:")
            if extracted is not None:
                extras.append(f"Occupation: {extracted}")
                continue
            extracted = _extract_label_value(detail, "Description:")
            if extracted is not None:
                extras.append(f"Description: {extracted}")
                continue
            extras.append(detail)

        raw_text = "\n".join(filter(None, [name, *address_lines, date_line, *detail_lines]))
        return AZOperatingExpenseEntry(
            name=name,
            address_lines=[line for line in address_lines if line],
            date=date,
            amount=amount,
            payment_method=payment_method,
            cycle_to_date=cycle_to_date,
            category=category,
            transaction_type=transaction_type,
            memo=memo,
            extra=extras,
            raw_text=raw_text,
        )

    def _build_other_receipt_entry(
        self,
        name: str,
        address_lines: List[str],
        date_line: str,
        detail_lines: List[str],
    ) -> Optional[AZOtherReceiptEntry]:
        match = _DATE_AMOUNT_RE.search(date_line)
        if not match:
            logging.debug("Unable to parse other receipt date/amount from %r", date_line)
            return None

        date = match.group("date")
        amount = _parse_decimal(match.group("amount"))
        payment_method: Optional[str] = None
        cycle_to_date: Optional[Decimal] = None
        transaction_type: Optional[str] = None
        memo: Optional[str] = None
        extras: List[str] = []

        for detail in detail_lines:
            if detail.startswith("$"):
                if cycle_to_date is None:
                    cycle_to_date = _parse_decimal(detail)
                else:
                    extras.append(detail)
                continue
            extracted = _extract_label_value(detail, "Address:")
            if extracted is not None:
                payment_method = extracted
                continue
            extracted = _extract_label_value(detail, "Trans. Type:")
            if extracted is not None:
                transaction_type = extracted
                continue
            extracted = _extract_label_value(detail, "Memo:")
            if extracted is not None:
                memo = extracted
                continue
            extracted = _extract_label_value(detail, "Category:")
            if extracted is not None:
                extras.append(f"Category: {extracted}")
                continue
            extracted = _extract_label_value(detail, "Description:")
            if extracted is not None:
                extras.append(f"Description: {extracted}")
                continue
            extras.append(detail)

        raw_text = "\n".join(filter(None, [name, *address_lines, date_line, *detail_lines]))
        return AZOtherReceiptEntry(
            name=name,
            address_lines=[line for line in address_lines if line],
            date=date,
            amount=amount,
            payment_method=payment_method,
            cycle_to_date=cycle_to_date,
            transaction_type=transaction_type,
            memo=memo,
            extra=extras,
            raw_text=raw_text,
        )

    def _build_summary_rows(self, lines: List[str], cycle_label: str) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        if not lines:
            return rows

        summary_re = re.compile(
            r"(?P<name>.+?)\s+(?P<date>\d{2}/\d{2}/\d{4})\s+\$(?P<amount>[\d,]+\.\d{2})\s+Name:"
        )
        match = summary_re.match(lines[0])
        if match:
            rows.append({"Label": "Name", "Value": match.group("name")})
            rows.append({"Label": "Date", "Value": match.group("date")})
            rows.append({"Label": "Amount", "Value": match.group("amount")})

        payment_method = None
        amounts: List[str] = []
        labels: List[str] = []

        for line in lines[1:]:
            address_value = _extract_label_value(line, "Address:")
            if address_value is not None and payment_method is None:
                payment_method = address_value
                continue
            if line.startswith("$"):
                amounts.append(line.replace("$", "").strip())
                continue
            if line.startswith("Total") or line.startswith("Net"):
                labels.append(line)
                continue
            extracted = _extract_label_value(line, "Trans. Type:")
            if extracted is not None:
                rows.append({"Label": "Transaction Type", "Value": extracted})
                continue

        if payment_method:
            rows.append({"Label": "Payment Method", "Value": payment_method})

        if amounts:
            rows.append({"Label": cycle_label, "Value": amounts[0]})
        for label, value in zip(labels, amounts[1:]):
            rows.append({"Label": label, "Value": value})

        return rows

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


def _write_csv_rows(rows: Sequence[Dict[str, str]], path: Path, headers: Optional[List[str]] = None) -> None:
    if not rows:
        if headers is None:
            logging.info("No rows written to %s (empty dataset)", path)
            return
        rows_to_write = [dict.fromkeys(headers, "")]
        fieldnames = headers
    else:
        fieldnames = headers or list(rows[0].keys())
        rows_to_write = rows

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_to_write:
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
    small_contrib_summary = report_parser.parse_in_state_small_contributions()
    operating_expenses = report_parser.parse_operating_expenses()
    small_expense_summary = report_parser.parse_aggregate_small_expenses()
    other_receipts = report_parser.parse_other_receipts()

    parsed_args.output_dir.mkdir(parents=True, exist_ok=True)

    if "json" in parsed_args.formats:
        _write_json(contributions, parsed_args.output_dir / "contributions.json", parsed_args.include_raw)
        _write_json(operating_expenses, parsed_args.output_dir / "operating_expenses.json", parsed_args.include_raw)
        _write_json(other_receipts, parsed_args.output_dir / "other_receipts.json", parsed_args.include_raw)
        (parsed_args.output_dir / "in_state_small_contributions.json").write_text(
            json.dumps(small_contrib_summary, indent=2),
            encoding="utf-8",
        )
        (parsed_args.output_dir / "aggregate_small_expenses.json").write_text(
            json.dumps(small_expense_summary, indent=2),
            encoding="utf-8",
        )
    if "csv" in parsed_args.formats:
        contribution_rows = [entry.to_csv_row() for entry in contributions]
        contribution_headers = (
            list(contribution_rows[0].keys())
            if contribution_rows
            else [
                "LAST NAME",
                "FIRST NAME",
                "ADDRESS (Line 1)",
                "STATE",
                "ZIP",
                "OCCUPATION",
                "EMPLOYER",
                "ADDRESS (Full)",
                "DATE",
                "AMOUNT",
                "TYPE",
                "TOTAL TO DATE",
                "RAW",
            ]
        )
        _write_csv_rows(
            contribution_rows,
            parsed_args.output_dir / "contributions.csv",
            headers=contribution_headers,
        )

        operating_rows = [entry.to_csv_row() for entry in operating_expenses]
        operating_headers = (
            list(operating_rows[0].keys())
            if operating_rows
            else [
                "NAME",
                "ADDRESS",
                "DATE",
                "AMOUNT",
                "PAYMENT METHOD",
                "CYCLE TO DATE",
                "CATEGORY",
                "TRANSACTION TYPE",
                "MEMO",
                "DETAILS",
                "RAW",
            ]
        )
        _write_csv_rows(
            operating_rows,
            parsed_args.output_dir / "operating_expenses.csv",
            headers=operating_headers,
        )

        other_rows = [entry.to_csv_row() for entry in other_receipts]
        other_headers = (
            list(other_rows[0].keys())
            if other_rows
            else [
                "NAME",
                "ADDRESS",
                "DATE",
                "AMOUNT",
                "PAYMENT METHOD",
                "CYCLE TO DATE",
                "TRANSACTION TYPE",
                "MEMO",
                "DETAILS",
                "RAW",
            ]
        )
        _write_csv_rows(
            other_rows,
            parsed_args.output_dir / "other_receipts.csv",
            headers=other_headers,
        )

        _write_csv_rows(
            small_contrib_summary,
            parsed_args.output_dir / "in_state_small_contributions.csv",
            headers=["Label", "Value"],
        )
        _write_csv_rows(
            small_expense_summary,
            parsed_args.output_dir / "aggregate_small_expenses.csv",
            headers=["Label", "Value"],
        )


if __name__ == "__main__":
    main()
