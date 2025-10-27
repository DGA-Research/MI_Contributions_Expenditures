"""
Parse extracted PDF text into structured CSV files.

Prerequisite: run `extract_pdf_text.py` to populate a `document.txt` via the
`--output` directory. This script walks the linearized text, infers the active
schedule/part, and writes CSV outputs that approximate the original PDF tables.
"""

from __future__ import annotations

import argparse
import csv
import html
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

DEFAULT_DOCUMENT_PATH = Path("text_output/document.txt")
DEFAULT_OUTPUT_DIR = Path("csv_output")
PAGE_TIMESTAMP_RE = re.compile(
    r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M"
)


def clean(line: str) -> str:
    """Trim whitespace and unescape HTML entities."""
    return html.unescape(line.strip())


def is_blank(line: str) -> bool:
    return not clean(line)


def parse_amount(text: str) -> str:
    match = re.search(r"\$ ([\d,]+\.\d{2})", text)
    return match.group(1) if match else ""


def parse_date_tokens(text: str) -> Tuple[str, str, str]:
    """
    Extract month/day/year from a line if present.

    Returns empty strings when not found.
    """
    match = re.search(r"(\d{1,2})\s+(\d{1,2})\s+(\d{4})", text)
    if not match:
        return ("", "", "")
    return match.group(1), match.group(2), match.group(3)


def split_city_state_zip(line: str) -> Tuple[str, str, str, str, str, str]:
    """
    Break a line containing 'City ... State ... Zip ...' into components.

    Returns (city, state_label, state_value, zip_label, zip_value, trailing_text)
    where labels may be echoed or empty depending on layout.
    """
    raw = clean(line)
    if not raw.startswith("City"):
        return ("", "", "", "", "", raw)

    city = ""
    state_label = ""
    state_value = ""
    zip_label = ""
    zip_value = ""
    trailing = ""

    match = re.match(r"^City\s+(?P<city>.+)\s+State\s+(?P<rest>.*)$", raw)
    if match:
        city = match.group("city").strip()
        rest = match.group("rest").strip()
        state_label = "State"
    else:
        # Fallback: no explicit state label on same line.
        rest = ""
        city = raw[len("City") :].strip()

    if rest:
        tokens = rest.split()
        if tokens and re.fullmatch(r"[A-Z]{2}", tokens[0]):
            state_value = tokens.pop(0)
            rest = " ".join(tokens)

        if rest.startswith("Zip Code (Plus 4)"):
            zip_label = "Zip Code (Plus 4)"
            rest = rest[len("Zip Code (Plus 4)") :].strip()
        elif rest.startswith("Zip Code"):
            zip_label = "Zip Code"
            rest = rest[len("Zip Code") :].strip()

        if zip_label:
            tokens = rest.split()
            new_tokens: List[str] = []
            for tok in tokens:
                cleaned = tok.strip()
                if not zip_value and cleaned.startswith("("):
                    # Skip placeholders like "(Plus".
                    continue
                if not zip_value and re.fullmatch(r"\d{5}(?:-\d{4})?", cleaned):
                    zip_value = cleaned
                    continue
                new_tokens.append(cleaned)
            rest = " ".join(new_tokens)

        trailing = rest.strip()

    return city, state_label, state_value, zip_label, zip_value, trailing


def parse_state_zip_line(line: str) -> Tuple[str, str]:
    tokens = clean(line).split()
    if not tokens:
        return ("", "")
    state = tokens[0]
    zip_code = ""
    if len(tokens) > 1:
        zip_code = tokens[1]
    if len(tokens) > 2:
        zip_code = " ".join(tokens[1:])
    return state, zip_code


def parse_employer_line(line: str) -> Tuple[str, str]:
    text = clean(line)
    match = re.match(r"Employer (?:Name|of Contributor)\s+(.*?)\s+Occupation\s+(.*)", text)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    # fallback
    return ("", "")


def parse_employer_address_line(line: str) -> Tuple[str, str, str, str]:
    text = clean(line)
    if not text:
        return ("", "", "", "")

    tokens = text.split()
    # Identify state token (two uppercase letters).
    state_idx = next(
        (idx for idx, tok in enumerate(tokens) if re.fullmatch(r"[A-Z]{2}", tok)),
        None,
    )
    if state_idx is None or state_idx == 0 or state_idx + 1 >= len(tokens):
        return ("", "", "", text)

    city_tokens = tokens[:state_idx]
    state = tokens[state_idx]
    zip_token = tokens[state_idx + 1]
    remainder_tokens = tokens[state_idx + 2 :]
    city = " ".join(city_tokens)
    description = " ".join(remainder_tokens)
    return (city, state, zip_token, description)


def parse_state_zip_description_line(line: str) -> Tuple[str, str, str]:
    """
    Parse lines formatted like 'PA 191304204 Salary'.
    """
    tokens = clean(line).split()
    if not tokens:
        return ("", "", "")
    state = tokens[0]
    zip_code = tokens[1] if len(tokens) > 1 else ""
    description = " ".join(tokens[2:]) if len(tokens) > 2 else ""
    return state, zip_code, description


def collect_name(lines: List[str], idx: int, stop_tokens: Tuple[str, ...]) -> Tuple[str, int]:
    """
    Accumulate lines into a single name until a line starts with any stop token.
    """
    parts: List[str] = []
    while idx < len(lines):
        candidate = clean(lines[idx])
        if not candidate:
            idx += 1
            continue
        if any(candidate.startswith(token) for token in stop_tokens):
            break
        parts.append(candidate)
        idx += 1
    return (" ".join(parts), idx)


@dataclass
class ContributionEntry:
    schedule: str
    part: str
    page: int
    name: str
    amount: str = ""
    month: str = ""
    day: str = ""
    year: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    contributor_type: str = ""
    employer_name: str = ""
    occupation: str = ""
    employer_city: str = ""
    employer_state: str = ""
    employer_zip: str = ""
    description: str = ""


@dataclass
class ExpenditureEntry:
    schedule: str
    page: int
    payee: str
    amount: str = ""
    month: str = ""
    day: str = ""
    year: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    description: str = ""


class DocumentParser:
    def __init__(self, lines: List[str]) -> None:
        self.lines = lines
        self.idx = 0
        self.page = 0
        self.schedule = ""
        self.part = ""
        self.results: Dict[str, List] = {
            "schedule_i_part_a": [],
            "schedule_i_part_b": [],
            "schedule_i_part_c": [],
            "schedule_i_part_d": [],
            "schedule_i_part_e": [],
            "schedule_ii_part_g": [],
            "schedule_iii": [],
        }

    def parse(self) -> None:
        while self.idx < len(self.lines):
            raw_line = self.lines[self.idx]
            line = clean(raw_line)
            if not line:
                self.idx += 1
                continue

            if line.startswith("PAGE "):
                try:
                    self.page = int(line.split()[1])
                except (IndexError, ValueError):
                    self.page = 0
                self.idx += 1
                continue

            if line.startswith("SCHEDULE "):
                self.schedule = line
                self.idx += 1
                continue

            if line.startswith("PART "):
                # Header like "PART A" -> part token after space.
                tokens = line.split()
                if len(tokens) >= 2:
                    self.part = tokens[1]
                else:
                    self.part = line.replace("PART", "").strip()
                self.idx += 1
                continue

            if self.schedule == "SCHEDULE I":
                if self.part == "A" and line.startswith("Full Name of Contributing Committee"):
                    entry, new_idx = self._parse_schedule_i_part_a()
                    self.results["schedule_i_part_a"].append(entry)
                    self.idx = new_idx
                    continue
                if self.part == "B" and line.startswith("Full Name of Contributor"):
                    entry, new_idx = self._parse_schedule_i_part_b()
                    self.results["schedule_i_part_b"].append(entry)
                    self.idx = new_idx
                    continue
                if self.part == "C" and line.startswith("Full Name of Contributing Committee"):
                    entry, new_idx = self._parse_schedule_i_part_c()
                    self.results["schedule_i_part_c"].append(entry)
                    self.idx = new_idx
                    continue
                if self.part == "D" and line.startswith("Full Name of Contributor"):
                    entry, new_idx = self._parse_schedule_i_part_d()
                    self.results["schedule_i_part_d"].append(entry)
                    self.idx = new_idx
                    continue
                if self.part == "E" and line.startswith("Full Name"):
                    entry, new_idx = self._parse_schedule_i_part_e()
                    self.results["schedule_i_part_e"].append(entry)
                    self.idx = new_idx
                    continue

            if self.schedule == "SCHEDULE II":
                if self.part == "G" and line.startswith("Full Name of Contributor"):
                    entry, new_idx = self._parse_schedule_ii_part_g()
                    self.results["schedule_ii_part_g"].append(entry)
                    self.idx = new_idx
                    continue

            if self.schedule == "SCHEDULE III":
                if line.startswith("To Whom Paid"):
                    entry, new_idx = self._parse_schedule_iii()
                    self.results["schedule_iii"].append(entry)
                    self.idx = new_idx
                    continue

            self.idx += 1

    def _parse_schedule_i_part_a(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name..." line
        if idx < len(self.lines) and clean(self.lines[idx]) == "MO DAY YEAR":
            idx += 1

        name, idx = collect_name(
            self.lines, idx, ("Mailing Address",)
        )

        mailing_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        amount = parse_amount(mailing_line)
        idx += 1

        month = day = year = ""
        if idx < len(self.lines):
            potential_date = clean(self.lines[idx])
            if re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", potential_date):
                month, day, year = potential_date.split()
                idx += 1

        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        city, _, _, _, _, trailing = split_city_state_zip(city_line)
        if not (month and day and year):
            m, d, y = parse_date_tokens(trailing)
            month = month or m
            day = day or d
            year = year or y
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="I",
            part="A",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="Political Committee",
        )
        return entry, idx

    def _parse_schedule_i_part_b(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name..." line
        if idx < len(self.lines) and clean(self.lines[idx]) == "MO DAY YEAR":
            idx += 1

        name, idx = collect_name(
            self.lines, idx, ("Mailing Address",)
        )

        mailing_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        amount = parse_amount(mailing_line)
        idx += 1

        month = day = year = ""
        # For part B, date may be appended to the next line.
        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if city_line and re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", city_line):
            month, day, year = city_line.split()
            idx += 1
            city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""

        city, _, _, _, _, trailing = split_city_state_zip(city_line)
        if not (month and day and year):
            m, d, y = parse_date_tokens(trailing)
            month = month or m
            day = day or d
            year = year or y
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="I",
            part="B",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="Other",
        )
        return entry, idx

    def _parse_schedule_i_part_c(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name..." line
        if idx < len(self.lines) and clean(self.lines[idx]) == "MO DAY YEAR":
            idx += 1

        name, idx = collect_name(
            self.lines, idx, ("$", "Mailing Address")
        )

        month = day = year = ""
        amount_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        amount = parse_amount(amount_line)
        if amount:
            idx += 1

        if idx < len(self.lines) and clean(self.lines[idx]).startswith("Mailing Address"):
            idx += 1

        month = day = year = ""
        date_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", date_line):
            month, day, year = date_line.split()
            idx += 1

        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        city, _, _, _, _, trailing = split_city_state_zip(city_line)
        if not (month and day and year):
            m, d, y = parse_date_tokens(trailing)
            month = month or m
            day = day or d
            year = year or y
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="I",
            part="C",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="Political Committee",
        )
        return entry, idx

    def _parse_schedule_i_part_d(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name..." line
        date_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        month, day, year = parse_date_tokens(date_line)
        amount = parse_amount(date_line)
        idx += 1

        name, idx = collect_name(
            self.lines, idx, ("Mailing Address",)
        )

        if idx < len(self.lines) and clean(self.lines[idx]).startswith("Mailing Address"):
            idx += 1

        date_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", date_line):
            month = month or date_line.split()[0]
            day = day or date_line.split()[1]
            year = year or date_line.split()[2]
            idx += 1

        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        city, _, _, _, _, _ = split_city_state_zip(city_line)
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        employer_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        employer_name, occupation = parse_employer_line(employer_line)
        idx += 1

        mailing_label_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if mailing_label_line.startswith("Employer Mailing Address"):
            idx += 1
        employer_address_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        employer_city, employer_state, employer_zip, _ = parse_employer_address_line(
            employer_address_line
        )
        idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="I",
            part="D",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="Other",
            employer_name=employer_name,
            occupation=occupation,
            employer_city=employer_city,
            employer_state=employer_state,
            employer_zip=employer_zip,
        )
        return entry, idx

    def _parse_schedule_i_part_e(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name" line
        amount = ""
        if idx < len(self.lines):
            prelim = clean(self.lines[idx])
            if prelim.startswith("MO DAY YEAR"):
                amount = parse_amount(prelim)
                idx += 1

        name, idx = collect_name(
            self.lines, idx, ("Mailing Address",)
        )

        if idx < len(self.lines) and clean(self.lines[idx]).startswith("Mailing Address"):
            idx += 1

        date_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        month = day = year = ""
        if re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", date_line):
            month, day, year = date_line.split()
            idx += 1

        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        city, _, _, _, _, _ = split_city_state_zip(city_line)
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        description_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        description = ""
        if description_line.startswith("Receipt Description"):
            description = description_line.split("Receipt Description", 1)[1].strip()
            idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="I",
            part="E",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="Receipt",
            description=description,
        )
        return entry, idx

    def _parse_schedule_ii_part_g(self) -> Tuple[ContributionEntry, int]:
        idx = self.idx
        idx += 1  # skip "Full Name..." line
        if idx < len(self.lines) and clean(self.lines[idx]) == "MO DAY YEAR":
            idx += 1

        name, idx = collect_name(
            self.lines, idx, ("$", "Mailing Address")
        )

        month = day = year = ""
        amount_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        amount = parse_amount(amount_line)
        if amount:
            idx += 1

        if idx < len(self.lines) and clean(self.lines[idx]).startswith("Mailing Address"):
            mail_line = clean(self.lines[idx])
            m, d, y = parse_date_tokens(mail_line)
            if m:
                month = m
                day = d
                year = y
            idx += 1

        date_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if re.fullmatch(r"\d{1,2}\s+\d{1,2}\s+\d{4}", date_line):
            month, day, year = date_line.split()
            idx += 1

        city_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        city, _, _, _, _, _ = split_city_state_zip(city_line)
        idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code = parse_state_zip_line(state_line)
        idx += 1

        employer_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        employer_name, occupation = parse_employer_line(employer_line)
        idx += 1

        mailing_label_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        if mailing_label_line.startswith("Employer Mailing Address"):
            idx += 1

        value_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        employer_city, employer_state, employer_zip, description = parse_employer_address_line(
            value_line
        )
        idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ContributionEntry(
            schedule="II",
            part="G",
            page=self.page,
            name=name,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            contributor_type="In-Kind",
            employer_name=employer_name,
            occupation=occupation,
            employer_city=employer_city,
            employer_state=employer_state,
            employer_zip=employer_zip,
            description=description,
        )
        return entry, idx

    def _parse_schedule_iii(self) -> Tuple[ExpenditureEntry, int]:
        idx = self.idx
        idx += 1  # skip "To Whom Paid"
        if idx < len(self.lines) and clean(self.lines[idx]) == "MO DAY YEAR":
            idx += 1

        payee, idx = collect_name(
            self.lines, idx, ("Mailing Address",)
        )

        mailing_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        month, day, year = parse_date_tokens(mailing_line)
        amount = parse_amount(mailing_line)
        idx += 1

        city = ""
        if idx < len(self.lines):
            city_info_line = clean(self.lines[idx])
            if city_info_line.startswith("City"):
                city, _, _, _, _, _ = split_city_state_zip(city_info_line)
                idx += 1

        state_line = clean(self.lines[idx]) if idx < len(self.lines) else ""
        state, zip_code, description = parse_state_zip_description_line(state_line)
        idx += 1

        description = description.strip()
        while idx < len(self.lines):
            continuation = clean(self.lines[idx])
            if not continuation:
                idx += 1
                continue
            if continuation.startswith(
                (
                    "To Whom Paid",
                    "MO DAY YEAR",
                    "Mailing Address",
                    "City ",
                )
            ):
                break
            if continuation.startswith("--- Page") or continuation.startswith("PAGE "):
                break
            if PAGE_TIMESTAMP_RE.fullmatch(continuation):
                break
            description = f"{description} {continuation}".strip() if description else continuation
            idx += 1

        idx = self._skip_blank_lines(idx)

        entry = ExpenditureEntry(
            schedule="III",
            page=self.page,
            payee=payee,
            amount=amount,
            month=month,
            day=day,
            year=year,
            city=city,
            state=state,
            zip_code=zip_code,
            description=description,
        )
        return entry, idx

    def _skip_blank_lines(self, idx: int) -> int:
        while idx < len(self.lines) and is_blank(self.lines[idx]):
            idx += 1
        return idx


def write_csv(
    filename: Path,
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    *,
    remove_fields: Tuple[str, ...] | None = None,
) -> None:
    if not rows:
        return
    if remove_fields:
        fieldnames = [f for f in fieldnames if f not in remove_fields]
        rows = [{k: v for k, v in row.items() if k not in remove_fields} for row in rows]
    filename.parent.mkdir(parents=True, exist_ok=True)
    with filename.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert extracted PDF text (document.txt) into structured CSV files."
    )
    parser.add_argument(
        "--document",
        type=Path,
        default=DEFAULT_DOCUMENT_PATH,
        help="Path to the extracted document.txt (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where CSV files will be written (default: %(default)s).",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    arg_parser = build_parser()
    args = arg_parser.parse_args(argv)

    document_path: Path = args.document
    output_dir: Path = args.output

    if not document_path.exists():
        arg_parser.error(f"Document not found: {document_path}")

    lines = document_path.read_text(encoding="utf-8").splitlines()
    doc_parser = DocumentParser(lines)
    doc_parser.parse()

    contribution_fields = list(
        asdict(ContributionEntry(schedule="I", part="A", page=0, name="")).keys()
    )
    expenditure_fields = list(
        asdict(ExpenditureEntry(schedule="III", page=0, payee="")).keys()
    )

    # Schedule I outputs
    write_csv(
        output_dir / "schedule_I_part_A.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_i_part_a"]],
        remove_fields=("contributor_type",),
    )
    write_csv(
        output_dir / "schedule_I_part_B.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_i_part_b"]],
        remove_fields=("contributor_type",),
    )
    write_csv(
        output_dir / "schedule_I_part_C.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_i_part_c"]],
        remove_fields=("contributor_type",),
    )
    write_csv(
        output_dir / "schedule_I_part_D.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_i_part_d"]],
        remove_fields=("contributor_type",),
    )
    write_csv(
        output_dir / "schedule_I_part_E.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_i_part_e"]],
        remove_fields=("contributor_type",),
    )

    write_csv(
        output_dir / "schedule_II_part_G.csv",
        [f for f in contribution_fields if f != "contributor_type"],
        [asdict(row) for row in doc_parser.results["schedule_ii_part_g"]],
        remove_fields=("contributor_type",),
    )

    write_csv(
        output_dir / "schedule_III.csv",
        expenditure_fields,
        [asdict(row) for row in doc_parser.results["schedule_iii"]],
    )

    print(f"CSV files written to {output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
