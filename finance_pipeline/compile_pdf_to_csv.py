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
    occupation: str = ""
    employer_name: str = ""
    employer_city: str = ""
    employer_state: str = ""
    employer_zip: str = ""
    employer_description: str = ""
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
    def __init__(self, lines: List[str]):
        self.lines = lines
        self.index = 0
        self.results: Dict[str, List] = {
            "schedule_i_part_a": [],
            "schedule_i_part_b": [],
            "schedule_i_part_c": [],
            "schedule_i_part_d": [],
            "schedule_i_part_e": [],
            "schedule_ii_part_g": [],
            "schedule_iii": [],
        }
        self.current_page = 0

    def parse(self) -> None:
        while self.index < len(self.lines):
            line = clean(self.lines[self.index])
            if not line:
                self.index += 1
                continue

            page_match = re.match(r"--- Page (\d+) ---", line)
            if page_match:
                self.current_page = int(page_match.group(1))
                self.index += 1
                continue

            if line.startswith("Schedule I Part A"):
                self.index = self._parse_schedule_i_part_a(self.index + 1)
            elif line.startswith("Schedule I Part B"):
                self.index = self._parse_schedule_i_part_b(self.index + 1)
            elif line.startswith("Schedule I Part C"):
                self.index = self._parse_schedule_i_part_c(self.index + 1)
            elif line.startswith("Schedule I Part D"):
                self.index = self._parse_schedule_i_part_d(self.index + 1)
            elif line.startswith("Schedule I Part E"):
                self.index = self._parse_schedule_i_part_e(self.index + 1)
            elif line.startswith("Schedule II Part G"):
                self.index = self._parse_schedule_ii_part_g(self.index + 1)
            elif line.startswith("Schedule III"):
                self.index = self._parse_schedule_iii(self.index + 1)
            else:
                self.index += 1

    def _parse_schedule_i_part_a(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="I", part="A")
            if entry:
                self.results["schedule_i_part_a"].append(entry)
        return idx

    def _parse_schedule_i_part_b(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="I", part="B")
            if entry:
                self.results["schedule_i_part_b"].append(entry)
        return idx

    def _parse_schedule_i_part_c(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="I", part="C")
            if entry:
                self.results["schedule_i_part_c"].append(entry)
        return idx

    def _parse_schedule_i_part_d(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="I", part="D")
            if entry:
                self.results["schedule_i_part_d"].append(entry)
        return idx

    def _parse_schedule_i_part_e(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="I", part="E")
            if entry:
                self.results["schedule_i_part_e"].append(entry)
        return idx

    def _parse_schedule_ii_part_g(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_i_entry(idx, schedule="II", part="G")
            if entry:
                self.results["schedule_ii_part_g"].append(entry)
        return idx

    def _parse_schedule_iii(self, idx: int) -> int:
        while idx < len(self.lines):
            idx = self._skip_blank_lines(idx)
            if idx >= len(self.lines):
                break
            line = clean(self.lines[idx])
            if not line or line.startswith("Schedule"):
                break
            entry, idx = self._parse_schedule_iii_entry(idx)
            if entry:
                self.results["schedule_iii"].append(entry)
        return idx

    def _parse_schedule_i_entry(
        self,
        idx: int,
        *,
        schedule: str,
        part: str,
    ) -> Tuple[ContributionEntry | None, int]:
        name, idx = collect_name(
            self.lines,
            idx,
            (
                "Address",
                "City",
                "Employer Name",
                "Occupation",
                "Date",
                "Contribution Type",
                "Amount",
            ),
        )
        name = clean(name)
        if not name:
            return None, idx

        entry = ContributionEntry(
            schedule=schedule,
            part=part,
            page=self.current_page,
            name=name,
        )

        city = ""
        state = ""
        zip_code = ""
        occupation = ""
        employer_name = ""
        employer_city = ""
        employer_state = ""
        employer_zip = ""
        employer_description = ""
        description = ""

        while idx < len(self.lines):
            line = clean(self.lines[idx])
            if not line:
                idx += 1
                continue
            if line.startswith("Schedule"):
                break
            if line.startswith("Amount"):
                entry.amount = parse_amount(line)
                idx += 1
                continue
            if line.startswith("Date"):
                month, day, year = parse_date_tokens(line)
                entry.month = month
                entry.day = day
                entry.year = year
                idx += 1
                continue
            if line.startswith("City"):
                city, state_label, state_value, zip_label, zip_value, trailing = split_city_state_zip(
                    line
                )
                entry.city = city
                entry.state = state_value
                entry.zip_code = zip_value
                if trailing:
                    description = trailing
                idx += 1
                continue
            if line.startswith("State"):
                entry.state, entry.zip_code = parse_state_zip_line(line)
                idx += 1
                continue
            if line.startswith("Zip Code"):
                _, entry.zip_code = parse_state_zip_line(line)
                idx += 1
                continue
            if line.startswith("Country"):
                idx += 1
                continue
            if line.startswith("Employer Name"):
                employer_name, occupation = parse_employer_line(line)
                entry.employer_name = employer_name
                entry.occupation = occupation
                idx += 1
                continue
            if line.startswith("Employer Address"):
                employer_city, employer_state, employer_zip, employer_description = parse_employer_address_line(
                    line
                )
                entry.employer_city = employer_city
                entry.employer_state = employer_state
                entry.employer_zip = employer_zip
                entry.employer_description = employer_description
                idx += 1
                continue
            if line.startswith("Contribution Type"):
                entry.contributor_type = clean(line[len("Contribution Type") :])
                idx += 1
                continue
            if line.startswith("Occupation"):
                occupation = clean(line[len("Occupation") :])
                entry.occupation = occupation
                idx += 1
                continue
            if line.startswith("Contribution Description"):
                description = clean(line[len("Contribution Description") :])
                idx += 1
                continue
            if line.startswith("Contribution Received Date"):
                month, day, year = parse_date_tokens(line)
                entry.month = month
                entry.day = day
                entry.year = year
                idx += 1
                continue
            if line.startswith("Contribution Amount"):
                entry.amount = parse_amount(line)
                idx += 1
                continue
            if line.startswith("Contribution Date"):
                month, day, year = parse_date_tokens(line)
                entry.month = month
                entry.day = day
                entry.year = year
                idx += 1
                continue
            if line.startswith("Address"):
                idx += 1
                continue
            if line.startswith("Type"):
                entry.contributor_type = clean(line[len("Type") :])
                idx += 1
                continue
            if line.startswith("Receipt"):
                idx += 1
                continue
            if line.startswith("Contribution Nature"):
                entry.description = clean(line[len("Contribution Nature") :])
                idx += 1
                continue
            if line.startswith("Contribution Purpose"):
                entry.description = clean(line[len("Contribution Purpose") :])
                idx += 1
                continue
            if line.startswith("Contribution Period"):
                idx += 1
                continue
            if line.startswith("Acknowledgement Number"):
                idx += 1
                continue
            if line.startswith("State Committee ID"):
                idx += 1
                continue
            if line.startswith("Payment Type"):
                idx += 1
                continue
            if line.startswith("City and State"):
                idx += 1
                continue
            if line.startswith("Employer"):
                idx += 1
                continue
            if line.startswith("Date Received"):
                month, day, year = parse_date_tokens(line)
                entry.month = month
                entry.day = day
                entry.year = year
                idx += 1
                continue
            if line.startswith("Amount Total"):
                entry.amount = parse_amount(line)
                idx += 1
                continue
            if line.startswith("Year To Date"):
                idx += 1
                continue
            if line.startswith("Occupation & Employer"):
                idx += 1
                continue
            if line.startswith("Occupation/Employer"):
                idx += 1
                continue
            if line.startswith("Other Type"):
                entry.contributor_type = clean(line[len("Other Type") :])
                idx += 1
                continue
            if line.startswith("Employer City"):
                entry.employer_city = clean(line[len("Employer City") :])
                idx += 1
                continue
            if line.startswith("Employer State"):
                entry.employer_state = clean(line[len("Employer State") :])
                idx += 1
                continue
            if line.startswith("Employer Zip"):
                entry.employer_zip = clean(line[len("Employer Zip") :])
                idx += 1
                continue
            if line.startswith("Employer Description"):
                entry.employer_description = clean(line[len("Employer Description") :])
                idx += 1
                continue
            if line.startswith("Employer City State Zip"):
                entry.employer_city, entry.employer_state, entry.employer_zip = parse_state_zip_description_line(line)
                idx += 1
                continue
            if line.startswith("City State Zip"):
                entry.city, entry.state, entry.zip_code = parse_state_zip_description_line(line)
                idx += 1
                continue
            if line.startswith("Zip"):
                _, entry.zip_code = parse_state_zip_line(line)
                idx += 1
                continue
            if line.startswith("Employer Address Line"):
                idx += 1
                continue
            if line.startswith("Occupation Description"):
                entry.description = clean(line[len("Occupation Description") :])
                idx += 1
                continue
            if line.startswith("Occupation Description Continued"):
                extra = clean(line[len("Occupation Description Continued") :])
                if entry.description:
                    entry.description = f"{entry.description} {extra}".strip()
                else:
                    entry.description = extra
                idx += 1
                continue
            if line.startswith("City State"):
                idx += 1
                continue
            if line.startswith("Account"):
                idx += 1
                continue
            if line.startswith("Zip Code"):
                idx += 1
                continue
            if line.startswith("City/State/Zip"):
                idx += 1
                continue
            if line.startswith("Occupation and Employer"):
                idx += 1
                continue
            if line.startswith("Occupation/Employer and Mailing Address"):
                idx += 1
                continue
            if line.startswith("Occupation and Mailing Address"):
                idx += 1
                continue

            idx += 1

        entry.occupation = entry.occupation or occupation
        entry.employer_name = entry.employer_name or employer_name
        entry.employer_city = entry.employer_city or employer_city
        entry.employer_state = entry.employer_state or employer_state
        entry.employer_zip = entry.employer_zip or employer_zip
        entry.employer_description = entry.employer_description or employer_description
        entry.description = entry.description or description

        return entry, idx

    def _parse_schedule_iii_entry(self, idx: int) -> Tuple[ExpenditureEntry | None, int]:
        name, idx = collect_name(
            self.lines,
            idx,
            (
                "Address",
                "Amount",
                "Date",
                "City",
                "State",
                "Zip Code",
                "Purpose",
                "Description",
            ),
        )
        name = clean(name)
        if not name:
            return None, idx

        entry = ExpenditureEntry(
            schedule="III",
            page=self.current_page,
            payee=name,
        )

        while idx < len(self.lines):
            line = clean(self.lines[idx])
            if not line:
                idx += 1
                continue
            if line.startswith("Schedule"):
                break
            if line.startswith("Amount"):
                entry.amount = parse_amount(line)
                idx += 1
                continue
            if line.startswith("Date"):
                month, day, year = parse_date_tokens(line)
                entry.month = month
                entry.day = day
                entry.year = year
                idx += 1
                continue
            if line.startswith("City"):
                entry.city, _, entry.state, _, entry.zip_code, _ = split_city_state_zip(line)
                idx += 1
                continue
            if line.startswith("State"):
                entry.state, entry.zip_code = parse_state_zip_line(line)
                idx += 1
                continue
            if line.startswith("Zip Code"):
                _, entry.zip_code = parse_state_zip_line(line)
                idx += 1
                continue
            if line.startswith("Purpose"):
                entry.description = clean(line[len("Purpose") :])
                idx += 1
                continue
            if line.startswith("Description"):
                description = clean(line[len("Description") :])
                if entry.description:
                    entry.description = f"{entry.description} {description}".strip()
                else:
                    entry.description = description
                idx += 1
                continue

            idx += 1

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
