import argparse
from collections import defaultdict
from pathlib import Path
import re
from typing import List

import pandas as pd
import pdfplumber

SCHEDULE_META = {
    "A": {"sheet": "Schedule A", "description": "Assets and Unearned Income"},
    "B": {"sheet": "Schedule B", "description": "Transactions"},
    "C": {"sheet": "Schedule C", "description": "Earned Income"},
    "D": {"sheet": "Schedule D", "description": "Liabilities"},
    "E": {"sheet": "Schedule E", "description": "Positions"},
    "F": {"sheet": "Schedule F", "description": "Agreements"},
    "G": {"sheet": "Schedule G", "description": "Gifts"},
    "H": {"sheet": "Schedule H", "description": "Travel Payments & Reimbursements"},
    "I": {"sheet": "Schedule I", "description": "Payments to Charity in Lieu of Honoraria"},
}

SCHEDULE_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]

SCHEDULE_COLUMNS = {
    "A": ["Asset", "Owner", "Value of Asset", "Income Type(s)", "Income", "Tx. > $1,000?"],
    "B": ["Asset", "Owner", "Date", "Tx. Type", "Amount", "Cap. Gains > $200?"],
    "C": ["Source", "Type", "Amount"],
    "D": ["Owner", "Creditor", "Date Incurred", "Type", "Amount of Liability"],
    "E": ["Position", "Name of Organization"],
    "F": ["Date", "Parties To", "Terms of Agreement"],
    "G": ["Source", "Description", "Value"],
    "H": ["Date(s)", "City and State", "Nature of Event", "Item(s) Provided/Expenses Paid", "Provided By"],
    "I": ["Date(s)", "Payee", "Amount"],
}

VALUE_TOKEN_PATTERN = re.compile(
    r"(Less than\s*\$[\d,]+|More than\s*\$[\d,]+|\$[\d,]+(?:\.\d+)?|None)",
    re.IGNORECASE,
)

COLUMN_NAMES_A = SCHEDULE_COLUMNS["A"]
COLUMN_NAMES_B = SCHEDULE_COLUMNS["B"]
COLUMN_NAMES_C = SCHEDULE_COLUMNS["C"]
COLUMN_NAMES_D = SCHEDULE_COLUMNS["D"]
COLUMN_NAMES_E = SCHEDULE_COLUMNS["E"]
COLUMN_NAMES_F = SCHEDULE_COLUMNS["F"]
HEADER_TOLERANCE = 3.0
ROW_TOLERANCE = 12.0
ROW_TOLERANCE_C = 6.0
ROW_TOLERANCE_D = 6.0
ROW_TOLERANCE_E = 6.0
ROW_TOLERANCE_F = 6.0

OWNER_CODES = {
    'SP', 'SELF', 'JT', 'DC', 'CH', 'SC', 'SC/DC', 'SC/CH', 'SELF/SP',
    'SELF/CH', 'SELF/SC', 'SELF/DC', 'FAM', 'TRUST', 'N/A', 'U/A',
    'UGMA', 'KIDS', 'SPOUSE', 'SELF/SPOUSE', 'SON', 'DAUGHTER', 'CHILD',
    'SELFANDCHILD', 'SELFANDSPOUSE', 'SPOUSEANDCHILD', 'SELF/SPOUSE/CHILD',
    'SELF&CHILD', 'SELF&SPOUSE', 'SELF/CH', 'SELF/SP', 'SELF/SC', 'SELF/CHILD'
}
OWNER_ALIASES = {
    'Self', 'Spouse', 'Child', 'Son', 'Daughter', 'Self/Spouse',
    'Self/Child', 'Spouse/Child', 'Self & Spouse', 'Self & Child',
    'Self/Spouse/Child', 'Trust', 'Custodian', 'Grandchild', 'Dependent',
    'Guardian'
}

INCOME_KEYWORDS = (
    'tax', 'defer', 'dividend', 'interest', 'income', 'gains', 'royalties',
    'rent', 'capital', 'trust', 'annu', 'partnership', 'salary', 'loss'
)

SCHEDULE_A_HEADER_TOKENS = {
    'asset',
    'owner',
    'value',
    'value of asset',
    'income type(s)',
    'income',
    'tx.',
    'tx',
    'tx. > $1,000?',
    '$1,000?',
    'type(s)',
    '>',
}
SCHEDULE_A_DESCRIPTION_PREFIXES = (
    'd :',
    'd:',
    'l :',
    'l:',
    'location :',
    'location:',
    'description :',
    'description:',
    'details :',
    'details:',
    'issuer :',
    'issuer:',
)
SCHEDULE_A_OPTION_KEYWORDS = (
    'option',
    'options',
    'call option',
    'put option',
    'strike price',
    'expiration date',
    'call options',
    'put options',
)

SCHEDULE_B_DESCRIPTION_PREFIXES = (
    "d :",
    "d:",
    "c :",
    "c:",
    "l :",
    "l:",
    "location :",
    "location:",
    "description :",
    "description:",
    "details :",
    "details:",
    "issuer :",
    "issuer:",
)

DATE_TOKEN_PATTERN = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")
ALT_DATE_TOKEN_PATTERN = re.compile(r"\b\d{4}-\d{1,2}-\d{1,2}\b")
TX_TYPE_KEYWORDS = {
    "purchase", "purchased", "sale", "sold", "sell", "buy", "bought",
    "acquisition", "dispose", "disposition", "exchange", "transfer",
    "gift", "exercise", "exercised", "redeem", "redemption",
    "contribution", "distribution", "withdrawal", "deposit", "vest",
    "vesting", "assignment", "conversion", "write", "option", "grant", "award"
}
TX_TYPE_SHORT_TOKENS = {"p", "s", "e", "x", "g", "sp", "sd", "ex"}
TRANSACTION_AMOUNT_PATTERN = re.compile(r"\$[\d,]+(?:\s*(?:-|to)\s*\$[\d,]+)?", re.IGNORECASE)
SCHEDULE_B_DESCRIPTION_KEYWORDS = {
    "share", "shares", "unit", "units", "option", "options", "call", "put",
    "strike", "exercise", "exercised", "exercising", "expiration",
    "maturity", "price", "vesting", "vest", "vested", "grant", "granted",
    "distribution", "acquiring", "managing", "investment", "hotel",
    "property", "fund", "loan"
}
SCHEDULE_B_HEADER_TOKENS = {
    "type",
    "tx. type",
    "tx type",
    "amount",
    "cap. gains > $200?",
    "cap gains > $200?",
    "gains > $200?",
    "type gains > $200?"
}
SCHEDULE_B_ASSET_NOISE_PATTERN = re.compile(r"^[\s\W\d]+$")

RANGE_CONNECTOR_TOKENS = {"-", "\u2013", "\u2014"}
RANGE_WORD_TOKENS = {"over", "under", "between"}
RANGE_PHRASE_PREFIXES = (
    "at least",
    "at most",
    "less than",
    "more than",
    "greater than",
)
VALUE_NON_NUMERIC_KEYWORDS = {"none", "n/a", "unknown", "variable", "varies", "var."}
INCOME_AMOUNT_KEYWORDS = VALUE_NON_NUMERIC_KEYWORDS | {"not applicable"}


def is_range_indicator(text: str) -> bool:
    cleaned = text.strip().lower() if text else ""

    if not cleaned:
        return False

    if cleaned in RANGE_CONNECTOR_TOKENS:
        return True

    if cleaned in RANGE_WORD_TOKENS:
        return True

    if cleaned.endswith("+"):
        return True

    for token in RANGE_WORD_TOKENS:
        if cleaned.startswith(f"{token} "):
            return True

    for prefix in RANGE_PHRASE_PREFIXES:
        if cleaned.startswith(prefix):
            return True

    return False


def is_income_amount_token(text: str) -> bool:
    if not text:
        return False

    lowered = text.strip().lower()

    if lowered in INCOME_AMOUNT_KEYWORDS:
        return True

    if is_value_token(text):
        return True

    return False


def is_owner_token(text: str) -> bool:
    normalized = text.replace('/', '').replace('-', '').replace('&', '').replace(' ', '')
    upper = text.upper()
    if upper in OWNER_CODES or normalized.upper() in OWNER_CODES:
        return True
    if text.strip() in OWNER_ALIASES:
        return True
    return False

def is_value_token(text: str) -> bool:
    if not text:
        return False

    cleaned = text.strip()
    lowered = cleaned.lower()

    if '$' in cleaned or '%' in cleaned:
        return True

    if any(char.isdigit() for char in cleaned):
        return True

    if lowered in VALUE_NON_NUMERIC_KEYWORDS:
        return True

    if 'less than' in lowered or 'more than' in lowered or 'greater than' in lowered:
        return True

    if is_range_indicator(cleaned):
        return True

    return False

def is_income_type_token(text: str) -> bool:
    lowered = text.lower()
    return any(key in lowered for key in INCOME_KEYWORDS)


def clean_cell(cell: str) -> str:
    if cell is None:
        return ""
    text = str(cell)
    text = text.replace("\x00", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text.strip())
    return text


def normalise_word_text(text: str) -> str:
    return text.replace("\x00", " ").strip()


def is_schedule_a_description_fragment(text: str) -> bool:
    if not text:
        return False
    normalized = clean_cell(text).lower()
    for prefix in SCHEDULE_A_DESCRIPTION_PREFIXES:
        if normalized.startswith(prefix):
            return True
    return False


def insert_into_placeholder(text: str, value: str, placeholder: str) -> tuple[str, bool]:
    formatted = value.strip()
    if not formatted:
        return text, False

    pattern = re.compile(
        rf"({re.escape(placeholder)})(\s+)(?P<follow>and\b|an\b|with\b|[,.;]|$)",
        re.IGNORECASE,
    )

    def repl(match: re.Match[str]) -> str:
        follow = match.group('follow')
        prefix = match.group(1)
        if follow == '':
            return f"{prefix} {formatted}"
        if follow in {',', ';', '.'}:
            return f"{prefix} {formatted}{follow}"
        return f"{prefix} {formatted} {follow}"

    new_text, count = pattern.subn(repl, text, count=1)
    if count:
        return new_text, True

    stripped = text.rstrip()
    if stripped.lower().endswith(placeholder):
        suffix_spaces = text[len(stripped):]
        return f"{stripped} {formatted}{suffix_spaces}", True

    return text, False


def is_schedule_b_description_fragment(text: str) -> bool:
    if not text:
        return False
    lowered = clean_cell(text).lower()
    for prefix in SCHEDULE_B_DESCRIPTION_PREFIXES:
        if lowered.startswith(prefix):
            return True
    return False


def is_transaction_amount_token(text: str) -> bool:
    if not text:
        return False

    cleaned = text.strip()
    lowered = cleaned.lower()

    if not cleaned:
        return False

    if lowered in VALUE_NON_NUMERIC_KEYWORDS:
        return True

    if 'less than' in lowered or 'more than' in lowered or 'greater than' in lowered:
        return True

    if lowered.startswith('over ') or lowered.startswith('under ') or lowered.startswith('approximately ') or lowered.startswith('approx '):
        if '$' in cleaned:
            return True

    if ' between ' in lowered and '$' in cleaned:
        return True

    if cleaned in {'-', '\u2013', '\u2014'}:
        return True

    if TRANSACTION_AMOUNT_PATTERN.search(cleaned):
        return True

    return False


def is_tx_type_token(text: str) -> bool:
    if not text:
        return False

    lowered = text.strip().lower()

    if lowered in TX_TYPE_SHORT_TOKENS:
        return True

    for keyword in TX_TYPE_KEYWORDS:
        if keyword in lowered:
            return True

    return False


def is_date_token(text: str) -> bool:
    if not text:
        return False

    cleaned = text.strip().strip(',.;')
    if not cleaned:
        return False

    if DATE_TOKEN_PATTERN.fullmatch(cleaned):
        return True

    if ALT_DATE_TOKEN_PATTERN.fullmatch(cleaned):
        return True

    return False


def is_capital_gain_flag(text: str) -> bool:
    if not text:
        return False

    lowered = text.strip().lower()
    return lowered in {'yes', 'no', 'n/a', 'na', 'none'}


def is_noise_schedule_b_row(row: List[str]) -> bool:
    if not row:
        return False

    asset_text = clean_cell(row[0])
    if not asset_text:
        return False

    if SCHEDULE_B_ASSET_NOISE_PATTERN.match(asset_text):
        return True

    collapsed = re.sub(r"\s+", "", asset_text)
    if not any(ch.isalpha() for ch in collapsed):
        return True

    if not re.search(r"[A-Za-z]{3,}", asset_text):
        return True

    return False


# --- Schedule A helpers ----------------------------------------------------
def is_schedule_a_description_fragment(text: str) -> bool:
    if not text:
        return False
    normalized = clean_cell(text).lower()
    for prefix in SCHEDULE_A_DESCRIPTION_PREFIXES:
        if normalized.startswith(prefix):
            return True
    return False


def insert_into_placeholder(text: str, value: str, placeholder: str) -> tuple[str, bool]:
    formatted = value.strip()
    if not formatted:
        return text, False
    pattern = re.compile(rf"({re.escape(placeholder)})(\s+)(?P<follow>and\b|an\b|with\b|[,.;]|$)", re.IGNORECASE)

    def repl(match: re.Match[str]) -> str:
        follow = match.group('follow')
        prefix = match.group(1)
        if follow == '':
            return f"{prefix} {formatted}"
        if follow in {',', ';', '.'}:
            return f"{prefix} {formatted}{follow}"
        return f"{prefix} {formatted} {follow}"

    new_text, count = pattern.subn(repl, text, count=1)
    if count:
        return new_text, True

    stripped = text.rstrip()
    if stripped.lower().endswith(placeholder):
        suffix_spaces = text[len(stripped):]
        return f"{stripped} {formatted}{suffix_spaces}", True

    return text, False


def is_schedule_b_description_fragment(text: str) -> bool:
    if not text:
        return False
    lowered = clean_cell(text).lower()
    for prefix in SCHEDULE_B_DESCRIPTION_PREFIXES:
        if lowered.startswith(prefix):
            return True
    return False


# --- Schedule A helpers ----------------------------------------------------

def find_schedule_a_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w["top"]):
        text = normalise_word_text(word["text"]).lower()
        if text == "asset":
            top = word["top"]
            has_owner = any(
                abs(other["top"] - top) < HEADER_TOLERANCE
                and normalise_word_text(other["text"]).lower() == "owner"
                for other in words
            )
            if has_owner:
                header_words = [
                    w for w in words if abs(w["top"] - top) < HEADER_TOLERANCE
                ]
                asset_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "asset"
                )
                owner_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "owner"
                )
                value_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "value"
                )
                income_words = sorted(
                    (
                        w
                        for w in header_words
                        if normalise_word_text(w["text"]).lower().startswith("income")
                    ),
                    key=lambda w: w["x0"],
                )
                if not income_words:
                    return None
                income_type_start = income_words[0]["x0"]
                income_start = (
                    income_words[1]["x0"]
                    if len(income_words) > 1
                    else income_type_start + 80
                )
                tx_candidates = [
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower().startswith("tx")
                ]
                if not tx_candidates:
                    return None
                tx_start = min(tx_candidates)
                column_starts = [
                    asset_start,
                    owner_start,
                    value_start,
                    income_type_start,
                    income_start,
                    tx_start,
                ]
                return top, column_starts
    return None


def assign_schedule_a_column(x0: float, column_starts: List[float]) -> str:
    asset_limit = column_starts[1] - 15
    owner_limit = column_starts[2] - 5
    value_limit = column_starts[3] - 5
    income_type_limit = column_starts[4] - 5
    income_limit = column_starts[5] - 5

    if x0 < asset_limit:
        return COLUMN_NAMES_A[0]
    if x0 < owner_limit:
        return COLUMN_NAMES_A[1]
    if x0 < value_limit:
        return COLUMN_NAMES_A[2]
    if x0 < income_type_limit:
        return COLUMN_NAMES_A[3]
    if x0 < income_limit:
        return COLUMN_NAMES_A[4]
    return COLUMN_NAMES_A[5]


def parse_schedule_a_page(words, column_starts, header_top):
    b_markers = [
        w["top"]
        for w in words
        if "B:" in normalise_word_text(w["text"])
    ]
    cutoff_top = min(b_markers) if b_markers else None

    data_words = [w for w in words if w["top"] > header_top + 5]
    data_words.sort(key=lambda w: (w["top"], w["x0"]))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_A}
    current_top = None

    for word in data_words:
        top = word["top"]
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word["text"])
        if not text:
            continue
        lower_text = text.lower()
        if text.startswith("*"):
            break
        if lower_text.startswith("asset class details"):
            break
        if lower_text.startswith("for the complete"):
            break
        if lower_text.startswith("https://"):
            continue

        column_name = assign_schedule_a_column(word["x0"], column_starts)
        if column_name == COLUMN_NAMES_A[1] and not is_owner_token(text):
            column_name = COLUMN_NAMES_A[0]
        elif column_name == COLUMN_NAMES_A[2] and not is_value_token(text):
            column_name = COLUMN_NAMES_A[0]
        elif column_name == COLUMN_NAMES_A[3] and not is_income_type_token(text):
            column_name = COLUMN_NAMES_A[0]
        elif column_name == COLUMN_NAMES_A[4] and not (is_income_amount_token(text) or is_income_type_token(text)):
            column_name = COLUMN_NAMES_A[0]
        elif column_name == COLUMN_NAMES_A[5] and text.strip().lower() not in {'yes', 'no'}:
            column_name = COLUMN_NAMES_A[0]
        if current_top is None or abs(top - current_top) > ROW_TOLERANCE:
            if any(current[col] for col in COLUMN_NAMES_A):
                rows.append([
                    clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_A
                ])
            current = {name: [] for name in COLUMN_NAMES_A}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_A):
        rows.append([
            clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_A
        ])

    merged_rows = []
    for row in rows:
        if merged_rows and row[0] and not any(row[1:]) and re.match(r'^([A-Z])\s*:', row[0]):
            merged_rows[-1][0] = f"{merged_rows[-1][0]}\n{row[0]}"
        else:
            merged_rows.append(row)

    rows = [row for row in merged_rows if any(row)]
    return rows, bool(cutoff_top)
















def is_schedule_b_description_fragment(text: str) -> bool:
    if not text:
        return False
    lowered = clean_cell(text).lower()
    for prefix in SCHEDULE_B_DESCRIPTION_PREFIXES:
        if lowered.startswith(prefix):
            return True
    return False


def is_transaction_amount_token(text: str) -> bool:
    if not text:
        return False

    cleaned = text.strip()
    lowered = cleaned.lower()

    if not cleaned:
        return False

    if lowered in VALUE_NON_NUMERIC_KEYWORDS:
        return True

    if 'less than' in lowered or 'more than' in lowered or 'greater than' in lowered:
        return True

    if lowered.startswith('over ') or lowered.startswith('under ') or lowered.startswith('approximately ') or lowered.startswith('approx '):
        if '$' in cleaned:
            return True

    if ' between ' in lowered and '$' in cleaned:
        return True

    if cleaned in {'-', '\u2013', '\u2014'}:
        return True

    if TRANSACTION_AMOUNT_PATTERN.search(cleaned):
        return True

    return False


def is_tx_type_token(text: str) -> bool:
    if not text:
        return False

    lowered = text.strip().lower()

    if lowered in TX_TYPE_SHORT_TOKENS:
        return True

    for keyword in TX_TYPE_KEYWORDS:
        if keyword in lowered:
            return True

    return False

def tidy_schedule_a_rows(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    merged: List[List[str]] = []

    for raw_row in rows:
        row = list(raw_row[:6]) + ["" for _ in range(max(0, 6 - len(raw_row)))]
        asset, owner, value, income_type, income, tx_flag = row

        if merged and is_schedule_a_description_fragment(asset):
            current = merged[-1]
            current[0] = current[0] + ("\n" if current[0] else "") + asset
            if owner:
                current[1] = current[1] + ("\n" if current[1] else "") + owner
            if value:
                current[2] = current[2] + ("\n" if current[2] else "") + value
            if income_type:
                current[3] = current[3] + ("\n" if current[3] else "") + income_type
            if income:
                current[4] = current[4] + ("\n" if current[4] else "") + income
            if tx_flag:
                current[5] = current[5] + ("\n" if current[5] else "") + tx_flag
            continue

        if merged and not asset and not owner and (value or income_type or income or tx_flag):
            current = merged[-1]
            if value:
                current[2] = current[2] + ("\n" if current[2] else "") + value
            if income_type:
                current[3] = current[3] + ("\n" if current[3] else "") + income_type
            if income:
                current[4] = current[4] + ("\n" if current[4] else "") + income
            if tx_flag:
                current[5] = current[5] + ("\n" if current[5] else "") + tx_flag
            continue

        merged.append(row)

    for row in merged:
        asset = row[0] or ""
        asset = re.sub(r"(\[[A-Z]+\]) None\b", r"\1", asset)
        asset = asset.replace("None\nD :", "D :")
        value = row[2] or ""
        income_type = row[3] or ""
        income_value = row[4] or ""

        if value:
            parts = [part.strip() for part in re.split(r"[\n;]+", value) if part.strip()]
            primary = parts[0] if parts else ""
            extras = parts[1:] if len(parts) > 1 else []
            for extra in extras:
                cleaned_extra = extra.rstrip('.;,')
                inserted = False
                if cleaned_extra:
                    if is_date_token(cleaned_extra):
                        asset, inserted = insert_into_placeholder(asset, cleaned_extra, 'expiration date of')
                        if not inserted:
                            asset = f"{asset}\n{extra}".strip()
                        continue
                    if cleaned_extra.startswith('$') or is_value_token(cleaned_extra):
                        if any(keyword in asset.lower() for keyword in SCHEDULE_A_OPTION_KEYWORDS):
                            asset, inserted = insert_into_placeholder(asset, cleaned_extra, 'strike price of')
                            if not inserted:
                                asset = f"{asset}\n{extra}".strip()
                            continue
                asset = f"{asset}\n{extra}".strip()
            row[0] = asset.strip()
            row[2] = primary.strip()
        else:
            row[0] = asset.strip()

        asset_lower = row[0].lower()
        if income_value and (income_type.strip().lower() in {"", "none", "n/a", "not applicable", "unknown"}):
            if any(keyword in asset_lower for keyword in SCHEDULE_A_OPTION_KEYWORDS):
                updated_asset, inserted = insert_into_placeholder(row[0], income_value.strip(), 'strike price of')
                if inserted:
                    row[0] = updated_asset
                else:
                    row[0] = f"{row[0]}\n{income_value.strip()}".strip()
                row[4] = ""

        if row[2].strip().lower() == "none":
            row[2] = "None"
        if row[3].strip().lower() == "none":
            row[3] = "None"
        if row[4].strip().lower() == "none":
            row[4] = "None"

    return merged


def extract_schedule_a_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_a_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next_schedule = parse_schedule_a_page(
            words,
            column_starts,
            header_top,
        )
        rows.extend(page_rows)
        if reached_next_schedule:
            break

    return consolidate_schedule_a(rows)


# --- Schedule B helpers ----------------------------------------------------

def find_schedule_b_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w["top"]):
        text = normalise_word_text(word["text"]).lower()
        if text == "asset":
            top = word["top"]
            has_owner = any(
                abs(other["top"] - top) < HEADER_TOLERANCE
                and normalise_word_text(other["text"]).lower() == "owner"
                for other in words
            )
            has_date = any(
                abs(other["top"] - top) < HEADER_TOLERANCE
                and normalise_word_text(other["text"]).lower() == "date"
                for other in words
            )
            if has_owner and has_date:
                header_words = [
                    w for w in words if abs(w["top"] - top) < HEADER_TOLERANCE
                ]
                asset_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "asset"
                )
                owner_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "owner"
                )
                date_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "date"
                )
                tx_words = [
                    w for w in header_words
                    if normalise_word_text(w["text"]).lower().startswith("tx")
                ]
                if not tx_words:
                    return None
                tx_start = min(w["x0"] for w in tx_words)
                amount_words = [
                    w for w in header_words
                    if normalise_word_text(w["text"]).lower().startswith("amount")
                ]
                if not amount_words:
                    return None
                amount_start = min(w["x0"] for w in amount_words)
                cap_words = [
                    w for w in header_words
                    if normalise_word_text(w["text"]).lower().startswith("cap")
                ]
                if not cap_words:
                    return None
                cap_start = min(w["x0"] for w in cap_words)
                return top, [
                    asset_start,
                    owner_start,
                    date_start,
                    tx_start,
                    amount_start,
                    cap_start,
                ]
    return None


def assign_schedule_b_column(x0: float, column_starts: List[float]) -> str:
    asset_limit = column_starts[1] - 15
    owner_limit = column_starts[2] - 5
    date_limit = column_starts[3] - 5
    type_limit = column_starts[4] - 5
    amount_limit = column_starts[5] - 5

    if x0 < asset_limit:
        return COLUMN_NAMES_B[0]
    if x0 < owner_limit:
        return COLUMN_NAMES_B[1]
    if x0 < date_limit:
        return COLUMN_NAMES_B[2]
    if x0 < type_limit:
        return COLUMN_NAMES_B[3]
    if x0 < amount_limit:
        return COLUMN_NAMES_B[4]
    return COLUMN_NAMES_B[5]


def parse_schedule_b_page(words, column_starts, header_top):
    c_markers = [
        w["top"]
        for w in words
        if normalise_word_text(w["text"]).startswith("S C:")
        or normalise_word_text(w["text"]) == "C:"
    ]
    cutoff_top = min(c_markers) if c_markers else None

    data_words = [w for w in words if w["top"] > header_top + 5]
    data_words.sort(key=lambda w: (w["top"], w["x0"]))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_B}
    current_top = None

    for word in data_words:
        top = word["top"]
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word["text"])
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("https://"):
            continue
        if lowered.startswith("* asset class"):
            continue

        column_name = assign_schedule_b_column(word["x0"], column_starts)

        if is_schedule_b_description_fragment(text):
            if current_top is None or abs(top - current_top) > ROW_TOLERANCE:
                if any(current[col] for col in COLUMN_NAMES_B):
                    rows.append([
                        clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_B
                    ])
                current = {name: [] for name in COLUMN_NAMES_B}
                current_top = top
            current[COLUMN_NAMES_B[0]].append(text)
            continue

        if column_name == COLUMN_NAMES_B[1] and not is_owner_token(text):
            if is_date_token(text):
                column_name = COLUMN_NAMES_B[2]
            elif is_tx_type_token(text):
                column_name = COLUMN_NAMES_B[3]
            elif is_transaction_amount_token(text):
                column_name = COLUMN_NAMES_B[4]
            else:
                column_name = COLUMN_NAMES_B[0]
        elif column_name == COLUMN_NAMES_B[2] and not is_date_token(text):
            if is_owner_token(text):
                column_name = COLUMN_NAMES_B[1]
            elif is_tx_type_token(text):
                column_name = COLUMN_NAMES_B[3]
            elif is_transaction_amount_token(text):
                column_name = COLUMN_NAMES_B[4]
            else:
                column_name = COLUMN_NAMES_B[0]
        elif column_name == COLUMN_NAMES_B[3] and not is_tx_type_token(text):
            if is_date_token(text):
                column_name = COLUMN_NAMES_B[2]
            elif is_transaction_amount_token(text):
                column_name = COLUMN_NAMES_B[4]
            else:
                column_name = COLUMN_NAMES_B[0]
        elif column_name == COLUMN_NAMES_B[4] and not is_transaction_amount_token(text):
            column_name = COLUMN_NAMES_B[0]

        if current_top is None or abs(top - current_top) > ROW_TOLERANCE:
            if any(current[col] for col in COLUMN_NAMES_B):
                rows.append([
                    clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_B
                ])
            current = {name: [] for name in COLUMN_NAMES_B}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_B):
        rows.append([
            clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_B
        ])

    filtered_rows = [
        row
        for row in rows
        if any(row)
        and row[0].strip().lower() not in SCHEDULE_B_HEADER_TOKENS
        and not (
            not row[0]
            and not row[1]
            and not row[2]
            and row[3].lower() == "type"
            and not row[4]
            and row[5]
            and "gains" in row[5].lower()
        )
    ]
    return filtered_rows, bool(cutoff_top)


def consolidate_schedule_a(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    consolidated: List[List[str]] = []
    current: List[str] | None = None

    for raw_row in rows:
        row = list(raw_row[:6]) + ["" for _ in range(max(0, 6 - len(raw_row)))]
        asset, owner, value, income_type, income, tx_flag = row

        has_value = bool(value)
        looks_like_value = bool(value and (VALUE_TOKEN_PATTERN.search(value) or value.lower() == "none"))

        if has_value and (looks_like_value or current is None):
            if current:
                consolidated.append(current)
            current = [asset, owner, value, income_type, income, tx_flag]
            continue

        if not current:
            continue

        if asset:
            current_asset = current[0]
            joiner = "\n" if current_asset else ""
            current[0] = f"{current_asset}{joiner}{asset}" if current_asset else asset
        if owner:
            current_owner = current[1]
            joiner = "\n" if current_owner else ""
            current[1] = f"{current_owner}{joiner}{owner}" if current_owner else owner
        if income_type:
            current_income_type = current[3]
            joiner = "\n" if current_income_type else ""
            current[3] = f"{current_income_type}{joiner}{income_type}" if current_income_type else income_type
        if income:
            current_income = current[4]
            joiner = "\n" if current_income else ""
            current[4] = f"{current_income}{joiner}{income}" if current_income else income
        if tx_flag:
            current_tx = current[5]
            joiner = "\n" if current_tx else ""
            current[5] = f"{current_tx}{joiner}{tx_flag}" if current_tx else tx_flag

    if current:
        consolidated.append(current)

    return consolidated


def consolidate_schedule_b(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    consolidated: List[List[str]] = []
    current: List[str] | None = None

    for raw_row in rows:
        row = list(raw_row[:6]) + ['' for _ in range(max(0, 6 - len(raw_row)))]
        asset, owner, date, tx_type, amount, cap = row

        if is_schedule_b_description_fragment(asset):
            if current:
                current[0] = current[0] + ('\n' if current[0] else '') + asset
                if owner:
                    current[1] = current[1] + ('\n' if current[1] else '') + owner
                if date:
                    current[2] = current[2] + (' ' if current[2] else '') + date
                if tx_type:
                    current[3] = current[3] + (' ' if current[3] else '') + tx_type
                if amount:
                    current[4] = current[4] + (' ' if current[4] else '') + amount
                if cap:
                    current[5] = current[5] + (' ' if current[5] else '') + cap
            continue

        is_new = False
        if owner and not owner.startswith('L:') and len(owner) <= 4:
            is_new = True
        if date or tx_type or amount:
            is_new = True

        if is_new:
            if current:
                consolidated.append(current)
            current = [asset, owner, date, tx_type, amount, cap]
            continue

        if current is None:
            current = [asset, owner, date, tx_type, amount, cap]
            continue

        if asset:
            current[0] = current[0] + ('\n' if current[0] else '') + asset
        if owner:
            current[1] = current[1] + ('\n' if current[1] else '') + owner
        if date:
            current[2] = current[2] + (' ' if current[2] else '') + date
        if tx_type:
            current[3] = current[3] + (' ' if current[3] else '') + tx_type
        if amount:
            current[4] = current[4] + (' ' if current[4] else '') + amount
        if cap:
            current[5] = current[5] + (' ' if current[5] else '') + cap

    if current:
        consolidated.append(current)

    return [row for row in consolidated if not is_noise_schedule_b_row(row)]


def extract_schedule_b_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_b_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next = parse_schedule_b_page(
            words,
            column_starts,
            header_top,
        )
        rows.extend(page_rows)
        if reached_next:
            break

    return consolidate_schedule_b(rows)


# --- Schedule C helpers ----------------------------------------------------

def find_schedule_c_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w["top"]):
        text = normalise_word_text(word["text"]).lower()
        if text == "source":
            top = word["top"]
            has_type = any(
                abs(other["top"] - top) < HEADER_TOLERANCE
                and normalise_word_text(other["text"]).lower() == "type"
                for other in words
            )
            has_amount = any(
                abs(other["top"] - top) < HEADER_TOLERANCE
                and normalise_word_text(other["text"]).lower() == "amount"
                for other in words
            )
            if has_type and has_amount:
                header_words = [
                    w for w in words if abs(w["top"] - top) < HEADER_TOLERANCE
                ]
                source_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "source"
                )
                type_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "type"
                )
                amount_start = min(
                    w["x0"]
                    for w in header_words
                    if normalise_word_text(w["text"]).lower() == "amount"
                )
                return top, [source_start, type_start, amount_start]
    return None


def assign_schedule_c_column(x0: float, column_starts: List[float]) -> str:
    source_limit = column_starts[1] - 10
    type_limit = column_starts[2] - 5

    if x0 < source_limit:
        return COLUMN_NAMES_C[0]
    if x0 < type_limit:
        return COLUMN_NAMES_C[1]
    return COLUMN_NAMES_C[2]


def parse_schedule_c_page(words, column_starts, header_top):
    d_markers = []
    for idx, w in enumerate(words):
        text = normalise_word_text(w["text"])
        if text.startswith("S D") or text == "D:":
            d_markers.append(w["top"])
        elif text == "S" and idx + 1 < len(words):
            next_text = normalise_word_text(words[idx + 1]["text"])
            if next_text.startswith("D"):
                d_markers.append(w["top"])

    cutoff_top = min(d_markers) if d_markers else None

    data_words = [w for w in words if w["top"] > header_top + 5]
    data_words.sort(key=lambda w: (w["top"], w["x0"]))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_C}
    current_top = None

    for word in data_words:
        top = word["top"]
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word["text"])
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("https://"):
            continue
        if lowered.startswith("* asset class"):
            continue

        column_name = assign_schedule_c_column(word["x0"], column_starts)
        if current_top is None or abs(top - current_top) > ROW_TOLERANCE_C:
            if any(current[col] for col in COLUMN_NAMES_C):
                rows.append([
                    clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_C
                ])
            current = {name: [] for name in COLUMN_NAMES_C}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_C):
        rows.append([
            clean_cell(" ".join(current[col])) for col in COLUMN_NAMES_C
        ])

    cleaned = [row for row in rows if any(row)]
    return cleaned, bool(cutoff_top)


def extract_schedule_c_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_c_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next = parse_schedule_c_page(words, column_starts, header_top)
        rows.extend(page_rows)
        if reached_next:
            break

    return rows



# --- Schedule D helpers ----------------------------------------------------

def find_schedule_d_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w['top']):
        text = normalise_word_text(word['text']).lower()
        if text == 'owner':
            top = word['top']
            has_creditor = any(
                abs(other['top'] - top) < HEADER_TOLERANCE
                and normalise_word_text(other['text']).lower() == 'creditor'
                for other in words
            )
            has_amount = any(
                abs(other['top'] - top) < HEADER_TOLERANCE
                and 'amount' in normalise_word_text(other['text']).lower()
                for other in words
            )
            if has_creditor and has_amount:
                header_words = [
                    w for w in words if abs(w['top'] - top) < HEADER_TOLERANCE
                ]
                owner_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower() == 'owner'
                )
                creditor_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower() == 'creditor'
                )
                date_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower().startswith('date')
                )
                type_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower() == 'type'
                )
                amount_start = min(
                    w['x0']
                    for w in header_words
                    if 'amount' in normalise_word_text(w['text']).lower()
                )
                return top, [owner_start, creditor_start, date_start, type_start, amount_start]
    return None

def assign_schedule_d_column(x0: float, column_starts: List[float]) -> str:
    owner_limit = column_starts[1] - 10
    creditor_limit = column_starts[2] - 5
    date_limit = column_starts[3] - 5
    type_limit = column_starts[4] - 5

    if x0 < owner_limit:
        return COLUMN_NAMES_D[0]
    if x0 < creditor_limit:
        return COLUMN_NAMES_D[1]
    if x0 < date_limit:
        return COLUMN_NAMES_D[2]
    if x0 < type_limit:
        return COLUMN_NAMES_D[3]
    return COLUMN_NAMES_D[4]

def parse_schedule_d_page(words, column_starts, header_top):
    e_markers = []
    for idx, w in enumerate(words):
        text = normalise_word_text(w['text'])
        if text.startswith('S E') or text == 'E:':
            e_markers.append(w['top'])
        elif text == 'S' and idx + 1 < len(words):
            next_text = normalise_word_text(words[idx + 1]['text'])
            if next_text.startswith('E'):
                e_markers.append(w['top'])
    cutoff_top = min(e_markers) if e_markers else None

    data_words = [w for w in words if w['top'] > header_top + 15]
    data_words.sort(key=lambda w: (w['top'], w['x0']))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_D}
    current_top = None

    for word in data_words:
        top = word['top']
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word['text'])
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith('https://') or lowered.startswith('* asset class'):
            continue
        if lowered == 'liability':
            continue

        column_name = assign_schedule_d_column(word['x0'], column_starts)
        if current_top is None or abs(top - current_top) > ROW_TOLERANCE_D:
            if any(current[col] for col in COLUMN_NAMES_D):
                rows.append([
                    clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_D
                ])
            current = {name: [] for name in COLUMN_NAMES_D}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_D):
        rows.append([
            clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_D
        ])

    cleaned = [row for row in rows if any(row)]
    return cleaned, bool(cutoff_top)




def consolidate_schedule_d(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    consolidated: List[List[str]] = []
    current: List[str] | None = None

    for raw_row in rows:
        row = list(raw_row[:5]) + ['' for _ in range(max(0, 5 - len(raw_row)))]
        owner, creditor, date, rtype, amount = row[:5]

        is_new = bool(owner or creditor or date)
        amount_new = amount and (VALUE_TOKEN_PATTERN.search(amount) or amount.lower() == 'none')
        if amount_new and (owner or creditor or date):
            is_new = True
        elif amount_new and current is None:
            is_new = True

        if is_new or current is None:
            if current:
                consolidated.append(current)
            current = [owner, creditor, date, rtype, amount]
            continue

        if owner:
            existing = current[0]
            joiner = '\n' if existing else ''
            current[0] = f"{existing}{joiner}{owner}" if existing else owner
        if creditor:
            existing = current[1]
            joiner = '\n' if existing else ''
            current[1] = f"{existing}{joiner}{creditor}" if existing else creditor
        if date:
            existing = current[2]
            joiner = ' ' if existing else ''
            current[2] = f"{existing}{joiner}{date}" if existing else date
        if rtype:
            existing = current[3]
            joiner = '\n' if existing else ''
            current[3] = f"{existing}{joiner}{rtype}" if existing else rtype
        if amount:
            existing = current[4]
            joiner = ' ' if existing else ''
            current[4] = f"{existing}{joiner}{amount}" if existing else amount

    if current:
        consolidated.append(current)

    return consolidated

def extract_schedule_d_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_d_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next = parse_schedule_d_page(words, column_starts, header_top)
        rows.extend(page_rows)
        if reached_next:
            break

    return consolidate_schedule_d(rows)


# --- Schedule E helpers ----------------------------------------------------

def find_schedule_e_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w['top']):
        text = normalise_word_text(word['text']).lower()
        if text == 'position':
            top = word['top']
            header_words = [
                w for w in words if abs(w['top'] - top) < HEADER_TOLERANCE
            ]
            org_candidates = [
                w for w in header_words
                if normalise_word_text(w['text']).lower() in {'name', 'organization'}
            ]
            if not org_candidates:
                continue
            position_start = min(
                w['x0']
                for w in header_words
                if normalise_word_text(w['text']).lower() == 'position'
            )
            org_start = min(w['x0'] for w in org_candidates)
            return top, [position_start, org_start]
    return None

def assign_schedule_e_column(x0: float, column_starts: List[float]) -> str:
    position_limit = column_starts[1] - 5
    if x0 < position_limit:
        return COLUMN_NAMES_E[0]
    return COLUMN_NAMES_E[1]

def parse_schedule_e_page(words, column_starts, header_top):
    f_markers = []
    for idx, w in enumerate(words):
        text = normalise_word_text(w['text'])
        if text.startswith('S F') or text == 'F:':
            f_markers.append(w['top'])
        elif text == 'S' and idx + 1 < len(words):
            next_text = normalise_word_text(words[idx + 1]['text'])
            if next_text.startswith('F'):
                f_markers.append(w['top'])
    cutoff_top = min(f_markers) if f_markers else None

    data_words = [w for w in words if w['top'] > header_top + 5]
    data_words.sort(key=lambda w: (w['top'], w['x0']))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_E}
    current_top = None

    for word in data_words:
        top = word['top']
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word['text'])
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith('https://') or lowered.startswith('* asset class'):
            continue

        column_name = assign_schedule_e_column(word['x0'], column_starts)
        if current_top is None or abs(top - current_top) > ROW_TOLERANCE_E:
            if any(current[col] for col in COLUMN_NAMES_E):
                rows.append([
                    clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_E
                ])
            current = {name: [] for name in COLUMN_NAMES_E}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_E):
        rows.append([
            clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_E
        ])

    cleaned = [row for row in rows if any(row)]
    return cleaned, bool(cutoff_top)


def consolidate_schedule_e(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    consolidated: List[List[str]] = []
    current: List[str] | None = None

    for raw_row in rows:
        row = list(raw_row[:2]) + ['' for _ in range(max(0, 2 - len(raw_row)))]
        position, organization = row[:2]

        is_new = bool(position or organization)

        if is_new or current is None:
            if current:
                consolidated.append(current)
            current = [position, organization]
            continue

        if position:
            existing = current[0]
            joiner = ' ' if existing else ''
            current[0] = f"{existing}{joiner}{position}" if existing else position
        if organization:
            existing = current[1]
            joiner = ' ' if existing else ''
            current[1] = f"{existing}{joiner}{organization}" if existing else organization

    if current:
        consolidated.append(current)

    return consolidated

def extract_schedule_e_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_e_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next = parse_schedule_e_page(words, column_starts, header_top)
        rows.extend(page_rows)
        if reached_next:
            break

    return consolidate_schedule_e(rows)



# --- Schedule F helpers ----------------------------------------------------

def find_schedule_f_header(words) -> tuple | None:
    for word in sorted(words, key=lambda w: w['top']):
        text = normalise_word_text(word['text']).lower()
        if text == 'date':
            top = word['top']
            has_parties = any(
                abs(other['top'] - top) < HEADER_TOLERANCE
                and normalise_word_text(other['text']).lower().startswith('parties')
                for other in words
            )
            has_terms = any(
                abs(other['top'] - top) < HEADER_TOLERANCE
                and 'terms' in normalise_word_text(other['text']).lower()
                for other in words
            )
            if has_parties and has_terms:
                header_words = [
                    w for w in words if abs(w['top'] - top) < HEADER_TOLERANCE
                ]
                date_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower() == 'date'
                )
                parties_start = min(
                    w['x0']
                    for w in header_words
                    if normalise_word_text(w['text']).lower().startswith('parties')
                )
                terms_start = min(
                    w['x0']
                    for w in header_words
                    if 'terms' in normalise_word_text(w['text']).lower()
                )
                return top, [date_start, parties_start, terms_start]
    return None

def assign_schedule_f_column(x0: float, column_starts: List[float]) -> str:
    date_limit = column_starts[1] - 5
    parties_limit = column_starts[2] - 5

    if x0 < date_limit:
        return COLUMN_NAMES_F[0]
    if x0 < parties_limit:
        return COLUMN_NAMES_F[1]
    return COLUMN_NAMES_F[2]

def parse_schedule_f_page(words, column_starts, header_top):
    g_markers = []
    for idx, w in enumerate(words):
        text = normalise_word_text(w['text'])
        if text.startswith('S G') or text == 'G:':
            g_markers.append(w['top'])
        elif text == 'S' and idx + 1 < len(words):
            next_text = normalise_word_text(words[idx + 1]['text'])
            if next_text.startswith('G'):
                g_markers.append(w['top'])
    cutoff_top = min(g_markers) if g_markers else None

    data_words = [w for w in words if w['top'] > header_top + 5]
    data_words.sort(key=lambda w: (w['top'], w['x0']))

    rows = []
    current = {name: [] for name in COLUMN_NAMES_F}
    current_top = None

    for word in data_words:
        top = word['top']
        if cutoff_top is not None and top >= cutoff_top - 1:
            break

        text = normalise_word_text(word['text'])
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith('https://') or lowered.startswith('* asset class'):
            continue

        column_name = assign_schedule_f_column(word['x0'], column_starts)
        if current_top is None or abs(top - current_top) > ROW_TOLERANCE_F:
            if any(current[col] for col in COLUMN_NAMES_F):
                rows.append([
                    clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_F
                ])
            current = {name: [] for name in COLUMN_NAMES_F}
            current_top = top

        current[column_name].append(text)

    if any(current[col] for col in COLUMN_NAMES_F):
        rows.append([
            clean_cell(' '.join(current[col])) for col in COLUMN_NAMES_F
        ])

    cleaned = [row for row in rows if any(row)]
    return cleaned, bool(cutoff_top)

def consolidate_schedule_f(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows

    consolidated: List[List[str]] = []
    current: List[str] | None = None

    for raw_row in rows:
        row = list(raw_row[:3]) + ['' for _ in range(max(0, 3 - len(raw_row)))]
        date, parties, terms = row[:3]

        is_new = bool(date or parties or terms)

        if is_new or current is None:
            if current:
                consolidated.append(current)
            current = [date, parties, terms]
            continue

        if date:
            existing = current[0]
            joiner = ' ' if existing else ''
            current[0] = f"{existing}{joiner}{date}" if existing else date
        if parties:
            existing = current[1]
            joiner = '\n' if existing else ''
            current[1] = f"{existing}{joiner}{parties}" if existing else parties
        if terms:
            existing = current[2]
            joiner = ' ' if existing else ''
            current[2] = f"{existing}{joiner}{terms}" if existing else terms

    if current:
        consolidated.append(current)

    return consolidated

def extract_schedule_f_from_pdf(pdf) -> List[List[str]]:
    rows: List[List[str]] = []
    collecting = False
    column_starts: List[float] | None = None
    header_top = None

    for page in pdf.pages:
        words = page.extract_words(keep_blank_chars=False)
        header_info = find_schedule_f_header(words)
        if header_info:
            header_top, column_starts = header_info
            collecting = True
        elif not collecting:
            continue

        if not collecting or column_starts is None:
            continue

        page_rows, reached_next = parse_schedule_f_page(words, column_starts, header_top)
        rows.extend(page_rows)
        if reached_next:
            break

    return consolidate_schedule_f(rows)


# --- Table helpers ---------------------------------------------------------

def normalise_header(row):
    cleaned = [clean_cell(cell) for cell in row]
    return tuple(label for label in cleaned if label)


def identify_schedule(header):
    lowered = [label.lower() for label in header]
    header_set = set(lowered)

    if {"asset", "value of asset", "income type(s)"}.issubset(header_set):
        return "A", SCHEDULE_COLUMNS["A"]
    if "cap. gains > $200?" in header_set or "tx. type" in header_set:
        return "B", SCHEDULE_COLUMNS["B"]
    if header_set == {"source", "type", "amount"}:
        return "C", SCHEDULE_COLUMNS["C"]
    if "amount of liability" in header_set:
        return "D", SCHEDULE_COLUMNS["D"]
    if "name of organization" in header_set:
        return "E", SCHEDULE_COLUMNS["E"]
    if "terms of agreement" in header_set:
        return "F", SCHEDULE_COLUMNS["F"]
    if "item(s) provided/expenses paid" in header_set or "nature of event" in header_set:
        return "H", SCHEDULE_COLUMNS["H"]
    if "description" in header_set and "value" in header_set and "source" in header_set:
        return "G", SCHEDULE_COLUMNS["G"]
    if "payee" in header_set and "date(s)" in header_set:
        return "I", SCHEDULE_COLUMNS["I"]

    return None, None


# --- Extraction orchestration ---------------------------------------------

def extract_schedule_tables(pdf_path: Path):
    schedule_rows = defaultdict(list)
    schedule_notes = defaultdict(list)
    schedule_presence = set()

    with pdfplumber.open(pdf_path) as pdf:
        schedule_a_rows = extract_schedule_a_from_pdf(pdf)
        schedule_b_rows = extract_schedule_b_from_pdf(pdf)
        schedule_c_rows = extract_schedule_c_from_pdf(pdf)
        schedule_d_rows = extract_schedule_d_from_pdf(pdf)
        schedule_e_rows = extract_schedule_e_from_pdf(pdf)
        schedule_f_rows = extract_schedule_f_from_pdf(pdf)

        for page_number, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or "")
            clean_text = text.replace("\x00", " ")
            lines = [line.strip() for line in clean_text.splitlines() if line.strip()]

            for idx, line in enumerate(lines):
                match = re.match(r"S\s+([A-Z])", line)
                if match:
                    key = match.group(1)
                    if key in SCHEDULE_META:
                        schedule_presence.add(key)
                        for look_ahead in lines[idx + 1:]:
                            if re.match(r"S\s+[A-Z]", look_ahead):
                                break
                            if look_ahead.lower().startswith("none disclosed"):
                                schedule_notes[key].append(look_ahead)
                                break

            try:
                tables = page.extract_tables()
            except Exception as exc:
                print(f"Warning: failed to read tables on page {page_number}: {exc}")
                continue

            for table in tables:
                if not table:
                    continue
                header = normalise_header(table[0])
                schedule_key, columns = identify_schedule(header)

                if schedule_key:
                    data_rows = table[1:]
                else:
                    continue

                if not columns:
                    columns = [f"Column {idx}" for idx in range(1, len(table[0]) + 1)]

                for raw_row in data_rows:
                    cleaned_row = [clean_cell(cell) for cell in raw_row[:len(columns)]]
                    if any(cell for cell in cleaned_row):
                        while len(cleaned_row) < len(columns):
                            cleaned_row.append("")
                        schedule_rows[schedule_key].append(cleaned_row)

    for key in schedule_presence:
        schedule_rows.setdefault(key, [])

    if schedule_rows.get("A"):
        schedule_rows["A"] = consolidate_schedule_a(schedule_rows["A"])
    if schedule_rows.get("B"):
        schedule_rows["B"] = consolidate_schedule_b(schedule_rows["B"])
    if schedule_rows.get("D"):
        schedule_rows["D"] = consolidate_schedule_d(schedule_rows["D"])
    if schedule_rows.get("E"):
        schedule_rows["E"] = consolidate_schedule_e(schedule_rows["E"])
    if schedule_rows.get("F"):
        schedule_rows["F"] = consolidate_schedule_f(schedule_rows["F"])

    if schedule_a_rows:
        schedule_rows["A"] = tidy_schedule_a_rows(schedule_a_rows)
    if schedule_b_rows:
        schedule_rows["B"] = schedule_b_rows
    if schedule_c_rows:
        schedule_rows["C"] = schedule_c_rows
    if schedule_d_rows:
        schedule_rows["D"] = schedule_d_rows
    if schedule_e_rows:
        schedule_rows["E"] = schedule_e_rows
    if schedule_f_rows:
        schedule_rows["F"] = schedule_f_rows

    return schedule_rows, schedule_notes


# --- Output ---------------------------------------------------------------

def write_excel(schedule_rows, schedule_notes, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for key in SCHEDULE_ORDER:
            sheet_name = SCHEDULE_META[key]["sheet"]
            rows = schedule_rows.get(key, [])

            if rows:
                columns = SCHEDULE_COLUMNS.get(key)
                if not columns:
                    columns = [f"Column {idx}" for idx in range(1, len(rows[0]) + 1)]
                df = pd.DataFrame(rows, columns=columns).fillna("")
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                notes = schedule_notes.get(key)
                df = pd.DataFrame({"Notes": notes or ["No data extracted."]})
                df.to_excel(writer, sheet_name=sheet_name, index=False)



def process_single_pdf(pdf_path: Path, output_path: Path | None = None) -> Path:
    """Parse a single disclosure PDF and write schedules to an Excel workbook."""
    if output_path is None:
        output_path = pdf_path.with_suffix(".xlsx")

    if not pdf_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {pdf_path}")

    schedule_rows, schedule_notes = extract_schedule_tables(pdf_path)
    write_excel(schedule_rows, schedule_notes, output_path)
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Split disclosure schedules into Excel sheets.")
    parser.add_argument("pdf", type=Path, help="Path to the disclosure PDF")
    parser.add_argument("-o", "--output", type=Path, help="Optional output .xlsx path")
    args = parser.parse_args()

    if not args.pdf.exists():
        raise SystemExit(f"Input PDF not found: {args.pdf}")

    output_path = args.output if args.output else args.pdf.with_suffix(".xlsx")

    schedule_rows, schedule_notes = extract_schedule_tables(args.pdf)
    write_excel(schedule_rows, schedule_notes, output_path)
    print(f"Wrote schedules to {output_path}")


if __name__ == "__main__":
    main()
