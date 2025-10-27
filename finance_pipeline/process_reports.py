"""
Batch process campaign finance PDFs into text, CSV, and Excel outputs.

For each PDF in the source directory:
1. Runs `extract_pdf_text.extract_pdf` to generate per-page text and document.txt.
2. Invokes `compile_pdf_to_csv.main` to produce schedule-specific CSV files.
3. Calls `csv_to_workbook.main` to package the CSVs into a single XLSX workbook.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from .compile_pdf_to_csv import main as compile_csv_main
from .csv_to_workbook import main as workbook_main
from .extract_pdf_text import ExtractionResult, extract_pdf


@dataclass
class ReportSummary:
    filename: str
    pages_processed: int
    text_dir: Path
    csv_dir: Path
    workbook_path: Path


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process all PDFs in a directory into text, CSV, and XLSX outputs."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("Shapiro Reports 1"),
        help="Directory containing PDF files (default: %(default)s).",
    )
    parser.add_argument(
        "--pattern",
        default="*.pdf",
        help="Glob pattern to match PDFs within the source directory (default: %(default)s).",
    )
    parser.add_argument(
        "--text-root",
        type=Path,
        default=Path("text_output"),
        help="Base directory for per-report text extraction (default: %(default)s).",
    )
    parser.add_argument(
        "--csv-root",
        type=Path,
        default=Path("csv_output"),
        help="Base directory for per-report CSV outputs (default: %(default)s).",
    )
    parser.add_argument(
        "--workbook-root",
        type=Path,
        default=Path("workbooks"),
        help="Directory to store generated Excel workbooks (default: %(default)s).",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Enable OCR fallback during text extraction (requires pytesseract & pillow).",
    )
    return parser


def process_pdf(
    pdf_path: Path,
    text_root: Path,
    csv_root: Path,
    workbook_root: Path,
    *,
    enable_ocr: bool = False,
) -> ReportSummary:
    stem = pdf_path.stem
    text_dir = text_root / stem
    csv_dir = csv_root / stem
    workbook_path = workbook_root / f"{stem}.xlsx"

    ensure_clean_dir(text_dir)
    ensure_clean_dir(csv_dir)
    workbook_root.mkdir(parents=True, exist_ok=True)

    extraction: ExtractionResult = extract_pdf(
        pdf_path,
        text_dir,
        enable_ocr=enable_ocr,
    )

    document_path = text_dir / "document.txt"
    if not document_path.exists():
        raise FileNotFoundError(f"Expected document.txt not found at {document_path}")

    compile_result = compile_csv_main(
        ["--document", str(document_path), "--output", str(csv_dir)]
    )
    if compile_result != 0:
        raise RuntimeError(f"compile_pdf_to_csv failed for {pdf_path}")

    csv_files = sorted(csv_dir.glob("*.csv"))
    if csv_files:
        workbook_result = workbook_main(
            ["--input", str(csv_dir), "--output", str(workbook_path)]
        )
        if workbook_result != 0:
            raise RuntimeError(f"csv_to_workbook failed for {pdf_path}")
    else:
        from openpyxl import Workbook  # type: ignore

        wb = Workbook()
        ws = wb.active
        ws.title = "Info"
        ws.append(["No CSV files were generated for this report."])
        wb.save(workbook_path)

    return ReportSummary(
        filename=pdf_path.name,
        pages_processed=extraction.pages_processed,
        text_dir=text_dir,
        csv_dir=csv_dir,
        workbook_path=workbook_path,
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.source.exists():
        parser.error(f"Source directory not found: {args.source}")
    if not args.source.is_dir():
        parser.error(f"Source path is not a directory: {args.source}")

    pdf_files = sorted(args.source.glob(args.pattern))
    if not pdf_files:
        parser.error(f"No PDF files found in {args.source} matching {args.pattern}")

    summaries: List[ReportSummary] = []
    for pdf in pdf_files:
        print(f"Processing {pdf.name}...")
        summary = process_pdf(
            pdf,
            args.text_root,
            args.csv_root,
            args.workbook_root,
            enable_ocr=args.ocr,
        )
        summaries.append(summary)
        print(
            f"  -> pages: {summary.pages_processed}, "
            f"text: {summary.text_dir}, "
            f"csv: {summary.csv_dir}, "
            f"workbook: {summary.workbook_path}"
        )

    print("\nCompleted processing of PDFs:")
    for summary in summaries:
        print(
            f"- {summary.filename}: {summary.pages_processed} pages "
            f"-> {summary.csv_dir} -> {summary.workbook_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
