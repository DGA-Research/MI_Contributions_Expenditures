"""
Utility helpers for processing campaign finance PDFs using the FinanceWork pipeline.

This package exposes a programmatic API for:
  * extracting text from PDFs (`extract_pdf_text.extract_pdf`)
  * compiling the normalized text into CSV schedules (`compile_pdf_to_csv`)
  * combining CSVs into a workbook (`csv_to_workbook`)
  * running the full end-to-end pipeline against a single PDF (`process_reports.process_pdf`)
"""

from .process_reports import ReportSummary, process_pdf
from .extract_pdf_text import ExtractionResult, extract_pdf

__all__ = [
    "ReportSummary",
    "extract_pdf",
    "ExtractionResult",
    "process_pdf",
]
