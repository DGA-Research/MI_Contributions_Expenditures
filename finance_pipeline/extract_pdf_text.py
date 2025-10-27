"""
Utility script to extract textual content from a PDF.

The script uses pdfplumber for layout-aware extraction and emits both a single
merged text file and individual per-page files under the specified output
directory. For image-based PDFs (no text layer), the script can optionally run
OCR via Tesseract if `--ocr` is supplied and Tesseract is installed.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys

try:
    import pdfplumber
except ImportError as exc:  # pragma: no cover - dependency guard
    raise SystemExit(
        "Missing dependency pdfplumber. Install with: pip install pdfplumber"
    ) from exc


@dataclass
class ExtractionResult:
    """Container for aggregated extraction statistics."""

    pages_processed: int = 0
    pages_with_text: int = 0
    pages_with_ocr: int = 0


def extract_pdf(
    input_pdf: Path,
    output_dir: Path,
    *,
    encode: str = "utf-8",
    enable_ocr: bool = False,
) -> ExtractionResult:
    """
    Extracts text from `input_pdf` and writes outputs into `output_dir`.

    Each page produces a `{page_number:04d}.txt` file. A combined `document.txt`
    is also written which concatenates page contents separated by delimiters.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    combined_path = output_dir / "document.txt"
    result = ExtractionResult()

    try:
        import pytesseract  # type: ignore
        from PIL import Image
    except ImportError:
        pytesseract = None  # type: ignore
        Image = None  # type: ignore

    if enable_ocr and (pytesseract is None or Image is None):
        raise SystemExit(
            "OCR requested but pytesseract and Pillow are not installed. "
            "Install with: pip install pytesseract pillow"
        )

    with pdfplumber.open(str(input_pdf)) as pdf, combined_path.open(
        "w", encoding=encode
    ) as combined_file:
        for page_number, page in enumerate(pdf.pages, start=1):
            result.pages_processed += 1
            text = page.extract_text() or ""

            if not text.strip() and enable_ocr:
                # For image-based pages, run OCR if enabled.
                page_image = page.to_image(resolution=300)
                ocr_text = pytesseract.image_to_string(page_image.original)
                if ocr_text.strip():
                    text = ocr_text
                    result.pages_with_ocr += 1

            if text.strip():
                result.pages_with_text += 1

            page_path = output_dir / f"{page_number:04d}.txt"
            page_path.write_text(text, encoding=encode)

            combined_file.write(f"--- Page {page_number} ---\n")
            combined_file.write(text)
            combined_file.write("\n\n")

    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract text from a PDF into per-page and merged text files."
    )
    parser.add_argument("pdf", type=Path, help="Path to the source PDF file.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("text_output"),
        help="Directory to write extracted text files (default: %(default)s).",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding used when writing text files (default: %(default)s).",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Enable OCR fallback for image-only pages (requires pytesseract & pillow).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.pdf.exists():
        parser.error(f"PDF not found: {args.pdf}")

    result = extract_pdf(
        args.pdf,
        args.output,
        encode=args.encoding,
        enable_ocr=args.ocr,
    )

    print(
        "Extraction complete:",
        f"{result.pages_processed} pages processed,",
        f"{result.pages_with_text} pages with text,",
        f"{result.pages_with_ocr} pages via OCR" if args.ocr else "",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
