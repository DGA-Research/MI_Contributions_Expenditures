#!/usr/bin/env python3
"""
Streamlit UI for multiple campaign finance PDF workflows:
  - Michigan Candidate Report View and Schedules (multi-schedule export)
  - Arizona Schedule C2 individual contributions
  - FinanceWork pipeline (text -> CSV schedules -> Excel workbook)
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from az_report_parser import AZReportParser
from finance_pipeline.process_reports import process_pdf
from mi_report_parser import ReportParser
from alaska_project import process_single_pdf


st.set_page_config(page_title="Campaign Finance Parser", layout="wide")
st.title("Campaign Finance Parser")
st.write(
    "Upload a campaign finance PDF and choose the parsing workflow. "
    "Michigan and Arizona paths mirror their respective schedules, while the Finance pipeline "
    "runs the text -> CSV -> Excel process from the FinanceWork tooling."
)


uploaded_pdf = st.file_uploader(
    "Select the PDF file",
    type=["pdf"],
    accept_multiple_files=False,
    help="Supported workflows: Michigan Candidate Report PDFs, Arizona Schedule C2 filings, or FinanceWork text -> CSV -> Excel pipeline.",
)

parser_selection = st.radio(
    "Select workflow",
    options=(
        "Michigan Campaign Report Summary and Schedules",
        "Arizona Campaign Finance Report",
        "Alaska POFD Schedules",
        "Pennsylvania Campaign Finance Report",
    ),
)


def _entries_to_dataframe(entries) -> pd.DataFrame:
    """Convert parser entries to a tabular structure suitable for Excel."""
    rows = [entry.to_csv_row() for entry in entries]
    return pd.DataFrame(rows)


if uploaded_pdf is not None:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
        tmp_pdf.write(uploaded_pdf.getbuffer())
        tmp_path = Path(tmp_pdf.name)

    report_stem = (
        Path(uploaded_pdf.name).stem if getattr(uploaded_pdf, "name", None) else "report"
    )

    try:
        if parser_selection == "Michigan Campaign Report Summary and Schedules":
            with st.spinner("Parsing Michigan campaign report..."):
                parser = ReportParser(tmp_path)
                contributions = parser.parse_contributions()
                other_receipts = parser.parse_other_receipts()
                in_kind_contributions = parser.parse_in_kind_contributions()
                fundraisers = parser.parse_fundraisers()
                expenditures = parser.parse_expenditures()

            st.success(
                "Parsed "
                f"{len(contributions)} direct contributions, "
                f"{len(other_receipts)} other receipts, "
                f"{len(in_kind_contributions)} in-kind contributions, "
                f"{len(fundraisers)} fundraisers, and "
                f"{len(expenditures)} expenditures."
            )

            contrib_df = _entries_to_dataframe(contributions)
            other_receipts_df = _entries_to_dataframe(other_receipts)
            in_kind_df = _entries_to_dataframe(in_kind_contributions)
            fundraisers_df = _entries_to_dataframe(fundraisers)
            expend_df = _entries_to_dataframe(expenditures)

            st.subheader("Contributions Preview")
            st.dataframe(contrib_df.head(25), use_container_width=True)

            st.subheader("Other Receipts Preview")
            st.dataframe(other_receipts_df.head(25), use_container_width=True)

            st.subheader("In-Kind Contributions Preview")
            st.dataframe(in_kind_df.head(25), use_container_width=True)

            st.subheader("Fundraisers Preview")
            st.dataframe(fundraisers_df.head(25), use_container_width=True)

            st.subheader("Expenditures Preview")
            st.dataframe(expend_df.head(25), use_container_width=True)

            output_stream = io.BytesIO()
            with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
                contrib_df.to_excel(writer, sheet_name="Contributions", index=False)
                other_receipts_df.to_excel(writer, sheet_name="Other Receipts", index=False)
                in_kind_df.to_excel(writer, sheet_name="In-Kind Contributions", index=False)
                fundraisers_df.to_excel(writer, sheet_name="Fundraisers", index=False)
                expend_df.to_excel(writer, sheet_name="Expenditures", index=False)
            output_stream.seek(0)

            st.download_button(
                "Download Excel Workbook",
                data=output_stream,
                file_name="mi_campaign_finance.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        elif parser_selection == "Arizona Campaign Finance Report":
            with st.spinner("Parsing Arizona campaign report..."):
                parser = AZReportParser(tmp_path)
                az_contributions = parser.parse_contributions()
                az_small_contrib = parser.parse_in_state_small_contributions()
                az_operating = parser.parse_operating_expenses()
                az_small_expenses = parser.parse_aggregate_small_expenses()
                az_other_receipts = parser.parse_other_receipts()

            st.success(
                "Parsed "
                f"{len(az_contributions)} individual contributions, "
                f"{len(az_operating)} operating expenses, and "
                f"{len(az_other_receipts)} other receipts."
            )

            contrib_columns = [
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
            contrib_df = pd.DataFrame(entry.to_csv_row() for entry in az_contributions)
            if contrib_df.empty:
                contrib_df = pd.DataFrame(columns=contrib_columns)
            else:
                contrib_df = contrib_df[contrib_columns]
                contrib_df["DATE"] = pd.to_datetime(contrib_df["DATE"], errors="coerce", format="%m/%d/%Y")
                contrib_df["AMOUNT"] = pd.to_numeric(
                    contrib_df["AMOUNT"]
                    .str.replace("(", "-", regex=False)
                    .str.replace(")", "", regex=False),
                    errors="coerce",
                )
                contrib_df["TOTAL TO DATE"] = pd.to_numeric(contrib_df["TOTAL TO DATE"], errors="coerce")

            small_contrib_df = pd.DataFrame(az_small_contrib)
            if small_contrib_df.empty:
                small_contrib_df = pd.DataFrame(columns=["Label", "Value"])

            operating_columns = [
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
            operating_df = pd.DataFrame(entry.to_csv_row() for entry in az_operating)
            if operating_df.empty:
                operating_df = pd.DataFrame(columns=operating_columns)
            else:
                operating_df = operating_df[operating_columns]

            small_expenses_df = pd.DataFrame(az_small_expenses)
            if small_expenses_df.empty:
                small_expenses_df = pd.DataFrame(columns=["Label", "Value"])

            other_receipt_columns = [
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
            other_receipts_df = pd.DataFrame(entry.to_csv_row() for entry in az_other_receipts)
            if other_receipts_df.empty:
                other_receipts_df = pd.DataFrame(columns=other_receipt_columns)
            else:
                other_receipts_df = other_receipts_df[other_receipt_columns]

            tab_titles = [
                "Schedule C2 - Individual Contributions",
                "Schedule In-State <=$100",
                "Schedule E1 - Operating Expenses",
                "Schedule E4 - Aggregate Small Expenses",
                "Schedule R1 - Other Receipts",
            ]
            tabs = st.tabs(tab_titles)

            with tabs[0]:
                st.dataframe(contrib_df.head(25), use_container_width=True)
                st.caption("Preview limited to the first 25 rows.")
            with tabs[1]:
                st.dataframe(small_contrib_df, use_container_width=True)
            with tabs[2]:
                st.dataframe(operating_df.head(25), use_container_width=True)
                st.caption("Preview limited to the first 25 rows.")
            with tabs[3]:
                st.dataframe(small_expenses_df, use_container_width=True)
            with tabs[4]:
                st.dataframe(other_receipts_df.head(25), use_container_width=True)
                st.caption("Preview limited to the first 25 rows.")

            output_stream = io.BytesIO()
            with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
                contrib_df.to_excel(writer, sheet_name="Schedule C2", index=False)
                small_contrib_df.to_excel(writer, sheet_name="Schedule In-State <=100", index=False)
                operating_df.to_excel(writer, sheet_name="Schedule E1", index=False)
                small_expenses_df.to_excel(writer, sheet_name="Schedule E4", index=False)
                other_receipts_df.to_excel(writer, sheet_name="Schedule R1", index=False)
            output_stream.seek(0)

            st.download_button(
                "Download Excel Workbook",
                data=output_stream,
                file_name="az_campaign_finance.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        elif parser_selection == "Alaska POFD Schedules":
            with st.spinner("Extracting Alaska POFD schedules..."):
                with tempfile.TemporaryDirectory() as pipeline_dir:
                    tmp_pdf_path = Path(pipeline_dir) / "pofd.pdf"
                    tmp_pdf_path.write_bytes(tmp_path.read_bytes())
                    output_workbook = Path(pipeline_dir) / "pofd_schedules.xlsx"

                    workbook_path = process_single_pdf(tmp_pdf_path, output_workbook)
                    workbook_bytes = workbook_path.read_bytes()

            st.success("Parsed Alaska POFD schedules.")

            workbook_stream = io.BytesIO(workbook_bytes)
            workbook_stream.seek(0)
            st.download_button(
                "Download POFD Workbook",
                data=workbook_stream,
                file_name=f"{report_stem}_pofd_schedules.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            with st.spinner("Running Pennsylvania campaign finance pipeline..."):
                with tempfile.TemporaryDirectory() as pipeline_dir:
                    base_path = Path(pipeline_dir)
                    text_root = base_path / "text_output"
                    csv_root = base_path / "csv_output"
                    workbook_root = base_path / "workbooks"

                    summary = process_pdf(
                        tmp_path,
                        text_root,
                        csv_root,
                        workbook_root,
                        enable_ocr=False,
                    )

                    csv_files = sorted(summary.csv_dir.glob("*.csv"))
                    workbook_bytes = summary.workbook_path.read_bytes()

                    pages_processed = summary.pages_processed

            st.success(f"Processed {pages_processed} pages.")
            if not csv_files:
                st.info(
                    "No schedule CSV files were produced; the downloaded workbook contains a status note "
                    "in place of detailed schedules."
                )

            workbook_stream = io.BytesIO(workbook_bytes)
            workbook_stream.seek(0)
            st.download_button(
                "Download Consolidated Workbook",
                data=workbook_stream,
                file_name=f"{report_stem}_finance_workbook.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as exc:  # pragma: no cover - surface unexpected errors
        st.error(f"Failed to process PDF: {exc}")

    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
