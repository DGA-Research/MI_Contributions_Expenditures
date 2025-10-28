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
from disclosure_parser import process_single_pdf as process_disclosure_pdf
from alaska_project import process_single_pdf as process_alaska_pofd


st.set_page_config(page_title="Campaign Finance Parser", layout="wide")
st.title("Campaign Finance Parser")
st.write(
    "Upload a campaign finance PDF and choose the parsing workflow. "
    "Michigan and Arizona paths mirror their respective schedules, while the Finance pipeline "
    "runs the text -> CSV -> Excel process from the FinanceWork tooling."
)


parser_selection = st.radio(
    "Select workflow",
    options=(
        "Michigan Campaign Report Summary and Schedules",
        "Arizona Campaign Finance Report",
        "Alaska POFD Schedules",
        "Federal Financial Disclosure Report",
        "Pennsylvania Campaign Finance Report",
    ),
)

uploader_key = f"uploader_{parser_selection.replace(' ', '_').replace('/', '_')}"
uploaded_pdf = st.file_uploader(
    "Upload a PDF for the selected workflow",
    type=["pdf"],
    accept_multiple_files=False,
    key=uploader_key,
)

if uploaded_pdf is None:
    st.info("Select a workflow and upload the corresponding PDF to begin parsing.")

def _entries_to_dataframe(entries) -> pd.DataFrame:
    """Convert parser entries to a tabular structure suitable for Excel."""
    rows = [entry.to_csv_row() for entry in entries]
    return pd.DataFrame(rows)


def _render_dataframe_tabs(tab_entries):
    if not tab_entries:
        st.info("No tabular data available.")
        return
    tab_objects = st.tabs([title for title, _ in tab_entries])
    for tab, (title, df) in zip(tab_objects, tab_entries):
        with tab:
            if df is None or (hasattr(df, "empty") and df.empty):
                st.info("No rows available.")
            else:
                st.dataframe(df.head(25), use_container_width=True)
                st.caption("Preview limited to the first 25 rows.")


def _preview_workbook_bytes(workbook_bytes: bytes) -> None:
    tab_entries = []
    with pd.ExcelFile(io.BytesIO(workbook_bytes)) as xl:
        for sheet_name in xl.sheet_names:
            df = xl.parse(sheet_name=sheet_name, nrows=25)
            tab_entries.append((sheet_name, df.fillna("")))
    _render_dataframe_tabs(tab_entries)


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

            contrib_df = _entries_to_dataframe(contributions).fillna("")
            other_receipts_df = _entries_to_dataframe(other_receipts).fillna("")
            in_kind_df = _entries_to_dataframe(in_kind_contributions).fillna("")
            fundraisers_df = _entries_to_dataframe(fundraisers).fillna("")
            expend_df = _entries_to_dataframe(expenditures).fillna("")

            michigan_tabs = [
                ("Contributions", contrib_df),
                ("Other Receipts", other_receipts_df),
                ("In-Kind Contributions", in_kind_df),
                ("Fundraisers", fundraisers_df),
                ("Expenditures", expend_df),
            ]
            _render_dataframe_tabs(michigan_tabs)

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

            small_contrib_df = pd.DataFrame(az_small_contrib).fillna("")
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
                operating_df = operating_df[operating_columns].fillna("")

            small_expenses_df = pd.DataFrame(az_small_expenses).fillna("")
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
                other_receipts_df = other_receipts_df[other_receipt_columns].fillna("")

            arizona_tabs = [
                ("Schedule C2 - Individual Contributions", contrib_df),
                ("Schedule In-State <=$100", small_contrib_df),
                ("Schedule E1 - Operating Expenses", operating_df),
                ("Schedule E4 - Aggregate Small Expenses", small_expenses_df),
                ("Schedule R1 - Other Receipts", other_receipts_df),
            ]
            _render_dataframe_tabs(arizona_tabs)

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

                    workbook_path = process_alaska_pofd(tmp_pdf_path, output_workbook)
                    workbook_bytes = workbook_path.read_bytes()

            st.success("Parsed Alaska POFD schedules.")

            _preview_workbook_bytes(workbook_bytes)

            workbook_stream = io.BytesIO(workbook_bytes)
            workbook_stream.seek(0)
            st.download_button(
                "Download POFD Workbook",
                data=workbook_stream,
                file_name=f"{report_stem}_pofd_schedules.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        elif parser_selection == "Federal Financial Disclosure Report":
            with st.spinner("Parsing federal financial disclosure..."):
                with tempfile.TemporaryDirectory() as pipeline_dir:
                    tmp_pdf_path = Path(pipeline_dir) / "disclosure.pdf"
                    tmp_pdf_path.write_bytes(tmp_path.read_bytes())
                    output_workbook = Path(pipeline_dir) / "financial_disclosure.xlsx"

                    workbook_path = process_disclosure_pdf(tmp_pdf_path, output_workbook)
                    workbook_bytes = workbook_path.read_bytes()

            st.success("Parsed federal financial disclosure schedules.")

            _preview_workbook_bytes(workbook_bytes)

            workbook_stream = io.BytesIO(workbook_bytes)
            workbook_stream.seek(0)
            st.download_button(
                "Download Financial Disclosure Workbook",
                data=workbook_stream,
                file_name=f"{report_stem}_financial_disclosure.xlsx",
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

            _preview_workbook_bytes(workbook_bytes)

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
