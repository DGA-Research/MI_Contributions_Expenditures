#!/usr/bin/env python3
"""
Streamlit UI that accepts a Michigan campaign finance PDF and returns a combined
Excel workbook with contributions and expenditures on separate sheets.
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from mi_report_parser import ReportParser


st.set_page_config(page_title="MI Campaign Finance Parser", layout="wide")
st.title("Michigan Campaign Finance Parser")
st.write(
    "Upload a “Candidate Report View and Schedules” PDF to extract contributions "
    "and direct expenditures into an Excel workbook."
)


uploaded_pdf = st.file_uploader(
    "Select the PDF file",
    type=["pdf"],
    accept_multiple_files=False,
    help="The app parses the Contributions and Direct Expenditures schedules.",
)


def _entries_to_dataframe(entries) -> pd.DataFrame:
    """Convert parser entries to a tabular structure suitable for Excel."""
    rows = [entry.to_csv_row() for entry in entries]
    return pd.DataFrame(rows)


if uploaded_pdf is not None:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
        tmp_pdf.write(uploaded_pdf.getbuffer())
        tmp_path = Path(tmp_pdf.name)

    try:
        with st.spinner("Parsing PDF…"):
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

    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
