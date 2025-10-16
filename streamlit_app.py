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
            expenditures = parser.parse_expenditures()

        st.success(
            f"Parsed {len(contributions)} contribution entries and "
            f"{len(expenditures)} expenditure entries."
        )

        contrib_df = _entries_to_dataframe(contributions)
        expend_df = _entries_to_dataframe(expenditures)

        st.subheader("Contributions Preview")
        st.dataframe(contrib_df.head(25), use_container_width=True)

        st.subheader("Expenditures Preview")
        st.dataframe(expend_df.head(25), use_container_width=True)

        output_stream = io.BytesIO()
        with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
            contrib_df.to_excel(writer, sheet_name="Contributions", index=False)
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
