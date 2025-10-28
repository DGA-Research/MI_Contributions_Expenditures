# Campaign Finance Parser

This Streamlit application unifies several state-level campaign finance parsers into one interface. Each workflow parses or converts a specific filing format and presents the results in consistent tabbed previews with downloadable outputs.

## Included Workflows

- **Michigan Campaign Report Summary and Schedules** (mi_report_parser.py)
- **Arizona Campaign Finance Report** (z_report_parser.py)
- **Alaska POFD Schedules** (laska_project/process_pofd_reports.py)
- **Federal Financial Disclosure Report** (disclosure_parser/split_schedules.py)
- **Pennsylvania TXT to CSV Converter** (pa_txt_parser.py)
- **Pennsylvania Campaign Finance Report** (inance_pipeline/process_reports.py)

## Project Structure

`
MI_Contributions_Expenditures-main/
+- streamlit_app.py             # Streamlit UI orchestrating all workflows
+- requirements.txt             # Python dependencies
+- mi_report_parser.py          # Michigan parser
+- az_report_parser.py          # Arizona parser
+- pa_txt_parser.py             # Pennsylvania TXT helper
+- alaska_project/
¦  +- process_pofd_reports.py   # Alaska POFD parser
¦  +- __init__.py
+- disclosure_parser/
¦  +- split_schedules.py        # Federal disclosure parser
¦  +- __init__.py
+- finance_pipeline/
   +- process_reports.py        # PA PDF pipeline
   +- compile_pdf_to_csv.py
   +- csv_to_workbook.py
   +- extract_pdf_text.py
   +- __init__.py
`

## Setup

`ash
pip install -r requirements.txt
streamlit run streamlit_app.py
`

## Usage

1. Select a workflow from the radio buttons.
2. Upload the relevant file (PDF for most workflows, TXT for the PA converter).
3. Review the tabbed preview (first 25 rows per sheet/schedule).
4. Download the generated workbook or CSV.

Each workflow uses shared helpers for consistent previews and conversions, keeping the UI uniform across different filing types.
