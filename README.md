# Campaign Finance Parser

Streamlit app that normalizes several campaign finance parsers behind a single UI. Each workflow accepts the appropriate filing format, shows a tabbed preview (first 25 rows per sheet), and lets you download the cleaned output.

## Workflows

| Workflow | Input | Output |
| --- | --- | --- |
| Michigan Campaign Report Summary | PDF | Multi-sheet Excel (Contributions, Other Receipts, In-Kind, Fundraisers, Expenditures) |
| Arizona Campaign Finance Report | PDF | Multi-sheet Excel (Schedules C2, In-State =, E1, E4, R1) |
| Alaska POFD Schedules | PDF | Excel workbook with POFD schedules + metadata summary |
| Federal Financial Disclosure | PDF | Excel workbook with schedules (A�I) |
| Pennsylvania TXT to CSV | TXT | Cleaned CSV (no blank rows) |
| Pennsylvania Campaign Finance Report | PDF | Excel workbook from text?CSV pipeline |

## Repo Layout

`
+- streamlit_app.py          # Streamlit UI wiring all workflows
+- requirements.txt          # Dependencies
+- mi_report_parser.py       # Michigan parser
+- az_report_parser.py       # Arizona parser
+- pa_txt_parser.py          # TXT?DataFrame helper
+- alaska_project/
�  +- process_pofd_reports.py
+- disclosure_parser/
�  +- split_schedules.py
+- finance_pipeline/
   +- process_reports.py
   +- compile_pdf_to_csv.py
   +- csv_to_workbook.py
   +- extract_pdf_text.py
`

## Quick Start

`ash
pip install -r requirements.txt
streamlit run streamlit_app.py
`

1. Choose a workflow in the app.
2. Upload the corresponding file (PDF or TXT).
3. Check the tabbed preview.
4. Download the generated workbook or CSV.

All workflows share the same preview and download patterns, so the user experience stays uniform.
