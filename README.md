# Campaign Finance Parser

Streamlit application that aggregates multiple campaign-finance parsers behind a single interface. Each workflow accepts state-specific filings, shows a tabbed preview, and offers cleaned downloads.

## Workflows

| Workflow | Input | Output |
| --- | --- | --- |
| Michigan Campaign Report Summary | PDF | Excel workbook with contributions, other receipts, in-kind contributions, fundraisers, expenditures |
| Arizona Campaign Finance Report | PDF | Excel workbook with schedules C2, In-State <= $100, E1, E4, R1 |
| Alaska POFD Schedules | PDF | Excel workbook containing POFD schedules and metadata summary |
| Federal Financial Disclosure | PDF | Excel workbook with schedules A-I |
| Pennsylvania TXT to CSV | TXT | Cleaned CSV file |
| Pennsylvania Campaign Finance Report | PDF | Excel workbook generated through the text->CSV pipeline |

## Repository Layout

- `streamlit_app.py` – Streamlit UI that orchestrates every workflow
- `requirements.txt` – Python dependencies
- `mi_report_parser.py` – Michigan parser
- `az_report_parser.py` – Arizona parser
- `pa_txt_parser.py` – TXT to DataFrame helper for Pennsylvania exports
- `alaska_project/process_pofd_reports.py` – Alaska POFD parser
- `disclosure_parser/split_schedules.py` – Federal disclosure parser
- `finance_pipeline/` – Pennsylvania text->CSV->Excel pipeline modules

## Quick Start

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Usage

1. Select a workflow in the sidebar.
2. Upload the matching file type (PDF or TXT as indicated).
3. Review the tabbed preview (first 25 rows per sheet).
4. Download the generated workbook or CSV.

All workflows share the same preview and download pattern to keep the user experience consistent across states.
