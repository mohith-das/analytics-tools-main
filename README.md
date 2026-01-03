# Analytics-Tools-main

Portfolio-ready Python utilities for loading CSVs to BigQuery and validating metrics. All credentials are supplied via environment variables.

## Components
- `csv_to_bq.py`: reads a CSV, coerces date types, and writes to a BigQuery table.
- `validate_script.py`: queries a BigQuery view and compares to a local CSV slice to spot mismatches.

## Configure
- `SERVICE_ACCOUNT_FILE` or `GOOGLE_APPLICATION_CREDENTIALS` – service account JSON for BigQuery.
- Update `project_id`, `dataset_id`, table/view names, and CSV file paths in the scripts.

## Run locally
```bash
pip install -r requirements.txt  # if present
python csv_to_bq.py
python validate_script.py
```

## Notes
- CSVs in this repo are sample placeholders; replace with non-sensitive data.
- Add schema validation and error handling before production use.
