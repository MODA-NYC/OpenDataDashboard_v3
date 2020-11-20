## Open Data Dashboard v3

The dashboard is hosted via [Google Data Studio](https://datastudio.google.com/u/0/navigation/reporting). View dashboard [here](https://datastudio.google.com/reporting/69d85d4d-4d5a-486a-87ee-c3b5ec31f527) (you need viewing permissions). Make sure to be logged in with `modanycga@gmail.com` account to edit it. 

Files in this directory:

> `dashboard_v3.py` - main script. Needs to be executed daily.
> `dashboard.ipynb` - QA notebook
> `credentials.py` - supplemental module, called within `dashboard.py`. Contains credentials for Socrata and Google Sheets. Also, calls Socrata and Google Sheets APIs.
> `requirements.txt` - `python3` requirements for the dashboard script to run. (has not been updated)
> `run_cron.sh` - daily `cron` job script
> `key.json` - Google Spreadsheets authentication file. Rename this file to `.gdrive_private` and save in `~`