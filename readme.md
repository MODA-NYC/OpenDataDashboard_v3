## Open Data Dashboard v3

The dashboard is hosted via [Google Data Studio](https://datastudio.google.com/u/0/navigation/reporting). Make sure to be logged in with `modanycga@gmail.com` account to edit it. 

Files in this directory:

> `dashboard.py` - main script. Needs to be executed daily.

> `credentials.py` - supplemental module, called within `dashboard.py`. Contains credentials for Socrata and Google Sheets. Also, calls Socrata and Google Sheets APIs.

> `Jupyter_and_Google_Sheets-12b039cdb296.json` - Google Sheets API credentials file (called through `credentials.py`)

> `requirements.txt` - `python3` requirements for the dashboard script to run.