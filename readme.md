## Open Data Dashboard v3

The dashboard is hosted via [Google Data Studio](https://datastudio.google.com/u/0/navigation/reporting). View dashboard [here](https://datastudio.google.com/reporting/69d85d4d-4d5a-486a-87ee-c3b5ec31f527) (you need viewing permissions). Make sure to be logged in with `modanycga@gmail.com` account to edit it. 

Files in this directory:

> `dashboard_prod.py` - main script executed hourly through GitHub Actions.

> `dashboard_dev.py` - a copy of main script for testing, executed hourly through GitHub Actions.

> `credentials.py` - supplemental module, called within main script. Contains helper functions to call Socrata and Google Sheets APIs. Does not contain any actual credentials. All credentials are stored in GitHub secrets.

> `requirements.txt` - `python3` requirements for the dashboard script to run. 