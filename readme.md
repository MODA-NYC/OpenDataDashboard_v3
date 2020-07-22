## Open Data Dashboard v3

The dashboard is hosted via [Google Data Studio](https://datastudio.google.com/u/0/navigation/reporting). View dashboard [here](https://datastudio.google.com/s/pWVm-fDdrzc) (you need viewing permissions). Make sure to be logged in with `modanycga@gmail.com` account to edit it. 

Files in this directory:

> `dashboard.py` - main script. Needs to be executed daily.
> 'dashboard.ipynb' revised script based on dashboard.py (use this)

> `credentials.py` - supplemental module, called within `dashboard.py`. Contains credentials for Socrata and Google Sheets. Also, calls Socrata and Google Sheets APIs.

> `Jupyter_and_Google_Sheets-12b039cdb296.json` - Google Sheets API credentials file (called through `credentials.py`)

> 'client_secret...' - Google sheets OAuth 2.0 Client ID's downloaded from https://console.developers.google.com/apis/credentials?project=ultra-envoy-236718. This file should be moved to /home/User/.gdrive_private in order to get the script to run.

> `requirements.txt` - `python3` requirements for the dashboard script to run. (has not been updated)