import pandas as pd

import requests

import json
from io import StringIO

from df2gspread import df2gspread as d2g

#### Socrata and Google Sheets credentials are associated with modanycga@gmail.com

#### Socrata
socrata_url = "https://data.cityofnewyork.us/resource/"

##### DELETE AUTHENTICATION #####
from requests.auth import HTTPBasicAuth
socrata_key = 'dpovnwa4lwoaka48233qr8b2f'
socrata_secret = '4taqkebdfxsaaf7sqo0qgze23enewwn50ro9jyjk9ury6qcen1'

#### Google Sheets

## Google Spreadsheet key from the URL
## DEV:
gs_key = '1uTuneWixsOlm5Cq8uVUzedJCM3jqNWxZ_HOkM5zAljU'
## PROD:
# gs_key = '1PyZUeeo_lY3Ox6e_577aiPly_Bu0y9Vpc1_NWwe3pK4'

#############################################################################################
## References: 
## Google Spreadsheet Authentication:
## https://df2gspread.readthedocs.io/en/latest/overview.html
## Follow instructions in the "Access Credentials" section
#############################################################################################


def call_socrata_api(uid, limit=100000):
    """
    Calls Soctata API to exctract a dataset based on its id

    Args:
        uid: str, Socrata id for the dataset to pull
    Returns:
        pandas df of the dataset
    """

    num_records = f"$limit={limit}"

    # r = requests.get(socrata_url + uid + '.json?' + num_records)
    r = requests.get(socrata_url + uid + '.json?' + num_records, 
                    auth=HTTPBasicAuth(socrata_key, socrata_secret))
    if r.status_code != 200:
        raise Exception('Error getting data')
    asset_df = pd.read_json(StringIO(json.dumps(r.json())))

    return asset_df

def gs_upload(df, wks_name):
    """
    Uploads df to Google Spreadsheets
    
    Args:
        gs_key: str, spreadhsheet key
        df: pandas dataframe to upload
        wks_name: str, worksheet name
    """

    d2g.upload(df=df,
               gfile=gs_key, 
               wks_name=wks_name, 
               row_names=False)





