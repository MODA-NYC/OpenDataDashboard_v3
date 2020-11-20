import pandas as pd

import requests
from requests.auth import HTTPBasicAuth

import os
import json

from google.oauth2.service_account import Credentials

import gspread
from df2gspread import df2gspread as d2g

#### Socrata and Google Sheets credentials are associated with modanycga@gmail.com

#### Socrata
socrata_url = "https://data.cityofnewyork.us/resource/"
socrata_key = 'dpovnwa4lwoaka48233qr8b2f'
socrata_secret = '4taqkebdfxsaaf7sqo0qgze23enewwn50ro9jyjk9ury6qcen1'

#### Google Sheets

## Google Spreadsheet key from the URL
gs_key = '1PyZUeeo_lY3Ox6e_577aiPly_Bu0y9Vpc1_NWwe3pK4'

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

    r = requests.get(socrata_url + uid + '.json?' + num_records, 
                    auth=HTTPBasicAuth(socrata_key, socrata_secret))
    if r.status_code != 200:
        raise Exception('Error getting data')
    asset_df = pd.read_json(json.dumps(r.json()))

    return asset_df

def get_socrata_row_count(limit=10000):
    """
    Calls Soctata API to exctract Dataset Facts
    https://data.cityofnewyork.us/dataset/Daily-Dataset-Facts/gzid-z3nh

    Kwargs:
        uid: str, Socrata id for the dataset to pull
    Returns:
        date of the dataset update, pandas df of the dataset
    """
    
    uid = "gzid-z3nh"
    # https://data.cityofnewyork.us/dataset/Daily-Dataset-Facts/gzid-z3nh/data
    
    # get the date of the last time the dataset was updated
    #assets_df = call_socrata_api("r8cp-r4rc") old asset inventory id
    assets_df = call_socrata_api("kvci-ugf9")
    # https://data.cityofnewyork.us/dataset/Asset-Inventory/kvci-ugf9
    
    #last_update_date = assets_df[assets_df.uid==uid]["last_update_date_data"]
    last_update_date = assets_df[assets_df.uid==uid]["last_data_updated_date"]

    last_update_date = pd.to_datetime(last_update_date.values[0]).strftime("%Y-%m-%d") + "T00:00:00.000"
   
    print(f"Dataset facts dataset was last updated on: {last_update_date}")
    
    # pull the data only for the last updated date
    
    num_records = f"$limit={limit}"
    filters = f"&$where=date>='{last_update_date}'"

    r = requests.get(socrata_url + uid + ".json?" + num_records + filters, 
                    auth=HTTPBasicAuth(socrata_key, socrata_secret))
    if r.status_code != 200:
        raise Exception('Error getting data')
    facts_df = pd.read_json(json.dumps(r.json()))
    
    return last_update_date, facts_df

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





