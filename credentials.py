import requests
import json
from io import StringIO

import os
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
import numpy as np
from datetime import date, datetime

import time

import warnings
warnings.filterwarnings('ignore')

#### Socrata and Google Sheets credentials are associated with modanycga@gmail.com

#### SETTING UP SOCRATA ####

socrata_url = "https://data.cityofnewyork.us/resource/"

def call_socrata_api(uid, limit=100000):
    """
    Calls Soctata API to exctract a dataset based on its id

    Args:
        uid: str, Socrata id for the dataset to pull
    Returns:
        pandas df of the dataset
    """

    num_records = f"$limit={limit}"

    r = requests.get(socrata_url + uid + '.json?' + num_records)
    if r.status_code != 200:
        raise Exception('Error getting data')
    asset_df = pd.read_json(StringIO(json.dumps(r.json())))

    return asset_df

#### SETTING UP GOOGLE SPREADSHEETS ####

# getting google service account credentials from 
# GitHub secrets and writing them to a file

google_credential = os.getenv('GS_CREDENTIALS')
home_path = os.getenv('HOME')
creds_location = os.path.join(home_path,'service_account.json')

with open(creds_location, 'w') as f:
    f.write(google_credential)

scope = ['https://spreadsheets.google.com/feeds']
gs_creds = ServiceAccountCredentials.from_json_keyfile_name(creds_location, scope)

## Google Spreadsheet key from the URL
## DEV:
gs_key = os.getenv('GS_ODD_DEV_KEY')

def gs_upload(df, wks_name):
    """
    Uploads df to Google Spreadsheets
    
    Args:
        gs_key: str, spreadhsheet key
        df: pandas dataframe to upload
        wks_name: str, worksheet name
    """
    d2g.upload(
        df=df,
        gfile=gs_key, 
        wks_name=wks_name, 
        row_names=False,
        credentials=gs_creds
    )