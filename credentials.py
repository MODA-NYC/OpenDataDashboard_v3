import pandas as pd

import requests
from requests.auth import HTTPBasicAuth

import os

from oauth2client.service_account import ServiceAccountCredentials
import gspread
from df2gspread import df2gspread as d2g

#### Socrata and Google Sheets credentials are associated with modanycga@gmail.com

#### Socrata
socrata_url = "https://data.cityofnewyork.us/resource/"
socrata_key = 'dpovnwa4lwoaka48233qr8b2f'
socrata_secret = '4taqkebdfxsaaf7sqo0qgze23enewwn50ro9jyjk9ury6qcen1'

#### Google Sheets
gs_scope = ['https://spreadsheets.google.com/feeds']
## IMPORTANT: adjust the path as necessary
# gs_credentials = ServiceAccountCredentials.from_json_keyfile_name('/media/sf_VBUM_FCTF/ODD/ODD_v3/dashboard/Jupyter_and_Google_Sheets-12b039cdb296.json', gs_scope)

gs_credentials = ServiceAccountCredentials.from_json_keyfile_name(os.getcwd()+'/Jupyter_and_Google_Sheets-12b039cdb296.json', gs_scope)

gs_key = '1PyZUeeo_lY3Ox6e_577aiPly_Bu0y9Vpc1_NWwe3pK4'

#############################################################################################
## References: 
## 1. https://socraticowl.com/post/integrate-google-sheets-and-jupyter-notebooks/
## This is a step by step guide to make Jupyter Notebooks and Google Sheets to communicate
## If data is pulled from MODA's GA account, there is no need to replicate them.
## The communication has been established!
## 2. https://df2gspread.readthedocs.io/en/latest/examples.html
## This is a general reference.
## 
## Google Spreadsheet:
## 1. create new or use existing Google Sheet
## 2. share it with client_email found in json credentials file
## "client_email": "google-sheets@<xxx>.iam.gserviceaccount.com"
## 3. copy spreadsheet key from the URL into gs_key
#############################################################################################


def call_socrata_api(uid, limit=1000000):
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
    asset_df = pd.read_json(r.text)

    return asset_df

def get_socrata_row_count(limit=1000000):
    """
    Calls Soctata API to exctract Dataset Facts
    https://data.cityofnewyork.us/dataset/Daily-Dataset-Facts/gzid-z3nh

    Kwargs:
        uid: str, Socrata id for the dataset to pull
    Returns:
        date of the dataset update, pandas df of the dataset
    """
    
    uid = "gzid-z3nh"
    
    # get the date of the last time the dataset was updated
    assets_df = call_socrata_api("r8cp-r4rc")
    last_update_date = assets_df[assets_df.u_id==uid]["last_update_date_data"]
    last_update_date = pd.to_datetime(last_update_date.values[0]).strftime("%Y-%m-%d") + "T00:00:00.000"
    
    print(f"Dataset facts dataset was last updated on: {last_update_date}")
    
    # pull the data only for the last updated date
    
    num_records = f"$limit={limit}"
    filters = f"&$where=date>'{last_update_date}'"

    r = requests.get(socrata_url + uid + ".json?" + num_records + filters, 
                    auth=HTTPBasicAuth(socrata_key, socrata_secret))
    facts_df = pd.read_json(r.text)
    
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
               credentials=gs_credentials, 
               row_names=False)

def getFromGS(wks_name):
    """
    Pulls data from Google Spreadshhet
    
    Args:
        gs_key: str, spreadhsheet key
        wks_name: str, worksheet name    
        
    Returns:
        pandas dataframe
    """
    
    gc = gspread.authorize(gs_credentials)
    workbook = gc.open_by_key(gs_key)
    worksheet = workbook.worksheet(wks_name)
    return pd.DataFrame(worksheet.get_all_values())




