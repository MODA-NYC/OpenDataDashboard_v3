from datetime import date

import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')


##### LOADING CREDENTIALS #####
import requests
import json
from io import StringIO
from df2gspread import df2gspread as d2g


#### Socrata and Google Sheets credentials are associated with modanycga@gmail.com

#### Socrata
socrata_url = "https://data.cityofnewyork.us/resource/"

#### Google Sheets

#############################################################################################
## References: 
## Google Spreadsheet Authentication:
## https://df2gspread.readthedocs.io/en/latest/overview.html
## Follow instructions in the "Access Credentials" section
#############################################################################################

## Google Spreadsheet key from the URL
## DEV:
# gs_key = '1uTuneWixsOlm5Cq8uVUzedJCM3jqNWxZ_HOkM5zAljU'
## PROD:
gs_key = '1PyZUeeo_lY3Ox6e_577aiPly_Bu0y9Vpc1_NWwe3pK4'

from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds']
gs_creds = ServiceAccountCredentials.from_json_keyfile_name('Jupyter_and_Google_Sheets-12b039cdb296.json', scope)

##### LOADING HELPER FUNCTIONS #####

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

def gs_upload(df, wks_name, creds=gs_creds):
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
        credentials=creds
    )


########## DASHBOARD ##########

#### QUANTITY ####

#### Step 1. Load Published Data Asset Inventory

# Local Law 251 of 2017: Published Data Asset Inventory
# https://data.cityofnewyork.us/City-Government/Local-Law-251-of-2017-Published-Data-Asset-Invento/5tqd-u88y
public_df = call_socrata_api('5tqd-u88y')

public_cols = [
    'datasetinformation_agency',
    'name',
    'uid',
    'url',
    'update_datemadepublic',
    'update_automation',
    'update_updatefrequency',
    'last_data_updated_date',
    'type',
    'row_count',
    'derived_view',
    'parent_uid'
]

public_df = public_df[public_cols]


#### Step 2. Get dates of the data updates

# get the dates each of datasets has been updated
dates_df = public_df[public_df.uid.isin(['5tqd-u88y','qj2z-ibhs'])]\
                [['uid', 'last_data_updated_date']]
dates_df['last_data_updated_date'] = pd.to_datetime(dates_df.last_data_updated_date, 
                                                     errors='coerce')\
                                            .dt.strftime("%Y-%m-%d")

today_df = pd.DataFrame({'uid':['NA'],
                         'last_data_updated_date':[date.today().strftime("%Y-%m-%d")],
                         'Source':['1. Dashboard']})
dates_df.loc[dates_df.uid=='5tqd-u88y','Source'] = '2. Published Asset Inventory'
dates_df.loc[dates_df.uid=='qj2z-ibhs','Source'] = '3. Open Plan Tracker'
dates_df = dates_df.append(today_df)
dates_df.reset_index(inplace=True, drop=True)
dates_df = dates_df[['Source', 'last_data_updated_date']]
dates_df.rename(columns={'last_data_updated_date':'Updated on'},inplace=True)


#### Step 3. Filter out assets

# Create merged_filter, the dataframe that has only assets defined as datasets
# ZF approved the list
dataset_filter_list = ['dataset','filter','map']
public_filtered_df = public_df[public_df.type.isin(dataset_filter_list)]

## remove derived assets if parent asset is public
# get parent ids for derived assets
parent_uids = public_filtered_df[public_filtered_df['derived_view']==True]['parent_uid']
# get ids for the assets derived from public assets
exc_parent_uids = public_filtered_df[public_filtered_df['uid'].isin(parent_uids)]['uid']
# remove derived assets if parent asset is public
public_filtered_df = public_filtered_df[~public_filtered_df['parent_uid'].isin(exc_parent_uids)]


#### Step 4. Create one main dataset-level dataframe

# fix one date typo
public_filtered_df.loc[public_filtered_df['update_datemadepublic']=='August 9, 2-019',\
                       'update_datemadepublic'] = 'August 9, 2019'

# convert to date
public_filtered_df['update_datemadepublic'] = pd.to_datetime(
                                            pd.to_datetime(public_filtered_df['update_datemadepublic'],
                                                           errors='coerce')\
                                            .dt.strftime('%m/%d/%Y'), format=('%m/%d/%Y'))
public_filtered_df['last_data_updated_date'] = pd.to_datetime(
                                                 pd.to_datetime(public_filtered_df['last_data_updated_date'])\
                                                 .dt.strftime('%m/%d/%Y'))

# if agency is missing, create NA category
public_filtered_df['datasetinformation_agency'] = public_filtered_df['datasetinformation_agency'].fillna('Not filled out')
public_filtered_df.loc[public_filtered_df['datasetinformation_agency']=='','datasetinformation_agency'] = 'Not filled out'

# keep only required columns
keep_quant_cols=[
 'uid',
 'datasetinformation_agency',
 'name',
 'url',
 'type',
 'update_datemadepublic',
 'last_data_updated_date',
 'row_count'
]
quantity_dataset_df = public_filtered_df[keep_quant_cols]


#### Step 5. Create one main agency-level dataframe

quantity_agency_df = quantity_dataset_df.groupby(['datasetinformation_agency'])\
                            .agg({'uid':'size','row_count':'sum'})\
                            .reset_index()\
                            .rename(columns={'uid':'numdatasets'})


#### QUALITY (Data Freshness) ####

#### Step 1. Build baseline dataset

freshness_df = public_filtered_df[[
    'datasetinformation_agency',
    'name',
    'uid',
    'update_updatefrequency',
    'url',
    'update_datemadepublic',
    'last_data_updated_date',
    'update_automation']]

# Remove datasets with update frequencies for which we cannot determine freshness
freshness_df = freshness_df[(~freshness_df['update_updatefrequency']\
                            .isin(['Historical Data', 'As needed'])) &\
                             ~freshness_df['update_updatefrequency'].isna()]\
                            .reset_index(drop=True)

def assign_dataframe_statuses(data):

    """
    Determines if the data has been updated on time
    """
    
    df = data.copy()

    # some values have spaces
    df['update_updatefrequency'] = df['update_updatefrequency'].str.strip()
    
    # assign time by update frequency
    status_conditions = [
        (df['update_updatefrequency']=='Annually'),
        (df['update_updatefrequency']=='Monthly'),
        (df['update_updatefrequency']=='Quarterly'),
        (df['update_updatefrequency']=='Daily'),
        (df['update_updatefrequency']=='Biannually'),
        (df['update_updatefrequency']=='Weekly'),
        (df['update_updatefrequency']=='Triannually'),
        (df['update_updatefrequency']=='Weekdays'),
        (df['update_updatefrequency']=='2 to 4 times per year'),
        (df['update_updatefrequency']=='Biweekly'),
        (df['update_updatefrequency']=='Several times per day'),
        (df['update_updatefrequency']=='Hourly'),
        (df['update_updatefrequency']=='Every four years')
    ]
    status_choices = [
        pd.Timedelta('365 days'),
        pd.Timedelta('31 days'),
        pd.Timedelta('92 days'),
        pd.Timedelta('25 hours'),
        pd.Timedelta('182 days'),
        pd.Timedelta('7 days'),
        pd.Timedelta('122 days'),
        pd.Timedelta('5 days'),
        pd.Timedelta('182 days'),
        pd.Timedelta('4 days'),
        pd.Timedelta('25 hours'),
        pd.Timedelta('25 hours'),
        pd.Timedelta('1460 days')
        ]
    
    df['update_threshold'] = np.select(status_conditions, status_choices, default=pd.Timedelta('50000 days'))
    
    # calculate when asset should have been last updated
    df['last_updated_ago'] = pd.to_datetime(date.today()) - df['last_data_updated_date']
    
    # assign status to automated, dictionary and geocoded columns
    df['fresh'] = np.where((df['last_updated_ago']>=df['update_threshold']),'No','Yes')
    
    df.drop(columns=['update_threshold'],inplace=True)
    
    return df

freshness_df = assign_dataframe_statuses(freshness_df)

keep_fresh_cols = [
 'uid',
 'datasetinformation_agency',
 'name',
 'url',
 'update_automation',
 'update_updatefrequency',
 'last_data_updated_date',
 'fresh'    
]

freshness_dataset_df = freshness_df[keep_fresh_cols]


#### Step 2. Calculate average data freshness by agency

# get the count of fresh dataset by agency
fresh_count_df = freshness_df[freshness_df['fresh']=='Yes'].groupby(['datasetinformation_agency'])\
                                .size()\
                                .reset_index()\
                                .rename(columns={0:'fresh_count'})

# get the total count of datasets by agency (excluding historical and as needed)
freshness_agency_df = freshness_df.groupby(['datasetinformation_agency'])\
                                .size()\
                                .reset_index()\
                                .rename(columns={0:'total_auto_count'})\
                                .merge(fresh_count_df, on='datasetinformation_agency',how='left')

# calculate percent freshly updated
freshness_agency_df['fresh_pct'] = freshness_agency_df['fresh_count'].fillna(0) / \
                                    freshness_agency_df['total_auto_count']


#### COMPLIANCE ####

#### Step 1. Build baseline dataset

# NYC Open Data Release Tracker
# https://data.cityofnewyork.us/City-Government/NYC-Open-Data-Release-Tracker/qj2z-ibhs
tracker_df = call_socrata_api('qj2z-ibhs')

# exclude Removed from the plan and Removed from the portal, 
release_status_filter = [
    'Released',
    'Scheduled for release',
    'Under Review'
]
tracker_df = tracker_df[tracker_df['release_status'].isin(release_status_filter)]

# convert dates to dates
tracker_df['original_plan_date'] = pd.to_datetime(tracker_df['original_plan_date'])
tracker_df['latest_plan_date'] = pd.to_datetime(tracker_df['latest_plan_date'])
tracker_df['release_date'] = pd.to_datetime(tracker_df['release_date'])

# number of days between release and planned date
tracker_df['plan_to_release'] = (tracker_df['release_date'] - tracker_df['latest_plan_date']).dt.days

# apply grace period for release date
grace_period_days = 14
today = date.today()

# create a check if released on time
tracker_df['within_grace_period'] = np.where((tracker_df['plan_to_release'] < grace_period_days), 'Yes', 'No')
tracker_df['within_grace_period_num'] = tracker_df['plan_to_release'] < grace_period_days

# subset datasets that were supposed to be released in the last 12 months
tracker_df['last_12_months'] = ((pd.to_datetime(today) - tracker_df['latest_plan_date']).dt.days < 365) & \
                                (tracker_df['latest_plan_date'] <= pd.to_datetime(today))

tracker_df['url'] = tracker_df['url1'].apply(lambda x: list(x.values())[0] \
                                                   if type(x) is dict else 'NA')
tracker_df.drop(columns=['url1'],inplace=True)

# drop duplicates for released datasets
# keep the one with the oldest release date
tracker_df = tracker_df[~tracker_df.u_id.isna()]\
                                .sort_values(by='release_date')\
                                .drop_duplicates(subset=['u_id'], keep='first')\
                                .append(tracker_df[tracker_df['u_id'].isna()])

tracker_12mo_df = tracker_df[tracker_df['last_12_months']]

tracker_12mo_df['latest_plan_date'] = tracker_12mo_df['latest_plan_date'].dt.strftime("%Y-%m-%d")
tracker_12mo_df['release_date'] = tracker_12mo_df['release_date'].dt.strftime("%Y-%m-%d")


#### Step 2. Build dataset-level dataset

keep_tracker_cols = [
 'u_id',
 'agency',
 'dataset',
 'dataset_description',
 'latest_plan_date',
 'release_status',
 'release_date',
 'within_grace_period',
 'within_grace_period_num',
 'url'
]

tracker_12mo_dataset_df = tracker_12mo_df[keep_tracker_cols]

# append type and agency from public inventory
tracker_12mo_dataset_df = tracker_12mo_dataset_df.merge(public_df[['uid','type','datasetinformation_agency']], 
                                                                    left_on='u_id',
                                                                    right_on='uid',
                                                                    how='left')

# update agency name to match public inventory (can only be done for already published datasets)
tracker_12mo_dataset_df['datasetinformation_agency'] = np.where((tracker_12mo_dataset_df.release_status=='Released') & \
                                                                ~tracker_12mo_dataset_df['datasetinformation_agency'].isna(),
                                                                tracker_12mo_dataset_df['datasetinformation_agency'],
                                                                tracker_12mo_dataset_df['agency'])

# exclude assets that are not datasets, filters and gis maps
# keeps assets scheduled for release with type NA

tracker_12mo_dataset_df = tracker_12mo_dataset_df[tracker_12mo_dataset_df['u_id'].isin(quantity_dataset_df['uid']) | \
                                                  (tracker_12mo_dataset_df['release_status']=='Scheduled for release')]
tracker_12mo_dataset_df.drop(columns=['u_id','agency'],inplace=True)

#### Step 3. Build agency-level dataset

# count number of overdue for release datasets
agency_overdue_df = tracker_12mo_dataset_df[tracker_12mo_dataset_df['release_status']=='Scheduled for release']\
                                .groupby(['datasetinformation_agency']).size().reset_index()\
                                .rename(columns={0:'overdue_datasets'})

tracker_12mo_agency_df = tracker_12mo_dataset_df.groupby(['datasetinformation_agency'])\
                                        .agg({'datasetinformation_agency':'size',
                                              'within_grace_period_num':'sum'})\
                                        .rename(columns={'datasetinformation_agency':'tracker_dataset_count',
                                                         'within_grace_period_num':'tracker_count_ontime'})\
                                        .merge(agency_overdue_df, on='datasetinformation_agency', how='left')\
                                        .reset_index(drop=True)\
                                        .fillna(0)

# calculate percent released on time
tracker_12mo_agency_df['pct_ontime'] = tracker_12mo_agency_df['tracker_count_ontime'].fillna(0)/tracker_12mo_agency_df['tracker_dataset_count']

#### DASHBOARD ####

#### Step 1. Get citywide metrics

# total number of rows
cw_numrows = quantity_agency_df['row_count'].sum()

# total number of datasets
cw_numdatasets = quantity_agency_df['numdatasets'].sum()
# percent updated on time
cw_freshness = freshness_dataset_df[freshness_dataset_df['fresh']=='Yes'].shape[0]/\
                    freshness_df.shape[0]
# percent released on time
cw_compliance = tracker_12mo_dataset_df['within_grace_period_num'].sum()/ \
                tracker_12mo_dataset_df.shape[0]

# number of assets that were supposed to be released but were not as of today 
cw_overdue = tracker_12mo_dataset_df[tracker_12mo_dataset_df['release_status']=='Scheduled for release'].shape[0]

citywide = pd.DataFrame([['Citywide',
                         cw_numrows,
                         cw_numdatasets,
                         cw_freshness,
                         cw_compliance,
                         cw_overdue]],
                       columns=['Scope',
                                'Number of rows',
                                'Number of datasets',
                                'Percent of datasets updated on time',
                                'Percent of planned releases released on time within last 12 months',
                                'Number of overdue for release datasets'])

#### Step 2. Build complete agency-level dataset

all_agency_df = quantity_agency_df.merge(freshness_agency_df, 
                                        on='datasetinformation_agency',
                                        how='outer')\
                                  .merge(tracker_12mo_agency_df, 
                                        on='datasetinformation_agency',
                                        how='outer')

all_agency_df['overdue_datasets'] = all_agency_df['overdue_datasets'].fillna(0)
all_agency_df['numdatasets'] = all_agency_df['numdatasets'].fillna(0)
all_agency_df['numrows'] = all_agency_df['row_count'].fillna(0)
all_agency_df['total_auto_count'] = all_agency_df['total_auto_count'].fillna(0)
all_agency_df['fresh_count'] = all_agency_df['fresh_count'].fillna(0)
all_agency_df['tracker_dataset_count'] = all_agency_df['tracker_dataset_count'].fillna(0)
all_agency_df['tracker_count_ontime'] = all_agency_df['tracker_count_ontime'].fillna(0)
all_agency_df['fresh_pct'] = all_agency_df['fresh_pct'].fillna('No automated datasets')
all_agency_df['pct_ontime'] = all_agency_df['pct_ontime'].fillna('No datasets in the tracker')

# maintain columns names to load data seamlessly to GDS
all_agency_df = all_agency_df[[
                'datasetinformation_agency',
                'numdatasets',
                'numrows',
                'fresh_pct',
                'pct_ontime',
                'overdue_datasets'    
]]

all_agency_df.rename(columns={
                'datasetinformation_agency':'Agency',
                'numdatasets':'Number of datasets',
                'numrows':'Number of rows',
                'fresh_pct':'Percent of datasets updated on time',
                'pct_ontime':'Percent of planned releases released on time within last 12 months',
                'overdue_datasets':'Number of overdue for release datasets'
}, inplace=True)

#### Step 3. Build complete dataset-level dataset

# aggregate freshness data and tracker data (for released datasets only)
all_datasets_df = quantity_dataset_df.merge(freshness_dataset_df[['uid',
                                                                  'update_automation',
                                                                  'update_updatefrequency',
                                                                  'fresh']], 
                                        on='uid',
                                        how='outer')\
                                  .merge(tracker_12mo_dataset_df[['uid',
                                                                  'dataset_description',
                                                                  'latest_plan_date',   
                                                                  'release_status',
                                                                  'release_date',
                                                                  'within_grace_period',
                                                                  'within_grace_period_num']], 
                                        on='uid',
                                        how='left')

# append non-released datasets data
# doing it as a separate step to keep more accurate data for released datasets

all_datasets_df = all_datasets_df.append(tracker_12mo_dataset_df[~tracker_12mo_dataset_df['uid'].isin(all_datasets_df['uid'])])\
                                 .reset_index(drop=True)

# merge name and dataset columns since they contain the same information
all_datasets_df.loc[all_datasets_df.name.isna(),'name'] = all_datasets_df['dataset']

# merge automation/update data for "historical" and "as needed" datasets
all_datasets_df = all_datasets_df.merge(public_df[['uid','update_automation','update_updatefrequency']], on='uid', how='left')

all_datasets_df['automation'] = np.where(all_datasets_df['update_automation_x'].isna(),
                                         all_datasets_df['update_automation_y'],
                                         all_datasets_df['update_automation_x'])
all_datasets_df['update_frequency'] = np.where(all_datasets_df['update_updatefrequency_x'].isna(),
                                         all_datasets_df['update_updatefrequency_y'],
                                         all_datasets_df['update_updatefrequency_x'])

# recode missing dates into string NA to properly read format in GDS
all_datasets_df['release_date_fix'] = pd.to_datetime(all_datasets_df['release_date'], errors='coerce')

all_datasets_df['fresh'] = all_datasets_df['fresh'].fillna('No regular updates')
all_datasets_df['within_grace_period'] = all_datasets_df['within_grace_period'].fillna('Not in Open Plan Tracker')

# maintain columns names to load data seamlessly to GDS

all_datasets_df= all_datasets_df[[
    'datasetinformation_agency',
    'name',
    'url',
    'type',
    'update_datemadepublic',
    'last_data_updated_date',
    'automation',
    'update_frequency',
    'fresh',
    'latest_plan_date',
    'release_date_fix',
    'within_grace_period',
    'row_count',
    'release_status',
    'dataset_description'
    ]]

all_datasets_df.rename(columns={
    'datasetinformation_agency':'Agency',
    'name':'Dataset name',
    'url':'URL',
    'type':'Asset type',
    'update_datemadepublic':'Date made public',
    'last_data_updated_date':'Last updated on',
    'automation':'Automation',
    'update_frequency':'Update frequency',
    'fresh':'Updated on time',
    'latest_plan_date':'Latest Open Data Plan release date',
    'release_date_fix':'Open Data Plan release date',
    'within_grace_period':'Planned releases released on  time within last 12mo?',
    'row_count':'Number of rows',
    'release_status':'Open Data Plan release status',
    'dataset_description':'Description'
},inplace=True)

#### Step 4. Upload data to Google Spreadsheets

gs_upload(df=citywide, 
                    wks_name='_citywide_')
print('Upload complete for citywide dataset')

gs_upload(df=all_agency_df, 
                    wks_name='_agency_')
print('Upload complete for agency dataset')

gs_upload(df=all_datasets_df, 
                    wks_name='_datasets_')
print('Upload complete for datasets dataset')

gs_upload(df=dates_df, 
                    wks_name='_dates_')
print('Upload complete for dates dataset')

print(f"Dashboard was updated on: {date.today()}")

## execute script with $ ~/ODD/dashboard_v3.py --noauth_local_webserver
