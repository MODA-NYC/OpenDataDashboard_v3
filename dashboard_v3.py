#!/usr/bin/env python3
# coding: utf-8

from datetime import date

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np

import credentials

#### QUANTITY ####

#### Step 1. Get number of rows

# pull the data with row counts and the date of its latest update
# https://data.cityofnewyork.us/dataset/Daily-Dataset-Facts/gzid-z3nh
row_count_updated, row_count_df = credentials.get_socrata_row_count()
dfacts = row_count_df[['asset_title',
                       'asset_id_4x4',
                       'agency',
                       'asset_rows']]\
                .drop_duplicates(subset=['asset_id_4x4'])
dfacts['asset_rows'] = pd.to_numeric(dfacts.asset_rows)

dfacts_agency_df = dfacts.groupby(['agency'])['asset_rows']\
                         .sum()\
                         .reset_index()\
                         .rename(columns={'asset_rows':'numrows'})

#### Step 2. Get dates of the data updates

# Asset Inventory (Private Access)
# https://data.cityofnewyork.us/dataset/Asset-Inventory/r8cp-r4rc
private_df = credentials.call_socrata_api('r8cp-r4rc')

# get the dates each of datasets has been updated
dates_df = private_df[private_df.u_id.isin(['gzid-z3nh','5tqd-u88y','qj2z-ibhs'])]\
                [['u_id', 'last_update_date_data']]
dates_df['last_update_date_data'] = pd.to_datetime(dates_df.last_update_date_data, 
                                                     errors='coerce')\
                                            .dt.strftime("%Y-%m-%d")

today_df = pd.DataFrame({'u_id':['NA'],
                         'last_update_date_data':[date.today().strftime("%Y-%m-%d")],
                         'Source':['1. Dashboard']})

dates_df.loc[dates_df.u_id=='gzid-z3nh','Source'] = '2. Row Count'
dates_df.loc[dates_df.u_id=='5tqd-u88y','Source'] = '3. Published Asset Inventory'
dates_df.loc[dates_df.u_id=='qj2z-ibhs','Source'] = '4. Open Plan Tracker'
dates_df = dates_df.append(today_df)
dates_df.reset_index(inplace=True, drop=True)
dates_df = dates_df[['Source', 'last_update_date_data']]


#### Step 3. Get number of datasets

# Local Law 251 of 2017: Published Data Asset Inventory
# https://data.cityofnewyork.us/City-Government/Local-Law-251-of-2017-Published-Data-Asset-Invento/5tqd-u88y
public_df = credentials.call_socrata_api('5tqd-u88y')

public_df = public_df[[
 'agency',
 'name',
 'u_id',
 'dataset_link',
 'date_made_public',
 'automation',
 'update_frequency',
 'last_update_date_data']]

# asset inventory has "type" of asset column (published view does not)
private_df = private_df[['u_id','type','derived_view']]
public_df = public_df.merge(private_df,on='u_id',how='left')

# Create merged_filter, the dataframe that has only assets defined as datasets
# ZF approved the list
dataset_filter_list = ['dataset','filter', 'gis map']
public_filtered_df = public_df[public_df.type.isin(dataset_filter_list)]

#### Step 4. Create one main dataset-level dataframe

# extract URL
public_filtered_df['dataset_link'] = public_filtered_df['dataset_link']\
                                            .apply(lambda x: list(x.values())[0])

# convert to date
# fix one date typo
public_filtered_df.loc[public_filtered_df['date_made_public']=='August 9, 2-019',\
                       'date_made_public'] = 'August 9, 2019'

public_filtered_df['date_made_public_dt'] = pd.to_datetime(
                                            pd.to_datetime(public_filtered_df['date_made_public'],
                                                           errors='coerce')\
                                            .dt.strftime('%m/%d/%Y'), format=('%m/%d/%Y'))
public_filtered_df['last_update_date_data_dt'] = pd.to_datetime(
                                                 pd.to_datetime(public_filtered_df['last_update_date_data'])\
                                                 .dt.strftime('%m/%d/%Y'))

public_filtered_df.drop(columns=['date_made_public','last_update_date_data'],inplace=True)

# append number of rows
quantity_dataset_df = public_filtered_df.merge(dfacts[['asset_id_4x4','asset_rows']], 
                                           left_on='u_id',
                                           right_on='asset_id_4x4',
                                           how='left')
quantity_dataset_df.rename(columns={'asset_rows':'numrows'}, inplace=True)

keep_quant_cols=[
 'u_id',
 'agency',
 'name',
 'dataset_link',
 'type',
 'date_made_public_dt',
 'last_update_date_data_dt',
 'numrows'
]

quantity_dataset_df = quantity_dataset_df[keep_quant_cols]

#### Step 5. Create one main agency-level dataframe

# if agency is missing, create NA category
quantity_dataset_df['agency'] = quantity_dataset_df.agency.fillna('NA')
quantity_agency_df = quantity_dataset_df.groupby(['agency'])\
                            .agg({'u_id':'size','numrows':'sum'})\
                            .reset_index()\
                            .rename(columns={'u_id':'numdatasets'})

#### QUALITY (Data Freshness) ####

#### Step 1. Build baseline dataset

freshness_df = public_filtered_df[[
    'agency',
    'name',
    'u_id',
    'update_frequency',
    'dataset_link',
    'date_made_public_dt',
    'last_update_date_data_dt',
    'automation']]

# Remove datasets with update frequencies for which we cannot determine freshness
freshness_df = freshness_df[(~freshness_df['update_frequency']\
                            .isin(['Historical Data', 'As needed'])) &\
                             ~freshness_df['update_frequency'].isna()]\
                            .reset_index(drop=True)

def assign_dataframe_statuses(data):

    """
    Determines if the data has been updated on time
    """
    
    df = data.copy()

    # some values have spaces
    df['update_frequency'] = df.update_frequency.str.strip()
    
    # assign time by update frequency
    status_conditions = [
        (df['update_frequency']=='Annually'),
        (df['update_frequency']=='Monthly'),
        (df['update_frequency']=='Quarterly'),
        (df['update_frequency']=='Daily'),
        (df['update_frequency']=='Biannually'),
        (df['update_frequency']=='Weekly'),
        (df['update_frequency']=='Triannually'),
        (df['update_frequency']=='Weekdays'),
        (df['update_frequency']=='2 to 4 times per year'),
        (df['update_frequency']=='Biweekly'),
        (df['update_frequency']=='Several times per day'),
        (df['update_frequency']=='Hourly'),
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
        pd.Timedelta('25 hours')
        ]
    
    df['update_threshold'] = np.select(status_conditions, status_choices, default=pd.Timedelta('50000 days'))
    
    # calculate when asset should have been last updated
    df['last_updated_ago'] = pd.to_datetime(date.today()) - df.last_update_date_data_dt
    
    # assign status to automated, dictionary and geocoded columns
    df['fresh'] = np.where((df['last_updated_ago']>=df['update_threshold']),'No','Yes')
    
    df.drop(columns=['update_threshold'],inplace=True)
    
    return df

freshness_df = assign_dataframe_statuses(freshness_df)

# ensure that datasets with missing agency value are accounted for
freshness_df['agency'] = freshness_df.agency.fillna('NA')

keep_fresh_cols = [
 'u_id',
 'agency',
 'name',
 'dataset_link',
 'automation',
 'update_frequency',
 'last_update_date_data_dt',
 'fresh'    
]

freshness_dataset_df = freshness_df[keep_fresh_cols]

#### Step 2. Calculate average data freshness by agency

# get the count of fresh dataset by agency
fresh_count_df = freshness_df[freshness_df.fresh=='Yes'].groupby(['agency'])\
                                .size()\
                                .reset_index()\
                                .rename(columns={0:'fresh_count'})

# get the total count of datasets by agency (excluding historical and as needed)
freshness_agency_df = freshness_df.groupby(['agency'])\
                                .size()\
                                .reset_index()\
                                .rename(columns={0:'total_auto_count'})\
                                .merge(fresh_count_df, on='agency',how='left')

# calculate percent freshly updated
freshness_agency_df['fresh_pct'] = freshness_agency_df.fresh_count / freshness_agency_df.total_auto_count

#### COMPLIANCE ####

#### Step 1. Build baseline dataset

# NYC Open Data Release Tracker
# https://data.cityofnewyork.us/City-Government/NYC-Open-Data-Release-Tracker/qj2z-ibhs
tracker_df = credentials.call_socrata_api('qj2z-ibhs')

# exclude Removed from the plan and Removed from the portal, 
release_status_filter = [
    'Released',
    'Scheduled for release',
    'Under Review'
]
tracker_df = tracker_df[tracker_df.release_status.isin(release_status_filter)]

# apply grace period for release date
grace_period_days = 14
today = date.today()

tracker_df['original_plan_date_dt'] = pd.to_datetime(tracker_df.original_plan_date)
tracker_df['latest_plan_date_dt'] = pd.to_datetime(tracker_df.latest_plan_date)
tracker_df['release_date_dt'] = pd.to_datetime(tracker_df.release_date)

# number of days between release and planned date
tracker_df['plan_to_release'] = (tracker_df.release_date_dt - tracker_df.latest_plan_date_dt).dt.days

# create a check if released on time
tracker_df['within_grace_period'] = np.where((tracker_df['plan_to_release'] < grace_period_days), 'Yes', 'No')
tracker_df['within_grace_period_num'] = tracker_df['plan_to_release'] < grace_period_days

# subset datasets that were supposed to be released in the last 12 months
tracker_df['last_12_months'] = ((pd.to_datetime(today) - tracker_df.latest_plan_date_dt).dt.days < 365) & \
                                (tracker_df.latest_plan_date_dt <= pd.to_datetime(today))

tracker_df['dataset_link'] = tracker_df['url1']\
                                            .apply(lambda x: list(x.values())[0] \
                                                   if type(x) is dict else 'NA')
# drop duplicates for released datasets
# keep the one with the oldest release date
tracker_df = tracker_df[~tracker_df.u_id.isna()]\
                                .sort_values(by='release_date_dt')\
                                .drop_duplicates(subset=['u_id'], keep='first')\
                                .append(tracker_df[tracker_df.u_id.isna()])


tracker_12mo_df = tracker_df[tracker_df.last_12_months]

tracker_12mo_df['latest_plan_date_dt'] = tracker_12mo_df.latest_plan_date_dt.dt.strftime("%Y-%m-%d")
tracker_12mo_df['release_date_dt'] = tracker_12mo_df.release_date_dt.dt.strftime("%Y-%m-%d")

#### Step 2. Build dataset-level dataset

keep_tracker_cols = [
 'u_id',
 'agency',
 'dataset',
 'dataset_description',
 'latest_plan_date_dt',
 'release_status',
 'release_date_dt',
 'within_grace_period',
 'within_grace_period_num',
 'dataset_link'
]

tracker_12mo_dataset_df = tracker_12mo_df[keep_tracker_cols]

# drop duplicates for released datasets
# keep the one with the oldest release date
# tracker_12mo_dataset_clean_df = tracker_12mo_dataset_df[~tracker_12mo_dataset_df.u_id.isna()]\
#                                 .sort_values(by='release_date_dt')\
#                                 .drop_duplicates(subset=['u_id'], keep='first')\
#                                 .append(tracker_12mo_dataset_df[tracker_12mo_dataset_df.u_id.isna()])

# append type and agency from public inventory
tracker_12mo_dataset_df = tracker_12mo_dataset_df.merge(public_df[['u_id','type','agency']], 
                                                                    on='u_id',
                                                                    how='left')

# update agency name to match public inventory (can only be done for already published datasets)
tracker_12mo_dataset_df['agency'] = np.where((tracker_12mo_dataset_df.release_status=='Released') & \
                                                   ~tracker_12mo_dataset_df.agency_y.isna(),
                                                  tracker_12mo_dataset_df.agency_y,
                                                  tracker_12mo_dataset_df.agency_x)
tracker_12mo_dataset_df.drop(columns=['agency_x', 'agency_y'], inplace=True)

#### Step 3. Build agency-level dataset
tracker_12mo_agency_df = tracker_12mo_dataset_df.groupby(['agency'])\
                                        .agg({'agency':'size',
                                              'within_grace_period_num':'sum'})\
                                        .rename(columns={'agency':'tracker_dataset_count',
                                                         'within_grace_period_num':'tracker_count_ontime'})\
                                        .reset_index()

# calculate percent released on time
tracker_12mo_agency_df['pct_ontime'] = tracker_12mo_agency_df.tracker_count_ontime/tracker_12mo_agency_df.tracker_dataset_count

#### DASHBOARD ####

#### Step 1. Get citywide metrics

# total number of rows
cw_numrows = quantity_agency_df.numrows.sum()

# total number of datasets
cw_numdatasets = quantity_agency_df.numdatasets.sum()
# percent updated on time
cw_freshness = freshness_dataset_df[freshness_dataset_df.fresh=='Yes'].shape[0]/\
                    freshness_df.shape[0]
# percent released on time
cw_compliance = tracker_12mo_dataset_df.within_grace_period_num.sum()/ \
                tracker_12mo_dataset_df.shape[0]

citywide = pd.DataFrame([['Citywide',
                         cw_numrows,
                         cw_numdatasets,
                         cw_freshness,
                         cw_compliance]],
                       columns=['Scope',
                                'Number of published rows',
                                'Number of published datasets',
                                'Percent of datasets updated on time',
                                'Percent of datasets released on time in the last 12 months'])

citywide = citywide.fillna('NA')

#### Step 2. Build complete agency-level dataset

all_agency_df = quantity_agency_df.merge(freshness_agency_df, 
                                        on='agency',
                                        how='outer')\
                                  .merge(tracker_12mo_agency_df, 
                                        on='agency',
                                        how='outer')

all_agency_df = all_agency_df.fillna('NA')

#### Step 3. Build complete dataset-level dataset

# aggregate freshness data and tracker data (for released datasets only)
all_datasets_df = quantity_dataset_df.merge(freshness_dataset_df.drop(columns=
                                                                      ['agency',
                                                                      'name',
                                                                      'dataset_link',
                                                                      'last_update_date_data_dt']), 
                                        on='u_id',
                                        how='outer')\
                                  .merge(tracker_12mo_dataset_df.drop(columns=
                                                                            ['agency',
                                                                             'dataset',
                                                                             'type',
                                                                             'dataset_link']), 
                                        on='u_id',
                                        how='left')

# append non-released datasets data
# doing it as a separate step to keep more accurate data for released datasets
all_datasets_df = all_datasets_df.append(tracker_12mo_dataset_df[~tracker_12mo_dataset_df.u_id.isin(all_datasets_df.u_id)])\
                                 .reset_index(drop=True)

# merge name and dataset columns since they contain the same information
all_datasets_df.loc[all_datasets_df.name.isna(),'name'] = all_datasets_df.dataset

# merge automation/update data for "historical" and "as needed" datasets
all_datasets_df = all_datasets_df.merge(public_df[['u_id','automation','update_frequency']], on='u_id', how='left')
all_datasets_df['automation'] = all_datasets_df.automation_x
all_datasets_df['update_frequency'] = all_datasets_df.update_frequency_x
all_datasets_df.loc[all_datasets_df.automation_x.isna(),'automation'] = all_datasets_df.automation_y
all_datasets_df.loc[all_datasets_df.update_frequency_x.isna(),'update_frequency'] = all_datasets_df.update_frequency_y

# recode missing dates into string NA to properly read format in GDS
all_datasets_df['release_date_dt_fix'] = pd.to_datetime(all_datasets_df.release_date_dt, errors='coerce')

all_datasets_df = all_datasets_df[[
 'agency',
 'u_id',
 'name',
 'dataset_description',
 'dataset_link',
 'type',
 'date_made_public_dt',
 'numrows',
 'automation',
 'update_frequency',
 'last_update_date_data_dt',
 'fresh',
 'latest_plan_date_dt',
 'release_status',
 'release_date_dt_fix',
 'within_grace_period']]

all_datasets_df = all_datasets_df.fillna('NA')

#### Step 4. Upload data to Google Spreadsheets

credentials.gs_upload(df=citywide, 
                    wks_name='_citywide_')
print('Upload complete for citywide dataset')

credentials.gs_upload(df=all_agency_df, 
                    wks_name='_agency_')
print('Upload complete for agency dataset')

credentials.gs_upload(df=all_datasets_df, 
                    wks_name='_datasets_')
print('Upload complete for datasets dataset')

credentials.gs_upload(df=dates_df, 
                    wks_name='_dates_')
print('Upload complete for dates dataset')