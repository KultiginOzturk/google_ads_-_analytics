from datetime import datetime
from datetime import timedelta
import mysql.connector
import numpy as np
import pandas as pd
import pymysql
from google.ads.googleads.client import GoogleAdsClient
from sqlalchemy import create_engine

# Active GoogleAds Accounts and their IDs
dict_accounts = {
    'brand_1': [['brand_1 - Managed Account', 'account_id1']],
    'brand_2': [['brand_2 - Managed Account', 'account_id2']],
    'brand_3': [['brand_3 - Managed Account', 'account_id3']],
    'brand_4': [['brand_4 - Managed Account', 'account_id4']],
    'brand_5': [['brand_5 - Managed Account', 'account_id5']]
}

# Credentials for the GoogleAds Oauth 2.0 Client in the opontia-ads-v1 google cloud project for the novimed.data@opontia.com
##Refresh token has no expire date and developer token is from Opontia MCC account's API Center part, login customer id is Opontia MCC's id.
credentials = {
    'client_id': 'client_id',
    'client_secret': 'client_secret',
    'refresh_token': 'refresh_token',
    'use_proto_plus': 'True',
    'developer_token': 'developer_token',
    # 'login_customer_id':'login_customer_id',
}

# Creating the client
client = GoogleAdsClient.load_from_dict(credentials)

# Specifiyng Service type, GoogleAds
ga_service = client.get_service("GoogleAdsService")

# Adjusting time ranges, it is %Y%m%d format because function works that way
start_date = (datetime.today() - timedelta(days=10)).date().strftime("%Y%m%d")
end_date = (datetime.today() - timedelta(days=0)).date().strftime("%Y%m%d")
print(start_date, end_date)

# metrics,dimensions and time ranges for the api call
# query= f"""SELECT
# metrics.clicks,
# metrics.conversions,
# metrics.conversions_value,
# metrics.cost_micros,
# metrics.impressions,
# campaign.name,
# customer.currency_code,
# segments.date
# FROM campaign
#  WHERE segments.date between '{start_date}' and '{end_date}'
# """

query = f"""
SELECT 
  metrics.clicks, 
  metrics.conversions, 
  metrics.conversions_value, 
  metrics.cost_micros, 
  metrics.impressions, 
  campaign.name, 
  customer.currency_code, 
  geographic_view.country_criterion_id,
  segments.date
FROM table_name 
WHERE
segments.date BETWEEN '{start_date}' AND '{end_date}'
"""

# loop iterate over every account and calls the function with specified id and query. Then another loop appends every value to the "z" list.
# stream is an iterator object so, it gets lost after opening once.
z = []
for brand, accounts in dict_accounts.items():
    for account in accounts:
        account_name = account[0]
        account_id = account[1]
        stream = ga_service.search_stream(customer_id=account_id, query=query)

        for row in stream:
            for x in row.results:
                z.append(
                    [x.segments.date, brand, account_name, x.campaign.name, x.customer.currency_code, x.metrics.clicks,
                     x.metrics.impressions,
                     x.metrics.cost_micros, x.metrics.conversions, x.metrics.conversions_value,
                     x.geographic_view.country_criterion_id, x.segments.date])
# Converting z list to dataframe with respective columns names
test = pd.DataFrame(z, columns=['dates', 'brand', 'account_name', 'campaign_name', 'currency', 'clicks', 'impressions',
                                'cost', 'conversion', 'conversion_value',
                                'country_criterion_id', 'date'
                                ])
test.drop_duplicates(inplace=True)

country_criterion_id = test.country_criterion_id.unique()

query_geo = f""" 
SELECT 
  table_name.country_code, 
  table_name.id, 
  table_name.target_type, 
  table_name.status, 
  table_name.resource_name, 
  table_name.name, 
  table_name.parent_geo_target, 
  table_name.canonical_name 
FROM table_name  WHERE table_name.id IN ({','.join(map(str, country_criterion_id))})"""

k = []
for brand, accounts in dict_accounts.items():
    for account in accounts:
        account_name = account[0]
        account_id = account[1]
        stream = ga_service.search_stream(customer_id=account_id, query=query_geo)

        for row in stream:
            for x in row.results:
                k.append(
                    [x.geo_target_constant.country_code, x.geo_target_constant.id, x.geo_target_constant.target_type,
                     x.geo_target_constant.status, x.geo_target_constant.resource_name, x.geo_target_constant.name,
                     x.geo_target_constant.parent_geo_target, x.geo_target_constant.canonical_name])
df_geo = pd.DataFrame(k, columns=['country_code', 'id', 'target_type', 'status', 'resource_name', 'name',
                                  'parent_geo_target', 'canonical_name'])

df_geo.drop_duplicates(inplace=True)

# Adding channel and ad_type columns and converting cost column to correct form.
test['ad_type'] = 'google_ads'
test['channel'] = 'D2C'

test['cost'] = test['cost'] / 1000000

# changing dates column to datetime
test['dates'] = pd.to_datetime(test.dates)
test['dates'] = test.dates.apply(lambda x: x.date())

test['account_type'] = np.where((test.campaign_name.str.lower().str.contains('tur') != 1) & (test.brand == 'brand_1'),
                                'International', 'Domestic')

# adjusting column order
test = test[
    ['dates', 'brand', 'channel', 'account_type', 'currency', 'ad_type', 'account_name', 'campaign_name', 'impressions',
     'clicks', 'cost', 'conversion', 'conversion_value',
     'country_criterion_id'
     ]]

# left join to get county data and names
test = test.merge(df_geo, how='left', left_on='table_name', right_on='id')

test = test[['dates', 'brand', 'channel', 'account_type', 'currency', 'ad_type',
             'account_name', 'campaign_name', 'impressions', 'clicks', 'cost',
             'conversion', 'conversion_value', 'country_criterion_id',
             'country_code', 'id', 'name']]

test.rename(columns={'name': 'country_name'}, inplace=True)
test.rename(columns={'id': 'country_id'}, inplace=True)
test.drop_duplicates(inplace=True)

host = "host"
user = "admin"
password = "password"
db_name = "db_name"
port = 3306

mydb = mysql.connector.connect(host=host, user=user, port=port, passwd=password, db=db_name)
cursor = mydb.cursor()

query = '''select concat('KILL ',id,'') as 'queue'
from information_schema.processlist
where Command = 'Sleep' and Time > '30' and user = 'admin' '''

kill_list = pd.read_sql(query, con=mydb)

for i in kill_list['queue']:
    cursor.execute(i)

sql = f"drop table if exists db_name.table_name"
cursor.execute(sql)
engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))
test.to_sql(f'table_name', con=engine, if_exists='replace', index=False)

# Update rows procedure
cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
cursor = cnxn.cursor()

cursor.callproc('db_name.table_name')

cnxn.commit()
cursor.close()
