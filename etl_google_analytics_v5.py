from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
import json
import os
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sqlalchemy
import datetime as dt

# dictionary for active GoogleAnalytics accounts
dict_GA4 = {
    'brand1_Domestic': 'account_id1',
    # 'brand1_GLOB':'account_id2',
    'brand1_International': 'account_id2',
    'brand2_Domestic': 'account_id3',
    'brand3_Domestic': 'account_id4',
    'brand4_Domestic': 'account_id5',
    'brand5_Domestic': 'account_id6',
}

# Service account credentials to connect Google Analytics.After reading json, adding it to the environment to create client.
creds_json = {
    "type": "service_account",
    "project_id": "project_id",
    "private_key_id": "",
    "private_key": "private_key",
    "client_email": "client_email",
    "client_id": "client_id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "client_x509_cert_url"
}

open('GA-python.json', 'w').write(json.dumps(creds_json))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'GA-python.json'

# creating client
client = BetaAnalyticsDataClient()


def report(property_id, client, data_date):
    # creating client and calling function with specified dimension and metrics for the selected property.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",

        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionDefaultChannelGroup"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="country"),
            Dimension(name="city"),
            Dimension(name="currencyCode"),

        ],
        metrics=[

            Metric(name='sessions'),
            Metric(name='engagedSessions'),
            Metric(name='userEngagementDuration'),
            Metric(name='screenPageViews'),
            Metric(name='newUsers'),
            Metric(name='activeUsers'),
            Metric(name='totalUsers'),
            Metric(name='addToCarts'),
            Metric(name='conversions:purchase'),
            Metric(name='totalRevenue'),

        ],

        date_ranges=[DateRange(start_date=data_date, end_date=data_date)],
    )

    response = client.run_report(request)

    return response


# This function is for converting api call to a dataframe.

def response_to_dataframe(response):
    # Extract the column headers (dimension names and metric names)
    column_headers = []
    for dimension in response.dimension_headers:
        column_headers.append(dimension.name)
    for metric in response.metric_headers:
        column_headers.append(metric.name)

    # Extract the data rows
    data_rows = response.rows

    # Create an empty DataFrame with the extracted column headers
    df = pd.DataFrame(columns=column_headers)

    # Populate the DataFrame with data rows
    for row in data_rows:
        row_data = []
        dimensions = row.dimension_values
        metrics = row.metric_values

        # Append dimension values
        row_data.extend([dimension.value for dimension in dimensions])

        # Append metric values
        row_data.extend([metric.value for metric in metrics])

        # Create a DataFrame row from the row data
        df_row = pd.DataFrame([row_data], columns=column_headers)

        # Append the DataFrame row to the main DataFrame
        df = pd.concat([df, df_row], ignore_index=True)

    return df


# Specifiyng date ranges
start_date = (dt.datetime.today() - dt.timedelta(days=15)).strftime("%Y-%m-%d")
end_date = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
date_range = pd.date_range(start_date, end_date).strftime("%Y-%m-%d")
print(date_range)

# loop iterate over every account and converting their api call to a dataframe.Master dataframe is unified data for all accounts.
master = pd.DataFrame()
for x, y in dict_GA4.items():
    for data_date in date_range:
        temp = response_to_dataframe(report(y, client, data_date))
        temp['brand'] = x
        master = pd.concat([master, temp], axis=0)

# Adjusting Column Nmaes
master.columns = ['dates', 'channel_group', 'source', 'medium', 'campaign_name', 'country', 'city', 'currency',
                  'sessions',
                  'engaged_sessions', 'user_engagement_duration', 'screen_page_views', 'new_users', 'active_users',
                  'total_users', 'add_to_carts', 'conversion', 'conversion_value', 'brand']
# Converting values to correct forms
master.sessions = master.sessions.astype(float)
master.engaged_sessions = master.engaged_sessions.astype(float)
master.new_users = master.new_users.astype(float)
master.active_users = master.active_users.astype(float)
master.total_users = master.total_users.astype(float)
master.add_to_carts = master.add_to_carts.astype(float)
master.conversion = master.conversion.astype(float)
master.conversion_value = master.conversion_value.astype(float)
master.user_engagement_duration = master.user_engagement_duration.astype(float)
master.screen_page_views = master.screen_page_views.astype(float)

master.dates = pd.to_datetime(master.dates).apply(lambda x: x.date())

# BP_TUR account gives currency TRY and gives conversion values USD. So it should be corrected here.
master.loc[(master.brand == 'brand1_TUR') & (master.currency == 'TRY'), 'currency'] = 'USD'

# Creating brand column for brand mapping and adjusting column order
brand = master.apply(lambda x: x.brand.split('_')[0], axis=1)
account_type = master.apply(lambda x: x.brand.split('_')[1], axis=1)

master['brand'] = brand
master['account_type'] = account_type
master = master[
    ['dates', 'brand', 'account_type', 'channel_group', 'source', 'medium', 'campaign_name', 'country', 'city',
     'currency', 'sessions', 'engaged_sessions', 'user_engagement_duration', 'screen_page_views', 'new_users',
     'active_users', 'total_users', 'add_to_carts', 'conversion', 'conversion_value']]

## DB PUSH
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

engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))

check_date = start_date
master.to_sql('table_name', con=engine, if_exists='replace', index=False)

query = f"""delete from stg.google_analytics_all where dates>='{check_date}';

insert into db_name.table_name
select * from db_name.table_name;

drop table db_name.table_name;

"""

iterator = cursor.execute(query, multi=True)

while True:
    try:
        next(iterator)
    except StopIteration:
        break

mydb.commit()
cursor.close()
