import datetime as dt
import json
import os
import mysql.connector
import numpy as np
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest
from sqlalchemy import create_engine

email = "email"

## Set up global variables

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
# property_id = 'GA4_property_id'

# property ids
dicts_creds = {
    'brand1_KSA': ['348812522', "KSA", "brand1"],
    'brand2_TUR': ['352185881', "TUR", "brand2"],
    'brand3_UAE': ['345690142', "UAE", "brand3"],
    'brand4_KSA': ['345737546', "KSA", "brand4"],
    'brand5_POL': ['346710205', "POL", "brand5"],
    'brand6_KSA': ['323024243', "KSA", "brand6"],
    'brand7_UAE': ['341444665', "UAE", "brand7"],
    'brand8_POL': ['322693030', "POL", "brand8"],
    'brand9_UAE': ['331601187', "UAE", "brand9"],
    'brand10_KSA': ['323041903', "KSA", "brand10"],
    'brand11_TUR': ["354466502", "TUR", "brand11"],
    'brand12_TUR': ["354298777", "TUR", "brand12"],
    'brand13_TUR': ["359046127", "TUR", "brand13"],
}

client = BetaAnalyticsDataClient()


## Format Report - run_report method
def format_report(request):
    response = client.run_report(request)

    # Row index
    row_index_names = [header.name for header in response.dimension_headers]
    row_header = []
    for i in range(len(row_index_names)):
        row_header.append([row.dimension_values[i].value for row in response.rows])

    row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names=np.array(row_index_names))
    # Row flat data
    metric_names = [header.name for header in response.metric_headers]
    data_values = []
    for i in range(len(metric_names)):
        data_values.append([row.metric_values[i].value for row in response.rows])

    output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')),
                          index=row_index_named, columns=metric_names)
    output.reset_index(inplace=True)
    output["date"] = pd.to_datetime(output["date"])

    return output


metric_list = [
    Metric(name="activeUsers"),
    Metric(name="addToCarts"),
    Metric(name="averageSessionDuration"),
    Metric(name="bounceRate"),
    Metric(name="cartToViewRate"),
    Metric(name="checkouts"),
    Metric(name="conversions"),
    Metric(name="engagedSessions"),
    Metric(name="engagementRate"),
    Metric(name="eventCount"),
    Metric(name="eventCountPerUser"),
    Metric(name="sessionConversionRate"),
    Metric(name="sessions"),
    Metric(name="totalAdRevenue"),
    Metric(name="totalRevenue"),
    Metric(name="totalUsers"),
    Metric(name="userConversionRate"),
]

start_date = (dt.datetime.today() - dt.timedelta(days=30)).strftime("%Y-%m-%d")
start_date = "2023-02-01"

# getting all metrics at a basic level
list_of_df_brands = []

for i in dicts_creds.items():
    brand = i[1][2]
    country = i[1][1]
    property_id = i[1][0]

    print(brand, country, property_id)

    list_of_dfs = []

    for j in range(int(len(metric_list) / 10) + 1):
        request = RunReportRequest(
            property='properties/' + property_id,

            dimensions=[Dimension(name="date"),
                        Dimension(name="sessionCampaignName"),
                        Dimension(name="sessionSource"),
                        Dimension(name="sessionMedium"),
                        ],

            metrics=metric_list[j * 10:(j + 1) * 10],

            order_bys=[OrderBy(dimension={'dimension_name': 'date',
                                          'dimension_name': 'sessionCampaignName',
                                          'dimension_name': 'sessionSource',
                                          'dimension_name': 'sessionMedium',
                                          }
                               )
                       ],

            date_ranges=[DateRange(start_date="2023-02-01", end_date="today")],
        )
        df_temp = format_report(request)
        if j == 0:
            df_temp["brand"] = brand
            df_temp["country"] = country
        list_of_dfs.append(df_temp)
        df_base = pd.DataFrame(pd.date_range(start="2023-02-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"),
                               columns=["date"])

    for i in range(len(list_of_dfs)):
        if i == 0:
            # left join the base df with the report df
            df = df_base.merge(list_of_dfs[i], how="left", on=["date"])
        else:
            df = df.merge(list_of_dfs[i], on=['date', 'sessionCampaignName', "sessionSource", "sessionMedium"],
                          how='outer')

    df["brand"] = brand
    df["country"] = country

    list_of_df_brands.append(df)

df = pd.concat(list_of_df_brands, ignore_index=True)

df = df[['brand', 'country', 'date', 'sessionCampaignName', 'sessionSource', 'sessionMedium',
         'activeUsers', 'addToCarts', 'averageSessionDuration', 'bounceRate',
         'cartToViewRate', 'checkouts', 'conversions', 'engagedSessions',
         'engagementRate', 'eventCount', 'eventCountPerUser',
         'sessionConversionRate', 'sessions', 'totalAdRevenue', 'totalRevenue',
         'totalUsers', 'userConversionRate']]

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
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name,
                                                            encoding='utf8'))
df.to_sql(f'table_name', con=engine, if_exists='replace', index=False, chunksize=1000)

cursor.close()
