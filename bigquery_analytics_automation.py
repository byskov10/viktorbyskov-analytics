import os
from google.cloud import bigquery
from datetime import date

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "analytics-399003-e59dddbd35b5.json"
client = bigquery.Client()

today = str(date.today().strftime("%Y%d%m"))

project = "analytics-399003"
ga_dataset = "analytics_406290682"
WebAnalytics = "WebAnalytics"
tablename = f"events_{today}"

start_table = f"{project}.{ga_dataset}.{tablename}"
target_table = f"{project}.{WebAnalytics}.{tablename}"

sql = f"SELECT * FROM `{start_table}`"
df = client.query(sql).to_dataframe()

columns_to_use = ['event_date', 'event_timestamp', 'event_name', 'event_params',
       'event_bundle_sequence_id',
       'user_pseudo_id',
       'user_first_touch_timestamp', 'device', 'geo',
       'traffic_source', 'stream_id', 'platform']
df = df[columns_to_use]

e = "event_params_"
df[e + "page_title"] = None
df[e + "ga_session_id"] = None
df[e + "page_location"] = None
df[e + "session_engaged"] = None
df[e + "engagement_time_msec"] = None
df[e + "ga_session_number"] = None
df[e + "engaged_session_event"] = None
for index, row in df.iterrows():
    eventparams = row["event_params"]
    for param in eventparams:
        if param['key'] == 'page_title':
            df.at[index, "event_params_page_title"] = param['value']['string_value']
        elif param['key'] == 'ga_session_id':
            df.at[index, "event_params_ga_session_id"] = param['value']['int_value']
        elif param['key'] == 'page_location':
            df.at[index, "event_params_page_location"] = param['value']['string_value']
        elif param['key'] == 'session_engaged':
            df.at[index, "event_params_session_engaged"] = param['value']['string_value']
        elif param['key'] == 'engagement_time_msec':
            df.at[index, "event_params_engagement_time_msec"] = param['value']['int_value']
        elif param['key'] == 'ga_session_number':
            df.at[index, "event_params_ga_session_number"] = param['value']['int_value']
        elif param['key'] == 'engaged_session_event':
            df.at[index, "event_params_engaged_session_event"] = param['value']['int_value']
            break

