import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

service_account_file = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])

table_name = 'Levis_KR_Product_excel'
csv_file = 'Product Name Korea.csv'

project_id = 'watchdog-340107'
dataset_id = 'Levis_KR_Watchdog'
table_id = f'{project_id}.{dataset_id}.{table_name}'

dataframe = pd.read_csv(csv_file)
dataframe['Date'] = pd.to_datetime(dataframe.Date, format='%Y-%m-%d')
print("Dataframe Head: \n",dataframe.head())
print("Dataframe Size: (Rows, Columns)",dataframe.shape)
print("Dataframe Types: \n",dataframe.dtypes)
print("Dataframe Head: \n",dataframe.head())

client = bigquery.Client(credentials=credentials, project=project_id)

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
print(f"Wrote {table_id} successfully")
