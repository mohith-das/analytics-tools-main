import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta, date

service_account_file = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])


def main():

    view_name = 'ga_jp_product_daily_kpi'
    primary_diamention = 'Product_Name'
    diamention_item = 'LR トートバッグ'
    csv_file = 'Japan Product Name.csv'

    project_id = 'watchdog-340107'
    dataset_id = 'levis_jp_watchdog'
    view_id = f'{project_id}.{dataset_id}.{view_name}'
    print(view_id)

    date_col = 'Date'
    start_date = date(2022,6,20) 
    end_date = date(2022,6,30)
    date_range = pd.date_range(start_date,end_date-timedelta(days=1),freq='d')

    
    if primary_diamention is not None:
        query_string = f"""
                SELECT
                *
                FROM `{project_id}.{dataset_id}.{view_name}` 
                WHERE ({date_col} BETWEEN '{start_date}' AND '{end_date}') AND {primary_diamention} = '{diamention_item}'
                ORDER BY {date_col}
                """
        print("Diamention Mode, Query: \n",query_string)
    else:
        query_string = f"""
                SELECT
                *
                FROM `{project_id}.{dataset_id}.{view_name}` 
                WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}' 
                ORDER BY {date_col}
                """
        print("Metric Mode, Query: \n",query_string)

    client = bigquery.Client(credentials=credentials, project=project_id)

    print("Start Date:",start_date," End Date:", end_date)

    bq_df = client.query(query_string).result().to_dataframe()
    bq_df[date_col] = pd.to_datetime(bq_df.Date, format='%Y-%m-%d')
    print("View Dataframe Head: \n",bq_df.head())
    print("View Dataframe Size: (Rows, Columns)",bq_df.shape)

    src_df = pd.read_csv(csv_file)
    src_df[date_col] = pd.to_datetime(src_df.Date)
    # src_df[date_col] = pd.to_datetime(src_df.Date, format='%Y-%m-%d')
    print("CSV Dataframe Head: \n",src_df.head())
    print("CSV Dataframe Size: (Rows, Columns)",src_df.shape)

    mask = ((src_df[date_col] >= str(start_date)) & (src_df[date_col] <= str(end_date)) & (src_df[primary_diamention] == diamention_item))
    src_masked_df = src_df.loc[mask]

    
    bq_cols = bq_df.dtypes
    src_cols = src_df.dtypes
    print("Table Schema(BQ): \n",bq_cols)
    print("Table Schema(SRC): \n",src_cols)
    
    validate_df = pd.DataFrame(columns=[date_col])
    validate_df[date_col] = date_range
    print(validate_df)



    if bq_cols.size == src_cols.size:
        for i in bq_cols.iteritems():
            for j in src_cols.iteritems():
                if (str(i[1]).lower() and str(j[1]).lower()) not in ["object","datetime64[ns]","bool"]:
                    if i[0].lower() == j[0].lower():
                        print("BQ Column: ",i[0]," | SRC Column: ",j[0])
                        
                        bq_metric_df = bq_df[[date_col, i[0]]]
                        src_metric_df = src_masked_df[[date_col, j[0]]]
                        
                        # bq_metric_df.rename(columns={i[0]: i[0]+"_BQ"})
                        # src_metric_df.rename(columns={j[0]: j[0]+"_BQ"})

                        # bq_metric_df = bq_metric_df[bq_metric_df[date_col].notnull()]
                        # src_metric_df = src_metric_df[src_metric_df[date_col].notnull()]

                        print(bq_metric_df)
                        print(src_metric_df)

                        final_df = bq_metric_df.merge(src_metric_df, on = date_col, how='left').sort_values(by=date_col,ascending=False)
                        
                        final_df.fillna(0)
                        # print(((bq_metric_df[i[0]] - src_metric_df[j[0]])/bq_metric_df[i[0]])*100)
                        final_df["% Error"]=((src_metric_df[j[0]] - bq_metric_df[i[0]])/src_metric_df[j[0]])*100
                        print(final_df)

                        # validate_df = validate_df.merge(final_df, on = date_col, how='left')
                        validate_df = validate_df.merge(final_df, on = date_col, how='left').sort_values(by=date_col,ascending=False)
                       
        print(validate_df)
        validate_df.to_csv('validation.csv')
    else:
        print("Number of Columns Do not Match!")
        


    

main()


