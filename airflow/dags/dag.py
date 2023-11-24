from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime as dt
import numpy as np
import sqlite3
import re


def customer_txn_load_database():
    df_customer_transaction = pd.read_json("/opt/airflow/json/customer_transaction_info.json")
    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_load_database.parquet")
    print("Successfully loaded the customer transaction database...")


def customer_txn_fix_name_formats():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_load_database.parquet")
    
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.lower()
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.lower()
    
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.title()
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.title()

    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace('\W', '', regex=True)
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace('\W', '', regex=True)

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_fix_name_format.parquet")
    print("Successfully fixed the name formats...")


def customer_txn_remove_invalid_dates():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_fix_name_format.parquet")

    now = dt.now()
    df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'])
    df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'])

    df_customer_transaction = df_customer_transaction[df_customer_transaction['avail_date'] <= now]
    df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= now]

    df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= df_customer_transaction['avail_date']]

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_invalid_dates.parquet")
    print("Successfully removed invalid dates...")


def customer_txn_remove_duplicates():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_invalid_dates.parquet")

    df_customer_transaction = df_customer_transaction.drop_duplicates()
    
    df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])

    df_customer_transaction = df_customer_transaction.dropna(axis=0, how="any")

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_duplicates.parquet")
    print("Successfully removed duplicates...")


def branch_service_txn_load_database():
    df_branch_service = pd.read_json("/opt/airflow/json/branch_service_transaction_info.json")
    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_load_database.parquet")
    print("Successfully loaded the branch service transaction database...")


def branch_service_txn_fix_branchservice_format():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_load_database.parquet")
    
    df_branch_service['branch_name'] = df_branch_service['branch_name'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)
    df_branch_service['service'] = df_branch_service['service'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)

    df_branch_service['branch_name'] = df_branch_service['branch_name'].replace(['None', 'N/A', 'NA', ''], [None, None, None, None])

    df_branch_service = df_branch_service.dropna(subset=['branch_name'])

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_branchservice_format.parquet")
    print("Successfully fixed branch name and service format...")


def branch_service_txn_fix_price_format():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_branchservice_format.parquet")

    df_branch_service['price'] = df_branch_service['price'].round(2)

    df_branch_service = df_branch_service.dropna(subset=['price'])

    df_branch_service = df_branch_service[df_branch_service['price'] > 0]

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_price_format.parquet")
    print("Successfully fixed price format...")


def branch_service_txn_remove_duplicates():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_price_format.parquet")

    df_branch_service = df_branch_service.drop_duplicates()

    df_branch_service = df_branch_service.dropna(axis=0, how="any")

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_remove_duplicates.parquet")
    print("Successfully removed duplicates...")


def merge_data_frames():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_remove_duplicates.parquet")
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_duplicates.parquet")

    df_merged = pd.merge(df_customer_transaction, df_branch_service)
    
    df_merged.to_parquet("/opt/airflow/parquet/merged/merged.parquet")
    print("Successfully merged data frames...")


def remove_duplicate_txn_id():
    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/merged.parquet")

    df_merged = df_merged.drop_duplicates(subset='txn_id')

    df_merged = df_merged.reset_index(drop=True)

    df_merged.to_parquet("/opt/airflow/parquet/merged/remove_duplicate_txn_id.parquet")
    print("Successfully removed duplicate txn ids...")


def add_age_column():
    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/remove_duplicate_txn_id.parquet")

    df_merged['birthday'] = pd.to_datetime(df_merged['birthday'])
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])

    df_merged['age'] = np.floor((df_merged['avail_date'] - df_merged['birthday']).dt.days / 365.25).astype(int)

    df_merged.to_parquet("/opt/airflow/parquet/merged/add_age.parquet")
    print("Successfully added age column...")


def ingest_to_database():
    conn = sqlite3.connect('/opt/airflow/database/merged.db')

    create_sql = "CREATE TABLE IF NOT EXISTS 'transaction' (txn_id TEXT, avail_date DATE, last_name TEXT, first_name TEXT, birthday DATE, branch_name TEXT, service TEXT, price INTEGER, age INTEGER)"
    cursor = conn.cursor()
    cursor.execute(create_sql)

    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/add_age.parquet")
    df_merged['birthday'] = pd.to_datetime(df_merged['birthday'])
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])

    for row in df_merged.itertuples():
        row_avail_date = row[2].strftime('%d-%m-%Y')
        row_birthday = row[5].strftime('%d-%m-%Y')
        insert_sql = f"INSERT INTO 'transaction' (txn_id, avail_date, last_name, first_name, birthday, branch_name, service, price, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.execute(insert_sql, (row[1], row_avail_date, row[3], row[4], row_birthday, row[6], row[7], row[8], row[9]))

    conn.commit()
    print("Successfully ingested data to database...")
    
def create_weekly_view():
    conn = sqlite3.connect('/opt/airflow/database/merged.db')
    view_sql = "CREATE VIEW weekly_summary AS \
    SELECT \
    MIN(start_week) as start_week, \
    MAX(end_week) as end_week, \
    GROUP_CONCAT(service, x'0a') as services, \
    GROUP_CONCAT(total_price, x'0a') AS total_price \
    FROM ( \
        SELECT \
        strftime('%Y-%W', avail_date) AS week, \
        date(avail_date, '-' || strftime('%w', avail_date) || ' days') AS start_week, \
        date(avail_date, '-' || (6 - strftime('%w', avail_date) % 7) || ' days') AS end_week, \
        service, \
        SUM(price) AS total_price \
        FROM 'transaction' \
        GROUP BY week, service \
    ) GROUP BY week"
    cursor = conn.cursor()
    cursor.execute(view_sql)
    conn.commit()
    print("Successfully created weekly view in database...")
    
    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/add_age.parquet")
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])
    weekly_view = df_merged.groupby([df_merged['avail_date'].dt.to_period("W-Mon"), 'service'])['price'].sum().to_frame()

    weekly_view = weekly_view.rename(columns={'price': 'Total Sales'}).rename_axis(index={'avail_date': 'Weeks', 'service': 'Service'})

    weekly_view.to_excel("weekly_view.xlsx")
    print("Successfully created weekly view in excel file...")
    

args = {
    'owner': 'JoshuaEntrata',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id = 'my_data_pipeline',
    default_args = args,
    # schedule_interval = '@hourly',
    schedule_interval = "*/2 * * * *",
    max_active_runs = 1,
)

with dag:
    ## customer txn
    ct_load_database = PythonOperator(
        task_id='customer_txn_load_database',
        python_callable=customer_txn_load_database,
    )

    ct_fix_name_formats = PythonOperator(
        task_id='customer_txn_fix_name_formats',
        python_callable=customer_txn_fix_name_formats,
    )

    ct_remove_invalid_dates = PythonOperator(
        task_id='customer_txn_remove_invalid_dates',
        python_callable=customer_txn_remove_invalid_dates,
    )

    ct_remove_duplicates = PythonOperator(
        task_id='customer_txn_remove_duplicates',
        python_callable=customer_txn_remove_duplicates,
    )

    ## branch service txn
    bst_load_database = PythonOperator(
        task_id='branch_service_txn_load_database',
        python_callable=branch_service_txn_load_database,
    )

    bst_fix_branch_service_format = PythonOperator(
        task_id='branch_service_txn_fix_branchservice_format',
        python_callable=branch_service_txn_fix_branchservice_format,
    )

    bst_fix_price_format = PythonOperator(
        task_id='branch_service_txn_fix_price_format',
        python_callable=branch_service_txn_fix_price_format,
    )

    bst_remove_duplicates = PythonOperator(
        task_id='branch_service_txn_remove_duplicates',
        python_callable=branch_service_txn_remove_duplicates,
    )

    ## merged
    m_merged_data_frames = PythonOperator(
        task_id='merge_data_frames',
        python_callable=merge_data_frames,
    )

    m_remove_duplicate_txn_id = PythonOperator(
        task_id='remove_duplicate_txn_id',
        python_callable=remove_duplicate_txn_id,
    )

    m_add_age_column = PythonOperator(
        task_id='add_age_column',
        python_callable=add_age_column,
    )

    m_ingest_to_database = PythonOperator(
        task_id='ingest_to_database',
        python_callable=ingest_to_database,
    )
    
    m_create_weekly_view = PythonOperator(
        task_id='create_weekly_view',
        python_callable=create_weekly_view,
    )

ct_load_database >> ct_fix_name_formats >> ct_remove_invalid_dates >> ct_remove_duplicates >> bst_load_database >> bst_fix_branch_service_format >> bst_fix_price_format >> bst_remove_duplicates >> m_merged_data_frames >> m_remove_duplicate_txn_id >> m_add_age_column >> m_ingest_to_database >> m_create_weekly_view
