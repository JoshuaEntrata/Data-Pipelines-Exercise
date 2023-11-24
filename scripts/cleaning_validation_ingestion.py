import pandas as pd
from datetime import datetime as dt
import numpy as np
import sqlite3
import re


def customer_txn_load_database():
    df_customer_transaction = pd.read_json("/opt/airflow/json/customer_transaction_info.json")
    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_load_database.parquet")


def customer_txn_fix_name_formats():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_load_database.parquet")
    
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.lower()
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.lower()
    
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.title()
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.title()

    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace('\W', '', regex=True)
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace('\W', '', regex=True)

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_fix_name_format.parquet")


def customer_txn_remove_invalid_dates():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_fix_name_format.parquet")

    now = dt.now()
    df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'])
    df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'])

    df_customer_transaction = df_customer_transaction[df_customer_transaction['avail_date'] <= now]
    df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= now]

    df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= df_customer_transaction['avail_date']]

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_invalid_dates.parquet")


def customer_txn_remove_duplicates():
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_invalid_dates.parquet")

    df_customer_transaction = df_customer_transaction.drop_duplicates()
    
    df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])

    df_customer_transaction = df_customer_transaction.dropna(axis=0, how="any")

    df_customer_transaction.to_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_duplicates.parquet")


def branch_service_txn_load_database():
    df_branch_service = pd.read_json("/opt/airflow/json/branch_service_transaction_info.json")
    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_load_database.parquet")


def branch_service_txn_fix_branchservice_format():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_load_database.parquet")
    
    df_branch_service['branch_name'] = df_branch_service['branch_name'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)
    df_branch_service['service'] = df_branch_service['service'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)

    df_branch_service['branch_name'] = df_branch_service['branch_name'].replace(['None', 'N/A', 'NA', ''], [None, None, None, None])

    df_branch_service = df_branch_service.dropna(subset=['branch_name'])

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_branchservice_format.parquet")


def branch_service_txn_fix_price_format():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_branchservice_format.parquet")

    df_branch_service['price'] = df_branch_service['price'].round(2)

    df_branch_service = df_branch_service.dropna(subset=['price'])

    df_branch_service = df_branch_service[df_branch_service['price'] > 0]

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_price_format.parquet")


def branch_service_txn_remove_duplicates():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_fix_price_format.parquet")

    df_branch_service = df_branch_service.drop_duplicates()

    df_branch_service = df_branch_service.dropna(axis=0, how="any")

    df_branch_service.to_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_remove_duplicates.parquet")


def merge_data_frames():
    df_branch_service = pd.read_parquet("/opt/airflow/parquet/branch_service/branch_service_txn_remove_duplicates.parquet")
    df_customer_transaction = pd.read_parquet("/opt/airflow/parquet/customer_transaction/customer_txn_remove_duplicates.parquet")

    df_merged = pd.merge(df_customer_transaction, df_branch_service)
    
    df_merged.to_parquet("/opt/airflow/parquet/merged/merged.parquet")


def remove_duplicate_txn_id():
    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/merged.parquet")

    df_merged = df_merged.drop_duplicates(subset='txn_id')

    df_merged = df_merged.reset_index(drop=True)

    df_merged.to_parquet("/opt/airflow/parquet/merged/remove_duplicate_txn_id.parquet")


def add_age_column():
    df_merged = pd.read_parquet("/opt/airflow/parquet/merged/remove_duplicate_txn_id.parquet")

    df_merged['birthday'] = pd.to_datetime(df_merged['birthday'])
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])

    df_merged['age'] = np.floor((df_merged['avail_date'] - df_merged['birthday']).dt.days / 365.25).astype(int)

    df_merged.to_parquet("/opt/airflow/parquet/merged/add_age.parquet")


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