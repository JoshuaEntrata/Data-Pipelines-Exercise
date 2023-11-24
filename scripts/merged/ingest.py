import pandas as pd
import sqlite3

conn = sqlite3.connect(r'D:\Github\School Projects\Data Warehousing Class\LabExercise3\database\merged.db')

create_sql = "CREATE TABLE IF NOT EXISTS 'transaction' (txn_id TEXT, avail_date DATE, last_name TEXT, first_name TEXT, birthday DATE, branch_name TEXT, service TEXT, price INTEGER, age INTEGER)"
cursor = conn.cursor()
cursor.execute(create_sql)

df_merged = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\add_age.parquet")
df_merged['birthday'] = pd.to_datetime(df_merged['birthday'])
df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])

for row in df_merged.itertuples():
    row_avail_date = row[2].strftime('%d-%m-%Y')
    row_birthday = row[5].strftime('%d-%m-%Y')
    insert_sql = f"INSERT INTO 'transaction' (txn_id, avail_date, last_name, first_name, birthday, branch_name, service, price, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.execute(insert_sql, (row[1], row_avail_date, row[3], row[4], row_birthday, row[6], row[7], row[8], row[9]))

conn.commit()