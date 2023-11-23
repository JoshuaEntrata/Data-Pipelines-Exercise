import pandas as pd
from datetime import datetime as dt

print("")
df_customer_transaction = pd.read_parquet("D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_fix_name_format.parquet")

now = dt.now()

print("Converting to datetime...")
df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'])
df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'])

print("")

print("Remove dates in future:")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction[df_customer_transaction['avail_date'] <= now]
df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= now]
print(df_customer_transaction.shape)

print("")

print("Check the latest dates:")
print(df_customer_transaction['avail_date'].max())
print(df_customer_transaction['birthday'].max())

print("")

print("Remove availment date earlier than birthday:")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction[df_customer_transaction['birthday'] <= df_customer_transaction['avail_date']]
print(df_customer_transaction.shape)

df_customer_transaction.to_parquet("D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_remove_invalid_dates.parquet")
print("")
print("Successfully removing invalid dates")