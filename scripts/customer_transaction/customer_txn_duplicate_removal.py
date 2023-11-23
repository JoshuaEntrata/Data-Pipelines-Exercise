import pandas as pd

print("")
df_customer_transaction = pd.read_parquet("D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_remove_invalid_dates.parquet")

print("Dropping duplicates:")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.drop_duplicates()
print(df_customer_transaction.shape)

print("")

print("Removing duplicate transaction id:")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])
print(df_customer_transaction.shape)

print("")

print("Dropping null values:")
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.dropna()
print(df_customer_transaction.shape)

df_customer_transaction.to_parquet("D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_duplicate_removal.parquet")
print("")
print("Successfully removed duplicate rows and null values")