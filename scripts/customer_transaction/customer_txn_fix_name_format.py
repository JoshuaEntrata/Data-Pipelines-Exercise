import pandas as pd

print("")
df_customer_transaction = pd.read_json("D:\Github\School Projects\Data Warehousing Class\LabExercise3\customer_transaction_info.json")

print("Lowercasing values...")
df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.lower()
df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.lower()

print("Capitalizing values...")
df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.title()
df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.title()

print("Removing special characters...")
df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace(
    '\W', '', regex=True)
df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace(
    '\W', '', regex=True)

df_customer_transaction.to_parquet("D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_fix_name_format.parquet")
print("")
print("Successfully fixed the naming format")