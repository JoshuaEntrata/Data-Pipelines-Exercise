import pandas as pd

print("")
df_customer_transaction = pd.read_json(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\customer_transaction_info.json")
print(df_customer_transaction.shape)

print("")
print("Succesfully loaded the Customer Transaction data set...")

df_customer_transaction.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_load_database.parquet")
print("")