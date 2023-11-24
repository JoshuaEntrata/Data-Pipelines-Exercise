import pandas as pd

print("")
df_branch_service = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_remove_duplicates.parquet")
df_customer_transaction = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\customer_transaction\customer_txn_remove_duplicates.parquet")

print("Merging customer transaction and branch service...")
df_merged = pd.merge(df_customer_transaction, df_branch_service)
print(df_merged.shape)

df_merged.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\merged.parquet")
print("")
print("Successfully merged data frames...")