import pandas as pd

print("")
df_branch_service = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_fix_price_format.parquet")

print("Dropping duplicate rows...")
print(df_branch_service.shape)
df_branch_service = df_branch_service.drop_duplicates()
print(df_branch_service.shape)

print("")

print("Dropping null values...")
print(df_branch_service.shape)
df_branch_service = df_branch_service.dropna(axis=0, how="any")
print(df_branch_service.shape)

df_branch_service.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_remove_duplicates.parquet")
print("")
print("Successfully removed duplicate rows...")