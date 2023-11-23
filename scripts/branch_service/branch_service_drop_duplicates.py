import pandas as pd

print("")
df_branch_service = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_fix_price_format.parquet")

print("Drop duplicate rows:")
print(df_branch_service.shape)
df_branch_service = df_branch_service.drop_duplicates()
print(df_branch_service.shape)

df_branch_service.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_drop_duplicates.parquet")
print("")
print("Successfully drop duplicate rows")