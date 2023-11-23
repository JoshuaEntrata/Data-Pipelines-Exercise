import pandas as pd
print("")
df_branch_service = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_fix_branch_name_service_format.parquet")

print("Rounding off to 2 decimal places...")
df_branch_service['price'] = df_branch_service['price'].round(2)

print("")

print("Drop rows with null values in price:")
print(df_branch_service.shape)
df_branch_service = df_branch_service.dropna(subset=['price'])
print(df_branch_service.shape)

print("")

print("Drop rows with 0 values in price:")
print(df_branch_service.shape)
df_branch_service = df_branch_service[df_branch_service['price'] > 0]
print(df_branch_service.shape)

print("")

print("Check values of branch name and service:")
print(df_branch_service.loc[(df_branch_service['price'] >= 0), ['service', 'price']].drop_duplicates())

df_branch_service.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_fix_price_format.parquet")
print("")
print("Successfully fixed branch name and service format")