import pandas as pd
import re

print("")
df_branch_service = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_load_database.parquet")

print("Adding space between words...")
df_branch_service['branch_name'] = df_branch_service['branch_name'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)
df_branch_service['service'] = df_branch_service['service'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', x) if x is not None else x)

print("")

print("Converting all null equivalent values to None...")
df_branch_service['branch_name'] = df_branch_service['branch_name'].replace(['None', 'N/A', 'NA', ''], [None, None, None, None])

print("")

print("Droping null values in branch name...")
print(df_branch_service.shape)
df_branch_service = df_branch_service.dropna(subset=['branch_name'])
print(df_branch_service.shape)

print("")

print("Checking values of branch name and service...")
print(df_branch_service['branch_name'].unique())
print(df_branch_service['service'].unique())

df_branch_service.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_fix_branchservice_format.parquet")
print("")
print("Successfully fixed branch name and service format...")