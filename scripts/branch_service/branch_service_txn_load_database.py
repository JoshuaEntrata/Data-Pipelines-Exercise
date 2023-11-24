import pandas as pd

print("")
df_branch_service = pd.read_json(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\branch_service_transaction_info.json")

print("")
print("Succesfully loaded the Branch Service Transaction data set...")

df_branch_service.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\branch_service\branch_service_txn_load_database.parquet")
print("")