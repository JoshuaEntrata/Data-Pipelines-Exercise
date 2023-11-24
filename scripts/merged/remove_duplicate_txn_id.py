import pandas as pd

print("")
df_merged = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\merged.parquet")

print("Dropping duplicate transaction ids...")
print(df_merged.shape)
df_merged = df_merged.drop_duplicates(subset='txn_id')
print(df_merged.shape)

print("")

print("Resetting index...")
df_merged = df_merged.reset_index(drop=True)

df_merged.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\remove_duplicate_txn_id.parquet")
print("")
print("Successfully merged data frames...")