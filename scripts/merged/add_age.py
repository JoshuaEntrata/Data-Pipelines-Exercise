import pandas as pd
import numpy as np

df_merged = pd.read_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\remove_duplicate_txn_id.parquet")

print("Adding age column...")
df_merged['birthday'] = pd.to_datetime(df_merged['birthday'])
df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'])

df_merged['age'] = np.floor((df_merged['avail_date'] - df_merged['birthday']).dt.days / 365.25).astype(int)

df_merged.to_parquet(r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\parquet\merged\add_age.parquet")
print("")
print("Successfully added age column...")