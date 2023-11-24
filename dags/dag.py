from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os

def customer_txn_load_database():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\customer_transaction\customer_txn_load_database.py"
    command = f"python {path}"
    os.system(command)

def customer_txn_fix_name_formats():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\customer_transaction\customer_txn_fix_name_format.py"
    command = f"python {path}"
    os.system(command)

def customer_txn_remove_invalid_dates():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\customer_transaction\customer_txn_remove_invalid_dates.py"
    command = f"python {path}"
    os.system(command)

def customer_txn_remove_duplicates():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\customer_transaction\customer_txn_remove_duplicates.py"
    command = f"python {path}"
    os.system(command)

def branch_service_txn_load_database():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\branch_service\branch_service_txn_load_database.py"
    command = f"python {path}"
    os.system(command)

def branch_service_txn_fix_branchservice_format():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\branch_service\branch_service_txn_fix_branchservice_format.py"
    command = f"python {path}"
    os.system(command)

def branch_service_txn_fix_price_format():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\branch_service\branch_service_txn_fix_price_format.py"
    command = f"python {path}"
    os.system(command)

def branch_service_txn_remove_duplicates():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\branch_service\branch_service_txn_remove_duplicates.py"
    command = f"python {path}"
    os.system(command)

def merge_data_frames():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\merged\merged.py"
    command = f"python {path}"
    os.system(command)

def remove_duplicate_txn_id():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\merged\remove_duplicate_txn_id.py"
    command = f"python {path}"
    os.system(command)

def add_age_column():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\merged\add_age.py"
    command = f"python {path}"
    os.system(command)

def ingest_to_database():
    path = r"D:\Github\School Projects\Data Warehousing Class\LabExercise3\scripts\merged\ingest.py"
    command = f"python {path}"
    os.system(command)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes=60),
)

## customer txn
ct_load_database = PythonOperator(
    task_id='customer_txn_load_database',
    python_callable=customer_txn_load_database,
    dag=dag,
)

ct_fix_name_formats = PythonOperator(
    task_id='customer_txn_fix_name_formats',
    python_callable=customer_txn_fix_name_formats,
    dag=dag,
)

ct_remove_invalid_dates = PythonOperator(
    task_id='customer_txn_remove_invalid_dates',
    python_callable=customer_txn_remove_invalid_dates,
    dag=dag,
)

ct_remove_duplicates = PythonOperator(
    task_id='customer_txn_remove_duplicates()',
    python_callable=customer_txn_remove_duplicates(),
    dag=dag,
)

## branch service txn
bst_load_database = PythonOperator(
    task_id='branch_service_txn_load_database',
    python_callable=branch_service_txn_load_database,
    dag=dag,
)

bst_fix_branch_service_format = PythonOperator(
    task_id='branch_service_txn_fix_branchservice_format',
    python_callable=branch_service_txn_fix_branchservice_format,
    dag=dag,
)

bst_fix_price_format = PythonOperator(
    task_id='branch_service_txn_fix_price_format',
    python_callable=branch_service_txn_fix_price_format,
    dag=dag,
)

bst_remove_duplicates = PythonOperator(
    task_id='branch_service_txn_remove_duplicates',
    python_callable=branch_service_txn_remove_duplicates,
    dag=dag,
)

## merged
m_merged_data_frames = PythonOperator(
    task_id='merge_data_frames',
    python_callable=merge_data_frames,
    dag=dag,
)

m_remove_duplicate_txn_id = PythonOperator(
    task_id='remove_duplicate_txn_id',
    python_callable=remove_duplicate_txn_id,
    dag=dag,
)

m_add_age_column = PythonOperator(
    task_id='add_age_column',
    python_callable=add_age_column,
    dag=dag,
)

m_ingest_to_database = PythonOperator(
    task_id='ingest_to_database',
    python_callable=ingest_to_database,
    dag=dag,
)

ct_load_database >> ct_fix_name_formats >> ct_remove_invalid_dates >> ct_remove_duplicates >> bst_load_database >> bst_fix_branch_service_format >> bst_fix_price_format >> bst_remove_duplicates >> m_merged_data_frames >> m_remove_duplicate_txn_id >> m_add_age_column >> m_ingest_to_database
