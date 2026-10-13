from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator

dag = DAG(
    dag_id="test3_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
)

def extract(**context):
    ti = context['ti']
    ti.xcom_push(key='output_path', value='/path/to/data.csv')
    print("Pushed XCom with key 'output_path' and value '/path/to/data.csv'")

def transform(**context):
    ti = context["ti"]
    extracted_data = ti.xcom_pull(task_ids="extract", key="output_path")

    input_path = extracted_data["output_path"]
    print(f"Transforming data from {input_path}")


extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
)

extract_task >> transform_task