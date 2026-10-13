from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum
import requests
import json

dag = DAG(dag_id="API_Incremental_Data_Load",
          start_date=datetime(2026, 1, 1),
          schedule=CronDataIntervalTimetable(cron="0 * * * *",
                                             timezone=pendulum.timezone("Asia/Kolkata")),
          )

def fetch_api_data(**context):
    url = context['templates_dict']['url']
    output_path = context['templates_dict']['output_path']
    start_date = context['ds']
    end_date = context['ds']
    payload = json.dumps({
                "start_date": start_date,
                "end_date": end_date
                })

    headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Basic YWRtaW46bWFuaXNo'
            }

    response = requests.request("POST", url, headers=headers, data=payload)
    data = response.json()

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved API output to: {output_path}")


pull_api_data = PythonOperator(
    dag=dag,
    task_id="pull_api_data",
    python_callable=fetch_api_data,
    # op_args=["http://fastapi-app:5000/getAll",
    #          f"/opt/airflow/output_files/dag_result_{{ds}}.json"]

    templates_dict={
        "output_path": "/opt/airflow/output_files/dag_result_{{ ds }}.json",
        "url": "http://fastapi-app:5000/getAll",
    }
)