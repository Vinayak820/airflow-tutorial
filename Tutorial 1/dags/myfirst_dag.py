from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

dag = DAG(dag_id="myfirst_dag")

def printContext(**kwargs):
    print(kwargs)
    print("This is a PythonOperator task.")

copy_file = BashOperator(
    task_id="copy_file",
    bash_command='echo "Copying file from source to destination"',
    dag=dag
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=printContext,
    dag=dag
)

copy_file >> task2
# copy_file.set_downstream(task2)