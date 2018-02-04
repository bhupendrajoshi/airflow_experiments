from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime
import airflow

dag_name = '06_ExportToDnxDag'
args = {
    'start_date': airflow.utils.dates.days_ago(7),
    'owner': 'bhupendrajoshi',
}

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    schedule_interval=None)

first_task = BashOperator(
    task_id="first_task",
    bash_command='echo "06_ExportToDnxDag" & sleep 2',
    dag=dag)

dag_complete = BashOperator(
    task_id="dag_complete",
    bash_command='echo "06_ExportToDnxDag Operation complete!!"',
    dag=dag)

# ------- Workflows ------- #
first_task >> dag_complete
