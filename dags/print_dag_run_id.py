from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id='print_dag_run_id',
    schedule_interval=None,
    start_date=datetime.utcnow()
)


def my_func(**kwargs):
    context = kwargs
    print(context['dag_run'].run_id)
    print(context['dag_run'].id)


t1 = PythonOperator(
    task_id='python_run_id',
    python_callable=my_func,
    provide_context=True,
    dag=dag
)

t2 = BashOperator(
    task_id='bash_run_id',
    bash_command='echo {{run_id}}',
    dag=dag)

t1 >> t2
