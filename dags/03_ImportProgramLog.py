from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import PyWindowsRmOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import airflow

dag_name = '03_ImportProgramLog'
args = {
    'start_date': airflow.utils.dates.days_ago(7),
    'owner': 'bhupendrajoshi',
}

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    schedule_interval=None)


def get_time_delta_func(ds, **kwargs):
    broadcast_day = datetime.utcnow()
    production_day = broadcast_day - timedelta(days=1)
    if 'dag_run' in kwargs:
        production_day = broadcast_day - \
            timedelta(days=kwargs['dag_run'].conf['daysDelta'])

    arguments = 'StartDate=' + \
        production_day.strftime('%Y-%m-%d') + ', EndDate=' + \
        broadcast_day.strftime('%Y-%m-%d')
    kwargs['ti'].xcom_push(key='arguments', value=arguments)


get_time_delta = PythonOperator(
    task_id='get_time_delta',
    provide_context=True,
    python_callable=get_time_delta_func,
    dag=dag)

first_task = PyWindowsRmOperator(
    task_id="first_task",
    provide_context=True,
    arguments="{{ti.xcom_pull(key='arguments', task_ids='get_time_delta')}}",
    dag=dag)

dag_complete = BashOperator(
    task_id="dag_complete",
    bash_command='echo "03_ImportProgramLog Operation complete!!"',
    dag=dag)

# ------- Workflows ------- #
get_time_delta >> first_task >> dag_complete
