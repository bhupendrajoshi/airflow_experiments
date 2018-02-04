from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime
import airflow

dag_name = '00_SensingDag'
args = {
    'start_date': airflow.utils.dates.days_ago(7),
    'owner': 'bhupendrajoshi',
}

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    schedule_interval='0 2 * * *')


def trigger_dag_run_pass_params(context, dag_run_obj):
    dag_run_obj.payload = context['params']
    return dag_run_obj


CleanUpSKOPages = TriggerDagRunOperator(task_id='CleanUpSKOPages',
                                        trigger_dag_id="01_CleanupDag",
                                        python_callable=trigger_dag_run_pass_params,
                                        params={},
                                        dag=dag)

ImportComtelData = TriggerDagRunOperator(task_id='ImportComtelData',
                                         trigger_dag_id="02_ImportDataDag",
                                         python_callable=trigger_dag_run_pass_params,
                                         params={},
                                         dag=dag)

WaitForImportComtelData = ExternalTaskSensor(task_id='WaitForImportComtelData',
                                             external_dag_id='02_ImportDataDag',
                                             external_task_id='dag_complete',
                                             execution_delta=None,  # Same day as today
                                             dag=dag)

ImportProgramLogMinus14 = TriggerDagRunOperator(task_id='ImportProgramLogMinus14',
                                                trigger_dag_id="03_ImportProgramLog",
                                                python_callable=trigger_dag_run_pass_params,
                                                params={'daysDelta': 14},
                                                dag=dag)

WaitForImportProgramLogMinus14 = ExternalTaskSensor(task_id='WaitForImportProgramLogMinus14',
                                                    external_dag_id='03_ImportProgramLog',
                                                    external_task_id='dag_complete',
                                                    execution_delta=None,  # Same day as today
                                                    dag=dag)

Validation = TriggerDagRunOperator(task_id='Validation',
                                   trigger_dag_id="04_ValidationDag",
                                   python_callable=trigger_dag_run_pass_params,
                                   params={},
                                   dag=dag)

WaitForValidation = ExternalTaskSensor(task_id='WaitForValidation',
                                       external_dag_id='04_ValidationDag',
                                       external_task_id='dag_complete',
                                       execution_delta=None,  # Same day as today
                                       dag=dag)

ImportProgramLogMinus1 = TriggerDagRunOperator(task_id='ImportProgramLogMinus1',
                                               trigger_dag_id="03_ImportProgramLog",
                                               python_callable=trigger_dag_run_pass_params,
                                               params={'daysDelta': 1},
                                               dag=dag)

WaitForImportProgramLogMinus1 = ExternalTaskSensor(task_id='WaitForImportProgramLogMinus1',
                                                   external_dag_id='03_ImportProgramLog',
                                                   external_task_id='dag_complete',
                                                   execution_delta=None,  # Same day as today
                                                   dag=dag)

ExportToKpi = TriggerDagRunOperator(task_id='ExportToKpi',
                                    trigger_dag_id="05_ExportToKpiDag",
                                    python_callable=trigger_dag_run_pass_params,
                                    params={},
                                    dag=dag)

WaitForExportToKpi = ExternalTaskSensor(task_id='WaitForExportToKpi',
                                        external_dag_id='05_ExportToKpiDag',
                                        external_task_id='dag_complete',
                                        execution_delta=None,  # Same day as today
                                        dag=dag)

ExportToDnx = TriggerDagRunOperator(task_id='ExportToDnx',
                                    trigger_dag_id="06_ExportToDnxDag",
                                    python_callable=trigger_dag_run_pass_params,
                                    params={},
                                    dag=dag)

WaitForExportToDnx = ExternalTaskSensor(task_id='WaitForExportToDnx',
                                        external_dag_id='06_ExportToDnxDag',
                                        external_task_id='dag_complete',
                                        execution_delta=None,  # Same day as today
                                        dag=dag)

# ------- Workflows ------- #
CleanUpSKOPages

ImportComtelData >> WaitForImportComtelData >> ImportProgramLogMinus14

WaitForImportProgramLogMinus14 >> Validation

WaitForValidation >> ExportToKpi >> WaitForExportToKpi

WaitForValidation >> ImportProgramLogMinus1

WaitForImportProgramLogMinus1 >> ExportToDnx

WaitForExportToKpi >> ExportToDnx

ExportToDnx >> WaitForExportToDnx
