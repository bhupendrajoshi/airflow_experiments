# from airflow.models import DagRun
# from airflow.models import TaskInstance
# from airflow.sensors.base_sensor_operator import BaseSensorOperator
# from airflow.utils.db import provide_session
# from airflow.utils.decorators import apply_defaults
# from airflow.utils.state import State


# class PyExternalTaskSensor(BaseSensorOperator):
#     template_fields = ['external_dag_id', 'external_task_id']
#     ui_color = '#19647e'

#     @apply_defaults
#     def __init__(self,
#                  external_dag_id,
#                  external_task_id,
#                  execution_date,
#                  start_date_after,
#                  end_day,
#                  allowed_states=None,
#                  *args,
#                  **kwargs):
#         super(PyExternalTaskSensor, self).__init__(*args, **kwargs)
#         self.allowed_states = allowed_states or [State.SUCCESS]

#         self.execution_date = execution_date
#         self.start_date_after = start_date_after
#         self.end_day = end_day
#         self.external_dag_id = external_dag_id
#         self.external_task_id = external_task_id

#     @provide_session
#     def poke(self, context, session=None):

#         serialized_dttm_filter = ','.join(
#             [datetime.isoformat() for datetime in dttm_filter])

#         self.log.info(
#             'Poking for '
#             '{self.external_dag_id}.'
#             '{self.external_task_id} with'
#             'execution_date {execution_date} '
#             'start_date_after {start_date_after} '
#             'end_day {end_day} ')

#         DR = DagRun
#         session.query(DR).filter(
#             DR.dag_id == self.external_dag_id,

#         )
#         TI = TaskInstance

#         count = session.query(TI).filter(
#             TI.dag_id == self.external_dag_id,
#             TI.task_id == self.external_task_id,
#             TI.state.in_(self.allowed_states),
#             TI.execution_date.in_(dttm_filter),
#         ).count()
#         session.commit()
#         return count == len(dttm_filter)


# class PyExternalTaskSensorPlugin(AirflowPlugin):
#     name = "pyexternaltasksensor_plugin"
#     operators = [PyExternalTaskSensor]
