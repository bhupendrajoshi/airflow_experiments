import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class PyWindowsRmOperator(BaseOperator):
    template_fields = ('arguments', )

    @apply_defaults
    def __init__(self, arguments, *args, **kwargs):
        self.arguments = arguments
        super(PyWindowsRmOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Hello World!")
        log.info('arguments: %s', self.arguments)


class PyWindowsRmOperatorPlugin(AirflowPlugin):
    name = "pywindowsrm_plugin"
    operators = [PyWindowsRmOperator]
