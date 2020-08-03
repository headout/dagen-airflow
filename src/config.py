from functools import cached_property

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

from dagen import SECTION_NAME


class Config(object):
    @cached_property
    def dag_template_folder(self):
        return conf.get(SECTION_NAME, 'dag_template_folder', fallback='templates')

config = Config()
