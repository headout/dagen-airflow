import imp
import os
import sys
from datetime import datetime
from functools import partial

import sqlalchemy
from airflow.configuration import conf

try:
    # Airflow v2.0
    from airflow.utils.file import list_py_file_paths
    list_py_file_paths = partial(
        list_py_file_paths, include_smart_sensor=False)
except ImportError:
    from airflow.utils.dag_processing import list_py_file_paths

from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timeout import timeout
from dagen.config import config
from dagen.dag_templates import BaseDagTemplate
from dagen.exceptions import TemplateNotFoundError
from dagen.models import DagenDag, DagenDagVersion
from dagen.query import DagenDagQueryset
from sqlalchemy.orm import joinedload


list_py_file_paths = partial(
    list_py_file_paths,
    include_examples=False,
    safe_mode=False
)


class TemplateLoader(LoggingMixin):
    TEMPLATE_IMPORT_TIMEOUT = conf.getint('core', 'DAGBAG_IMPORT_TIMEOUT')

    def __init__(self, templates_dir=None):
        self.template_classes = {}
        self.templates_dir = templates_dir or config.dag_template_folder
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.import_errors = {}

    def process_file(self, filepath, only_if_updated=True, safe_mode=False):
        found_templates = []

        if filepath is None or not os.path.isfile(filepath):
            return found_templates

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(
                os.path.getmtime(filepath))
            if only_if_updated and file_last_changed_on_disk == self.file_last_changed.get(filepath, None):
                return found_templates
        except Exception as e:
            self.log.exception(e)
            return found_templates

        if safe_mode:
            # TODO: heuristic to process only if file contains template
            pass

        self.log.debug(f'Importing {filepath}')
        modname, _ = os.path.splitext(os.path.split(filepath)[-1])
        mods = []
        with timeout(self.TEMPLATE_IMPORT_TIMEOUT):
            try:
                m = imp.load_source(modname, filepath)
                mods.append(m)
            except Exception as e:
                self.log.exception(f'Failed to import: {filepath}')
                self.import_errors[filepath] = str(e)
                self.file_last_changed[filepath] = file_last_changed_on_disk
        for mod in mods:
            for val in list(m.__dict__.values()):
                if isinstance(val, type) and val != BaseDagTemplate and issubclass(val, BaseDagTemplate):
                    tmpl = val
                    self._add_template(tmpl)
                    found_templates.append(tmpl)
        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_templates

    def _add_template(self, dag_template):
        self.template_classes[dag_template.get_template_id()] = dag_template

    def load_templates(self, only_if_updated=True):
        self.log.info(f'Loading DAG Templates from "{self.templates_dir}"...')
        for filepath in list_py_file_paths(self.templates_dir):
            self.process_file(filepath, only_if_updated=only_if_updated)
        self.templates = {key: cls()
                          for key, cls in self.template_classes.items()}
        return self.templates

    def get_managed_dags(self):
        try:
            dbDags = DagenDagQueryset().get_all(published=True)
        except Exception as e:
            dbDags = list()
            self.log.warn(
                "failed to load DAGs (probably since dagen tables are not loaded)", exc_info=e)
        for dbDag in dbDags:
            options = dbDag.live_version.dag_options
            options = dict(dag_id=dbDag.dag_id,
                           schedule_interval=dbDag.live_version.schedule_interval,
                           category=dbDag.category,
                           **options)
            try:
                dag = self.get_template(
                    dbDag.template_id).create_dag(**options)
            except TemplateNotFoundError as e:
                self.log.warn(
                    f'Skipping DAG with ID "{dbDag.dag_id}": {e}')
            except Exception as e:
                self.log.exception(
                    f'Errored on creating DAG with ID "{dbDag.dag_id}',
                    exc_info=e
                )
            else:
                yield dag

    def get_template_class(self, template_id):
        try:
            return self.template_classes[template_id]
        except KeyError:
            raise TemplateNotFoundError(template_id)

    def get_template(self, template_id):
        try:
            return self.templates[template_id]
        except KeyError:
            raise TemplateNotFoundError(template_id)
