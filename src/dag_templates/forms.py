import logging
from collections import OrderedDict

from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from dagen.dag_templates.fields import (field_category, field_dag_id,
                                        field_pool, field_schedule_interval,
                                        field_synchronized_runs, field_start_date)
from dagen.models import DagenDag, DagenDagVersion
from sqlalchemy.exc import IntegrityError
from wtforms.form import Form

DEFAULT_OPTIONS = OrderedDict(
    category=field_category,
    dag_id=field_dag_id,
    schedule_interval=field_schedule_interval,
    synchronized_runs=field_synchronized_runs,
    pool=field_pool,
    start_date=field_start_date
)


class DagVersionForm(Form, LoggingMixin):
    def set_form_processor(self, processor):
        self.process_form_data = processor

    @provide_session
    def create(self, template_id, user=None, session=None):
        data = self.process_form_data(**self.data)
        dag_id = data['dag_id']
        options, default_opts = self._get_options(data)
        dag_version = self._get_new_dag_version(
            dag_id, options, default_opts, creator=user)
        try:
            dag = self._add_dag_with_version(
                dag_id, template_id, dag_version, default_opts)
        except IntegrityError as e:
            self.log.exception(e)
            self.dag_id.errors.append('DAG ID must be unique!')
            return None
        dag._live_version = dag_version.version
        session.query(DagenDag).get(dag_id)._live_version = dag_version.version
        return dag

    @provide_session
    def update(self, dbDag, user=None, form_version=None, session=None):
        data = self.process_form_data(**self.data)
        dag_id = dbDag.dag_id
        options, default_opts = self._get_options(data)
        if form_version and form_version == 'none':
            # Special case - Disable DAG by setting live version to None
            session.query(DagenDag).get(dag_id)._live_version = None
            return True
        dag_version = self._get_new_dag_version(
            dag_id, options, default_opts, creator=user)
        if dbDag.live_version == dag_version:
            # Nothing to update
            return False
        else:
            new_version = next(
                (dbVersion for dbVersion in dbDag.versions if dbVersion == dag_version), None)
            if new_version:
                # Different version is selected to be live
                dag_version = new_version
            else:
                # Just bloody create a new version!
                self._add_dag_version(dag_version)
        dbDag._live_version = dag_version.version
        session.query(DagenDag).get(dag_id)._live_version = dag_version.version
        return True

    def _get_options(self, data):
        data.pop('dag_id')
        default_opts = {}
        for key in DEFAULT_OPTIONS.keys():
            if key in ('dag_id', 'synchronized_runs'):
                continue
            default_opts[key] = data.pop(key, None)
        options = data
        return options, default_opts

    def _get_new_dag_version(self, dag_id, options, default_opts, creator=None):
        if creator:
            creator = creator.id
        kwargs = dict(default_opts)
        kwargs.pop('category')
        return DagenDagVersion(
            dag_id, creator=creator, **kwargs, **options
        )

    @provide_session
    def _add_dag_version(self, dag_version, session=None):
        session.add(dag_version)

    @provide_session
    def _add_dag_with_version(self, dag_id, template_id, dag_version, default_opts={}, session=None):
        dag = DagenDag(dag_id, template_id, default_opts.get('category', None))
        session.add(dag)
        self._add_dag_version(dag_version)
        return dag
