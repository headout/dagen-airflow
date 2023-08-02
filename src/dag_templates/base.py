import inspect
from functools import cached_property

from airflow import DAG

from dagen.dag_templates.forms import DEFAULT_OPTIONS, DagVersionForm


class BaseDagTemplate(object):
    options = {}
    template_id = None
    format_dag_id = '{category}.{dag_id}'

    def create_dag(self, dag_id, schedule_interval, catchup=None, dagrun_timeout=None ,is_paused_upon_creation=None, default_args={}, owner='Dagen', **options):
        default_args['owner'] = owner
        default_args['pool'] = options.get(
            'pool', 'default_pool') or 'default_pool'
        if 'start_date' in options:
            default_args['start_date'] = options['start_date']
        dag = DAG(
            dag_id,
            default_args=default_args,
            schedule_interval=schedule_interval,
            is_paused_upon_creation=is_paused_upon_creation,
        )
        max_active_runs = options.get('max_active_runs', None)
        if max_active_runs is not None:
            dag.max_active_runs = max_active_runs
        if catchup is not None:
            dag.catchup = catchup
        if dagrun_timeout is not None:
            dag.dagrun_timeout = dagrun_timeout
        return dag

    @classmethod
    def get_template_id(cls):
        return cls.template_id or cls.__name__

    @cached_property
    def initial_template(self):
        return inspect.getsource(self.create_dag)

    @classmethod
    def get_dag_id(cls, data):
        try:
            return cls.format_dag_id.format(**data)
        except KeyError as e:
            return data['dag_id']

    @classmethod
    def process_form_data(cls, **data):
        sync_runs = data.pop('synchronized_runs', False)
        if sync_runs:
            data['max_active_runs'] = 1
        data['dag_id'] = cls.get_dag_id(data)
        return data

    @classmethod
    def get_form_fields(cls):
        result = dict(**DEFAULT_OPTIONS)
        result.update(cls.options)
        return result

    @classmethod
    def prehook_form(cls, form):
        pass

    @classmethod
    def as_form(cls, *args, **kwargs):
        class TemplateForm(DagVersionForm):
            pass
        for key, field in cls.get_form_fields().items():
            setattr(TemplateForm, key, field)
        form = TemplateForm(*args, **kwargs)
        cls.prehook_form(form)
        form.set_form_processor(cls.process_form_data)
        return form
