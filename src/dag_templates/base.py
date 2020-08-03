import inspect
from functools import cached_property

from dagen.dag_templates.forms import DEFAULT_OPTIONS, DagVersionForm


class BaseDagTemplate(object):
    options = {}
    template_id = None
    format_dag_id = '{category}.{dag_id}'

    def create_dag(self, **data):
        pass

    @classmethod
    def get_template_id(cls):
        return cls.template_id or cls.__name__

    @cached_property
    def initial_template(self):
        return inspect.getsource(self.create_dag)

    @classmethod
    def get_dag_id(cls, **kwargs):
        try:
            return cls.format_dag_id.format(**kwargs)
        except KeyError as e:
            return kwargs['dag_id']

    @classmethod
    def as_form(cls, *args, **kwargs):
        class TemplateForm(DagVersionForm):
            pass
        for key, field in dict(**DEFAULT_OPTIONS, **cls.options).items():
            setattr(TemplateForm, key, field)
        form = TemplateForm(*args, **kwargs)
        form.set_dag_id_getter(cls.get_dag_id)
        return form
