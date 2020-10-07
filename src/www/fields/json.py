import json

from markupsafe import Markup, escape
from wtforms.fields import HiddenField, StringField
from wtforms.widgets import Select, TextInput, html_params


class TextInputGroup(TextInput):
    def __init__(self, prepend=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text_prepend = prepend

    def __call__(self, field, group_kwargs={}, **kwargs):
        group_kwargs['class_'] = f'{group_kwargs["class_"]} input-group'
        return Markup(
            f'''
            <div {html_params(**group_kwargs)}>
                <span class="input-group-addon">{self.text_prepend}</span>
                {super().__call__(field, **kwargs)}
            </div>
            '''
        )


class JsonFormMultipleField(StringField):
    widget = TextInputGroup("Change")

    def __init__(self, *args, selector_name=None, choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.selector_name = selector_name
        self.choices = choices

    def __call__(self, **kwargs):
        kwargs['group_kwargs'] = {
            "class_": f'{kwargs.get("class_", "")} btn-jsonform {self.name}'
        }
        kwargs['readonly'] = True
        result = None
        for target_value, jsonschema in self.choices:
            kwargs['group_kwargs'].update(**{
                "dep-enable-expr": f'select[name={self.selector_name}]={target_value}',
                "data_jsonschema": json.dumps(jsonschema),
                "data_target_name": self.name
            })
            sup_call = super().__call__(**kwargs)
            result = sup_call if result is None else result + sup_call
        return result
