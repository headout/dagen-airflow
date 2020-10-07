import json

from markupsafe import Markup, escape
from wtforms.fields import HiddenField, StringField
from wtforms.widgets import Select, TextInput, html_params


class TextInputGroup(TextInput):
    field_flags = ('hidden',)

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
    def __init__(self, label, *args, selector_name=None, choices=None, **kwargs):
        self.widget = TextInputGroup(label)
        super().__init__(label, *args, **kwargs)
        self.selector_name = selector_name
        self.choices = choices

    def __call__(self, **kwargs):
        kwargs['group_kwargs'] = {
            "class_": f'btn-jsonform {self.name}'
        }
        kwargs['readonly'] = True
        result = None
        for target_value, jsonschema in self.choices:
            value_kwargs = dict(kwargs)
            value_kwargs['id'] = f'{self.name}-{target_value}'
            value_kwargs['group_kwargs'].update(**{
                "data_jsonschema": json.dumps(jsonschema),
                "data_target_id": value_kwargs['id']
            })
            value_kwargs["dep-enable-expr"] = f'select[name={self.selector_name}]={target_value}'
            sup_call = super().__call__(**value_kwargs)
            result = sup_call if result is None else result + sup_call
        return result
