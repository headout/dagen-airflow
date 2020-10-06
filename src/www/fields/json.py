import json

from markupsafe import Markup, escape
from wtforms.fields import HiddenField
from wtforms.widgets import html_params, Select


class JsonFormWidget(object):
    html_params = staticmethod(html_params)

    def __init__(self, json_schema):
        self.json_schema = json_schema

    def __call__(self, field, **kwargs):
        kwargs.setdefault('id', f'form-{field.id}')
        kwargs.setdefault('data_jsonschema', json.dumps(self.json_schema))
        return Markup(
            f'''
            <form {self.html_params(**kwargs)}></form>
            <script>
                $(function() {{
                    const $targetField = $('input[name={field.name}]');
                    $('form[data-jsonschema]#{kwargs['id']}').jsonForm({{
                        schema: {self.json_schema},
                        onSubmit: function(errors, values) {{
                            console.log(errors, values)
                            $targetField.val(JSON.stringify(values))
                        }}
                    }});
                }})
            </script>
            '''
        )


class JsonFormHiddenField(HiddenField):
    def __init__(self, *args, json_schema=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.jsonform_widget = JsonFormWidget(json_schema)

    def __call__(self, **kwargs):
        result = super().__call__(**kwargs)
        result += self.jsonform_widget(self)
        return result


class JsonFormMultipleField(HiddenField):
    def __init__(self, *args, selector_name=None, choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.selector_name = selector_name
        self.choices = choices
        self.value_widgets = (
            (choice[0], JsonFormWidget(choice[1]))
            for choice in choices
        )

    def __call__(self, **kwargs):
        result = super().__call__(**kwargs)
        for target_value, widget in self.value_widgets:
            result += widget(self, **{
                "dep-enable-expr": f'select[name={self.selector_name}]={target_value}',
                "class_": f'jsonform {self.name}'
            })
        return result
