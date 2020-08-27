from airflow.utils.db import provide_session
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, file_allowed, file_required
from wtforms import validators
from wtforms.fields import SelectField

cron_dump_csv = file_allowed(('csv',))


class BulkSyncDagenForm(FlaskForm):
    template_id = SelectField(
        'Template ID',
        validators=(validators.required(),),
    )
    csv_data = FileField(
        'Choose CSV file',
        validators=(file_required(), cron_dump_csv),
        description='Please pick a CSV file containing all the required DAGs (relevant to this template) to generate'
    )

    def __init__(self, templates=[], **kwargs):
        super().__init__(**kwargs)
        self.template_id.choices = tuple(zip(templates, templates))

    @provide_session
    def save(self, session=None):
        print(self.csv_data.data)
