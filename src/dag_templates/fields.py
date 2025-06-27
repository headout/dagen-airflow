import re
from datetime import datetime

from airflow.models.base import ID_LEN
from airflow.utils.dates import cron_presets
from croniter.croniter import CroniterError, croniter
from wtforms import validators
from wtforms.fields import *


class CronExpression(validators.Regexp):
    def __init__(self):
        regex = re.compile(
            "^(?P<preset>{presets})$".format(
                presets='|'.join(cron_presets.keys())
            )
        )
        message = f'Invalid cron expression'
        super().__init__(regex, message=message)

    def __call__(self, form, field, message=None):
        try:
            # try regex for cron presets
            return super().__call__(form, field, message=message)
        except validators.ValidationError as e:
            # check validation using croniter
            try:
                croniter.expand(field.data or '')
            except CroniterError as e:
                msg = message or self.message
                raise validators.ValidationError(f'{msg}: {e}')
            else:
                return True


class FixedBooleanField(BooleanField):
    """
        Fixed buggy implementation of `process_data` when Form(data=<dict>) is processed.
        It was incorrectly checking for truthy condition of the value.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = 'BooleanField'

    def process_data(self, value):
        self.data = value not in self.false_values


class JsonString(object):
    """
        Validates that input is a valid JSON string.
    """

    def __init__(self):
        import json
        self.json = json

    def __call__(self, form, field):
        string = field.data or ''
        try:
            self.json.loads(string)
            return True
        except Exception as e:
            message = f'Not a valid JSON string: {e}'
            raise validators.ValidationError(message)


field_dag_id = StringField(
    'Cron ID', validators=(
        validators.optional(), validators.regexp("^[a-zA-Z0-9._-]+$"), validators.length(max=ID_LEN)),
    description='Cron ID is made of alphanumeric characters, dashes, dots and underscores exclusively.'
)

field_schedule_interval = StringField(
    'Cron Schedule', validators=(
        validators.optional(), CronExpression()),
    description=f'Specify the schedule interval in standard crontab format (https://crontab.guru/) or use one of the presets ({", ".join(cron_presets.keys())})'
)

field_category = StringField(
    'Category', default='default', validators=(validators.optional(), validators.length(max=50)))

field_synchronized_runs = FixedBooleanField(
    'Are runs synchronized?', default=True, description='If the DAG runs are synchronized, then there will only be one active DAG run at a time.'
)

field_start_date = DateTimeField(
    'Start Date of DAG', default=datetime(2020, 9, 1), description='Specify the Start datetime of DAG, this affects how the crons schedule be decided based on the schedule interval.'
)

field_pool = StringField(
    'Pool', default='default_pool', validators=(validators.optional(),),
    description='Configure the pool to limit the execution parallelism of all the tasks of this DAG.'
)
