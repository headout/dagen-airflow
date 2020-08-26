import re

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


field_dag_id = StringField(
    'Dag ID', validators=(
        validators.optional(), validators.regexp("^[a-zA-Z0-9._-]+$"), validators.length(max=ID_LEN)),
    description='Dag ID is made of alphanumeric characters, dashes, dots and underscores exclusively.'
)

field_schedule_interval = StringField(
    'Cron expression', validators=(
        validators.required(), CronExpression()),
    description=f'Specify the schedule interval in standard crontab format (https://crontab.guru/) or use one of the presets ({", ".join(cron_presets.keys())})'
)

field_category = StringField(
    'Category', default='default', validators=(validators.optional(), validators.length(max=50)))

field_synchronized_runs = BooleanField(
    'Are runs synchronized?', default=True, description='If the DAG runs are synchronized, then there will only be one active DAG run at a time.'
)
