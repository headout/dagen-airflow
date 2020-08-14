import re

from airflow.utils.dates import cron_presets
from wtforms import validators
from wtforms.fields import *


class CronExpression(validators.Regexp):
    def __init__(self):
        regex = re.compile(
            "^(?P<preset>{presets})$|^{0}\s+{1}\s+{2}\s+{3}\s+{4}$".format(
                "(?P<minute>\*|[0-5]?\d)",
                "(?P<hour>\*|[01]?\d|2[0-3])",
                "(?P<day>\*|0?[1-9]|[12]\d|3[01])",
                "(?P<month>\*|0?[1-9]|1[012])",
                "(?P<day_of_week>\*|[0-6](\-[0-6])?)",
                presets='|'.join(cron_presets.keys())
            )
        )
        message = f'Invalid cron expression. Either use one of the presets ({", ".join(cron_presets.keys())}) or use crontab format (https://crontab.guru/)'
        super().__init__(regex, message=message)


field_dag_id = StringField(
    'Dag ID', validators=(
        validators.optional(), validators.regexp("^[a-zA-Z._-]+$")),
    description='Dag ID is made of alphanumeric characters, dashes, dots and underscores exclusively.'
)

field_schedule_interval = StringField(
    'Cron expression', validators=(validators.required(), CronExpression()))

field_category = StringField(
    'Category', default='default', validators=(validators.optional(), validators.length(max=50)))
