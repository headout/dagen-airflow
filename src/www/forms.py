import asyncio
import csv
import json
from io import StringIO

from airflow.utils.db import provide_session
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, file_allowed, file_required
from wtforms import validators
from wtforms.fields import BooleanField, SelectField

from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.utils import get_template_loader
from dagen.www.utils import async_gather_dict

cron_dump_csv = file_allowed(('csv',))


async_loop = asyncio.get_event_loop()


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
    mark_approved = BooleanField(
        'Mark approved?',
        default=False,
        description='Additionally mark all the new DAG versions as approved if checked.'
    )

    def __init__(self, templates=[], **kwargs):
        super().__init__(**kwargs)
        self.template_id.choices = tuple(zip(templates, templates))

    async def process_row(self, row) -> (bool, str):
        form = self.tmpl_clazz.as_form(data=row)
        cleaned_data = form.process_form_data(**row)
        dag_id = cleaned_data['dag_id']
        dbDag = self.dagmap.get(dag_id, None)
        is_update = dbDag is not None
        is_valid = form.validate()
        if is_valid:
            if is_update:
                is_success = form.update(dbDag, self.user)
            else:
                dbDag = form.create(self.template_id.data, self.user)
                is_success = dbDag is not None
                if is_success:
                    self.dagmap[dbDag.dag_id] = dbDag
        msg = None
        if is_valid and is_success:
            msg = f'{dbDag.version_str} {"updated" if is_update else "created"} successfully'
        elif is_valid:
            if is_update:
                msg = f'{dbDag.version_str} unchanged'
                # version unchanged is a success condition and should be shown to user
                # as such, hence set it to True here
                is_success = True
        if msg is None:
            msg = f'Encounted errors: {json.dumps(form.errors)}'
        is_success = is_valid and is_success
        if self.mark_approved.data and is_success:
            try:
                self.version_qs.approve_live_version(dag_id, self.user.id)
            except ValueError as e:
                msg = f'{msg}\nApproval failed: {e}'
            else:
                msg = f'{msg}\nApproved successfully'
        return is_success, msg

    async def asyncSave(self):
        self.version_qs = DagenDagVersionQueryset()
        self.dagmap = {
            dbDag.dag_id: dbDag for dbDag in DagenDagQueryset().get_all()}
        self.tmpl_clazz = get_template_loader().get_template_class(self.template_id.data)

        stream = StringIO(self.csv_data.data.read().decode('UTF-8'))
        # Close the file object
        self.csv_data.data.close()

        cronreader = csv.DictReader(stream)
        result = await async_gather_dict({
            row['dag_id']: self.process_row(row)
            for row in cronreader
        })
        self.version_qs.done()
        return result

    def save(self, user):
        self.user = user
        return async_loop.run_until_complete(self.asyncSave())
