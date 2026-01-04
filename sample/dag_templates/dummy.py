import json
from datetime import datetime, timedelta
from random import choice

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from wtforms.fields import StringField, TextField

from dagen.dag_templates import BaseDagTemplate

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2024, 1, 1),
}


class DummyTemplate(BaseDagTemplate):
    options = {
        'bash_command': StringField('Bash command')
    }

    def create_dag(self, **options):
        dag = DAG(
            options['dag_id'],
            default_args=default_args,
            schedule_interval=options['schedule_interval']
        )
        start_task = EmptyOperator(task_id="start", dag=dag)
        stop_task = EmptyOperator(task_id="stop", dag=dag)

        cmd = BashOperator(
            task_id='bash_cmd',
            bash_command=options["bash_command"],
            dag=dag
        )

        start_task >> cmd >> stop_task
        return dag
