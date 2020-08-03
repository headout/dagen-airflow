# Dagen - Dynamically-generated DAGS

## Installation

- Clone and add the plugin to <AIRFLOW_HOME>/plugins

## Usage

- Put all DAG templates in <AIRFLOW_HOME>/dag_templates.
- Each template should be a subclass of `dagen.dag_templates.BaseDagTemplate` and `create_dag` function should be overriden to return the created DAG.
- A sample template (related to bash DAG) is [here](sample/dag_templates/dummy.py).

## Configuration

Sample:

```ini
[dagen]
dag_template_folder = /opt/airflow/dag_templates
```

Explanation:

- `dag_template_folder` - Absolute path to folder containing all python files with DAG templates

