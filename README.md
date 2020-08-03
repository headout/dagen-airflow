# Dagen - Dynamically-generated DAGS

## Installation

- Clone the repository.
- Link src/ directory as a new plugin in <AIRFLOW_HOME>/plugins.

## Usage

- Put all DAG templates in <AIRFLOW_HOME>/dag_templates.
- Each template should be a subclass of `dagen.dag_templates.BaseDagTemplate` and `create_dag` function should be overriden to return the created DAG.
- **IMPORTANT!** Add a dummy DAG in your DAGs folder (<AIRFLOW_HOME>/dags) to collect and load all the Dynamic DAGs. You can use [this script](sample/dags/dag_dagen.py).
- A sample template (related to bash DAG) is [here](sample/dag_templates/dummy.py).

## Configuration

Sample:

```ini
[dagen]
dag_template_folder = /opt/airflow/dag_templates
```

Explanation:

- `dag_template_folder` - Absolute path to folder containing all python files with DAG templates
