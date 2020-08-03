# Dummy file to load dynamic dags from dagen
from dagen.utils import get_managed_dags

for dag in get_managed_dags():
    globals()[dag.dag_id] = dag
