from airflow.models import DAG, DagBag


def refresh_dagbag(dag_bag=None, dag_id=None, force_fetch=False):
    if not dag_bag:
        dag_bag = DagBag(store_serialized_dags=False)
    try:
        # Use bulk sync to refresh DAGs (implemented by https://github.com/apache/airflow/pull/7477)
        dag_bag.sync_to_db()
    except AttributeError:
        # Bulk sync not possible
        if dag_id:
            dag = dag_bag.get_dag(dag_id)
            if dag:
                dag.sync_to_db()
            else:
                if force_fetch:
                    # TODO: not possible to update in-memory dagbag instance
                    pass
                if not force_fetch or not dag_bag.get_dag(dag_id):
                    print(f'++++++++Deactivating unknown DAGs+++++++')
                    # DAG is missing in the DagBag
                    # probably since it's disabled because it's unapproved.
                    # in that case, the DB DAG also needs to be deactivated
                    DAG.deactivate_unknown_dags(dag_bag.dag_ids)
        else:
            raise ValueError('Pass "dag_id" to refresh!')
