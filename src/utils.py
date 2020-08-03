from airflow.models import DAG, DagBag

__loader = None


def get_template_loader():
    from dagen.loader import TemplateLoader  # Avoid circular imports

    global __loader
    if __loader is None:
        __loader = TemplateLoader()
    return __loader


def get_managed_dags():
    loader = get_template_loader()
    if not loader.template_classes:
        loader.load_templates()
    return loader.get_managed_dags()


def refresh_managed_dags():
    loader = get_template_loader()
    loader.load_templates(only_if_updated=False)


def refresh_dagbag(dag_bag=None, dag_id=None):
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
                # DAG is missing in the DagBag
                # probably since it's disabled because it's unapproved.
                # in that case, the DB DAG also needs to be deactivated
                DAG.deactivate_unknown_dags(dag_bag.dag_ids)
        else:
            raise ValueError('Pass "dag_id" to refresh!')
