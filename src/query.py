from airflow.utils.db import provide_session

from dagen.models import DagenDag, DagenDagVersion


class BaseQueryset(object):
    @provide_session
    def __init__(self, session=None):
        super().__init__()
        self.session = session

    def done(self):
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        finally:
            self.session.close()


class DagenDagQueryset(BaseQueryset):
    def get_dag(self, dag_id):
        return self.session.query(DagenDag).get(dag_id)

    def delete_dag(self, dag_id):
        self.session.query(DagenDag).filter(DagenDag.dag_id == dag_id).delete()
        return self

    def get_all(self, published=None):
        dags = self.session.query(DagenDag).all()
        if published is not None:
            dags = list(filter(lambda dag: published == dag.is_enabled, dags))
        return dags


class DagenDagVersionQueryset(BaseQueryset):
    def get_dag_versions(self, dag_id):
        return self.session.query(DagenDagVersion).filter(DagenDagVersion.dag_id == dag_id)

    def approve_live_version(self, dag_id, user_id):
        dbDag = DagenDagQueryset().get_dag(dag_id)
        if not dbDag.is_published:
            raise ValueError(f'Live version not set for given DAG "{dag_id}"!')
        if dbDag.is_enabled:
            raise ValueError(f'DAG "{dag_id}" already approved!')
        dbDag.live_version.approve(user_id)
        self.session.add(dbDag.live_version)
        return self
