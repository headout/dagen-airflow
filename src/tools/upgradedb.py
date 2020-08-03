import os

from airflow import settings
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from airflow.hooks.mysql_hook import MySqlHook

MYSQL_CONN_ID = "dagen_sql_alchemy_conn"


def get_mysql_hook():
    os.environ[CONN_ENV_PREFIX +
               MYSQL_CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID)


def run_sql(sql, ignore_error=False):
    hook = get_mysql_hook()
    print("sql:\n%s" % sql)
    try:
        res = hook.get_records(sql)
    except Exception as e:
        if not ignore_error:
            raise e
        res = None
    print(res)
    return res


def run_version_0_0_1():
    print("Running Dagen Migrations - v0.0.1")
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS dagen_dag (
            dag_id VARCHAR(250) NOT NULL,
            category VARCHAR(50) NOT NULL,
            live_version INTEGER,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (dag_id)
        );
        """
    )
    run_sql(
        """
        CREATE INDEX ix_dagen_dag_created_at ON dagen_dag (created_at);
        CREATE INDEX ix_dagen_dag_updated_at ON dagen_dag (updated_at);
        """,
        ignore_error=True
    )

    run_sql(
        """
        CREATE TABLE IF NOT EXISTS dagen_dag_version (
            dag_id VARCHAR(250) NOT NULL,
            version INTEGER NOT NULL,
            dag_options TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            creator INTEGER,
            approver INTEGER,
            approved_at DATETIME,
            PRIMARY KEY (dag_id, version),
            FOREIGN KEY(approver) REFERENCES ab_user (id) ON DELETE SET NULL,
            FOREIGN KEY(creator) REFERENCES ab_user (id) ON DELETE SET NULL,
            FOREIGN KEY(dag_id) REFERENCES dagen_dag (dag_id) ON DELETE CASCADE
        );
        """
    )

    run_sql(
        """
        CREATE INDEX ix_dagen_dag_version_approved_at ON dagen_dag_version (approved_at);
        CREATE INDEX ix_dagen_dag_version_created_at ON dagen_dag_version (created_at);
        CREATE INDEX ix_dagen_dag_version_dag_id ON dagen_dag_version (dag_id);
        """,
        ignore_error=True
    )


def run_version_0_0_2():
    print("Running Dagen Migrations - v0.0.2")
    run_sql(
        """
        ALTER TABLE dagen_dag ADD COLUMN template_id VARCHAR(250) NOT NULL;
        CREATE INDEX ix_dagen_dag_template_id ON dagen_dag (template_id);
        """,
        ignore_error=True
    )


def run_version_0_0_3():
    print("Running Dagen Migrations - v0.0.3")
    run_sql(
        """
        ALTER TABLE dagen_dag_version ADD COLUMN schedule_interval VARCHAR(50) NOT NULL;
        """,
        ignore_error=True
    )


def main():
    run_version_0_0_1()
    run_version_0_0_2()
    run_version_0_0_3()


if __name__ == "__main__":
    main()
