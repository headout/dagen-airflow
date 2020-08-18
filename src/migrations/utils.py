import logging
import os

from airflow import settings

log = logging.getLogger(__name__)


def initdb():
    """
    Initializes Dagen database
    """
    return upgradedb()


def upgradedb():
    """
    Upgrade the database.
    """
    from alembic import command

    log.info("Creating Dagen tables")
    config = _get_alembic_config()
    return command.upgrade(config, 'heads')


def _get_alembic_config():
    from alembic.config import Config

    current_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(current_dir, '..'))
    directory = os.path.join(package_dir, 'migrations')
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory.replace('%', '%%'))
    config.set_main_option(
        'sqlalchemy.url', settings.SQL_ALCHEMY_CONN.replace('%', '%%'))
    return config
