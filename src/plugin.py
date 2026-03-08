import logging

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from dagen.migrations.utils import initdb
from dagen.utils import get_template_loader
from dagen.www.api_views import dagen_fastapi_app
from dagen.www.web_views import dagen_web_app


class DagenPlugin(AirflowPlugin, LoggingMixin):
    name = 'dagen'
    # Airflow 3.x: use fastapi_apps instead of flask_blueprints/appbuilder_views
    fastapi_apps = [
        {
            "name": "dagen_api",
            "app": dagen_fastapi_app,
            "url_prefix": "/dagen/api",
        },
        {
            "name": "dagen_ui",
            "app": dagen_web_app,
            "url_prefix": "/dagen/ui",
        },
    ]

    log = logging.root.getChild(f'{__name__}.{"DagenPlugin"}')

    @classmethod
    def validate(cls):
        # HACK: since on_load is only called for entrypoint plugins
        super().validate()

        # Load templates per each airflow process
        loader = get_template_loader()
        if not loader.template_classes:
            loader.load_templates()
