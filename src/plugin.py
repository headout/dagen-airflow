import logging

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from dagen.migrations.utils import initdb
from dagen.utils import get_template_loader
from dagen.www.api_views import dagen_rest_bp
from dagen.www.views import DagenFABView
from flask import Blueprint

ab_dagen_view = DagenFABView()
ab_dagen_package = {
    'name': 'List Dagen DAGs',
    'category': 'Dagen',
    'view': ab_dagen_view
}
ab_dagen_create_mitem = {
    'name': 'Create Dagen DAG',
    'category': 'Dagen',
    'category_icon': 'fa-th',
    'href': '/dagen/dags/create'
}

dagen_bp = Blueprint(
    "dagen_bp",
    __name__,
    template_folder='www/templates',
    static_folder='www/static',
    static_url_path='/static/dagen'
)


class DagenPlugin(AirflowPlugin, LoggingMixin):
    name = 'dagen'
    appbuilder_views = (ab_dagen_package,)
    appbuilder_menu_items = (ab_dagen_create_mitem,)
    flask_blueprints = (dagen_bp, dagen_rest_bp)

    log = logging.root.getChild(f'{__name__}.{"DagenPlugin"}')

    @classmethod
    def validate(cls):
        # HACK: since on_load is only called for entrypoint plugins
        super().validate()

        # Load templates per each airflow process
        loader = get_template_loader()
        if not loader.template_classes:
            loader.load_templates()
