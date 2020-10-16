import logging

try:
    from airflow.www_rbac.app import csrf
except ImportError:
    # Airflow 2.0.0
    from airflow.www.app import csrf
from flask import (Blueprint, current_app, flash, g, jsonify, make_response,
                   redirect, request, url_for)

from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.www.utils import login_required

dagen_rest_bp = Blueprint('DagenRestView', __name__, url_prefix='/dagen/api')

log = logging.root.getChild(f'{__name__}.{"DagenRestView"}')


@csrf.exempt
@dagen_rest_bp.route('/dags/approve/all', methods=('POST',))
@login_required
def approve_all():
    user_id = g.user.id
    qs = DagenDagVersionQueryset()
    unapproved_versions = qs.get_all_current_unapproved()
    qs.approve_all(unapproved_versions, user_id).done()
    return _render_json({
        "count_approved": len(unapproved_versions)
    })


def _render_json(data, status=200):
    response = make_response(jsonify(**data), status)
    response.headers["Content-Type"] = "application/json"
    return response
