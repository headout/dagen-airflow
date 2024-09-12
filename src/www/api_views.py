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


#supports selenium team's requirement - optimus bot
@csrf.exempt
@expose('/api/dags/ext/create', methods=['POST'])
@login_required
def create_dag_json(self):
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        tmpls = get_template_loader().template_classes
        forms = {key: tmpl.as_form() for key, tmpl in tmpls.items()}

        tmpl_id = data.get('template_id')
        if not tmpl_id or tmpl_id not in forms:
            return jsonify({"error": "Invalid or missing template_id"}), 400

        form = forms[tmpl_id]
        form.process(formdata=None, data=data)

        if form.validate():
            ret = form.create(template_id=tmpl_id, user=g.user)
            if ret:
                msg = f'"{ret.dag_id}" created successfully'
                refresh_dagbag(dag_id=ret.dag_id)
                return jsonify({
                    "message": msg,
                    "dag_id": ret.dag_id,
                    "template_id": tmpl_id
                }), 201
            else:
                return jsonify({"error": "Failed to create DAG"}), 500
        else:
            return jsonify({"error": "Validation failed", "errors": form.errors}), 400

    except Exception as e:
        self.log.exception(f"Error creating DAG via JSON API: {str(e)}")
        return jsonify({"error": str(e)}), 500
