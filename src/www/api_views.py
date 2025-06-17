import logging
from datetime import datetime
try:
    from airflow.www_rbac.app import csrf
except ImportError:
    # Airflow 2.0.0
    from airflow.www.app import csrf
from flask import (Blueprint, current_app, flash, g, jsonify, make_response,
                   redirect, request, url_for)
from functools import wraps
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.configuration import conf
from airflow.utils.session import create_session
from croniter import croniter
from datetime import datetime, timezone
from dagen.models import DagenDag, DagenDagVersion
from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.www.utils import login_required
from flask_appbuilder import expose, has_access
from dagen.utils import get_template_loader
from dagen.internal import refresh_dagbag

dagen_rest_bp = Blueprint('DagenRestView', __name__, url_prefix='/dagen/api')

log = logging.root.getChild(f'{__name__}.{"DagenRestView"}')

EXTERNAL_SCHEDULER_USER_ID = int(conf.get("ergo", "external_scheduler_user_id"))
API_KEY = conf.get("ergo", "api_key")

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
@dagen_rest_bp.route('/api/dags/ext/create', methods=('POST',))
@login_required
def create_dag_json():
    try:
    
        data = request.json
        data["start_date"] =  datetime.strptime(data.get('start_date'), "%d-%m-%Y")
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
        return jsonify({"error": str(e)}), 500

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 403
        return f(*args, **kwargs)
    return decorated

#
@csrf.exempt
@dagen_rest_bp.route("/dags/run", methods=["POST"])
@require_api_key
def trigger_dag_run():
    """
    Trigger a manual DAG run with a specific execution datetime and optional configuration.

    Expects a JSON payload:
        - dag_id (str): Required. ID of the DAG to trigger.
        - execution_date_time (str): Required. ISO 8601 datetime string with timezone (e.g., '2025-06-23T10:30:00+00:00').
        - conf (dict): Optional. Configuration dictionary passed to the DAG.

    Returns:
        Response 200: DAG successfully triggered, returns run_id and execution_date_time.
        Response 400: Missing or invalid parameters.
        Response 500: Internal error while triggering DAG.
    """
    try:
        data = request.get_json(force=True)

        dag_id = data.get("dag_id")
        execution_date_time_str = data.get("execution_date_time")
        conf = data.get("conf", {})

        if not dag_id:
            return jsonify({"error": "Missing 'dag_id' in request body"}), 400

        if not execution_date_time_str:
            return jsonify({"error": "Missing 'execution_date_time' in request body"}), 400

        try:
            exec_date = datetime.fromisoformat(execution_date_time_str)
        except ValueError:
            return jsonify({"error": "Invalid 'execution_date_time' format, must be ISO 8601"}), 400

        run_id = f"manual__{exec_date.isoformat()}"

        dag_run = trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            execution_date=exec_date,
            conf=conf,
        )

        return jsonify({
            "message": f"DAG '{dag_id}' triggered successfully",
            "run_id": dag_run.run_id,
            "execution_date_time": exec_date.isoformat()
        }), 200

    except Exception as e:
        log.exception("Failed to trigger DAG")
        return jsonify({"error": str(e)}), 500

@dagen_rest_bp.route("/dags/schedule/update", methods=["PATCH"])
@csrf.exempt
@require_api_key
def update_dag_schedule():
    """
    Update the schedule interval of a DAG by creating a new DAG version.

    Expects a JSON payload:
        - dag_id (str): Required. ID of the DAG to update.
        - schedule_interval (str): Required. New cron expression (validated using croniter).

    Returns:
        Response 200: DAG schedule updated, returns the new version number.
        Response 400: Missing or invalid parameters (e.g., invalid cron).
        Response 404: DAG or DAG version not found.
        Response 500: Internal error while updating the schedule.
    """
    try:
        data = request.get_json(force=True)
        dag_id = data.get("dag_id")
        new_schedule = data.get("schedule_interval")

        if not dag_id or not new_schedule:
            return jsonify({"error": "Missing 'dag_id' or 'schedule_interval'"}), 400

        if not croniter.is_valid(new_schedule):
            return jsonify({"error": "Invalid 'schedule_interval'. Must be a valid cron expression."}), 400

        with create_session() as session:
            dag_obj = session.query(DagenDag).filter(DagenDag.dag_id == dag_id).first()
            if not dag_obj:
                return jsonify({"error": f"DAG '{dag_id}' not found"}), 404

            latest_version = (
                session.query(DagenDagVersion)
                .filter(DagenDagVersion.dag_id == dag_id)
                .order_by(DagenDagVersion.version.desc())
                .first()
            )
            if not latest_version:
                return jsonify({"error": f"No version found for DAG '{dag_id}'"}), 404

            # Create a new version by copying values
            new_version_number = latest_version.version + 1
            new_version = DagenDagVersion(
                dag_id=dag_id,
                schedule_interval=new_schedule,
                creator=EXTERNAL_SCHEDULER_USER_ID
            )
            new_version.set_options(latest_version.dag_options)
            new_version.version = new_version_number
            new_version.approver_id = EXTERNAL_SCHEDULER_USER_ID
            new_version.approved_at = datetime.now(timezone.utc)


            session.add(new_version)

            dag_obj._live_version = new_version_number
            dag_obj.updated_at = datetime.now(timezone.utc)

            session.commit()

        refresh_dagbag(dag_id=dag_id)
        return jsonify({
            "message": f"Schedule for DAG '{dag_id}' updated to '{new_schedule}', version {new_version_number}"
        }), 200

    except Exception as e:
        log.exception("Failed to update DAG schedule")
        return jsonify({"error": str(e)}), 500

@csrf.exempt
@dagen_rest_bp.route("/dags/schedule/revert/latest", methods=["POST"])
@require_api_key
def revert_latest_external_schedule_override():
    """
    Revert the most recent DAG version created by the external scheduler.

    Expects a JSON payload:
        - dag_id (str): Required. ID of the DAG to revert.

    Returns:
        Response 200: DAG reverted to previous version, lists deleted version.
        Response 400: No suitable previous version found.
        Response 404: DAG or versions not found.
        Response 500: Internal error during revert.
    """
    return _revert_dag_schedule_override(delete_all=False)

@csrf.exempt
@dagen_rest_bp.route("/dags/schedule/revert/all", methods=["POST"])
@require_api_key
def revert_all_external_schedule_overrides():
    """
    Revert top DAG versions created by the external scheduler for a given DAG.
    This reverts all the latest DAG versions created by the external scheduler
    until a version created by a non-external user is encountered.

    Expects a JSON payload:
        - dag_id (str): Required. ID of the DAG to revert.

    Returns:
        Response 200: DAG reverted to latest non-external version, lists all deleted versions.
        Response 400: No non-external versions available to revert to.
        Response 404: DAG or versions not found.
        Response 500: Internal error during revert.
    """
    return _revert_dag_schedule_override(delete_all=True)

def _revert_dag_schedule_override(delete_all: bool):
    try:
        data = request.get_json(force=True)
        dag_id = data.get("dag_id")

        if not dag_id:
            return jsonify({"error": "Missing 'dag_id' in request body"}), 400

        with create_session() as session:
            dag_obj = session.query(DagenDag).filter(DagenDag.dag_id == dag_id).first()
            if not dag_obj:
                return jsonify({"error": f"DAG '{dag_id}' not found"}), 404

            versions = (
                session.query(DagenDagVersion)
                .filter(DagenDagVersion.dag_id == dag_id)
                .order_by(DagenDagVersion.version.desc())
                .all()
            )

            if not versions:
                return jsonify({"error": f"No versions found for DAG '{dag_id}'"}), 404

            new_live_version = None
            deleted_versions = []

            for version in versions:
                if version.creator_id == EXTERNAL_SCHEDULER_USER_ID:
                    if delete_all or len(deleted_versions) == 0:
                        deleted_versions.append(version.version)
                        session.delete(version)
                    else:
                        new_live_version = version.version
                        break
                else:
                    new_live_version = version.version
                    break

            if new_live_version is None or new_live_version < 1:
                return jsonify({
                    "error": f"No non-external versions found for DAG '{dag_id}'. Nothing to revert to."
                }), 400

            dag_obj._live_version = new_live_version
            dag_obj.updated_at = datetime.now(timezone.utc)
            session.commit()

        refresh_dagbag(dag_id=dag_id)

        return jsonify({
            "message": f"Reverted DAG '{dag_id}' to version {new_live_version}",
            "deleted_versions": deleted_versions
        }), 200

    except Exception as e:
        log.exception("Failed to revert DAG schedule override")
        return jsonify({"error": str(e)}), 500