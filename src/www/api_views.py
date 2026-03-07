import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from airflow.configuration import conf
from airflow.utils.session import create_session
from airflow.models.dagrun import DagRun
from airflow.utils.types import DagRunType
from croniter import croniter
from dagen.models import DagenDag, DagenDagVersion
from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.utils import get_template_loader
from dagen.internal import refresh_dagbag

log = logging.root.getChild(f'{__name__}.{"DagenRestView"}')

EXTERNAL_SCHEDULER_USER_ID = int(conf.get("ergo", "external_scheduler_user_id"))
API_KEY = conf.get("ergo", "api_key")

dagen_fastapi_app = FastAPI(title="Dagen REST API")


# --- Auth helpers ---

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")


# --- Request models ---

class TriggerDagRunRequest(BaseModel):
    dag_id: str
    execution_date_time: str
    conf: dict[str, Any] = {}


class UpdateScheduleRequest(BaseModel):
    dag_id: str
    schedule_interval: str


class RevertRequest(BaseModel):
    dag_id: str


# --- Endpoints ---

@dagen_fastapi_app.post("/dags/approve/all")
def approve_all():
    """Approve all unapproved DAG versions. Uses external scheduler user."""
    try:
        user_id = EXTERNAL_SCHEDULER_USER_ID
        qs = DagenDagVersionQueryset()
        unapproved_versions = qs.get_all_current_unapproved()
        qs.approve_all(unapproved_versions, user_id).done()
        return {"count_approved": len(unapproved_versions)}
    except Exception as e:
        log.exception("Failed to approve all")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.post("/dags/run")
def trigger_dag_run(req: TriggerDagRunRequest, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    try:
        try:
            exec_date = datetime.fromisoformat(req.execution_date_time)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid 'execution_date_time' format, must be ISO 8601")

        run_id = f"manual__{exec_date.isoformat()}"

        with create_session() as session:
            dag_run = DagRun(
                dag_id=req.dag_id,
                run_id=run_id,
                execution_date=exec_date,
                run_type=DagRunType.MANUAL,
                conf=req.conf,
            )
            session.add(dag_run)
            session.commit()

        return {
            "message": f"DAG '{req.dag_id}' triggered successfully",
            "run_id": run_id,
            "execution_date_time": exec_date.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to trigger DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.patch("/dags/schedule/update")
def update_dag_schedule(req: UpdateScheduleRequest, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    try:
        if not croniter.is_valid(req.schedule_interval):
            raise HTTPException(status_code=400, detail="Invalid 'schedule_interval'. Must be a valid cron expression.")

        with create_session() as session:
            dag_obj = session.query(DagenDag).filter(DagenDag.dag_id == req.dag_id).first()
            if not dag_obj:
                raise HTTPException(status_code=404, detail=f"DAG '{req.dag_id}' not found")

            latest_version = (
                session.query(DagenDagVersion)
                .filter(DagenDagVersion.dag_id == req.dag_id)
                .order_by(DagenDagVersion.version.desc())
                .first()
            )
            if not latest_version:
                raise HTTPException(status_code=404, detail=f"No version found for DAG '{req.dag_id}'")

            new_version_number = latest_version.version + 1
            new_version = DagenDagVersion(
                dag_id=req.dag_id,
                schedule_interval=req.schedule_interval,
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

        refresh_dagbag(dag_id=req.dag_id)
        return {
            "message": f"Schedule for DAG '{req.dag_id}' updated to '{req.schedule_interval}', version {new_version_number}"
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to update DAG schedule")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.post("/dags/schedule/revert/latest")
def revert_latest_external_schedule_override(req: RevertRequest, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    return _revert_dag_schedule_override(req.dag_id, delete_all=False)


@dagen_fastapi_app.post("/dags/schedule/revert/all")
def revert_all_external_schedule_overrides(req: RevertRequest, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    return _revert_dag_schedule_override(req.dag_id, delete_all=True)


def _revert_dag_schedule_override(dag_id: str, delete_all: bool):
    try:
        with create_session() as session:
            dag_obj = session.query(DagenDag).filter(DagenDag.dag_id == dag_id).first()
            if not dag_obj:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")

            versions = (
                session.query(DagenDagVersion)
                .filter(DagenDagVersion.dag_id == dag_id)
                .order_by(DagenDagVersion.version.desc())
                .all()
            )

            if not versions:
                raise HTTPException(status_code=404, detail=f"No versions found for DAG '{dag_id}'")

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
                raise HTTPException(
                    status_code=400,
                    detail=f"No non-external versions found for DAG '{dag_id}'. Nothing to revert to."
                )

            dag_obj._live_version = new_live_version
            dag_obj.updated_at = datetime.now(timezone.utc)
            session.commit()

        refresh_dagbag(dag_id=dag_id)

        return {
            "message": f"Reverted DAG '{dag_id}' to version {new_live_version}",
            "deleted_versions": deleted_versions
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to revert DAG schedule override")
        raise HTTPException(status_code=500, detail=str(e))
