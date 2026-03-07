import logging
from datetime import datetime, timezone
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
from dagen.utils import get_template_loader, refresh_dagen_templates
from dagen.internal import refresh_dagbag

log = logging.root.getChild(f'{__name__}.{"DagenRestView"}')

EXTERNAL_SCHEDULER_USER_ID = int(conf.get("ergo", "external_scheduler_user_id"))
API_KEY = conf.get("ergo", "api_key")

dagen_fastapi_app = FastAPI(title="Dagen REST API")


# --- Auth helpers ---

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")


# --- Request/Response models ---

class TriggerDagRunRequest(BaseModel):
    dag_id: str
    execution_date_time: str
    conf: dict[str, Any] = {}


class UpdateScheduleRequest(BaseModel):
    dag_id: str
    schedule_interval: str


class RevertRequest(BaseModel):
    dag_id: str


class CreateDagRequest(BaseModel):
    template_id: str
    dag_id: str
    category: str = "default"
    schedule_interval: str
    options: dict[str, Any] = {}
    auto_approve: bool = False


class UpdateDagVersionRequest(BaseModel):
    schedule_interval: str | None = None
    options: dict[str, Any] | None = None
    auto_approve: bool = False
    disable: bool = False


# --- Helper functions ---

def _dag_to_dict(dag: DagenDag) -> dict:
    lv = dag.live_version
    return {
        "dag_id": dag.dag_id,
        "template_id": dag.template_id,
        "category": dag.category,
        "live_version": dag._live_version,
        "is_enabled": dag.is_enabled if dag.is_published else False,
        "is_published": dag.is_published,
        "created_at": str(dag.created_at),
        "updated_at": str(dag.updated_at),
        "live_schedule_interval": lv.schedule_interval if lv else None,
        "live_options": lv.dag_options if lv else None,
        "live_is_approved": lv.is_approved if lv else None,
    }


def _version_to_dict(v: DagenDagVersion) -> dict:
    return {
        "dag_id": v.dag_id,
        "version": v.version,
        "schedule_interval": v.schedule_interval,
        "options": v.dag_options,
        "is_approved": v.is_approved,
        "creator_id": v.creator_id,
        "approver_id": v.approver_id,
        "approved_at": str(v.approved_at) if v.approved_at else None,
        "created_at": str(v.created_at),
    }


# --- CRUD Endpoints ---

@dagen_fastapi_app.get("/dags")
def list_dags():
    """List all dagen-managed DAGs."""
    try:
        qs = DagenDagQueryset()
        dags = qs.get_all()
        return {"dags": [_dag_to_dict(d) for d in dags]}
    except Exception as e:
        log.exception("Failed to list DAGs")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.get("/dags/{dag_id}")
def get_dag(dag_id: str):
    """Get a dagen DAG with all its versions."""
    try:
        qs = DagenDagQueryset()
        dag = qs.get_dag(dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")
        versions = sorted(dag.versions, key=lambda v: v.version, reverse=True)
        result = _dag_to_dict(dag)
        result["versions"] = [_version_to_dict(v) for v in versions]
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to get DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.post("/dags", status_code=201)
def create_dag(req: CreateDagRequest):
    """Create a new dagen DAG with an initial version.

    The `options` dict should contain template-specific fields like:
    pool, start_date, synchronized_runs, ergo_task_id, ergo_task_data, etc.
    """
    try:
        # Validate template exists
        loader = get_template_loader()
        if req.template_id not in loader.template_classes:
            raise HTTPException(
                status_code=400,
                detail=f"Template '{req.template_id}' not found. "
                       f"Available: {list(loader.template_classes.keys())}"
            )

        if not croniter.is_valid(req.schedule_interval):
            raise HTTPException(
                status_code=400,
                detail="Invalid 'schedule_interval'. Must be a valid cron expression."
            )

        user_id = EXTERNAL_SCHEDULER_USER_ID

        with create_session() as session:
            existing = session.query(DagenDag).filter(DagenDag.dag_id == req.dag_id).first()
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"DAG '{req.dag_id}' already exists"
                )

            dag = DagenDag(req.dag_id, req.template_id, req.category)
            session.add(dag)
            session.flush()

            version = DagenDagVersion(
                req.dag_id,
                schedule_interval=req.schedule_interval,
                creator=user_id,
                **req.options
            )
            session.add(version)
            session.flush()

            dag._live_version = version.version
            if req.auto_approve:
                version.approve(user_id)

            session.commit()

            result = {
                "message": f"DAG '{req.dag_id}' created successfully",
                "dag_id": req.dag_id,
                "version": version.version,
                "is_approved": version.is_approved,
            }

        refresh_dagbag(dag_id=req.dag_id)
        return result

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to create DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.put("/dags/{dag_id}")
def update_dag(dag_id: str, req: UpdateDagVersionRequest):
    """Update a dagen DAG by creating a new version.

    If `disable` is true, sets live_version to None (disables the DAG).
    Otherwise creates a new version with the provided schedule/options.
    Omitted fields are carried over from the current live version.
    """
    try:
        with create_session() as session:
            dag = session.query(DagenDag).filter(DagenDag.dag_id == dag_id).first()
            if not dag:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")

            if req.disable:
                dag._live_version = None
                dag.updated_at = datetime.now(timezone.utc)
                session.commit()
                return {"message": f"DAG '{dag_id}' disabled"}

            # Carry over from current live version
            current = dag.live_version
            schedule = req.schedule_interval or (current.schedule_interval if current else None)
            if not schedule:
                raise HTTPException(status_code=400, detail="schedule_interval required (no existing version to inherit from)")

            if req.schedule_interval and not croniter.is_valid(req.schedule_interval):
                raise HTTPException(status_code=400, detail="Invalid schedule_interval")

            # Merge options: start from current, overlay new
            current_opts = current.dag_options if current else {}
            new_opts = {**current_opts, **(req.options or {})}

            user_id = EXTERNAL_SCHEDULER_USER_ID
            version = DagenDagVersion(
                dag_id,
                schedule_interval=schedule,
                creator=user_id,
                **new_opts
            )
            session.add(version)
            session.flush()

            dag._live_version = version.version
            dag.updated_at = datetime.now(timezone.utc)

            if req.auto_approve:
                version.approve(user_id)

            session.commit()

            result = {
                "message": f"DAG '{dag_id}' updated to version {version.version}",
                "version": version.version,
            }

        refresh_dagbag(dag_id=dag_id)
        return result

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to update DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.delete("/dags/{dag_id}")
def delete_dag(dag_id: str):
    """Delete a dagen DAG and all its versions."""
    try:
        qs = DagenDagQueryset()
        dag = qs.get_dag(dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")
        qs.delete_dag(dag_id).done()
        refresh_dagen_templates()
        return {"message": f"DAG '{dag_id}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Failed to delete DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.post("/dags/{dag_id}/approve")
def approve_dag(dag_id: str):
    """Approve the live version of a dagen DAG."""
    try:
        user_id = EXTERNAL_SCHEDULER_USER_ID
        qs = DagenDagVersionQueryset()
        qs.approve_live_version(dag_id, user_id).done()
        refresh_dagbag(dag_id=dag_id)
        return {"message": f"DAG '{dag_id}' approved"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.exception("Failed to approve DAG")
        raise HTTPException(status_code=500, detail=str(e))


@dagen_fastapi_app.get("/templates")
def list_templates():
    """List available DAG templates and their fields."""
    try:
        loader = get_template_loader()
        templates = {}
        for tid, cls in loader.template_classes.items():
            fields = {}
            for fname, field in cls.get_form_fields().items():
                fields[fname] = {
                    "label": field.label.text if hasattr(field.label, 'text') else str(field.label),
                    "type": type(field).__name__,
                    "description": field.description or "",
                    "default": field.default if hasattr(field, 'default') else None,
                }
            templates[tid] = {
                "template_id": tid,
                "format_dag_id": cls.format_dag_id,
                "fields": fields,
            }
        return {"templates": templates}
    except Exception as e:
        log.exception("Failed to list templates")
        raise HTTPException(status_code=500, detail=str(e))


# --- Existing Endpoints ---

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
