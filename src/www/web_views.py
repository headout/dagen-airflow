"""
Dagen Web UI - FastAPI app serving HTML pages.
Replaces the old Flask AppBuilder views for Airflow 3.x compatibility.
"""
import csv
import json
import logging
from io import StringIO
from pathlib import Path
from urllib.parse import quote, urlencode

from fastapi import FastAPI, Form, Query, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from airflow.configuration import conf
from dagen.db import create_session

from dagen.exceptions import TemplateNotFoundError
from dagen.internal import refresh_dagbag
from dagen.models import DagenDag, DagenDagVersion
from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.utils import get_template_loader, refresh_dagen_templates

log = logging.root.getChild(f'{__name__}.{"DagenWebView"}')

EXTERNAL_SCHEDULER_USER_ID = int(conf.get("ergo", "external_scheduler_user_id"))

# Paths
WWW_DIR = Path(__file__).parent
TEMPLATES_DIR = WWW_DIR / "templates"
STATIC_DIR = WWW_DIR / "static"

# FastAPI app
dagen_web_app = FastAPI(title="Dagen Web UI")
dagen_web_app.add_middleware(SessionMiddleware, secret_key="dagen-session-key")
dagen_web_app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="dagen_static")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Base URL prefix - matches plugin registration
BASE_URL = "/dagen/ui"
API_URL = "/dagen/api"


def _get_flash_messages(request: Request) -> list:
    """Get and clear flash messages from session."""
    messages = request.session.pop("flash_messages", [])
    return messages


def _flash(request: Request, message: str, category: str = "info"):
    """Add a flash message to session."""
    messages = request.session.get("flash_messages", [])
    messages.append((category, message))
    request.session["flash_messages"] = messages


def _ctx(request: Request, **kwargs):
    """Build common template context."""
    return {
        "request": request,
        "base_url": BASE_URL,
        "api_url": API_URL,
        "flash_messages": _get_flash_messages(request),
        **kwargs,
    }


def _redirect(path: str):
    """Redirect to a path under BASE_URL."""
    return RedirectResponse(url=f"{BASE_URL}{path}", status_code=303)


def _json_safe(obj):
    """Make an object JSON-serializable by converting datetimes to strings."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return obj


# --- Routes ---

@dagen_web_app.get("/", response_class=HTMLResponse)
@dagen_web_app.get("/dags", response_class=HTMLResponse)
async def list_dags(request: Request):
    """List all dagen-managed DAGs."""
    dag_list = []
    try:
        with create_session() as session:
            from sqlalchemy.orm import joinedload, subqueryload
            db_dags = (
                session.query(DagenDag)
                .options(joinedload(DagenDag.versions))
                .all()
            )
            # Pre-serialize to avoid detached session errors in templates
            for d in db_dags:
                lv = d.live_version
                # Access creator_str while session is active
                creator = None
                schedule = None
                if lv:
                    try:
                        creator = str(lv.creator_id)
                    except Exception:
                        creator = str(lv.creator_id)
                    schedule = lv._schedule_interval
                dag_list.append({
                    "dag_id": d.dag_id,
                    "template_id": d.template_id,
                    "category": d.category,
                    "is_enabled": d.is_enabled if d.is_published else False,
                    "is_published": d.is_published,
                    "version_str": d.version_str,
                    "schedule_interval": schedule,
                    "creator_str": creator,
                    "str": str(d),
                })
    except Exception as e:
        log.exception("Failed to list DAGs")

    return templates.TemplateResponse("dagen/dags.html", _ctx(
        request,
        dbDags=dag_list,
    ))


@dagen_web_app.get("/dags/create", response_class=HTMLResponse)
async def create_form(request: Request):
    """Show create DAG form."""
    tmpls = get_template_loader().template_classes
    forms = {key: tmpl.as_form() for key, tmpl in tmpls.items()}
    return templates.TemplateResponse("dagen/create-dag.html", _ctx(
        request,
        template_classes=tmpls,
        template_id=None,
        forms=forms,
    ))


@dagen_web_app.post("/dags/create", response_class=HTMLResponse)
async def create_submit(request: Request):
    """Handle create DAG form submission."""
    form_data = await request.form()
    tmpls = get_template_loader().template_classes
    forms = {key: tmpl.as_form() for key, tmpl in tmpls.items()}

    tmpl_id = form_data.get("template_id")
    if not tmpl_id or tmpl_id not in forms:
        _flash(request, "Invalid template ID", "error")
        return templates.TemplateResponse("dagen/create-dag.html", _ctx(
            request,
            template_classes=tmpls,
            template_id=tmpl_id,
            forms=forms,
        ))

    form = forms[tmpl_id]
    # Process the form with submitted data (WTForms-compatible)
    form.process(form_data)

    if form.validate():
        try:
            user_id = EXTERNAL_SCHEDULER_USER_ID
            # Create a simple user-like object
            class UserProxy:
                def __init__(self, uid):
                    self.id = uid
            ret = form.create(template_id=tmpl_id, user=UserProxy(user_id))
            if ret:
                _flash(request, f'DAG "{ret.dag_id}" created successfully!')
                # Handle form submission buttons
                if form_data.get("_add_another"):
                    return _redirect("/dags/create")
                elif form_data.get("_continue_editing"):
                    return _redirect(f"/dags/edit?dag_id={quote(ret.dag_id)}")
                return _redirect("/")
            else:
                _flash(request, "Failed to create DAG", "error")
        except Exception as e:
            log.exception("Failed to create DAG")
            _flash(request, f"Error: {e}", "error")

    return templates.TemplateResponse("dagen/create-dag.html", _ctx(
        request,
        template_classes=tmpls,
        template_id=tmpl_id,
        forms=forms,
    ))


@dagen_web_app.get("/dags/edit", response_class=HTMLResponse)
async def edit_form(request: Request, dag_id: str = Query(...)):
    """Show edit DAG form."""
    with create_session() as session:
        from sqlalchemy.orm import joinedload
        db_dag = (
            session.query(DagenDag)
            .options(joinedload(DagenDag.versions))
            .filter(DagenDag.dag_id == dag_id)
            .first()
        )
        if not db_dag:
            _flash(request, f"DAG '{dag_id}' not found", "error")
            return _redirect("/")

        # Pre-serialize versions while session is active
        versions = {}
        for v in db_dag.versions:
            versions[v.version] = _json_safe(v.dict_repr)

        # Get data for form initialization
        dag_info = {
            "dag_id": db_dag.dag_id,
            "template_id": db_dag.template_id,
            "category": db_dag.category,
            "_live_version": db_dag._live_version,
        }
        try:
            init_data = {**db_dag.dict_repr, **db_dag.live_version.dict_repr}
        except Exception:
            init_data = db_dag.dict_repr

    try:
        tmpl = get_template_loader().get_template_class(dag_info["template_id"])
    except TemplateNotFoundError as e:
        _flash(request, str(e), "error")
        _flash(request, "Either delete this DAG or add back the template with given template ID")
        return _redirect("/")

    form = tmpl.as_form(data=init_data)

    return templates.TemplateResponse("dagen/edit-dag.html", _ctx(
        request,
        dbDag=dag_info,
        dagVersions=versions,
        dag_id=dag_id,
        form=form,
    ))


@dagen_web_app.post("/dags/edit", response_class=HTMLResponse)
async def edit_submit(request: Request, dag_id: str = Query(...)):
    """Handle edit DAG form submission."""
    form_data = await request.form()

    qs = DagenDagQueryset()
    db_dag = qs.get_dag(dag_id)
    if not db_dag:
        _flash(request, f"DAG '{dag_id}' not found", "error")
        return _redirect("/")

    try:
        tmpl = get_template_loader().get_template_class(db_dag.template_id)
    except TemplateNotFoundError as e:
        _flash(request, str(e), "error")
        return _redirect("/")

    try:
        init_data = {**db_dag.dict_repr, **db_dag.live_version.dict_repr}
    except Exception:
        init_data = db_dag.dict_repr

    form = tmpl.as_form(data=init_data)
    form.process(form_data)

    if form.validate():
        try:
            class UserProxy:
                def __init__(self, uid):
                    self.id = uid
            user = UserProxy(EXTERNAL_SCHEDULER_USER_ID)
            ret = form.update(db_dag, user=user, form_version=form_data.get("live_version"))
            if ret:
                _flash(request, f'DAG "{dag_id}" version updated!')
                refresh_dagbag(dag_id=dag_id)
            else:
                _flash(request, f'DAG "{dag_id}" version unchanged!')

            if form_data.get("_add_another"):
                return _redirect("/dags/create")
            elif form_data.get("_continue_editing"):
                return _redirect(f"/dags/edit?dag_id={quote(dag_id)}")
            return _redirect("/")
        except Exception as e:
            log.exception("Failed to update DAG")
            _flash(request, f"Error: {e}", "error")

    # Re-fetch for template rendering
    versions = {}
    for v in db_dag.versions:
        versions[v.version] = _json_safe(v.dict_repr)
    dag_info = {
        "dag_id": db_dag.dag_id,
        "template_id": db_dag.template_id,
        "_live_version": db_dag._live_version,
    }
    qs.done()

    return templates.TemplateResponse("dagen/edit-dag.html", _ctx(
        request,
        dbDag=dag_info,
        dagVersions=versions,
        dag_id=dag_id,
        form=form,
    ))


@dagen_web_app.get("/dags/delete", response_class=HTMLResponse)
async def delete_dag(request: Request, dag_id: str = Query(...)):
    """Delete a dagen DAG."""
    try:
        DagenDagQueryset().delete_dag(dag_id).done()
        refresh_dagen_templates()
        _flash(request, f"Deleting DAG '{dag_id}'. May take a couple minutes to fully disappear.")
    except Exception:
        _flash(request, f"DAG '{dag_id}' could not be deleted.", "error")
    return _redirect("/")


@dagen_web_app.get("/dags/detail", response_class=HTMLResponse)
async def detail(request: Request, dag_id: str = Query(...)):
    """Show DAG details."""
    with create_session() as session:
        from sqlalchemy.orm import joinedload
        db_dag = (
            session.query(DagenDag)
            .options(joinedload(DagenDag.versions))
            .filter(DagenDag.dag_id == dag_id)
            .first()
        )
        if not db_dag:
            _flash(request, f"DAG '{dag_id}' not found", "error")
            return _redirect("/")

        lv = db_dag.live_version
        dag_info = {
            "dag_id": db_dag.dag_id,
            "template_id": db_dag.template_id,
            "category": db_dag.category,
            "is_enabled": db_dag.is_enabled if db_dag.is_published else False,
            "is_published": db_dag.is_published,
            "version_str": db_dag.version_str,
            "_live_version": db_dag._live_version,
            "created_at": str(db_dag.created_at),
            "updated_at": str(db_dag.updated_at),
            "live_options": _json_safe(lv.dag_options) if lv else None,
        }
        versions_list = []
        for v in sorted(db_dag.versions, key=lambda v: v.version, reverse=True):
            try:
                c_str = str(v.creator_id)
            except Exception:
                c_str = str(v.creator_id)
            versions_list.append({
                "version": v.version,
                "schedule_interval": v._schedule_interval,
                "is_approved": v.is_approved,
                "creator_str": c_str,
                "created_at": str(v.created_at),
            })

    return templates.TemplateResponse("dagen/detail.html", _ctx(
        request,
        dbDag=dag_info,
        dag_id=dag_id,
        versions=versions_list,
    ))


@dagen_web_app.get("/dags/approve", response_class=HTMLResponse)
async def approve_dag(request: Request, dag_id: str = Query(...)):
    """Approve a DAG's live version."""
    try:
        user_id = EXTERNAL_SCHEDULER_USER_ID
        DagenDagVersionQueryset().approve_live_version(dag_id, user_id).done()
        refresh_dagbag(dag_id=dag_id)
        _flash(request, f'DAG "{dag_id}" approved! Please wait 5-10 minutes for workers to refresh.')
    except ValueError as e:
        _flash(request, str(e), "error")
    except Exception as e:
        log.exception("Failed to approve DAG")
        _flash(request, f"Error: {e}", "error")
    return _redirect("/")


@dagen_web_app.get("/dags/save", response_class=HTMLResponse)
async def bulk_save_form(request: Request):
    """Show bulk save form."""
    tmpls = list(get_template_loader().template_classes.keys())
    return templates.TemplateResponse("dagen/bulk-save.html", _ctx(
        request,
        templates_list=tmpls,
        res_success=None,
        res_failure=None,
    ))


@dagen_web_app.post("/dags/save", response_class=HTMLResponse)
async def bulk_save_submit(request: Request):
    """Handle bulk save CSV upload."""
    form_data = await request.form()
    tmpls_list = list(get_template_loader().template_classes.keys())

    template_id = form_data.get("template_id")
    csv_file = form_data.get("csv_data")
    mark_approved = form_data.get("mark_approved") == "on"

    success_results = {}
    failed_results = {}

    if not template_id or not csv_file:
        _flash(request, "Template ID and CSV file are required", "error")
    else:
        try:
            loader = get_template_loader()
            tmpl_clazz = loader.get_template_class(template_id)

            # Read CSV
            content = await csv_file.read()
            stream = StringIO(content.decode("UTF-8"))
            reader = csv.DictReader(stream)

            qs = DagenDagVersionQueryset()
            existing = DagenDagQueryset().get_all(eager_load_versions=True)
            dagmap = {d.dag_id: d for d in existing}

            class UserProxy:
                def __init__(self, uid):
                    self.id = uid
            user = UserProxy(EXTERNAL_SCHEDULER_USER_ID)

            for row in reader:
                try:
                    form = tmpl_clazz.as_form(data=row)
                    if form.validate():
                        cleaned = form.process_form_data(**form.data)
                        dag_id = cleaned["dag_id"]
                        db_dag = dagmap.get(dag_id)

                        if db_dag:
                            is_success = form.update(db_dag, user)
                            msg = f"{db_dag.version_str} {'updated' if is_success else 'unchanged'}"
                        else:
                            db_dag = form.create(template_id, user)
                            is_success = db_dag is not None
                            if is_success:
                                dagmap[db_dag.dag_id] = db_dag
                            msg = f"{db_dag.version_str} created" if is_success else "creation failed"

                        if mark_approved and is_success:
                            try:
                                qs.approve_live_version(dag_id, user.id)
                                msg += " | Approved"
                            except ValueError as e:
                                msg += f" | Approval failed: {e}"

                        if is_success:
                            success_results[dag_id] = msg
                        else:
                            failed_results[row.get("dag_id", "?")] = msg
                    else:
                        failed_results[row.get("dag_id", "?")] = f"Validation errors: {json.dumps(form.errors)}"
                except Exception as e:
                    failed_results[row.get("dag_id", "?")] = str(e)

            qs.done()
        except Exception as e:
            log.exception("Bulk save failed")
            _flash(request, f"Error: {e}", "error")

    return templates.TemplateResponse("dagen/bulk-save.html", _ctx(
        request,
        templates_list=tmpls_list,
        res_success=success_results or None,
        res_failure=failed_results or None,
    ))


@dagen_web_app.post("/dags/approve/all")
async def approve_all(request: Request):
    """Approve all unapproved DAG versions (AJAX endpoint)."""
    try:
        user_id = EXTERNAL_SCHEDULER_USER_ID
        qs = DagenDagVersionQueryset()
        unapproved = qs.get_all_current_unapproved()
        qs.approve_all(unapproved, user_id).done()
        return {"count_approved": len(unapproved)}
    except Exception as e:
        log.exception("Failed to approve all")
        return {"error": str(e)}
