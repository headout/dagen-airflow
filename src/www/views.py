import logging
from functools import wraps

import airflow
from airflow.api.common.experimental import delete_dag
from airflow.exceptions import DagFileExists, DagNotFound
from airflow.utils.log.logging_mixin import LoggingMixin
from flask import current_app, flash, g, redirect, request, url_for
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose, has_access

from dagen.exceptions import TemplateNotFoundError
from dagen.internal import refresh_dagbag
from dagen.models import DagenDag
from dagen.query import DagenDagQueryset, DagenDagVersionQueryset
from dagen.utils import get_template_loader, refresh_dagen_templates
from dagen.www.forms import BulkSyncDagenForm
from dagen.www.utils import login_required


class DagenFABView(AppBuilderBaseView, LoggingMixin):
    route_base = '/dagen'

    log = logging.root.getChild(f'{__name__}.{"DagenFABView"}')

    @expose('/')
    @expose('/dags')
    @login_required
    @has_access
    def list(self):
        dbDags = DagenDagQueryset().get_all()
        return self.render_template(
            'dagen/dags.html',
            dbDags=dbDags
        )

    @expose('/dags/create', methods=('GET', 'POST'))
    @login_required
    @has_access
    def create(self):
        template = 'dagen/create-dag.html'

        tmpls = get_template_loader().template_classes
        forms = {key: tmpl.as_form() for key, tmpl in tmpls.items()}
        if request.method == 'POST' and request.form:
            tmplId = request.form.get('template_id')
            form = forms[tmplId]
            form.process(request.form)
            if form.validate():
                ret = form.create(template_id=tmplId, user=g.user)
                msg = f'"{ret.dag_id}" created successfully' if ret else "failed to create"
                flash(f'Dagen - {msg}!')
                if ret:
                    return self._handle_form_submission(request.form)
            return self.render_template(
                template,
                template_classes=tmpls,
                template_id=tmplId,
                forms=forms
            )
        return self.render_template(
            template,
            template_classes=tmpls,
            forms=forms
        )

    @expose('/dags/save', methods=('GET', 'POST'))
    @login_required
    @has_access
    def bulk_save(self):
        template = 'dagen/bulk-save.html'
        tmpls = get_template_loader().template_classes.keys()

        # make mark_approved no-op if no permission to approve
        has_approve_perm = self._has_permission('can_approve')

        form = BulkSyncDagenForm(
            templates=tmpls, has_approve_perm=has_approve_perm)
        context = {
            'form': form
        }
        if request.method == 'POST' and form.validate():
            dag_results = form.save(g.user)
            success_results, failed_results = dict(), dict()
            for dag_id, (is_success, message) in dag_results.items():
                if is_success:
                    success_results[dag_id] = message
                else:
                    failed_results[dag_id] = message
            context['res_success'] = success_results
            context['res_failure'] = failed_results
        return self.render_template(template, **context)

    @expose('/dags/edit', methods=('GET', 'POST'))
    @login_required
    @has_access
    def edit(self,):
        template = 'dagen/edit-dag.html'

        dag_id = request.args.get('dag_id')
        qs = DagenDagQueryset()
        dbDag = qs.get_dag(dag_id)
        versions = {
            version.version: version.dict_repr for version in dbDag.versions}
        try:
            tmpl = get_template_loader().get_template_class(dbDag.template_id)
        except TemplateNotFoundError as e:
            flash(e, category='error')
            flash(
                'Either delete this DAG or add back the template with given template ID')
            return self._redirect_home()
        try:
            init_data = {**dbDag.dict_repr, **dbDag.live_version.dict_repr}
        except Exception as e:
            self.log.exception(e)
            init_data = dbDag.dict_repr
        form = tmpl.as_form(data=init_data)
        if request.method == 'POST' and request.form:
            form.process(request.form)
            if form.validate():
                ret = form.update(dbDag, user=g.user,
                                  form_version=request.form.get('live_version'))
                flash(
                    f'Dagen "{dag_id}" version {"updated" if ret else "unchanged"}!')
                if ret:
                    refresh_dagbag(dag_id=dag_id)
                return self._handle_form_submission(request.form)
        return self.render_template(
            template,
            dbDag=dbDag,
            dagVersions=versions,
            dag_id=dag_id,
            form=form
        )

    @expose('/dags/delete')
    @login_required
    @has_access
    def delete(self, session=None):
        dag_id = request.args.get('dag_id')
        DagenDagQueryset().delete_dag(dag_id).done()
        refresh_dagen_templates()
        try:
            delete_dag.delete_dag(dag_id)
        except DagNotFound:
            flash("DAG with id {} not found. Cannot delete".format(dag_id), 'error')
            return self._redirect_home()
        except DagFileExists:
            flash("Dag id {} is still in DagBag. "
                  "Remove the DAG file first.".format(dag_id),
                  'error')
            return self._redirect_home()

        flash("Deleting DAG with id {}. May take a couple minutes to fully"
              " disappear.".format(dag_id))

        # Upon success return to home.
        return self._redirect_home()

    @expose('/dags/detail')
    @login_required
    @has_access
    def detail(self):
        tmpls = get_template_loader().templates
        dag_id = request.args.get('dag_id')
        dbDag = DagenDagQueryset().get_dag(dag_id)
        template = tmpls[dbDag. template_id]
        return self.render_template(
            'dagen/detail.html',
            dbDag=dbDag,
            dag_id=dag_id
        )

    @expose('/dags/approve')
    @login_required
    @has_access
    def approve(self):
        dag_id = request.args.get('dag_id')
        user_id = g.user.id
        try:
            DagenDagVersionQueryset().approve_live_version(dag_id, user_id).done()
            refresh_dagbag(dag_id=dag_id)
            flash(
                f'DAG "{dag_id}" approved successfully! Please wait for 5-10 minutes for workers to refresh and the DAG to go live.')
        except ValueError as e:
            flash(str(e))
        return self._redirect_home()

    def render_template(self, template, **kwargs):
        extra_ctx = {
            'perm_can_create': self._has_permission('can_create'),
            'perm_can_bulk_save': self._has_permission('can_bulk_save'),
            'perm_can_edit': self._has_permission('can_edit'),
            'perm_can_approve': self._has_permission('can_approve'),
            'perm_can_delete': self._has_permission('can_delete'),
            'perm_can_list': self._has_permission('can_list'),
            'perm_can_detail': self._has_permission('can_detail')
        }
        return super().render_template(template, **kwargs, **extra_ctx)

    def _handle_form_submission(self, data):
        if data.get('_add_another', None):
            return redirect(url_for('DagenFABView.create'))
        elif data.get('_continue_editing', None):
            return redirect(url_for('DagenFABView.edit', dag_id=data.get('dag_id')))
        return self._redirect_home()

    def _redirect_home(self):
        return redirect(url_for('DagenFABView.list'))

    def _has_permission(self, permission_str, user=None) -> bool:
        return self.appbuilder.sm.has_access(permission_str, 'DagenFABView', user=user)
