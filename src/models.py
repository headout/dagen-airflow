import json
from functools import cached_property

from airflow.models.base import ID_LEN
from airflow.utils import timezone
from airflow.utils.dates import cron_presets
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from croniter import croniter
from flask_appbuilder.security.sqla.models import User
from sqlalchemy import Column, ForeignKey, Integer, String, Text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload, relationship, sessionmaker

Base = declarative_base()


class DagenDag(Base):
    __tablename__ = 'dagen_dag'
    VALID_ATTRIBUTES = (
        'dag_id', 'template_id', 'category', 'created_at', '_live_version',
        'updated_at'
    )

    dag_id = Column(String(ID_LEN), primary_key=True)
    template_id = Column(String(ID_LEN), index=True, nullable=False)
    category = Column(String(50), default="default", nullable=False)
    _live_version = Column('live_version', Integer)
    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )
    updated_at = Column(
        UtcDateTime, index=True, nullable=False,
        default=timezone.utcnow, onupdate=timezone.utcnow
    )

    versions = relationship('DagenDagVersion', back_populates='dag')
    live_version = relationship(
        'DagenDagVersion',
        primaryjoin='and_(DagenDagVersion.dag_id == DagenDag.dag_id, '
                    'DagenDagVersion.version == DagenDag._live_version)',
        lazy='immediate',
        uselist=False
    )

    def __str__(self):
        version = f'v{self._live_version}' if self.is_published else 'Disabled'
        return f'({self.category}) {self.dag_id} - {version}'

    def __init__(self, dag_id, template_id, category=None):
        self.dag_id = dag_id
        self.template_id = template_id
        if category is not None:
            self.category = category

    @property
    def is_enabled(self):
        return self.is_published and self.live_version.is_approved

    @property
    def is_published(self):
        return self._live_version is not None

    @property
    def version_str(self):
        return f'v{self._live_version}' if self.is_published else None

    @provide_session
    def get_version(self, version, session=None):
        return session.query(DagenDagVersion).get({
            'dag_id': self.dag_id,
            'version': version
        })

    def __getattr__(self, name):
        if hasattr(DagenDagVersion, name):
            return getattr(self.live_version, name)
        return super().__getattr__(name)

    def __eq__(self, value):
        if isinstance(value, type(self)):
            return self.dict_repr == value.dict_repr
        return super().__eq__(value)

    @cached_property
    def dict_repr(self):
        return self.toDict(DagenDag.VALID_ATTRIBUTES)

    def toDict(self, keep_attrs):
        return {attr: getattr(self, attr, None) for attr in keep_attrs}


class DagenDagVersion(Base):
    __tablename__ = 'dagen_dag_version'
    VALID_ATTRIBUTES = (
        'dag_id', 'version', 'dag_options', 'created_at', 'schedule_interval',
        'creator_str', 'approver_str', 'approved_at'
    )
    EDIT_ATTRIBUTES = (
        'dag_id', 'dag_options', 'schedule_interval',
    )

    dag_id = Column(
        ForeignKey("dagen_dag.dag_id", ondelete='CASCADE'),
        index=True,
        primary_key=True
    )
    version = Column(Integer, primary_key=True)
    _options = Column('dag_options', Text, default='{}', nullable=False)
    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )
    _schedule_interval = Column(
        'schedule_interval', String(50), nullable=False)
    # Foreign keys to FAB's ab_user model
    creator_id = Column('creator', ForeignKey(User.id, ondelete='SET NULL'))
    approver_id = Column('approver', ForeignKey(User.id, ondelete='SET NULL'))
    approved_at = Column(UtcDateTime, index=True)

    dag = relationship('DagenDag', back_populates='versions')
    creator = relationship(User, foreign_keys=(creator_id,), lazy='immediate')
    approver = relationship(User, foreign_keys=(approver_id,), lazy='immediate')

    def __str__(self):
        return f'{self.dag_id} - v{self.version}'

    def __init__(self, dag_id, schedule_interval=None, options=None, creator=None):
        self.dag_id = dag_id
        if schedule_interval is not None:
            self.set_schedule_interval(schedule_interval)
        if options is not None:
            self.set_options(options)
        self.creator_id = creator

    def set_schedule_interval(self, schedule_interval):
        self._schedule_interval = schedule_interval

    def set_options(self, options):
        try:
            self._options = json.dumps(options)
        except Exception:
            self._options = str(options)

    @cached_property
    def creator_str(self):
        return str(self.creator)

    @cached_property
    def approver_str(self):
        return str(self.approver)

    @cached_property
    def dag_options(self):
        return json.loads(self._options)

    @cached_property
    def schedule_interval(self):
        return self._schedule_interval
    #     if self._schedule_interval == '@once':
    #         interval = None
    #     else:
    #         interval = cron_presets.get(
    #             self._schedule_interval, self._schedule_interval)
    #     return interval

    @cached_property
    def cron_interval(self):
        return croniter(self.schedule_interval)

    @cached_property
    def is_approved(self):
        return self.approver_id is not None

    def approve(self, user):
        self.approver_id = user
        self.approved_at = timezone.utcnow()

    def __eq__(self, value):
        if isinstance(value, type(self)):
            return self.toDict(DagenDagVersion.EDIT_ATTRIBUTES) == value.toDict(DagenDagVersion.EDIT_ATTRIBUTES)
        return super().__eq__(value)

    @cached_property
    def dict_repr(self):
        return self.toDict(DagenDagVersion.VALID_ATTRIBUTES)

    def toDict(self, keep_attrs):
        result = {}
        for attr in keep_attrs:
            if attr == 'dag_options':
                result.update(self.dag_options)
            else:
                result[attr] = getattr(self, attr, None)
        return result


@event.listens_for(DagenDagVersion, 'before_insert')
def autoincrement_version(mapper, connection, target):
    """
        Hack needed since MySQL InnoDB engine doesn't support
        having one of the composite primary keys as autoincrement

        SO - https://stackoverflow.com/a/18949951/2674983
    """
    session = sessionmaker(bind=connection)()
    if target.version is None:
        target.version = (
            session.query(DagenDagVersion)
            .filter(DagenDagVersion.dag_id == target.dag_id)
        ).count() + 1
