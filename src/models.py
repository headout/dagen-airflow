import logging
from functools import cached_property

from airflow.models.base import ID_LEN
from airflow.sdk import timezone
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from croniter import croniter
from dagen.serialization import dumps, loads
from flask_appbuilder.security.sqla.models import User
from sqlalchemy import Column, ForeignKey, Integer, String, Text, event
from sqlalchemy.orm import relationship, sessionmaker
from airflow.models.base import Base

logger = logging.getLogger(__name__)


class DagenDag(Base):
    __tablename__ = 'dagen_dag'
    __table_args__ = {'extend_existing': True}

    VALID_ATTRIBUTES = (
        'dag_id',
        'template_id',
        'category',
        'created_at',
        '_live_version',
        'updated_at',
    )

    dag_id = Column(String(ID_LEN), primary_key=True)
    template_id = Column(String(ID_LEN), index=True, nullable=False)
    category = Column(String(50), default="default", nullable=False)
    _live_version = Column('live_version', Integer)

    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )
    updated_at = Column(
        UtcDateTime,
        index=True,
        nullable=False,
        default=timezone.utcnow,
        onupdate=timezone.utcnow,
    )

    versions = relationship(
        'DagenDagVersion',
        back_populates='dag',
        cascade='all, delete-orphan',
    )

    @cached_property
    def live_version(self):
        if self._live_version is None:
            return None
        from dagen.query import DagenDagVersionQueryset
        return (
            DagenDagVersionQueryset()
            .get_dag_versions(self.dag_id)
            .filter(DagenDagVersion.version == self._live_version)
            .first()
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
        return session.query(DagenDagVersion).get(
            {'dag_id': self.dag_id, 'version': version}
        )

    def __getattr__(self, name):
        if hasattr(DagenDagVersion, name):
            return getattr(self.live_version, name)
        raise AttributeError

    @cached_property
    def dict_repr(self):
        return self.toDict(self.VALID_ATTRIBUTES)

    def toDict(self, keep_attrs):
        return {attr: getattr(self, attr, None) for attr in keep_attrs}


class DagenDagVersion(Base):
    __tablename__ = 'dagen_dag_version'
    __table_args__ = {'extend_existing': True}

    VALID_ATTRIBUTES = (
        'dag_id',
        'version',
        'dag_options',
        'created_at',
        'schedule_interval',
        'creator_str',
        'approver_str',
        'approved_at',
    )

    dag_id = Column(
        ForeignKey("dagen_dag.dag_id", ondelete='CASCADE'),
        primary_key=True,
        index=True,
    )
    version = Column(Integer, primary_key=True)

    _options = Column('dag_options', Text, default='{}', nullable=False)

    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )

    _schedule_interval = Column(
        'schedule_interval', String(50), nullable=False
    )

    creator_id = Column('creator', Integer, ForeignKey('ab_user.id', ondelete='SET NULL'))
    approver_id = Column('approver', Integer, ForeignKey('ab_user.id', ondelete='SET NULL'))
    approved_at = Column(UtcDateTime, index=True)

    dag = relationship('DagenDag', back_populates='versions')
    creator = relationship(
        User,
        primaryjoin=lambda: DagenDagVersion.creator_id == User.id,
        foreign_keys=[creator_id],
        viewonly=True
    )
    approver = relationship(
        User,
        primaryjoin=lambda: DagenDagVersion.approver_id == User.id,
        foreign_keys=[approver_id],
        viewonly=True
    )

    def __str__(self):
        return f'{self.dag_id} - v{self.version}'

    def __init__(self, dag_id, schedule_interval=None, creator=None, **options):
        self.dag_id = dag_id
        if schedule_interval is not None:
            self.set_schedule_interval(schedule_interval)
        self.set_options(options)
        if creator is not None:
            self.creator_id = creator

    def set_schedule_interval(self, schedule_interval):
        self._schedule_interval = schedule_interval

    def set_options(self, options):
        try:
            self._options = dumps(options)
        except Exception as e:
            logger.exception("could not serialize options", exc_info=e)
            self._options = str(options)

    @cached_property
    def creator_str(self):
        return str(self.creator) if self.creator else None

    @cached_property
    def approver_str(self):
        return str(self.approver) if self.approver else None

    @cached_property
    def dag_options(self):
        return loads(self._options)

    @cached_property
    def schedule_interval(self):
        return self._schedule_interval

    @cached_property
    def cron_interval(self):
        return croniter(self.schedule_interval)

    @cached_property
    def is_approved(self):
        return self.approver_id is not None

    def approve(self, user):
        self.approver_id = user
        self.approved_at = timezone.utcnow()

    @cached_property
    def dict_repr(self):
        return self.toDict(self.VALID_ATTRIBUTES)

    def get_options_for_form(self):
        data = dict(self.dag_options)
        data['synchronized_runs'] = (
            data.pop('max_active_runs', None) == 1
        )
        return data

    def toDict(self, keep_attrs):
        result = {}
        for attr in keep_attrs:
            if attr == 'dag_options':
                result.update(self.get_options_for_form())
            else:
                result[attr] = getattr(self, attr, None)
        return result


@event.listens_for(DagenDagVersion, 'before_insert')
def autoincrement_version(mapper, connection, target):
    session = sessionmaker(bind=connection)()
    if target.version is None:
        target.version = (
            session.query(DagenDagVersion)
            .filter(DagenDagVersion.dag_id == target.dag_id)
            .count()
            + 1
        )
