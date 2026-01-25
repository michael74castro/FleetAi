"""
FleetAI - Report Models
SQLAlchemy models for report configuration and execution
"""

from datetime import datetime
from typing import Optional, List
import uuid

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Text
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.database import Base
from app.core.config import settings


# Use schema only for MSSQL, not SQLite
def get_app_table_args():
    if settings.DATABASE_TYPE == 'sqlite':
        return {}

class Report(Base):
    """Report configuration model"""
    __tablename__ = 'reports'
    __table_args__ = get_app_table_args()

    report_id: Mapped[str] = mapped_column(
        UNIQUEIDENTIFIER,
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1000))
    report_type: Mapped[str] = mapped_column(String(50), nullable=False)  # tabular, summary, detail
    dataset_name: Mapped[Optional[str]] = mapped_column(String(200))
    config: Mapped[str] = mapped_column(Text, nullable=False)  # JSON configuration
    is_template: Mapped[bool] = mapped_column(Boolean, default=False)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[int] = mapped_column(
        Integer,
        ForeignKey('users.user_id'),
        nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships
    creator: Mapped["User"] = relationship(
        "User",
        back_populates="reports",
        foreign_keys=[created_by]
    )
    access_list: Mapped[List["ReportAccess"]] = relationship(
        "ReportAccess",
        back_populates="report",
        cascade="all, delete-orphan"
    )
    schedules: Mapped[List["ReportSchedule"]] = relationship(
        "ReportSchedule",
        back_populates="report",
        cascade="all, delete-orphan"
    )
    executions: Mapped[List["ReportExecution"]] = relationship(
        "ReportExecution",
        back_populates="report"
    )

    def __repr__(self):
        return f"<Report(name={self.name}, type={self.report_type})>"


class ReportAccess(Base):
    """Report sharing/access control"""
    __tablename__ = 'report_access'
    __table_args__ = get_app_table_args()

    access_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[str] = mapped_column(
        UNIQUEIDENTIFIER,
        ForeignKey('reports.report_id', ondelete='CASCADE'),
        nullable=False
    )
    user_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey('users.user_id')
    )
    role_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey('roles.role_id')
    )
    customer_id: Mapped[Optional[str]] = mapped_column(String(20))
    access_level: Mapped[str] = mapped_column(String(20), default='view')
    granted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    report: Mapped["Report"] = relationship("Report", back_populates="access_list")
    user: Mapped[Optional["User"]] = relationship("User")
    role: Mapped[Optional["Role"]] = relationship("Role")

    def __repr__(self):
        return f"<ReportAccess(report_id={self.report_id}, level={self.access_level})>"


class ReportSchedule(Base):
    """Report scheduling configuration"""
    __tablename__ = 'report_schedules'
    __table_args__ = get_app_table_args()

    schedule_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[str] = mapped_column(
        UNIQUEIDENTIFIER,
        ForeignKey('reports.report_id', ondelete='CASCADE'),
        nullable=False
    )
    schedule_name: Mapped[Optional[str]] = mapped_column(String(200))
    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False)
    export_format: Mapped[str] = mapped_column(String(20), nullable=False)  # excel, pdf, csv
    recipients: Mapped[Optional[str]] = mapped_column(Text)  # JSON array of emails
    parameters: Mapped[Optional[str]] = mapped_column(Text)  # JSON parameters
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_run: Mapped[Optional[datetime]] = mapped_column(DateTime)
    next_run: Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_by: Mapped[int] = mapped_column(
        Integer,
        ForeignKey('users.user_id'),
        nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    report: Mapped["Report"] = relationship("Report", back_populates="schedules")
    creator: Mapped["User"] = relationship("User")

    def __repr__(self):
        return f"<ReportSchedule(report_id={self.report_id}, cron={self.cron_expression})>"


class ReportExecution(Base):
    """Report execution history"""
    __tablename__ = 'report_executions'
    __table_args__ = get_app_table_args()

    execution_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[str] = mapped_column(
        UNIQUEIDENTIFIER,
        ForeignKey('reports.report_id'),
        nullable=False
    )
    schedule_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey('report_schedules.schedule_id')
    )
    executed_by: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey('users.user_id')
    )
    execution_type: Mapped[str] = mapped_column(String(20), nullable=False)  # manual, scheduled
    parameters: Mapped[Optional[str]] = mapped_column(Text)  # JSON parameters used
    row_count: Mapped[Optional[int]] = mapped_column(Integer)
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    export_format: Mapped[Optional[str]] = mapped_column(String(20))
    file_path: Mapped[Optional[str]] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # running, success, failed
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    report: Mapped["Report"] = relationship("Report", back_populates="executions")
    schedule: Mapped[Optional["ReportSchedule"]] = relationship("ReportSchedule")
    executor: Mapped[Optional["User"]] = relationship("User")

    def __repr__(self):
        return f"<ReportExecution(report_id={self.report_id}, status={self.status})>"


class Dataset(Base):
    """Available datasets for report/dashboard building"""
    __tablename__ = 'datasets'
    __table_args__ = get_app_table_args()

    dataset_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(String(1000))
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)  # table, view, query
    source_object: Mapped[str] = mapped_column(String(200), nullable=False)
    schema_definition: Mapped[Optional[str]] = mapped_column(Text)  # JSON schema
    default_filters: Mapped[Optional[str]] = mapped_column(Text)  # JSON filters
    rbac_column: Mapped[Optional[str]] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<Dataset(name={self.name}, source={self.source_object})>"


# Forward references
from app.models.user import User, Role  # noqa: E402
