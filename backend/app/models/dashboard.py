"""
FleetAI - Dashboard Models
SQLAlchemy models for dashboard configuration
"""

from datetime import datetime
from typing import Optional, List
import uuid

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey,
    Text, UniqueConstraint, TypeDecorator
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.database import Base
from app.core.config import settings


# Use schema only for MSSQL, not SQLite
def get_app_table_args():
    if settings.DATABASE_TYPE == 'sqlite':
        return {}


# UUID type that works for both SQLite (as TEXT) and MSSQL (as UNIQUEIDENTIFIER)
class GUID(TypeDecorator):
    """Platform-independent GUID type - uses UNIQUEIDENTIFIER on MSSQL, TEXT on SQLite"""
    impl = String(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mssql':
            return dialect.type_descriptor(UNIQUEIDENTIFIER())
        return dialect.type_descriptor(String(36))


class Dashboard(Base):
    """Dashboard configuration model"""
    __tablename__ = 'dashboards'
    __table_args__ = get_app_table_args()

    dashboard_id: Mapped[str] = mapped_column(
        GUID(),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1000))
    layout_type: Mapped[str] = mapped_column(String(20), default='grid')
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
        back_populates="dashboards",
        foreign_keys=[created_by]
    )
    widgets: Mapped[List["DashboardWidget"]] = relationship(
        "DashboardWidget",
        back_populates="dashboard",
        cascade="all, delete-orphan"
    )
    access_list: Mapped[List["DashboardAccess"]] = relationship(
        "DashboardAccess",
        back_populates="dashboard",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Dashboard(name={self.name}, created_by={self.created_by})>"


class DashboardWidget(Base):
    """Dashboard widget configuration"""
    __tablename__ = 'dashboard_widgets'
    __table_args__ = get_app_table_args()

    widget_id: Mapped[str] = mapped_column(
        GUID(),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )
    dashboard_id: Mapped[str] = mapped_column(
        GUID(),
        ForeignKey('dashboards.dashboard_id', ondelete='CASCADE'),
        nullable=False
    )
    widget_type: Mapped[str] = mapped_column(String(50), nullable=False)
    position_x: Mapped[int] = mapped_column(Integer, default=0)
    position_y: Mapped[int] = mapped_column(Integer, default=0)
    width: Mapped[int] = mapped_column(Integer, default=3)
    height: Mapped[int] = mapped_column(Integer, default=2)
    config: Mapped[str] = mapped_column(Text, nullable=False)  # JSON configuration
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships
    dashboard: Mapped["Dashboard"] = relationship(
        "Dashboard",
        back_populates="widgets"
    )

    def __repr__(self):
        return f"<DashboardWidget(type={self.widget_type}, pos=({self.position_x},{self.position_y}))>"


class DashboardAccess(Base):
    """Dashboard sharing/access control"""
    __tablename__ = 'dashboard_access'
    __table_args__ = get_app_table_args()

    access_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dashboard_id: Mapped[str] = mapped_column(
        GUID(),
        ForeignKey('dashboards.dashboard_id', ondelete='CASCADE'),
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
    dashboard: Mapped["Dashboard"] = relationship(
        "Dashboard",
        back_populates="access_list"
    )
    user: Mapped[Optional["User"]] = relationship("User")
    role: Mapped[Optional["Role"]] = relationship("Role")

    def __repr__(self):
        return f"<DashboardAccess(dashboard_id={self.dashboard_id}, level={self.access_level})>"


# Widget type definitions (for reference)
WIDGET_TYPES = {
    'kpi_card': {
        'name': 'KPI Card',
        'description': 'Single metric with optional trend comparison',
        'default_size': {'w': 3, 'h': 2},
        'config_schema': {
            'dataset': 'string',
            'metric': 'string',
            'label': 'string',
            'format': 'string',
            'comparison': 'string'
        }
    },
    'line_chart': {
        'name': 'Line Chart',
        'description': 'Time series visualization',
        'default_size': {'w': 6, 'h': 4},
        'config_schema': {
            'dataset': 'string',
            'metrics': 'array',
            'dimensions': 'array',
            'filters': 'array'
        }
    },
    'bar_chart': {
        'name': 'Bar Chart',
        'description': 'Categorical comparison',
        'default_size': {'w': 6, 'h': 4},
        'config_schema': {
            'dataset': 'string',
            'metrics': 'array',
            'dimension': 'string',
            'orientation': 'string'
        }
    },
    'pie_chart': {
        'name': 'Pie Chart',
        'description': 'Distribution visualization',
        'default_size': {'w': 4, 'h': 4},
        'config_schema': {
            'dataset': 'string',
            'metric': 'string',
            'dimension': 'string'
        }
    },
    'donut_chart': {
        'name': 'Donut Chart',
        'description': 'Distribution with center metric',
        'default_size': {'w': 4, 'h': 4},
        'config_schema': {
            'dataset': 'string',
            'metric': 'string',
            'dimension': 'string',
            'center_metric': 'string'
        }
    },
    'table': {
        'name': 'Data Table',
        'description': 'Tabular data display',
        'default_size': {'w': 6, 'h': 4},
        'config_schema': {
            'dataset': 'string',
            'columns': 'array',
            'filters': 'array',
            'sorting': 'array',
            'pageSize': 'number'
        }
    },
    'map': {
        'name': 'Map',
        'description': 'Geographic visualization',
        'default_size': {'w': 6, 'h': 5},
        'config_schema': {
            'dataset': 'string',
            'latitude': 'string',
            'longitude': 'string',
            'marker_field': 'string'
        }
    },
    'gauge': {
        'name': 'Gauge',
        'description': 'Progress indicator',
        'default_size': {'w': 3, 'h': 3},
        'config_schema': {
            'dataset': 'string',
            'metric': 'string',
            'target': 'number',
            'thresholds': 'array'
        }
    }
}


# Forward references
from app.models.user import User, Role  # noqa: E402
