"""
FleetAI - User and Role Models
SQLAlchemy models for user management
"""

from datetime import datetime
from typing import Optional, List, ClassVar

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey,
    Table, Text, UniqueConstraint
)
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.database import Base
from app.core.config import settings


# Use schema only for MSSQL, not SQLite
def get_app_table_args():
    if settings.DATABASE_TYPE == 'sqlite':
        return {}
    return {'schema': 'app'}

def get_schema():
    return 'app' if settings.DATABASE_TYPE != 'sqlite' else None


# Association table for role-permissions
role_permissions = Table(
    'role_permissions',
    Base.metadata,
    Column('role_id', Integer, ForeignKey('roles.role_id'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.permission_id'), primary_key=True),
    schema=get_schema()
)

class Role(Base):
    """User role model"""
    __tablename__ = 'roles'
    __table_args__ = get_app_table_args()

    role_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    role_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    role_description: Mapped[Optional[str]] = mapped_column(String(500))
    role_level: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    users: Mapped[List["User"]] = relationship("User", back_populates="role")
    permissions: Mapped[List["Permission"]] = relationship(
        "Permission",
        secondary=role_permissions,
        back_populates="roles"
    )

    def __repr__(self):
        return f"<Role(role_name={self.role_name}, level={self.role_level})>"


class Permission(Base):
    """Permission model"""
    __tablename__ = 'permissions'
    __table_args__ = get_app_table_args()

    permission_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    permission_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    permission_category: Mapped[Optional[str]] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(String(500))

    # Relationships
    roles: Mapped[List["Role"]] = relationship(
        "Role",
        secondary=role_permissions,
        back_populates="permissions"
    )

    def __repr__(self):
        return f"<Permission({self.permission_name})>"


class User(Base):
    """User model - synced with Azure AD"""
    __tablename__ = 'users'
    __table_args__ = get_app_table_args()

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    azure_ad_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(200))
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    password_hash: Mapped[Optional[str]] = mapped_column(String(255))
    role_id: Mapped[int] = mapped_column(Integer, ForeignKey('roles.role_id'), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    role: Mapped["Role"] = relationship("Role", back_populates="users")
    customer_access: Mapped[List["UserCustomerAccess"]] = relationship(
        "UserCustomerAccess",
        back_populates="user",
        foreign_keys="UserCustomerAccess.user_id"
    )
    driver_link: Mapped[Optional["UserDriverLink"]] = relationship(
        "UserDriverLink",
        back_populates="user",
        uselist=False
    )
    dashboards: Mapped[List["Dashboard"]] = relationship(
        "Dashboard",
        back_populates="creator",
        foreign_keys="Dashboard.created_by"
    )
    reports: Mapped[List["Report"]] = relationship(
        "Report",
        back_populates="creator",
        foreign_keys="Report.created_by"
    )
    conversations: Mapped[List["AIConversation"]] = relationship(
        "AIConversation",
        back_populates="user"
    )

    def set_role_cache(self, role: "Role") -> None:
        """Cache role data to avoid lazy loading issues with async"""
        # Store in __dict__ directly to avoid ORM mapping
        self.__dict__['_cached_role_level'] = role.role_level
        self.__dict__['_cached_role_name'] = role.role_name

    def get_role_level(self) -> int:
        """Get role level from cache or default"""
        return self.__dict__.get('_cached_role_level', 0)

    def get_role_name(self) -> str:
        """Get role name from cache or default"""
        return self.__dict__.get('_cached_role_name', 'user')

    @property
    def full_name(self) -> str:
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.display_name or self.email

    def has_permission(self, permission_name: str) -> bool:
        """Check if user has a specific permission"""
        return any(
            p.permission_name == permission_name
            for p in self.role.permissions
        )

    def has_customer_access(self, customer_id: str) -> bool:
        """Check if user has access to a specific customer"""
        # Super users and admins have access to all
        if self.role.role_level >= 50:
            return True
        return any(
            ca.customer_id == customer_id
            for ca in self.customer_access
        )

    def __repr__(self):
        return f"<User(email={self.email}, role={self.role.role_name if self.role else None})>"


class UserCustomerAccess(Base):
    """User-Customer access mapping for row-level security"""
    __tablename__ = 'user_customer_access'
    __table_args__ = (
        UniqueConstraint('user_id', 'customer_id', name='uq_uca_user_customer'),
        get_app_table_args()
    )

    access_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.user_id'), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(20), nullable=False)
    access_level: Mapped[str] = mapped_column(String(20), default='read')
    granted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    granted_by: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('users.user_id'))

    # Relationships
    user: Mapped["User"] = relationship(
        "User",
        back_populates="customer_access",
        foreign_keys=[user_id]
    )
    granter: Mapped[Optional["User"]] = relationship(
        "User",
        foreign_keys=[granted_by]
    )

    def __repr__(self):
        return f"<UserCustomerAccess(user_id={self.user_id}, customer_id={self.customer_id})>"


class UserDriverLink(Base):
    """Link between user account and driver record (for driver role)"""
    __tablename__ = 'user_driver_link'
    __table_args__ = get_app_table_args()

    link_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.user_id'), unique=True, nullable=False)
    driver_id: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    linked_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="driver_link")

    def __repr__(self):
        return f"<UserDriverLink(user_id={self.user_id}, driver_id={self.driver_id})>"


class UserSession(Base):
    """User session tracking"""
    __tablename__ = 'user_sessions'
    __table_args__ = get_app_table_args()

    session_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.user_id'), nullable=False)
    access_token_hash: Mapped[Optional[str]] = mapped_column(String(64))
    ip_address: Mapped[Optional[str]] = mapped_column(String(50))
    user_agent: Mapped[Optional[str]] = mapped_column(String(500))
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_activity: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Relationships
    user: Mapped["User"] = relationship("User")

    def __repr__(self):
        return f"<UserSession(session_id={self.session_id}, user_id={self.user_id})>"


class AuditLog(Base):
    """Audit log for tracking user actions"""
    __tablename__ = 'audit_log'
    __table_args__ = get_app_table_args()

    log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('users.user_id'))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[Optional[str]] = mapped_column(String(100))
    entity_id: Mapped[Optional[str]] = mapped_column(String(100))
    old_values: Mapped[Optional[str]] = mapped_column(Text)
    new_values: Mapped[Optional[str]] = mapped_column(Text)
    ip_address: Mapped[Optional[str]] = mapped_column(String(50))
    user_agent: Mapped[Optional[str]] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    user: Mapped[Optional["User"]] = relationship("User")

    def __repr__(self):
        return f"<AuditLog(action={self.action}, entity={self.entity_type}:{self.entity_id})>"


# Import forward references for relationships
from app.models.dashboard import Dashboard  # noqa: E402
from app.models.report import Report  # noqa: E402
from app.models.ai import AIConversation  # noqa: E402

# Alias for auth.py compatibility
RolePermission = role_permissions
