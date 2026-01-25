"""
FleetAI - API Dependencies
Common dependencies used across API endpoints
"""

from typing import Annotated, Optional
import logging

from fastapi import Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import joinedload
from jose import jwt, JWTError

from app.core.database import get_async_session, AsyncSessionLocal
from app.core.azure_ad import get_current_user, get_optional_user, AzureADUser
from app.core.security import rate_limiter, SECRET_KEY, ALGORITHM
from app.core.config import settings
from app.models.user import User
from app.schemas.common import PaginationParams

logger = logging.getLogger(__name__)

# OAuth2 scheme for local JWT authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)


# Type aliases for common dependencies
AsyncDB = Annotated[AsyncSession, Depends(get_async_session)]
CurrentUser = Annotated[AzureADUser, Depends(get_current_user)]
OptionalUser = Annotated[Optional[AzureADUser], Depends(get_optional_user)]


async def get_db() -> AsyncSession:
    """Dependency for getting database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_current_db_user(
    db: AsyncDB,
    token: Optional[str] = Depends(oauth2_scheme)
) -> User:
    """
    Get current user from database.

    This dependency:
    1. Validates JWT token (local or Azure AD)
    2. Finds user in database
    3. Returns database user with roles and permissions

    Supports both local JWT tokens and Azure AD tokens.
    """
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Try local JWT validation first (for development/testing)
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id:
            from app.models.user import Role
            # Load user and role in a single joined query
            result = await db.execute(
                select(User, Role)
                .join(Role, User.role_id == Role.role_id)
                .where(User.user_id == int(user_id))
            )
            row = result.one_or_none()
            if row:
                user, role = row
                # Cache role data to avoid lazy loading issues
                user.set_role_cache(role)
                if not user.is_active:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="User account is disabled"
                    )
                return user
    except JWTError:
        pass  # Not a valid local JWT, try Azure AD

    # Try Azure AD validation if configured
    if settings.AZURE_TENANT_ID and settings.AZURE_CLIENT_ID:
        try:
            from app.core.azure_ad import validate_azure_ad_token
            azure_user = await validate_azure_ad_token(token)
            result = await db.execute(
                select(User)
                .options(joinedload(User.role))
                .where(User.azure_ad_id == azure_user.oid)
            )
            user = result.scalar_one_or_none()

            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not provisioned in FleetAI. Contact administrator."
                )

            if not user.is_active:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User account is disabled"
                )

            return user
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Azure AD validation failed: {e}")

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )


DBUser = Annotated[User, Depends(get_current_db_user)]


async def get_user_customer_ids(db: AsyncDB, user: DBUser) -> list[str]:
    """
    Get list of customer IDs the user has access to.

    Returns:
        List of customer_id strings the user can access
    """
    from app.models.user import UserCustomerAccess

    # Super users and admins have access to all
    if user.get_role_level() >= 50:
        # For admins, return None to indicate full access (handled by caller)
        # or fetch all customer IDs if needed
        return []  # Empty list means full access for admins

    # Otherwise, get from user_customer_access using ORM
    result = await db.execute(
        select(UserCustomerAccess.customer_id).where(
            UserCustomerAccess.user_id == user.user_id
        )
    )
    return [row[0] for row in result.fetchall()]


UserCustomerIDs = Annotated[list[str], Depends(get_user_customer_ids)]


def get_pagination(
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=100, description="Items per page"),
    sort_by: Optional[str] = Query(default=None, description="Sort field"),
    sort_order: str = Query(default="asc", pattern="^(asc|desc)$", description="Sort order")
) -> PaginationParams:
    """
    Pagination parameters dependency.

    Usage:
        @app.get("/items")
        async def list_items(pagination: PaginationParams = Depends(get_pagination)):
            offset = (pagination.page - 1) * pagination.page_size
            ...
    """
    return PaginationParams(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order
    )


Pagination = Annotated[PaginationParams, Depends(get_pagination)]


class RateLimitDependency:
    """
    Rate limiting dependency.

    Usage:
        rate_limit = RateLimitDependency(requests=10, seconds=60)

        @app.get("/limited")
        async def limited_endpoint(
            _: None = Depends(rate_limit),
            user: DBUser = ...
        ):
            ...
    """

    def __init__(self, requests: int = 100, seconds: int = 60):
        self.requests = requests
        self.seconds = seconds

    async def __call__(self, user: User = Depends(get_current_db_user)) -> None:
        key = f"rate_limit:{user.user_id}"
        if not rate_limiter.is_allowed(key):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Please try again later."
            )


# Pre-configured rate limiters
standard_rate_limit = RateLimitDependency(requests=100, seconds=60)
strict_rate_limit = RateLimitDependency(requests=10, seconds=60)
ai_rate_limit = RateLimitDependency(requests=20, seconds=60)


class PermissionDependency:
    """
    Permission checking dependency.

    Usage:
        require_dashboard_create = PermissionDependency("dashboard:create")

        @app.post("/dashboards")
        async def create_dashboard(
            user: User = Depends(require_dashboard_create)
        ):
            ...
    """

    def __init__(self, permission: str):
        self.permission = permission

    async def __call__(
        self,
        db: AsyncSession = Depends(get_async_session),
        user: User = Depends(get_current_db_user)
    ) -> User:
        from app.models.user import Permission, RolePermission

        # Check if user's role has this permission using SQLAlchemy ORM
        result = await db.execute(
            select(Permission.permission_id)
            .join(RolePermission, RolePermission.c.permission_id == Permission.permission_id)
            .where(
                RolePermission.c.role_id == user.role_id,
                Permission.permission_name == self.permission
            )
        )

        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {self.permission}"
            )

        return user


# Pre-configured permission dependencies
require_view_dashboards = PermissionDependency("dashboard:view")
require_create_dashboards = PermissionDependency("dashboard:create")
require_edit_dashboards = PermissionDependency("dashboard:edit")
require_view_reports = PermissionDependency("report:view")
require_create_reports = PermissionDependency("report:create")
require_export_reports = PermissionDependency("report:export")
require_ai_access = PermissionDependency("ai:assistant")
require_admin_access = PermissionDependency("admin:users")

# Annotated type aliases for permission-checked users
UserViewDashboards = Annotated[User, Depends(require_view_dashboards)]
UserCreateDashboards = Annotated[User, Depends(require_create_dashboards)]
UserEditDashboards = Annotated[User, Depends(require_edit_dashboards)]
UserViewReports = Annotated[User, Depends(require_view_reports)]
UserCreateReports = Annotated[User, Depends(require_create_reports)]
UserExportReports = Annotated[User, Depends(require_export_reports)]
UserAIAccess = Annotated[User, Depends(require_ai_access)]
UserAdminAccess = Annotated[User, Depends(require_admin_access)]


async def set_session_context(db: AsyncSession, user_id: int):
    """Set session context for row-level security"""
    await db.execute(
        text("EXEC sp_set_session_context @key=N'user_id', @value=:user_id"),
        {"user_id": user_id}
    )


async def get_db_with_rls(
    db: AsyncDB,
    user: DBUser
) -> AsyncSession:
    """
    Get database session with RLS context set.

    Usage:
        @app.get("/data")
        async def get_data(db: AsyncSession = Depends(get_db_with_rls)):
            # RLS is automatically applied
            result = await db.execute(select(Contract))
            ...
    """
    await set_session_context(db, user.user_id)
    return db


DBWithRLS = Annotated[AsyncSession, Depends(get_db_with_rls)]
