"""
FleetAI - Administration API Routes
"""

from typing import List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import (
    AsyncDB, Pagination, UserAdminAccess
)
from app.models.user import User, Role, Permission, UserCustomerAccess, UserDriverLink
from app.models.report import Dataset
from app.schemas.user import (
    UserCreate, UserUpdate, UserResponse, UserWithRole,
    RoleCreate, RoleResponse, RoleWithPermissions, PermissionResponse,
    UserCustomerAccessCreate, UserCustomerAccessResponse,
    UserDriverLinkCreate, UserDriverLinkResponse
)
from app.schemas.report import DatasetCreate, DatasetResponse
from app.schemas.common import PaginatedResponse, SuccessResponse, CountResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# User Management
@router.get("/users", response_model=PaginatedResponse[UserWithRole])
async def list_users(
    db: AsyncDB,
    admin: UserAdminAccess,
    pagination: Pagination,
    search: Optional[str] = Query(None, max_length=100),
    role_id: Optional[int] = None,
    is_active: Optional[bool] = None
):
    """List all users (admin only)"""
    query = select(User).join(Role)

    if search:
        query = query.where(
            User.email.ilike(f"%{search}%") |
            User.display_name.ilike(f"%{search}%")
        )
    if role_id:
        query = query.where(User.role_id == role_id)
    if is_active is not None:
        query = query.where(User.is_active == is_active)

    # Count
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Paginate
    query = query.offset(pagination.offset).limit(pagination.page_size)
    query = query.order_by(User.display_name)

    result = await db.execute(query)
    users = result.scalars().all()

    user_responses = []
    for u in users:
        # Get role
        role_result = await db.execute(select(Role).where(Role.role_id == u.role_id))
        role = role_result.scalar_one()

        user_responses.append(UserWithRole(
            user_id=u.user_id,
            azure_ad_id=u.azure_ad_id,
            email=u.email,
            display_name=u.display_name,
            first_name=u.first_name,
            last_name=u.last_name,
            role_id=u.role_id,
            is_active=u.is_active,
            last_login=u.last_login,
            created_at=u.created_at,
            updated_at=u.updated_at,
            role=RoleResponse(
                role_id=role.role_id,
                role_name=role.role_name,
                role_description=role.role_description,
                role_level=role.role_level,
                created_at=role.created_at
            )
        ))

    return PaginatedResponse.create(
        items=user_responses,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    data: UserCreate,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Create a new user (admin only)"""
    # Check if user already exists
    result = await db.execute(
        select(User).where(
            (User.azure_ad_id == data.azure_ad_id) | (User.email == data.email)
        )
    )
    existing = result.scalar_one_or_none()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this Azure AD ID or email already exists"
        )

    # Verify role exists
    result = await db.execute(select(Role).where(Role.role_id == data.role_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Invalid role_id")

    user = User(
        azure_ad_id=data.azure_ad_id,
        email=data.email,
        display_name=data.display_name,
        first_name=data.first_name,
        last_name=data.last_name,
        role_id=data.role_id
    )

    db.add(user)
    await db.commit()
    await db.refresh(user)

    logger.info(f"User created: {user.email} by admin {admin.email}")

    return UserResponse(
        user_id=user.user_id,
        azure_ad_id=user.azure_ad_id,
        email=user.email,
        display_name=user.display_name,
        first_name=user.first_name,
        last_name=user.last_name,
        role_id=user.role_id,
        is_active=user.is_active,
        last_login=user.last_login,
        created_at=user.created_at,
        updated_at=user.updated_at
    )


@router.get("/users/{user_id}", response_model=UserWithRole)
async def get_user(
    user_id: int,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Get user by ID (admin only)"""
    result = await db.execute(select(User).where(User.user_id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Get role
    role_result = await db.execute(select(Role).where(Role.role_id == user.role_id))
    role = role_result.scalar_one()

    return UserWithRole(
        user_id=user.user_id,
        azure_ad_id=user.azure_ad_id,
        email=user.email,
        display_name=user.display_name,
        first_name=user.first_name,
        last_name=user.last_name,
        role_id=user.role_id,
        is_active=user.is_active,
        last_login=user.last_login,
        created_at=user.created_at,
        updated_at=user.updated_at,
        role=RoleResponse(
            role_id=role.role_id,
            role_name=role.role_name,
            role_description=role.role_description,
            role_level=role.role_level,
            created_at=role.created_at
        )
    )


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    data: UserUpdate,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Update a user (admin only)"""
    result = await db.execute(select(User).where(User.user_id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if data.display_name is not None:
        user.display_name = data.display_name
    if data.first_name is not None:
        user.first_name = data.first_name
    if data.last_name is not None:
        user.last_name = data.last_name
    if data.role_id is not None:
        # Verify role exists
        result = await db.execute(select(Role).where(Role.role_id == data.role_id))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Invalid role_id")
        user.role_id = data.role_id
    if data.is_active is not None:
        user.is_active = data.is_active

    await db.commit()
    await db.refresh(user)

    logger.info(f"User updated: {user.email} by admin {admin.email}")

    return UserResponse(
        user_id=user.user_id,
        azure_ad_id=user.azure_ad_id,
        email=user.email,
        display_name=user.display_name,
        first_name=user.first_name,
        last_name=user.last_name,
        role_id=user.role_id,
        is_active=user.is_active,
        last_login=user.last_login,
        created_at=user.created_at,
        updated_at=user.updated_at
    )


@router.delete("/users/{user_id}", response_model=SuccessResponse)
async def delete_user(
    user_id: int,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Delete a user (admin only)"""
    result = await db.execute(select(User).where(User.user_id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.user_id == admin.user_id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")

    await db.delete(user)
    await db.commit()

    logger.info(f"User deleted: {user.email} by admin {admin.email}")

    return SuccessResponse(message="User deleted")


# Customer Access Management
@router.get("/users/{user_id}/customers", response_model=List[UserCustomerAccessResponse])
async def list_user_customers(
    user_id: int,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """List customer access for a user"""
    result = await db.execute(
        select(UserCustomerAccess).where(UserCustomerAccess.user_id == user_id)
    )
    access_list = result.scalars().all()

    return [
        UserCustomerAccessResponse(
            access_id=a.access_id,
            user_id=a.user_id,
            customer_id=a.customer_id,
            access_level=a.access_level,
            granted_at=a.granted_at,
            granted_by=a.granted_by
        )
        for a in access_list
    ]


@router.post("/users/{user_id}/customers", response_model=UserCustomerAccessResponse, status_code=201)
async def grant_customer_access(
    user_id: int,
    data: UserCustomerAccessCreate,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Grant customer access to a user"""
    # Verify user exists
    result = await db.execute(select(User).where(User.user_id == user_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="User not found")

    # Check if access already exists
    result = await db.execute(
        select(UserCustomerAccess).where(
            UserCustomerAccess.user_id == user_id,
            UserCustomerAccess.customer_id == data.customer_id
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Access already granted")

    access = UserCustomerAccess(
        user_id=user_id,
        customer_id=data.customer_id,
        access_level=data.access_level,
        granted_by=admin.user_id
    )

    db.add(access)
    await db.commit()
    await db.refresh(access)

    return UserCustomerAccessResponse(
        access_id=access.access_id,
        user_id=access.user_id,
        customer_id=access.customer_id,
        access_level=access.access_level,
        granted_at=access.granted_at,
        granted_by=access.granted_by
    )


@router.delete("/users/{user_id}/customers/{customer_id}", response_model=SuccessResponse)
async def revoke_customer_access(
    user_id: int,
    customer_id: str,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Revoke customer access from a user"""
    result = await db.execute(
        select(UserCustomerAccess).where(
            UserCustomerAccess.user_id == user_id,
            UserCustomerAccess.customer_id == customer_id
        )
    )
    access = result.scalar_one_or_none()

    if not access:
        raise HTTPException(status_code=404, detail="Access not found")

    await db.delete(access)
    await db.commit()

    return SuccessResponse(message="Access revoked")


# Role Management
@router.get("/roles", response_model=List[RoleWithPermissions])
async def list_roles(
    db: AsyncDB,
    admin: UserAdminAccess
):
    """List all roles with permissions"""
    result = await db.execute(select(Role).order_by(Role.role_level))
    roles = result.scalars().all()

    role_responses = []
    for role in roles:
        # Get permissions
        result = await db.execute(
            select(Permission).join(
                Role.permissions
            ).where(Role.role_id == role.role_id)
        )
        permissions = result.scalars().all()

        role_responses.append(RoleWithPermissions(
            role_id=role.role_id,
            role_name=role.role_name,
            role_description=role.role_description,
            role_level=role.role_level,
            created_at=role.created_at,
            permissions=[
                PermissionResponse(
                    permission_id=p.permission_id,
                    permission_name=p.permission_name,
                    permission_category=p.permission_category,
                    description=p.description
                )
                for p in permissions
            ]
        ))

    return role_responses


@router.get("/permissions", response_model=List[PermissionResponse])
async def list_permissions(
    db: AsyncDB,
    admin: UserAdminAccess
):
    """List all permissions"""
    result = await db.execute(
        select(Permission).order_by(Permission.permission_category, Permission.permission_name)
    )
    permissions = result.scalars().all()

    return [
        PermissionResponse(
            permission_id=p.permission_id,
            permission_name=p.permission_name,
            permission_category=p.permission_category,
            description=p.description
        )
        for p in permissions
    ]


# Dataset Management
@router.get("/datasets", response_model=List[DatasetResponse])
async def list_all_datasets(
    db: AsyncDB,
    admin: UserAdminAccess
):
    """List all datasets (admin view)"""
    result = await db.execute(select(Dataset).order_by(Dataset.name))
    datasets = result.scalars().all()

    return [
        DatasetResponse(
            dataset_id=d.dataset_id,
            name=d.name,
            display_name=d.display_name,
            description=d.description,
            source_type=d.source_type,
            source_object=d.source_object,
            rbac_column=d.rbac_column,
            is_active=d.is_active,
            schema_definition=None,
            default_filters=None,
            created_at=d.created_at,
            updated_at=d.updated_at
        )
        for d in datasets
    ]


@router.post("/datasets", response_model=DatasetResponse, status_code=201)
async def create_dataset(
    data: DatasetCreate,
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Create a new dataset"""
    # Check if name exists
    result = await db.execute(select(Dataset).where(Dataset.name == data.name))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Dataset name already exists")

    import json
    dataset = Dataset(
        name=data.name,
        display_name=data.display_name,
        description=data.description,
        source_type=data.source_type,
        source_object=data.source_object,
        schema_definition=json.dumps(data.schema_definition) if data.schema_definition else None,
        default_filters=json.dumps(data.default_filters) if data.default_filters else None,
        rbac_column=data.rbac_column,
        is_active=data.is_active
    )

    db.add(dataset)
    await db.commit()
    await db.refresh(dataset)

    return DatasetResponse(
        dataset_id=dataset.dataset_id,
        name=dataset.name,
        display_name=dataset.display_name,
        description=dataset.description,
        source_type=dataset.source_type,
        source_object=dataset.source_object,
        rbac_column=dataset.rbac_column,
        is_active=dataset.is_active,
        schema_definition=data.schema_definition,
        default_filters=data.default_filters,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at
    )


# Statistics
@router.get("/stats")
async def get_admin_stats(
    db: AsyncDB,
    admin: UserAdminAccess
):
    """Get system statistics"""
    from app.models.dashboard import Dashboard
    from app.models.report import Report
    from app.models.ai import AIConversation

    # Get counts
    user_count = (await db.execute(select(func.count(User.user_id)))).scalar()
    active_users = (await db.execute(
        select(func.count(User.user_id)).where(User.is_active == True)
    )).scalar()
    dashboard_count = (await db.execute(select(func.count(Dashboard.dashboard_id)))).scalar()
    report_count = (await db.execute(select(func.count(Report.report_id)))).scalar()
    conversation_count = (await db.execute(select(func.count(AIConversation.conversation_id)))).scalar()

    return {
        "users": {
            "total": user_count,
            "active": active_users
        },
        "dashboards": dashboard_count,
        "reports": report_count,
        "ai_conversations": conversation_count
    }
