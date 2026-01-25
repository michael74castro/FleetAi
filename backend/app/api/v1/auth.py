"""
FleetAI - Authentication API Routes
Supports both Azure AD and local authentication for testing
"""

from typing import List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose import jwt, JWTError

from app.core.database import get_async_session
from app.core.security import (
    create_access_token,
    create_refresh_token,
    verify_password,
    get_password_hash,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    SECRET_KEY,
    ALGORITHM,
)
from app.core.config import settings
from app.models.user import User, Role, UserCustomerAccess, Permission, RolePermission

logger = logging.getLogger(__name__)

router = APIRouter()

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)


# ============================================================================
# Request/Response Models
# ============================================================================

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = ACCESS_TOKEN_EXPIRE_MINUTES * 60


class UserResponse(BaseModel):
    user_id: int
    email: str
    display_name: str
    role_name: str
    is_active: bool
    customer_ids: Optional[List[str]] = None
    permissions: List[str] = []


class RegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str


# ============================================================================
# Helper Functions
# ============================================================================

async def get_or_create_admin_role(db: AsyncSession) -> Role:
    """Get or create admin role"""
    result = await db.execute(select(Role).where(Role.role_name == "admin"))
    role = result.scalar_one_or_none()

    if not role:
        role = Role(
            role_name="admin",
            role_description="System Administrator",
            role_level=100
        )
        db.add(role)
        await db.flush()

    return role


async def get_or_create_test_admin(db: AsyncSession) -> User:
    """Get or create a test admin user for development"""
    result = await db.execute(
        select(User).where(User.email == "admin@fleetai.local")
    )
    user = result.scalar_one_or_none()

    if user:
        return user

    admin_role = await get_or_create_admin_role(db)

    user = User(
        azure_ad_id="local-admin-001",
        email="admin@fleetai.local",
        display_name="Test Admin",
        role_id=admin_role.role_id,
        password_hash=get_password_hash("admin123"),
        is_active=True
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    return user


async def get_current_user_from_token(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_session)
) -> User:
    """Get current user from JWT token"""
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    result = await db.execute(
        select(User).where(User.user_id == int(user_id))
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User is disabled"
        )

    return user


async def get_user_permissions(db: AsyncSession, user: User) -> List[str]:
    """Get permissions for a user based on their role"""
    if not user.role_id:
        return []

    # RolePermission is a Table object, use .c for column access
    result = await db.execute(
        select(Permission.permission_name)
        .join(RolePermission, RolePermission.c.permission_id == Permission.permission_id)
        .where(RolePermission.c.role_id == user.role_id)
    )
    return [row[0] for row in result.fetchall()]


# ============================================================================
# Authentication Endpoints
# ============================================================================

@router.post("/login", response_model=TokenResponse)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_async_session)
):
    """
    Login with username/password.

    Default test credentials:
    - Email: admin@fleetai.local
    - Password: admin123
    """
    result = await db.execute(
        select(User).where(User.email == form_data.username)
    )
    user = result.scalar_one_or_none()

    # Auto-create test admin if needed
    if not user and form_data.username == "admin@fleetai.local":
        user = await get_or_create_test_admin(db)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not user.password_hash or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled"
        )

    # Load role name
    role_name = "user"
    if user.role_id:
        result = await db.execute(select(Role).where(Role.role_id == user.role_id))
        role = result.scalar_one_or_none()
        if role:
            role_name = role.role_name

    access_token = create_access_token(
        data={
            "sub": str(user.user_id),
            "email": user.email,
            "role": role_name
        }
    )
    refresh_token = create_refresh_token(data={"sub": str(user.user_id)})

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_session)
):
    """
    Get current authenticated user information.
    For testing without token, returns test admin.
    """
    # If no token provided, create/return test admin for easy testing
    if not token:
        user = await get_or_create_test_admin(db)
    else:
        user = await get_current_user_from_token(token, db)

    # Get role
    role_name = "user"
    if user.role_id:
        result = await db.execute(select(Role).where(Role.role_id == user.role_id))
        role = result.scalar_one_or_none()
        if role:
            role_name = role.role_name

    # Get permissions
    permissions = await get_user_permissions(db, user)

    # Get customer IDs
    result = await db.execute(
        select(UserCustomerAccess.customer_id).where(
            UserCustomerAccess.user_id == user.user_id
        )
    )
    customer_ids = [row[0] for row in result.fetchall()]

    return UserResponse(
        user_id=user.user_id,
        email=user.email,
        display_name=user.display_name,
        role_name=role_name,
        is_active=user.is_active,
        customer_ids=customer_ids if customer_ids else None,
        permissions=permissions
    )


@router.post("/register", response_model=UserResponse)
async def register(
    data: RegisterRequest,
    db: AsyncSession = Depends(get_async_session)
):
    """Register a new user (for testing)"""
    result = await db.execute(
        select(User).where(User.email == data.email)
    )
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Get or create default user role
    result = await db.execute(select(Role).where(Role.role_name == "user"))
    default_role = result.scalar_one_or_none()

    if not default_role:
        default_role = Role(
            role_name="user",
            role_description="Standard User",
            role_level=10
        )
        db.add(default_role)
        await db.flush()

    user = User(
        azure_ad_id=f"local-{data.email}",
        email=data.email,
        display_name=data.display_name,
        role_id=default_role.role_id,
        password_hash=get_password_hash(data.password),
        is_active=True
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    return UserResponse(
        user_id=user.user_id,
        email=user.email,
        display_name=user.display_name,
        role_name=default_role.role_name,
        is_active=user.is_active,
        customer_ids=None,
        permissions=[]
    )


@router.post("/logout")
async def logout():
    """Logout - client should discard tokens"""
    return {"message": "Logged out successfully"}


@router.get("/validate")
async def validate_token(
    user: User = Depends(get_current_user_from_token)
):
    """Validate current authentication token"""
    return {
        "valid": True,
        "user_id": user.user_id,
        "email": user.email,
        "name": user.display_name
    }


@router.get("/permissions", response_model=List[str])
async def get_my_permissions(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_session)
):
    """Get list of permissions for current user"""
    if not token:
        user = await get_or_create_test_admin(db)
    else:
        user = await get_current_user_from_token(token, db)

    return await get_user_permissions(db, user)


@router.post("/init-test-data")
async def init_test_data(db: AsyncSession = Depends(get_async_session)):
    """
    Initialize test data including admin user and all roles.
    Call this once to set up the application for testing.
    """
    roles_data = [
        ("admin", "System Administrator", 100),
        ("super_user", "Super User - Full Read Access", 50),
        ("account_manager", "Account Manager", 40),
        ("fleet_admin", "Fleet Administrator", 30),
        ("client_contact", "Client Contact", 20),
        ("driver", "Driver", 10),
        ("user", "Standard User", 5),
    ]

    created_roles = []
    for name, description, level in roles_data:
        result = await db.execute(select(Role).where(Role.role_name == name))
        role = result.scalar_one_or_none()
        if not role:
            role = Role(
                role_name=name,
                role_description=description,
                role_level=level
            )
            db.add(role)
            created_roles.append(name)

    await db.flush()

    # Create test admin
    admin = await get_or_create_test_admin(db)
    await db.commit()

    return {
        "message": "Test data initialized successfully",
        "roles_created": created_roles,
        "test_admin": {
            "email": "admin@fleetai.local",
            "password": "admin123"
        }
    }
