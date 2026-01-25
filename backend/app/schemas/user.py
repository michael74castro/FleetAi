"""
FleetAI - User Pydantic Schemas
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr

from .common import FleetAIBaseModel, TimestampMixin


class RoleBase(BaseModel):
    """Base role schema"""
    role_name: str = Field(max_length=50)
    role_description: Optional[str] = Field(None, max_length=500)
    role_level: int = Field(ge=0, le=100)


class RoleCreate(RoleBase):
    """Schema for creating a role"""
    pass


class RoleResponse(RoleBase, FleetAIBaseModel):
    """Schema for role response"""
    role_id: int
    created_at: datetime


class PermissionResponse(FleetAIBaseModel):
    """Schema for permission response"""
    permission_id: int
    permission_name: str
    permission_category: Optional[str]
    description: Optional[str]


class RoleWithPermissions(RoleResponse):
    """Role with permissions"""
    permissions: List[PermissionResponse] = []


class UserBase(BaseModel):
    """Base user schema"""
    email: EmailStr
    display_name: Optional[str] = Field(None, max_length=200)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    role_id: int


class UserCreate(UserBase):
    """Schema for creating a user"""
    azure_ad_id: str = Field(max_length=100)


class UserUpdate(BaseModel):
    """Schema for updating a user"""
    display_name: Optional[str] = Field(None, max_length=200)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    role_id: Optional[int] = None
    is_active: Optional[bool] = None


class UserResponse(UserBase, FleetAIBaseModel):
    """Schema for user response"""
    user_id: int
    azure_ad_id: str
    is_active: bool
    last_login: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class UserWithRole(UserResponse):
    """User with role details"""
    role: RoleResponse


class UserWithPermissions(UserWithRole):
    """User with role and permissions"""
    permissions: List[str] = []
    customer_ids: List[str] = []


class UserCustomerAccessBase(BaseModel):
    """Base customer access schema"""
    customer_id: str = Field(max_length=20)
    access_level: str = Field(default="read", pattern="^(read|write|admin)$")


class UserCustomerAccessCreate(UserCustomerAccessBase):
    """Schema for granting customer access"""
    user_id: int


class UserCustomerAccessResponse(UserCustomerAccessBase, FleetAIBaseModel):
    """Schema for customer access response"""
    access_id: int
    user_id: int
    granted_at: datetime
    granted_by: Optional[int]


class UserDriverLinkCreate(BaseModel):
    """Schema for linking user to driver"""
    user_id: int
    driver_id: str = Field(max_length=20)


class UserDriverLinkResponse(FleetAIBaseModel):
    """Schema for driver link response"""
    link_id: int
    user_id: int
    driver_id: str
    linked_at: datetime


class CurrentUserResponse(BaseModel):
    """Schema for current user info"""
    user_id: int
    email: str
    display_name: Optional[str]
    role_name: str
    role_level: int
    permissions: List[str]
    customer_ids: List[str]
    is_driver: bool = False
    driver_id: Optional[str] = None


class SessionResponse(FleetAIBaseModel):
    """Schema for session response"""
    session_id: str
    user_id: int
    ip_address: Optional[str]
    started_at: datetime
    last_activity: datetime
    is_active: bool
