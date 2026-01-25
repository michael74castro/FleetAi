"""
FleetAI - Dashboard Pydantic Schemas
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .common import FleetAIBaseModel


class WidgetPosition(BaseModel):
    """Widget position on grid"""
    x: int = Field(ge=0, default=0)
    y: int = Field(ge=0, default=0)
    w: int = Field(ge=1, le=12, default=3)
    h: int = Field(ge=1, le=12, default=2)


class WidgetConfig(BaseModel):
    """Widget configuration"""
    dataset: Optional[str] = None
    metric: Optional[str] = None
    metrics: Optional[List[str]] = None
    dimension: Optional[str] = None
    dimensions: Optional[List[str]] = None
    filters: Optional[List[Dict[str, Any]]] = None
    label: Optional[str] = None
    format: Optional[str] = None
    comparison: Optional[str] = None
    colors: Optional[List[str]] = None
    limit: Optional[int] = None
    options: Optional[Dict[str, Any]] = None


class WidgetBase(BaseModel):
    """Base widget schema"""
    widget_type: str = Field(max_length=50)
    position_x: int = Field(ge=0, default=0)
    position_y: int = Field(ge=0, default=0)
    width: int = Field(ge=1, le=12, default=3)
    height: int = Field(ge=1, le=12, default=2)
    config: Dict[str, Any]


class WidgetCreate(WidgetBase):
    """Schema for creating a widget"""
    pass


class WidgetUpdate(BaseModel):
    """Schema for updating a widget"""
    widget_type: Optional[str] = None
    position_x: Optional[int] = None
    position_y: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    config: Optional[Dict[str, Any]] = None


class WidgetResponse(WidgetBase, FleetAIBaseModel):
    """Schema for widget response"""
    widget_id: str
    dashboard_id: str
    created_at: datetime
    updated_at: datetime


class DashboardFilter(BaseModel):
    """Dashboard filter configuration"""
    field: str
    type: str = "dropdown"  # dropdown, date_range, text
    label: Optional[str] = None
    multi: bool = False
    default_value: Optional[Any] = None


class DashboardRBAC(BaseModel):
    """Dashboard RBAC configuration"""
    roles: List[str] = []
    row_filter: Optional[str] = None


class DashboardConfigSchema(BaseModel):
    """Full dashboard configuration schema"""
    layout: str = "grid"
    widgets: List[Dict[str, Any]] = []
    filters: List[DashboardFilter] = []
    rbac: Optional[DashboardRBAC] = None
    theme: Optional[Dict[str, Any]] = None
    refresh_interval: Optional[int] = None  # seconds


class DashboardBase(BaseModel):
    """Base dashboard schema"""
    name: str = Field(max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    layout_type: str = Field(default="grid", max_length=20)
    is_template: bool = False
    is_public: bool = False


class DashboardCreate(DashboardBase):
    """Schema for creating a dashboard"""
    config: Dict[str, Any] = Field(default_factory=dict)


class DashboardUpdate(BaseModel):
    """Schema for updating a dashboard"""
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    layout_type: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    is_template: Optional[bool] = None
    is_public: Optional[bool] = None


class DashboardResponse(DashboardBase, FleetAIBaseModel):
    """Schema for dashboard response"""
    dashboard_id: str
    config: Dict[str, Any]
    created_by: int
    created_at: datetime
    updated_at: datetime


class DashboardWithWidgets(DashboardResponse):
    """Dashboard with widgets"""
    widgets: List[WidgetResponse] = []


class DashboardSummary(FleetAIBaseModel):
    """Dashboard summary for list views"""
    dashboard_id: str
    name: str
    description: Optional[str]
    is_template: bool
    is_public: bool
    widget_count: int = 0
    created_by: int
    created_at: datetime
    updated_at: datetime


class DashboardAccessBase(BaseModel):
    """Base dashboard access schema"""
    user_id: Optional[int] = None
    role_id: Optional[int] = None
    customer_id: Optional[str] = None
    access_level: str = Field(default="view", pattern="^(view|edit|admin)$")


class DashboardAccessCreate(DashboardAccessBase):
    """Schema for granting dashboard access"""
    pass


class DashboardAccessResponse(DashboardAccessBase, FleetAIBaseModel):
    """Schema for dashboard access response"""
    access_id: int
    dashboard_id: str
    granted_at: datetime


class DashboardCloneRequest(BaseModel):
    """Request to clone a dashboard"""
    name: str = Field(max_length=200)
    description: Optional[str] = None


class WidgetDataRequest(BaseModel):
    """Request for widget data"""
    widget_id: str
    filters: Optional[Dict[str, Any]] = None
    date_range: Optional[Dict[str, str]] = None


class WidgetDataResponse(BaseModel):
    """Response with widget data"""
    widget_id: str
    data: Any
    metadata: Optional[Dict[str, Any]] = None
