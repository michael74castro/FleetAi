"""
FleetAI - Common Pydantic Schemas
Shared schemas used across the application
"""

from datetime import datetime
from typing import Any, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field, ConfigDict


# Generic type for paginated responses
T = TypeVar('T')


class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    sort_by: Optional[str] = None
    sort_order: str = Field(default="asc", pattern="^(asc|desc)$")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper"""
    items: List[T]
    total: int
    page: int
    page_size: int
    pages: int

    @classmethod
    def create(
        cls,
        items: List[T],
        total: int,
        page: int,
        page_size: int
    ) -> "PaginatedResponse[T]":
        return cls(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            pages=(total + page_size - 1) // page_size
        )


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = True
    message: str = "Operation completed successfully"
    data: Optional[Any] = None


class ErrorResponse(BaseModel):
    """Generic error response"""
    success: bool = False
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None


class FilterOperator(BaseModel):
    """Filter operator for queries"""
    field: str
    operator: str = Field(pattern="^(eq|ne|gt|gte|lt|lte|in|nin|like|ilike|between)$")
    value: Any


class SortOrder(BaseModel):
    """Sort order specification"""
    field: str
    direction: str = Field(default="asc", pattern="^(asc|desc)$")


class QueryParams(BaseModel):
    """Generic query parameters"""
    filters: List[FilterOperator] = Field(default_factory=list)
    sort: List[SortOrder] = Field(default_factory=list)
    search: Optional[str] = None


class DateRange(BaseModel):
    """Date range filter"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class IDResponse(BaseModel):
    """Response with ID"""
    id: str


class CountResponse(BaseModel):
    """Response with count"""
    count: int


class HealthCheck(BaseModel):
    """Health check response"""
    status: str
    app: str
    version: str
    environment: str


class DetailedHealthCheck(HealthCheck):
    """Detailed health check response"""
    components: dict


# Base model with common configuration
class FleetAIBaseModel(BaseModel):
    """Base model with common configuration"""
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


class TimestampMixin(BaseModel):
    """Mixin for models with timestamps"""
    created_at: datetime
    updated_at: Optional[datetime] = None


class AuditMixin(TimestampMixin):
    """Mixin for models with audit fields"""
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
