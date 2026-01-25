"""
FleetAI - Report Pydantic Schemas
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, EmailStr

from .common import FleetAIBaseModel


class ColumnConfig(BaseModel):
    """Report column configuration"""
    field: str
    label: Optional[str] = None
    format: Optional[str] = None  # number, currency, date, percentage
    sortable: bool = True
    width: Optional[int] = None
    align: Optional[str] = None


class FilterConfig(BaseModel):
    """Report filter configuration"""
    field: str
    operator: str = "="  # =, !=, >, <, >=, <=, IN, LIKE, BETWEEN
    value: Optional[Any] = None
    param: Optional[str] = None  # Parameter name for runtime injection


class AggregationConfig(BaseModel):
    """Report aggregation configuration"""
    field: str
    function: str  # SUM, AVG, COUNT, MIN, MAX
    label: Optional[str] = None


class SortConfig(BaseModel):
    """Report sorting configuration"""
    field: str
    direction: str = "asc"


class ScheduleConfig(BaseModel):
    """Report schedule configuration"""
    enabled: bool = False
    cron: str = "0 8 1 * *"  # Default: 8 AM on 1st of month
    recipients: List[EmailStr] = []
    format: str = "excel"  # excel, pdf, csv


class ReportConfigSchema(BaseModel):
    """Full report configuration schema"""
    columns: List[ColumnConfig]
    filters: List[FilterConfig] = []
    grouping: List[str] = []
    aggregations: List[AggregationConfig] = []
    sorting: List[SortConfig] = []
    schedule: Optional[ScheduleConfig] = None
    limit: Optional[int] = None
    parameters: Optional[Dict[str, Any]] = None


class ReportBase(BaseModel):
    """Base report schema"""
    name: str = Field(max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    report_type: str = Field(max_length=50)  # tabular, summary, detail
    dataset_name: Optional[str] = Field(None, max_length=200)
    is_template: bool = False
    is_public: bool = False


class ReportCreate(ReportBase):
    """Schema for creating a report"""
    config: Dict[str, Any]


class ReportUpdate(BaseModel):
    """Schema for updating a report"""
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    report_type: Optional[str] = None
    dataset_name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    is_template: Optional[bool] = None
    is_public: Optional[bool] = None


class ReportResponse(ReportBase, FleetAIBaseModel):
    """Schema for report response"""
    report_id: str
    config: Dict[str, Any]
    created_by: int
    created_at: datetime
    updated_at: datetime


class ReportSummary(FleetAIBaseModel):
    """Report summary for list views"""
    report_id: str
    name: str
    description: Optional[str]
    report_type: str
    dataset_name: Optional[str]
    is_template: bool
    is_public: bool
    created_by: int
    created_at: datetime
    updated_at: datetime


class ReportAccessBase(BaseModel):
    """Base report access schema"""
    user_id: Optional[int] = None
    role_id: Optional[int] = None
    customer_id: Optional[str] = None
    access_level: str = Field(default="view", pattern="^(view|edit|admin)$")


class ReportAccessCreate(ReportAccessBase):
    """Schema for granting report access"""
    pass


class ReportAccessResponse(ReportAccessBase, FleetAIBaseModel):
    """Schema for report access response"""
    access_id: int
    report_id: str
    granted_at: datetime


class ReportScheduleBase(BaseModel):
    """Base schedule schema"""
    schedule_name: Optional[str] = Field(None, max_length=200)
    cron_expression: str = Field(max_length=100)
    export_format: str = Field(max_length=20)
    recipients: List[EmailStr]
    parameters: Optional[Dict[str, Any]] = None
    is_active: bool = True


class ReportScheduleCreate(ReportScheduleBase):
    """Schema for creating a schedule"""
    report_id: str


class ReportScheduleUpdate(BaseModel):
    """Schema for updating a schedule"""
    schedule_name: Optional[str] = None
    cron_expression: Optional[str] = None
    export_format: Optional[str] = None
    recipients: Optional[List[EmailStr]] = None
    parameters: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class ReportScheduleResponse(ReportScheduleBase, FleetAIBaseModel):
    """Schema for schedule response"""
    schedule_id: int
    report_id: str
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    created_by: int
    created_at: datetime


class ReportExecutionResponse(FleetAIBaseModel):
    """Schema for execution response"""
    execution_id: int
    report_id: str
    schedule_id: Optional[int]
    executed_by: Optional[int]
    execution_type: str
    parameters: Optional[Dict[str, Any]]
    row_count: Optional[int]
    execution_time_ms: Optional[int]
    export_format: Optional[str]
    file_path: Optional[str]
    status: str
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]


class ReportExecuteRequest(BaseModel):
    """Request to execute a report"""
    parameters: Optional[Dict[str, Any]] = None
    export_format: Optional[str] = None  # None = view only, excel/pdf/csv = export


class ReportDataResponse(BaseModel):
    """Response with report data"""
    columns: List[Dict[str, Any]]
    data: List[Dict[str, Any]]
    total_rows: int
    page: int
    page_size: int
    aggregations: Optional[Dict[str, Any]] = None


class DatasetBase(BaseModel):
    """Base dataset schema"""
    name: str = Field(max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    source_type: str = Field(max_length=50)
    source_object: str = Field(max_length=200)
    rbac_column: Optional[str] = Field(None, max_length=100)
    is_active: bool = True


class DatasetCreate(DatasetBase):
    """Schema for creating a dataset"""
    schema_definition: Optional[Dict[str, Any]] = None
    default_filters: Optional[Dict[str, Any]] = None


class DatasetResponse(DatasetBase, FleetAIBaseModel):
    """Schema for dataset response"""
    dataset_id: int
    schema_definition: Optional[Dict[str, Any]]
    default_filters: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class DatasetFieldInfo(BaseModel):
    """Dataset field information"""
    name: str
    display_name: Optional[str]
    data_type: str
    nullable: bool
    description: Optional[str] = None


class DatasetSchemaResponse(BaseModel):
    """Response with dataset schema"""
    dataset_name: str
    fields: List[DatasetFieldInfo]
    row_count: Optional[int] = None
