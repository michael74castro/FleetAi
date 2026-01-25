"""
FleetAI - Report API Routes
"""

from typing import List, Optional
import json
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_

from app.api.deps import (
    AsyncDB, Pagination,
    UserViewReports, UserCreateReports, UserExportReports
)
from app.models.report import Report, ReportAccess, ReportSchedule, ReportExecution
from app.schemas.report import (
    ReportCreate, ReportUpdate, ReportResponse, ReportSummary,
    ReportAccessCreate, ReportAccessResponse,
    ReportScheduleCreate, ReportScheduleUpdate, ReportScheduleResponse,
    ReportExecutionResponse, ReportExecuteRequest, ReportDataResponse
)
from app.schemas.common import PaginatedResponse, SuccessResponse
from app.services.report_engine import ReportEngine

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=PaginatedResponse[ReportSummary])
async def list_reports(
    db: AsyncDB,
    user: UserViewReports,
    pagination: Pagination,
    search: Optional[str] = Query(None, max_length=100),
    report_type: Optional[str] = None,
    is_template: Optional[bool] = None
):
    """List reports accessible to current user"""
    query = select(Report)

    # Filter by access
    if user.get_role_level() < 50:
        query = query.where(
            or_(
                Report.created_by == user.user_id,
                Report.is_public == True,
                Report.report_id.in_(
                    select(ReportAccess.report_id).where(
                        or_(
                            ReportAccess.user_id == user.user_id,
                            ReportAccess.role_id == user.role_id
                        )
                    )
                )
            )
        )

    # Apply filters
    if search:
        query = query.where(
            or_(
                Report.name.ilike(f"%{search}%"),
                Report.description.ilike(f"%{search}%")
            )
        )
    if report_type:
        query = query.where(Report.report_type == report_type)
    if is_template is not None:
        query = query.where(Report.is_template == is_template)

    # Count
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Pagination & sorting
    query = query.offset(pagination.offset).limit(pagination.page_size)
    query = query.order_by(Report.updated_at.desc())

    result = await db.execute(query)
    reports = result.scalars().all()

    summaries = [
        ReportSummary(
            report_id=str(r.report_id),
            name=r.name,
            description=r.description,
            report_type=r.report_type,
            dataset_name=r.dataset_name,
            is_template=r.is_template,
            is_public=r.is_public,
            created_by=r.created_by,
            created_at=r.created_at,
            updated_at=r.updated_at
        )
        for r in reports
    ]

    return PaginatedResponse.create(
        items=summaries,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.post("/", response_model=ReportResponse, status_code=201)
async def create_report(
    data: ReportCreate,
    db: AsyncDB,
    user: UserCreateReports
):
    """Create a new report"""
    report = Report(
        name=data.name,
        description=data.description,
        report_type=data.report_type,
        dataset_name=data.dataset_name,
        config=json.dumps(data.config),
        is_template=data.is_template,
        is_public=data.is_public,
        created_by=user.user_id
    )

    db.add(report)
    await db.commit()
    await db.refresh(report)

    logger.info(f"Report created: {report.report_id} by user {user.user_id}")

    return ReportResponse(
        report_id=str(report.report_id),
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        dataset_name=report.dataset_name,
        config=json.loads(report.config),
        is_template=report.is_template,
        is_public=report.is_public,
        created_by=report.created_by,
        created_at=report.created_at,
        updated_at=report.updated_at
    )


@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: str,
    db: AsyncDB,
    user: UserViewReports
):
    """Get report by ID"""
    result = await db.execute(
        select(Report).where(Report.report_id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return ReportResponse(
        report_id=str(report.report_id),
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        dataset_name=report.dataset_name,
        config=json.loads(report.config),
        is_template=report.is_template,
        is_public=report.is_public,
        created_by=report.created_by,
        created_at=report.created_at,
        updated_at=report.updated_at
    )


@router.put("/{report_id}", response_model=ReportResponse)
async def update_report(
    report_id: str,
    data: ReportUpdate,
    db: AsyncDB,
    user: UserCreateReports
):
    """Update a report"""
    result = await db.execute(
        select(Report).where(Report.report_id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if report.created_by != user.user_id and user.get_role_level() < 50:
        raise HTTPException(status_code=403, detail="Can only edit own reports")

    # Update fields
    if data.name is not None:
        report.name = data.name
    if data.description is not None:
        report.description = data.description
    if data.report_type is not None:
        report.report_type = data.report_type
    if data.dataset_name is not None:
        report.dataset_name = data.dataset_name
    if data.config is not None:
        report.config = json.dumps(data.config)
    if data.is_template is not None:
        report.is_template = data.is_template
    if data.is_public is not None:
        report.is_public = data.is_public

    await db.commit()
    await db.refresh(report)

    return ReportResponse(
        report_id=str(report.report_id),
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        dataset_name=report.dataset_name,
        config=json.loads(report.config),
        is_template=report.is_template,
        is_public=report.is_public,
        created_by=report.created_by,
        created_at=report.created_at,
        updated_at=report.updated_at
    )


@router.delete("/{report_id}", response_model=SuccessResponse)
async def delete_report(
    report_id: str,
    db: AsyncDB,
    user: UserCreateReports
):
    """Delete a report"""
    result = await db.execute(
        select(Report).where(Report.report_id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if report.created_by != user.user_id and user.get_role_level() < 100:
        raise HTTPException(status_code=403, detail="Can only delete own reports")

    await db.delete(report)
    await db.commit()

    return SuccessResponse(message="Report deleted")


@router.post("/{report_id}/execute", response_model=ReportDataResponse)
async def execute_report(
    report_id: str,
    request: ReportExecuteRequest,
    db: AsyncDB,
    user: UserViewReports,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000)
):
    """Execute a report and return data"""
    result = await db.execute(
        select(Report).where(Report.report_id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    # Get customer IDs for RLS
    customer_ids = [ca.customer_id for ca in user.customer_access]
    if user.get_role_level() >= 50:
        customer_ids = None

    # Execute report
    engine = ReportEngine(db)
    start_time = datetime.utcnow()

    data_result = await engine.execute_report(
        report_config=json.loads(report.config),
        dataset_name=report.dataset_name,
        parameters=request.parameters,
        customer_ids=customer_ids,
        page=page,
        page_size=page_size
    )

    # Log execution
    execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    execution = ReportExecution(
        report_id=report_id,
        executed_by=user.user_id,
        execution_type="manual",
        parameters=json.dumps(request.parameters) if request.parameters else None,
        row_count=data_result["total_rows"],
        execution_time_ms=execution_time,
        status="success",
        started_at=start_time,
        completed_at=datetime.utcnow()
    )
    db.add(execution)
    await db.commit()

    return ReportDataResponse(
        columns=data_result["columns"],
        data=data_result["data"],
        total_rows=data_result["total_rows"],
        page=page,
        page_size=page_size,
        aggregations=data_result.get("aggregations")
    )


@router.post("/{report_id}/export")
async def export_report(
    report_id: str,
    request: ReportExecuteRequest,
    background_tasks: BackgroundTasks,
    db: AsyncDB,
    user: UserExportReports
):
    """Export report to file (Excel, PDF, CSV)"""
    if not request.export_format:
        raise HTTPException(status_code=400, detail="export_format is required")

    result = await db.execute(
        select(Report).where(Report.report_id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    # Start export in background
    engine = ReportEngine(db)
    customer_ids = [ca.customer_id for ca in user.customer_access]
    if user.get_role_level() >= 50:
        customer_ids = None

    # Create execution record
    execution = ReportExecution(
        report_id=report_id,
        executed_by=user.user_id,
        execution_type="manual",
        parameters=json.dumps(request.parameters) if request.parameters else None,
        export_format=request.export_format,
        status="running",
        started_at=datetime.utcnow()
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)

    # Run export in background
    background_tasks.add_task(
        engine.export_report,
        execution_id=execution.execution_id,
        report_config=json.loads(report.config),
        dataset_name=report.dataset_name,
        parameters=request.parameters,
        export_format=request.export_format,
        customer_ids=customer_ids
    )

    return {
        "execution_id": execution.execution_id,
        "status": "processing",
        "message": "Export started. Check status with GET /reports/executions/{execution_id}"
    }


@router.get("/executions/{execution_id}", response_model=ReportExecutionResponse)
async def get_execution_status(
    execution_id: int,
    db: AsyncDB,
    user: UserViewReports
):
    """Get report execution status"""
    result = await db.execute(
        select(ReportExecution).where(ReportExecution.execution_id == execution_id)
    )
    execution = result.scalar_one_or_none()

    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    return ReportExecutionResponse(
        execution_id=execution.execution_id,
        report_id=str(execution.report_id),
        schedule_id=execution.schedule_id,
        executed_by=execution.executed_by,
        execution_type=execution.execution_type,
        parameters=json.loads(execution.parameters) if execution.parameters else None,
        row_count=execution.row_count,
        execution_time_ms=execution.execution_time_ms,
        export_format=execution.export_format,
        file_path=execution.file_path,
        status=execution.status,
        error_message=execution.error_message,
        started_at=execution.started_at,
        completed_at=execution.completed_at
    )


@router.get("/executions/{execution_id}/download")
async def download_export(
    execution_id: int,
    db: AsyncDB,
    user: UserExportReports
):
    """Download exported report file"""
    result = await db.execute(
        select(ReportExecution).where(ReportExecution.execution_id == execution_id)
    )
    execution = result.scalar_one_or_none()

    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    if execution.status != "success":
        raise HTTPException(status_code=400, detail=f"Export not complete. Status: {execution.status}")

    if not execution.file_path:
        raise HTTPException(status_code=404, detail="Export file not found")

    # Determine media type
    media_types = {
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pdf": "application/pdf",
        "csv": "text/csv"
    }
    media_type = media_types.get(execution.export_format, "application/octet-stream")

    return FileResponse(
        path=execution.file_path,
        media_type=media_type,
        filename=f"report_{execution.execution_id}.{execution.export_format}"
    )


# Schedule endpoints
@router.get("/{report_id}/schedules", response_model=List[ReportScheduleResponse])
async def list_schedules(
    report_id: str,
    db: AsyncDB,
    user: UserViewReports
):
    """List schedules for a report"""
    result = await db.execute(
        select(ReportSchedule).where(ReportSchedule.report_id == report_id)
    )
    schedules = result.scalars().all()

    return [
        ReportScheduleResponse(
            schedule_id=s.schedule_id,
            report_id=str(s.report_id),
            schedule_name=s.schedule_name,
            cron_expression=s.cron_expression,
            export_format=s.export_format,
            recipients=json.loads(s.recipients) if s.recipients else [],
            parameters=json.loads(s.parameters) if s.parameters else None,
            is_active=s.is_active,
            last_run=s.last_run,
            next_run=s.next_run,
            created_by=s.created_by,
            created_at=s.created_at
        )
        for s in schedules
    ]


@router.post("/{report_id}/schedules", response_model=ReportScheduleResponse, status_code=201)
async def create_schedule(
    report_id: str,
    data: ReportScheduleCreate,
    db: AsyncDB,
    user: UserCreateReports
):
    """Create a report schedule"""
    schedule = ReportSchedule(
        report_id=report_id,
        schedule_name=data.schedule_name,
        cron_expression=data.cron_expression,
        export_format=data.export_format,
        recipients=json.dumps([str(r) for r in data.recipients]),
        parameters=json.dumps(data.parameters) if data.parameters else None,
        is_active=data.is_active,
        created_by=user.user_id
    )

    db.add(schedule)
    await db.commit()
    await db.refresh(schedule)

    return ReportScheduleResponse(
        schedule_id=schedule.schedule_id,
        report_id=str(schedule.report_id),
        schedule_name=schedule.schedule_name,
        cron_expression=schedule.cron_expression,
        export_format=schedule.export_format,
        recipients=data.recipients,
        parameters=data.parameters,
        is_active=schedule.is_active,
        last_run=schedule.last_run,
        next_run=schedule.next_run,
        created_by=schedule.created_by,
        created_at=schedule.created_at
    )
