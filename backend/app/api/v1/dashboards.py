"""
FleetAI - Dashboard API Routes
"""

from typing import List, Optional
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_

from app.api.deps import (
    AsyncDB, Pagination,
    UserViewDashboards, UserCreateDashboards, UserEditDashboards
)
from app.models.dashboard import Dashboard, DashboardWidget, DashboardAccess
from app.schemas.dashboard import (
    DashboardCreate, DashboardUpdate, DashboardResponse, DashboardWithWidgets,
    DashboardSummary, WidgetCreate, WidgetUpdate, WidgetResponse,
    DashboardAccessCreate, DashboardAccessResponse, DashboardCloneRequest,
    WidgetDataRequest, WidgetDataResponse
)
from app.schemas.common import PaginatedResponse, SuccessResponse, IDResponse
from app.services.dashboard_engine import DashboardEngine

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=PaginatedResponse[DashboardSummary])
async def list_dashboards(
    db: AsyncDB,
    user: UserViewDashboards,
    pagination: Pagination,
    search: Optional[str] = Query(None, max_length=100),
    is_template: Optional[bool] = None
):
    """
    List dashboards accessible to current user.
    Supports search, filtering, and pagination.
    """
    # Build base query
    query = select(Dashboard)

    # Filter by access
    if user.get_role_level() < 50:  # Not super user/admin
        # User can see: own dashboards, public dashboards, or explicitly shared
        query = query.where(
            or_(
                Dashboard.created_by == user.user_id,
                Dashboard.is_public == True,
                Dashboard.dashboard_id.in_(
                    select(DashboardAccess.dashboard_id).where(
                        or_(
                            DashboardAccess.user_id == user.user_id,
                            DashboardAccess.role_id == user.role_id
                        )
                    )
                )
            )
        )

    # Apply filters
    if search:
        query = query.where(
            or_(
                Dashboard.name.ilike(f"%{search}%"),
                Dashboard.description.ilike(f"%{search}%")
            )
        )

    if is_template is not None:
        query = query.where(Dashboard.is_template == is_template)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Apply pagination
    query = query.offset(pagination.offset).limit(pagination.page_size)

    # Apply sorting
    if pagination.sort_by:
        sort_col = getattr(Dashboard, pagination.sort_by, Dashboard.updated_at)
        if pagination.sort_order == "desc":
            query = query.order_by(sort_col.desc())
        else:
            query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(Dashboard.updated_at.desc())

    # Execute
    result = await db.execute(query)
    dashboards = result.scalars().all()

    # Build summaries with widget counts
    summaries = []
    for d in dashboards:
        widget_count_result = await db.execute(
            select(func.count()).where(DashboardWidget.dashboard_id == d.dashboard_id)
        )
        widget_count = widget_count_result.scalar()

        summaries.append(DashboardSummary(
            dashboard_id=str(d.dashboard_id),
            name=d.name,
            description=d.description,
            is_template=d.is_template,
            is_public=d.is_public,
            widget_count=widget_count,
            created_by=d.created_by,
            created_at=d.created_at,
            updated_at=d.updated_at
        ))

    return PaginatedResponse.create(
        items=summaries,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.post("/", response_model=DashboardResponse, status_code=status.HTTP_201_CREATED)
async def create_dashboard(
    data: DashboardCreate,
    db: AsyncDB,
    user: UserCreateDashboards
):
    """Create a new dashboard"""
    dashboard = Dashboard(
        name=data.name,
        description=data.description,
        layout_type=data.layout_type,
        config=json.dumps(data.config),
        is_template=data.is_template,
        is_public=data.is_public,
        created_by=user.user_id
    )

    db.add(dashboard)
    await db.commit()
    await db.refresh(dashboard)

    logger.info(f"Dashboard created: {dashboard.dashboard_id} by user {user.user_id}")

    return DashboardResponse(
        dashboard_id=str(dashboard.dashboard_id),
        name=dashboard.name,
        description=dashboard.description,
        layout_type=dashboard.layout_type,
        config=json.loads(dashboard.config),
        is_template=dashboard.is_template,
        is_public=dashboard.is_public,
        created_by=dashboard.created_by,
        created_at=dashboard.created_at,
        updated_at=dashboard.updated_at
    )


@router.get("/{dashboard_id}", response_model=DashboardWithWidgets)
async def get_dashboard(
    dashboard_id: str,
    db: AsyncDB,
    user: UserViewDashboards
):
    """Get dashboard by ID with all widgets"""
    result = await db.execute(
        select(Dashboard).where(Dashboard.dashboard_id == dashboard_id)
    )
    dashboard = result.scalar_one_or_none()

    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dashboard not found"
        )

    # Check access
    if not _can_access_dashboard(dashboard, user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )

    # Get widgets
    result = await db.execute(
        select(DashboardWidget).where(
            DashboardWidget.dashboard_id == dashboard_id
        ).order_by(DashboardWidget.position_y, DashboardWidget.position_x)
    )
    widgets = result.scalars().all()

    widget_responses = [
        WidgetResponse(
            widget_id=str(w.widget_id),
            dashboard_id=str(w.dashboard_id),
            widget_type=w.widget_type,
            position_x=w.position_x,
            position_y=w.position_y,
            width=w.width,
            height=w.height,
            config=json.loads(w.config),
            created_at=w.created_at,
            updated_at=w.updated_at
        )
        for w in widgets
    ]

    return DashboardWithWidgets(
        dashboard_id=str(dashboard.dashboard_id),
        name=dashboard.name,
        description=dashboard.description,
        layout_type=dashboard.layout_type,
        config=json.loads(dashboard.config),
        is_template=dashboard.is_template,
        is_public=dashboard.is_public,
        created_by=dashboard.created_by,
        created_at=dashboard.created_at,
        updated_at=dashboard.updated_at,
        widgets=widget_responses
    )


@router.put("/{dashboard_id}", response_model=DashboardResponse)
async def update_dashboard(
    dashboard_id: str,
    data: DashboardUpdate,
    db: AsyncDB,
    user: UserEditDashboards
):
    """Update a dashboard"""
    result = await db.execute(
        select(Dashboard).where(Dashboard.dashboard_id == dashboard_id)
    )
    dashboard = result.scalar_one_or_none()

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    # Check ownership or admin
    if dashboard.created_by != user.user_id and user.get_role_level() < 50:
        raise HTTPException(status_code=403, detail="Can only edit own dashboards")

    # Update fields
    if data.name is not None:
        dashboard.name = data.name
    if data.description is not None:
        dashboard.description = data.description
    if data.layout_type is not None:
        dashboard.layout_type = data.layout_type
    if data.config is not None:
        dashboard.config = json.dumps(data.config)
    if data.is_template is not None:
        dashboard.is_template = data.is_template
    if data.is_public is not None:
        dashboard.is_public = data.is_public

    await db.commit()
    await db.refresh(dashboard)

    return DashboardResponse(
        dashboard_id=str(dashboard.dashboard_id),
        name=dashboard.name,
        description=dashboard.description,
        layout_type=dashboard.layout_type,
        config=json.loads(dashboard.config),
        is_template=dashboard.is_template,
        is_public=dashboard.is_public,
        created_by=dashboard.created_by,
        created_at=dashboard.created_at,
        updated_at=dashboard.updated_at
    )


@router.delete("/{dashboard_id}", response_model=SuccessResponse)
async def delete_dashboard(
    dashboard_id: str,
    db: AsyncDB,
    user: UserEditDashboards
):
    """Delete a dashboard"""
    result = await db.execute(
        select(Dashboard).where(Dashboard.dashboard_id == dashboard_id)
    )
    dashboard = result.scalar_one_or_none()

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    # Check ownership or admin
    if dashboard.created_by != user.user_id and user.get_role_level() < 100:
        raise HTTPException(status_code=403, detail="Can only delete own dashboards")

    await db.delete(dashboard)
    await db.commit()

    logger.info(f"Dashboard deleted: {dashboard_id} by user {user.user_id}")

    return SuccessResponse(message="Dashboard deleted successfully")


@router.post("/{dashboard_id}/clone", response_model=DashboardResponse)
async def clone_dashboard(
    dashboard_id: str,
    data: DashboardCloneRequest,
    db: AsyncDB,
    user: UserCreateDashboards
):
    """Clone a dashboard (including widgets)"""
    # Get source dashboard
    result = await db.execute(
        select(Dashboard).where(Dashboard.dashboard_id == dashboard_id)
    )
    source = result.scalar_one_or_none()

    if not source:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    # Create clone
    clone = Dashboard(
        name=data.name,
        description=data.description or source.description,
        layout_type=source.layout_type,
        config=source.config,
        is_template=False,
        is_public=False,
        created_by=user.user_id
    )

    db.add(clone)
    await db.flush()

    # Clone widgets
    result = await db.execute(
        select(DashboardWidget).where(DashboardWidget.dashboard_id == dashboard_id)
    )
    source_widgets = result.scalars().all()

    for sw in source_widgets:
        widget = DashboardWidget(
            dashboard_id=clone.dashboard_id,
            widget_type=sw.widget_type,
            position_x=sw.position_x,
            position_y=sw.position_y,
            width=sw.width,
            height=sw.height,
            config=sw.config
        )
        db.add(widget)

    await db.commit()
    await db.refresh(clone)

    return DashboardResponse(
        dashboard_id=str(clone.dashboard_id),
        name=clone.name,
        description=clone.description,
        layout_type=clone.layout_type,
        config=json.loads(clone.config),
        is_template=clone.is_template,
        is_public=clone.is_public,
        created_by=clone.created_by,
        created_at=clone.created_at,
        updated_at=clone.updated_at
    )


# Widget endpoints
@router.post("/{dashboard_id}/widgets", response_model=WidgetResponse, status_code=201)
async def add_widget(
    dashboard_id: str,
    data: WidgetCreate,
    db: AsyncDB,
    user: UserEditDashboards
):
    """Add a widget to a dashboard"""
    # Verify dashboard exists and user can edit
    result = await db.execute(
        select(Dashboard).where(Dashboard.dashboard_id == dashboard_id)
    )
    dashboard = result.scalar_one_or_none()

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    if dashboard.created_by != user.user_id and user.get_role_level() < 50:
        raise HTTPException(status_code=403, detail="Cannot edit this dashboard")

    widget = DashboardWidget(
        dashboard_id=dashboard_id,
        widget_type=data.widget_type,
        position_x=data.position_x,
        position_y=data.position_y,
        width=data.width,
        height=data.height,
        config=json.dumps(data.config)
    )

    db.add(widget)
    await db.commit()
    await db.refresh(widget)

    return WidgetResponse(
        widget_id=str(widget.widget_id),
        dashboard_id=str(widget.dashboard_id),
        widget_type=widget.widget_type,
        position_x=widget.position_x,
        position_y=widget.position_y,
        width=widget.width,
        height=widget.height,
        config=json.loads(widget.config),
        created_at=widget.created_at,
        updated_at=widget.updated_at
    )


@router.put("/{dashboard_id}/widgets/{widget_id}", response_model=WidgetResponse)
async def update_widget(
    dashboard_id: str,
    widget_id: str,
    data: WidgetUpdate,
    db: AsyncDB,
    user: UserEditDashboards
):
    """Update a widget"""
    result = await db.execute(
        select(DashboardWidget).where(
            and_(
                DashboardWidget.widget_id == widget_id,
                DashboardWidget.dashboard_id == dashboard_id
            )
        )
    )
    widget = result.scalar_one_or_none()

    if not widget:
        raise HTTPException(status_code=404, detail="Widget not found")

    # Update fields
    if data.widget_type is not None:
        widget.widget_type = data.widget_type
    if data.position_x is not None:
        widget.position_x = data.position_x
    if data.position_y is not None:
        widget.position_y = data.position_y
    if data.width is not None:
        widget.width = data.width
    if data.height is not None:
        widget.height = data.height
    if data.config is not None:
        widget.config = json.dumps(data.config)

    await db.commit()
    await db.refresh(widget)

    return WidgetResponse(
        widget_id=str(widget.widget_id),
        dashboard_id=str(widget.dashboard_id),
        widget_type=widget.widget_type,
        position_x=widget.position_x,
        position_y=widget.position_y,
        width=widget.width,
        height=widget.height,
        config=json.loads(widget.config),
        created_at=widget.created_at,
        updated_at=widget.updated_at
    )


@router.delete("/{dashboard_id}/widgets/{widget_id}", response_model=SuccessResponse)
async def delete_widget(
    dashboard_id: str,
    widget_id: str,
    db: AsyncDB,
    user: UserEditDashboards
):
    """Delete a widget"""
    result = await db.execute(
        select(DashboardWidget).where(
            and_(
                DashboardWidget.widget_id == widget_id,
                DashboardWidget.dashboard_id == dashboard_id
            )
        )
    )
    widget = result.scalar_one_or_none()

    if not widget:
        raise HTTPException(status_code=404, detail="Widget not found")

    await db.delete(widget)
    await db.commit()

    return SuccessResponse(message="Widget deleted")


@router.post("/{dashboard_id}/widgets/{widget_id}/data", response_model=WidgetDataResponse)
async def get_widget_data(
    dashboard_id: str,
    widget_id: str,
    request: WidgetDataRequest,
    db: AsyncDB,
    user: UserViewDashboards
):
    """Get data for a specific widget"""
    result = await db.execute(
        select(DashboardWidget).where(DashboardWidget.widget_id == widget_id)
    )
    widget = result.scalar_one_or_none()

    if not widget:
        raise HTTPException(status_code=404, detail="Widget not found")

    # Get user's customer IDs for RLS
    customer_ids = [ca.customer_id for ca in user.customer_access]

    # Execute widget query
    engine = DashboardEngine(db)
    data = await engine.execute_widget_query(
        widget_config=json.loads(widget.config),
        widget_type=widget.widget_type,
        filters=request.filters,
        date_range=request.date_range,
        customer_ids=customer_ids if user.get_role_level() < 50 else None
    )

    return WidgetDataResponse(
        widget_id=widget_id,
        data=data,
        metadata={"widget_type": widget.widget_type}
    )


def _can_access_dashboard(dashboard: Dashboard, user, db) -> bool:
    """Check if user can access dashboard"""
    # Owner always has access
    if dashboard.created_by == user.user_id:
        return True
    # Super users/admins have access to all
    if user.get_role_level() >= 50:
        return True
    # Public dashboards
    if dashboard.is_public:
        return True
    # Would need to check dashboard_access table for explicit grants
    return False
