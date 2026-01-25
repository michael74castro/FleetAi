"""
FleetAI - Dataset API Routes
"""

from typing import List, Optional
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from app.api.deps import AsyncDB, UserViewReports
from app.models.report import Dataset
from app.schemas.report import (
    DatasetResponse, DatasetSchemaResponse, DatasetFieldInfo
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[DatasetResponse])
async def list_datasets(
    db: AsyncDB,
    user: UserViewReports,
    is_active: Optional[bool] = True
):
    """
    List available datasets for building reports and dashboards.
    """
    query = select(Dataset)

    if is_active is not None:
        query = query.where(Dataset.is_active == is_active)

    query = query.order_by(Dataset.display_name)

    result = await db.execute(query)
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
            schema_definition=json.loads(d.schema_definition) if d.schema_definition else None,
            default_filters=json.loads(d.default_filters) if d.default_filters else None,
            created_at=d.created_at,
            updated_at=d.updated_at
        )
        for d in datasets
    ]


@router.get("/{dataset_name}", response_model=DatasetResponse)
async def get_dataset(
    dataset_name: str,
    db: AsyncDB,
    user: UserViewReports
):
    """Get dataset by name"""
    result = await db.execute(
        select(Dataset).where(Dataset.name == dataset_name)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return DatasetResponse(
        dataset_id=dataset.dataset_id,
        name=dataset.name,
        display_name=dataset.display_name,
        description=dataset.description,
        source_type=dataset.source_type,
        source_object=dataset.source_object,
        rbac_column=dataset.rbac_column,
        is_active=dataset.is_active,
        schema_definition=json.loads(dataset.schema_definition) if dataset.schema_definition else None,
        default_filters=json.loads(dataset.default_filters) if dataset.default_filters else None,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at
    )


@router.get("/{dataset_name}/schema", response_model=DatasetSchemaResponse)
async def get_dataset_schema(
    dataset_name: str,
    db: AsyncDB,
    user: UserViewReports
):
    """
    Get schema (columns) for a dataset.
    Dynamically retrieves column information from the database.
    """
    result = await db.execute(
        select(Dataset).where(Dataset.name == dataset_name)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Parse source object (schema.table or schema.view)
    parts = dataset.source_object.split(".")
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = "dbo"
        table_name = dataset.source_object

    # Query column information
    column_query = text("""
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            ep.value AS description
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.extended_properties ep
            ON ep.major_id = OBJECT_ID(:full_name)
            AND ep.minor_id = c.ORDINAL_POSITION
            AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = :schema_name
        AND c.TABLE_NAME = :table_name
        ORDER BY c.ORDINAL_POSITION
    """)

    result = await db.execute(
        column_query,
        {
            "full_name": dataset.source_object,
            "schema_name": schema_name,
            "table_name": table_name
        }
    )
    columns = result.fetchall()

    # Get row count
    try:
        count_query = text(f"SELECT COUNT(*) FROM {dataset.source_object}")
        count_result = await db.execute(count_query)
        row_count = count_result.scalar()
    except Exception:
        row_count = None

    fields = []
    for col in columns:
        # Format data type
        data_type = col[1]
        if col[3]:  # Character length
            data_type = f"{data_type}({col[3]})"
        elif col[4] and col[5]:  # Numeric precision/scale
            data_type = f"{data_type}({col[4]},{col[5]})"

        # Create display name from column name
        display_name = col[0].replace("_", " ").title()

        fields.append(DatasetFieldInfo(
            name=col[0],
            display_name=display_name,
            data_type=data_type,
            nullable=col[2] == "YES",
            description=col[6] if col[6] else None
        ))

    return DatasetSchemaResponse(
        dataset_name=dataset_name,
        fields=fields,
        row_count=row_count
    )


@router.get("/{dataset_name}/preview")
async def preview_dataset(
    dataset_name: str,
    db: AsyncDB,
    user: UserViewReports,
    limit: int = Query(10, ge=1, le=100)
):
    """
    Preview sample data from a dataset.
    Applies row-level security based on user's customer access.
    """
    result = await db.execute(
        select(Dataset).where(Dataset.name == dataset_name)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Build query with RLS
    base_query = f"SELECT TOP {limit} * FROM {dataset.source_object}"

    # Apply RLS if dataset has rbac_column and user is not super user
    if dataset.rbac_column and user.get_role_level() < 50:
        customer_ids = [ca.customer_id for ca in user.customer_access]
        if customer_ids:
            id_list = ",".join([f"'{cid}'" for cid in customer_ids])
            base_query += f" WHERE {dataset.rbac_column} IN ({id_list})"
        else:
            # User has no customer access
            return {"columns": [], "data": [], "total_preview": 0}

    try:
        result = await db.execute(text(base_query))
        rows = result.fetchall()
        columns = list(result.keys())

        data = [dict(zip(columns, row)) for row in rows]

        return {
            "columns": columns,
            "data": data,
            "total_preview": len(data)
        }
    except Exception as e:
        logger.error(f"Error previewing dataset {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching preview data")


@router.get("/{dataset_name}/distinct/{column_name}")
async def get_distinct_values(
    dataset_name: str,
    column_name: str,
    db: AsyncDB,
    user: UserViewReports,
    limit: int = Query(100, ge=1, le=1000),
    search: Optional[str] = None
):
    """
    Get distinct values for a column (for filter dropdowns).
    """
    result = await db.execute(
        select(Dataset).where(Dataset.name == dataset_name)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Build query
    query = f"""
        SELECT DISTINCT TOP {limit} [{column_name}]
        FROM {dataset.source_object}
        WHERE [{column_name}] IS NOT NULL
    """

    if search:
        query += f" AND [{column_name}] LIKE '%{search}%'"

    # Apply RLS
    if dataset.rbac_column and user.get_role_level() < 50:
        customer_ids = [ca.customer_id for ca in user.customer_access]
        if customer_ids:
            id_list = ",".join([f"'{cid}'" for cid in customer_ids])
            query += f" AND {dataset.rbac_column} IN ({id_list})"

    query += f" ORDER BY [{column_name}]"

    try:
        result = await db.execute(text(query))
        values = [row[0] for row in result.fetchall()]

        return {"column": column_name, "values": values}
    except Exception as e:
        logger.error(f"Error getting distinct values: {e}")
        raise HTTPException(status_code=500, detail="Error fetching distinct values")


@router.get("/{dataset_name}/aggregate")
async def aggregate_dataset(
    dataset_name: str,
    db: AsyncDB,
    user: UserViewReports,
    metric: str = Query(..., description="Column to aggregate"),
    function: str = Query("SUM", description="Aggregation function"),
    group_by: Optional[str] = Query(None, description="Group by column"),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Perform aggregation on a dataset.
    Supports SUM, AVG, COUNT, MIN, MAX.
    """
    result = await db.execute(
        select(Dataset).where(Dataset.name == dataset_name)
    )
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Validate function
    valid_functions = ["SUM", "AVG", "COUNT", "MIN", "MAX"]
    if function.upper() not in valid_functions:
        raise HTTPException(status_code=400, detail=f"Invalid function. Use: {valid_functions}")

    # Build query
    if group_by:
        query = f"""
            SELECT TOP {limit}
                [{group_by}],
                {function.upper()}([{metric}]) as aggregated_value
            FROM {dataset.source_object}
        """
    else:
        query = f"""
            SELECT {function.upper()}([{metric}]) as aggregated_value
            FROM {dataset.source_object}
        """

    # Apply RLS
    where_clause = ""
    if dataset.rbac_column and user.get_role_level() < 50:
        customer_ids = [ca.customer_id for ca in user.customer_access]
        if customer_ids:
            id_list = ",".join([f"'{cid}'" for cid in customer_ids])
            where_clause = f" WHERE {dataset.rbac_column} IN ({id_list})"

    query += where_clause

    if group_by:
        query += f" GROUP BY [{group_by}] ORDER BY aggregated_value DESC"

    try:
        result = await db.execute(text(query))
        rows = result.fetchall()

        if group_by:
            data = [{"group": row[0], "value": row[1]} for row in rows]
        else:
            data = {"value": rows[0][0] if rows else None}

        return {
            "dataset": dataset_name,
            "metric": metric,
            "function": function.upper(),
            "group_by": group_by,
            "result": data
        }
    except Exception as e:
        logger.error(f"Error aggregating dataset: {e}")
        raise HTTPException(status_code=500, detail="Error performing aggregation")
