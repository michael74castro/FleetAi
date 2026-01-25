"""
FleetAI - Dashboard Engine Service
Executes widget queries and transforms data for visualization
"""

from typing import Any, Dict, List, Optional
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


class DashboardEngine:
    """Engine for executing dashboard widget queries"""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def execute_widget_query(
        self,
        widget_config: Dict[str, Any],
        widget_type: str,
        filters: Optional[Dict[str, Any]] = None,
        date_range: Optional[Dict[str, str]] = None,
        customer_ids: Optional[List[str]] = None
    ) -> Any:
        """
        Execute query for a widget and return formatted data.

        Args:
            widget_config: Widget configuration from database
            widget_type: Type of widget (kpi_card, line_chart, etc.)
            filters: Runtime filters to apply
            date_range: Date range filter
            customer_ids: Customer IDs for RLS (None = no restriction)

        Returns:
            Formatted data appropriate for the widget type
        """
        dataset = widget_config.get("dataset")
        if not dataset:
            return None

        # Build query based on widget type
        if widget_type == "kpi_card":
            return await self._execute_kpi_query(widget_config, filters, customer_ids)
        elif widget_type in ["line_chart", "bar_chart", "area_chart"]:
            return await self._execute_chart_query(widget_config, filters, date_range, customer_ids)
        elif widget_type in ["pie_chart", "donut_chart"]:
            return await self._execute_distribution_query(widget_config, filters, customer_ids)
        elif widget_type == "table":
            return await self._execute_table_query(widget_config, filters, customer_ids)
        elif widget_type == "gauge":
            return await self._execute_gauge_query(widget_config, filters, customer_ids)
        else:
            logger.warning(f"Unknown widget type: {widget_type}")
            return None

    async def _execute_kpi_query(
        self,
        config: Dict[str, Any],
        filters: Optional[Dict[str, Any]],
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Execute KPI card query"""
        dataset = config.get("dataset")
        metric = config.get("metric", "COUNT(*)")

        query = f"SELECT {metric} AS value FROM {dataset}"
        query = self._apply_where_clause(query, config.get("filters", []), filters, customer_ids)

        result = await self.db.execute(text(query))
        row = result.fetchone()
        value = row[0] if row else 0

        # Get comparison value if configured
        comparison = config.get("comparison")
        comparison_value = None
        if comparison == "previous_month":
            # Query for previous month
            comp_query = f"""
                SELECT {metric} AS value FROM {dataset}
                WHERE date_key >= CONVERT(INT, FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM01'))
                AND date_key < CONVERT(INT, FORMAT(GETDATE(), 'yyyyMM01'))
            """
            comp_result = await self.db.execute(text(comp_query))
            comp_row = comp_result.fetchone()
            comparison_value = comp_row[0] if comp_row else None

        return {
            "value": value,
            "comparison_value": comparison_value,
            "format": config.get("format", "number"),
            "label": config.get("label", "")
        }

    async def _execute_chart_query(
        self,
        config: Dict[str, Any],
        filters: Optional[Dict[str, Any]],
        date_range: Optional[Dict[str, str]],
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Execute time series chart query"""
        dataset = config.get("dataset")
        metrics = config.get("metrics", ["COUNT(*)"])
        dimensions = config.get("dimensions", ["date_key"])

        # Build SELECT
        select_parts = []
        for dim in dimensions:
            select_parts.append(dim)
        for metric in metrics:
            select_parts.append(f"{metric} AS metric_{len(select_parts)}")

        query = f"SELECT {', '.join(select_parts)} FROM {dataset}"
        query = self._apply_where_clause(query, config.get("filters", []), filters, customer_ids)

        # Group by dimensions
        if dimensions:
            query += f" GROUP BY {', '.join(dimensions)}"
            query += f" ORDER BY {dimensions[0]}"

        # Apply limit
        limit = config.get("limit", 100)
        query = f"SELECT TOP {limit} * FROM ({query}) sub"

        result = await self.db.execute(text(query))
        rows = result.fetchall()
        columns = list(result.keys())

        # Format for charts
        data = []
        for row in rows:
            item = {}
            for i, col in enumerate(columns):
                item[col] = row[i]
            data.append(item)

        return {
            "data": data,
            "dimensions": dimensions,
            "metrics": metrics
        }

    async def _execute_distribution_query(
        self,
        config: Dict[str, Any],
        filters: Optional[Dict[str, Any]],
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Execute pie/donut chart query"""
        dataset = config.get("dataset")
        metric = config.get("metric", "COUNT(*)")
        dimension = config.get("dimension")

        if not dimension:
            return {"data": [], "total": 0}

        query = f"""
            SELECT {dimension} AS category, {metric} AS value
            FROM {dataset}
        """
        query = self._apply_where_clause(query, config.get("filters", []), filters, customer_ids)
        query += f" GROUP BY {dimension} ORDER BY value DESC"

        limit = config.get("limit", 10)
        query = f"SELECT TOP {limit} * FROM ({query}) sub"

        result = await self.db.execute(text(query))
        rows = result.fetchall()

        data = [{"category": row[0], "value": row[1]} for row in rows]
        total = sum(item["value"] or 0 for item in data)

        return {
            "data": data,
            "total": total
        }

    async def _execute_table_query(
        self,
        config: Dict[str, Any],
        filters: Optional[Dict[str, Any]],
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Execute table widget query"""
        dataset = config.get("dataset")
        columns = config.get("columns", ["*"])

        # Build column list
        if isinstance(columns, list) and columns != ["*"]:
            col_list = ", ".join([c.get("field", c) if isinstance(c, dict) else c for c in columns])
        else:
            col_list = "*"

        query = f"SELECT {col_list} FROM {dataset}"
        query = self._apply_where_clause(query, config.get("filters", []), filters, customer_ids)

        # Sorting
        sorting = config.get("sorting", [])
        if sorting:
            order_parts = [f"{s['field']} {s.get('direction', 'asc')}" for s in sorting]
            query += f" ORDER BY {', '.join(order_parts)}"

        # Pagination
        page_size = config.get("pageSize", 25)
        query = f"SELECT TOP {page_size} * FROM ({query}) sub"

        result = await self.db.execute(text(query))
        rows = result.fetchall()
        result_columns = list(result.keys())

        data = [dict(zip(result_columns, row)) for row in rows]

        return {
            "columns": result_columns,
            "data": data,
            "pageSize": page_size
        }

    async def _execute_gauge_query(
        self,
        config: Dict[str, Any],
        filters: Optional[Dict[str, Any]],
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Execute gauge widget query"""
        dataset = config.get("dataset")
        metric = config.get("metric", "COUNT(*)")
        target = config.get("target", 100)

        query = f"SELECT {metric} AS value FROM {dataset}"
        query = self._apply_where_clause(query, config.get("filters", []), filters, customer_ids)

        result = await self.db.execute(text(query))
        row = result.fetchone()
        value = row[0] if row else 0

        percentage = (value / target * 100) if target > 0 else 0

        return {
            "value": value,
            "target": target,
            "percentage": min(percentage, 100),
            "thresholds": config.get("thresholds", [])
        }

    def _apply_where_clause(
        self,
        query: str,
        config_filters: List[Dict[str, Any]],
        runtime_filters: Optional[Dict[str, Any]],
        customer_ids: Optional[List[str]]
    ) -> str:
        """Apply WHERE clause to query"""
        conditions = []

        # Config filters
        for f in config_filters:
            field = f.get("field")
            op = f.get("op", "=")
            value = f.get("value")

            if field and value is not None:
                if op.upper() == "IN" and isinstance(value, list):
                    values_str = ", ".join([f"'{v}'" for v in value])
                    conditions.append(f"{field} IN ({values_str})")
                elif op == "=":
                    conditions.append(f"{field} = '{value}'")
                elif op == "LIKE":
                    conditions.append(f"{field} LIKE '%{value}%'")
                else:
                    conditions.append(f"{field} {op} '{value}'")

        # Runtime filters
        if runtime_filters:
            for field, value in runtime_filters.items():
                if value is not None:
                    if isinstance(value, list):
                        values_str = ", ".join([f"'{v}'" for v in value])
                        conditions.append(f"{field} IN ({values_str})")
                    else:
                        conditions.append(f"{field} = '{value}'")

        # Customer IDs (RLS)
        if customer_ids is not None:
            if customer_ids:
                ids_str = ", ".join([f"'{cid}'" for cid in customer_ids])
                conditions.append(f"customer_key IN (SELECT customer_key FROM reporting.dim_customer WHERE customer_id IN ({ids_str}))")
            else:
                # No access
                conditions.append("1 = 0")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        return query
