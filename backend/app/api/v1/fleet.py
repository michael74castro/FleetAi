"""
FleetAI - Fleet API Endpoints
Provides vehicle listing and KPI data from the semantic layer tables.
"""

import logging
import math
from typing import Optional

from fastapi import APIRouter, Query
from app.core.database import execute_raw_query

logger = logging.getLogger(__name__)

router = APIRouter()

# Whitelist of sortable columns to prevent SQL injection
SORTABLE_COLUMNS = {
    'registration_number': 'v.registration_number',
    'driver_name': 'd.driver_name',
    'make_and_model': 'v.make_and_model',
    'customer_name': 'v.customer_name',
    'vehicle_status': 'v.vehicle_status',
    'current_odometer_km': 'v.current_odometer_km',
    'vehicle_year': 'v.vehicle_year',
    'color_name': 'v.color_name',
    'fuel_type': 'v.fuel_type',
    'monthly_lease_amount': 'v.monthly_lease_amount',
    'vin_number': 'v.vin_number',
    'driver_email': 'd.email_address',
    'driver_phone': 'd.phone_mobile',
    'vehicle_id': 'v.vehicle_id',
}


@router.get("/operations/kpis")
async def get_operations_kpis():
    """Get fleet operations KPI summary from dim_vehicle."""
    try:
        # Count active vehicles (status_code = 1)
        active_result = await execute_raw_query(
            "SELECT COUNT(*) as cnt FROM dim_vehicle WHERE vehicle_status_code = 1"
        )
        active_vehicles = active_result[0]['cnt'] if active_result else 0

        # Count vehicles started in last 12 months (by lease_start_date)
        started_result = await execute_raw_query(
            "SELECT COUNT(*) as cnt FROM dim_vehicle "
            "WHERE lease_start_date >= date('now', '-12 months') "
            "AND lease_start_date IS NOT NULL"
        )
        started_last_12_months = started_result[0]['cnt'] if started_result else 0

        # Count vehicles terminated in last 12 months (by lease_end_date + status)
        terminated_result = await execute_raw_query(
            "SELECT COUNT(*) as cnt FROM dim_vehicle "
            "WHERE vehicle_status_code >= 2 "
            "AND lease_end_date >= date('now', '-12 months') "
            "AND lease_end_date IS NOT NULL"
        )
        terminated_last_12_months = terminated_result[0]['cnt'] if terminated_result else 0

        # Active within 12 months = active + started + terminated within 12m
        active_within_12_months = active_vehicles + started_last_12_months + terminated_last_12_months

        return {
            "active_within_12_months": active_within_12_months,
            "active_vehicles": active_vehicles,
            "started_last_12_months": started_last_12_months,
            "terminated_last_12_months": terminated_last_12_months,
            "last_data_update": "2026-01-23T04:33:32Z",
            "last_monthly_closure": "December 2025"
        }
    except Exception as e:
        logger.error(f"Error fetching operations KPIs: {e}")
        raise


@router.get("/vehicles")
async def get_vehicles_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    driver: Optional[str] = Query(None),
    license_plate: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    make: Optional[str] = Query(None),
    customer_id: Optional[int] = Query(None),
    sort_by: str = Query("registration_number"),
    sort_order: str = Query("asc"),
):
    """Get paginated vehicle list with driver info from dim_vehicle + dim_driver."""
    try:
        # Build WHERE conditions
        conditions = []
        params = {}

        if driver:
            conditions.append("d.driver_name LIKE :driver")
            params["driver"] = f"%{driver}%"

        if license_plate:
            conditions.append("v.registration_number LIKE :license_plate")
            params["license_plate"] = f"%{license_plate}%"

        if search:
            conditions.append(
                "(v.registration_number LIKE :search "
                "OR v.make_and_model LIKE :search "
                "OR d.driver_name LIKE :search "
                "OR v.vin_number LIKE :search)"
            )
            params["search"] = f"%{search}%"

        if status:
            if status == "Terminated":
                conditions.append("v.vehicle_status LIKE 'Terminated%'")
            elif status == "active_within_12m":
                conditions.append("v.is_active = 1")
            elif status == "started_12m":
                conditions.append(
                    "v.lease_start_date >= date('now', '-12 months') "
                    "AND v.lease_start_date IS NOT NULL"
                )
            elif status == "terminated_12m":
                conditions.append(
                    "v.vehicle_status_code >= 2 "
                    "AND v.lease_end_date >= date('now', '-12 months') "
                    "AND v.lease_end_date IS NOT NULL"
                )
            else:
                conditions.append("v.vehicle_status = :status")
                params["status"] = status

        if make:
            conditions.append("v.make_name = :make")
            params["make"] = make

        if customer_id:
            conditions.append("v.customer_id = :customer_id")
            params["customer_id"] = customer_id

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Validate sort column
        sort_col = SORTABLE_COLUMNS.get(sort_by, 'v.registration_number')
        sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"

        # Count total
        count_query = f"""
            SELECT COUNT(*) as total
            FROM dim_vehicle v
            LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
            {where_clause}
        """
        count_result = await execute_raw_query(count_query, params)
        total = count_result[0]['total'] if count_result else 0
        total_pages = math.ceil(total / page_size) if total > 0 else 1

        # Fetch page
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT
                v.vehicle_id,
                v.registration_number,
                v.vin_number,
                v.make_name,
                v.model_name,
                v.make_and_model,
                v.vehicle_year,
                v.color_name,
                v.fuel_type,
                v.customer_name,
                v.vehicle_status,
                v.current_odometer_km,
                v.monthly_lease_amount,
                d.driver_name,
                d.email_address AS driver_email,
                d.phone_mobile AS driver_phone
            FROM dim_vehicle v
            LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
            {where_clause}
            ORDER BY {sort_col} {sort_dir}
            LIMIT :limit OFFSET :offset
        """
        params["limit"] = page_size
        params["offset"] = offset

        rows = await execute_raw_query(data_query, params)

        return {
            "items": rows,
            "total": total,
            "total_pages": total_pages,
            "page": page,
            "page_size": page_size,
        }
    except Exception as e:
        logger.error(f"Error fetching vehicles list: {e}")
        raise
