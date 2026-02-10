"""
FleetAI - Fleet API Endpoints
Provides vehicle listing and KPI data from the semantic layer tables.
"""

import csv
import io
import logging
import math
from typing import Optional, Literal

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.core.database import execute_raw_query

logger = logging.getLogger(__name__)

router = APIRouter()

# Filter types for renewals/orders list
RenewalsFilterType = Literal[
    "overdue_with_order",
    "overdue_no_order",
    "due_no_order",
    "renewal_orders",
    "new_orders",
    "all"
]

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


@router.get("/vehicles/{vehicle_id}")
async def get_vehicle_details(vehicle_id: int):
    """Get comprehensive details for a single vehicle."""
    try:
        # Main vehicle + driver + contract + customer query
        query = """
            SELECT
                v.vehicle_id,
                v.vehicle_id AS object_no,
                v.registration_number,
                v.vin_number,
                v.make_name,
                v.model_name,
                v.make_and_model,
                v.vehicle_year,
                v.color_name,
                v.body_type,
                v.fuel_type,
                v.vehicle_status,
                v.vehicle_status_code,
                v.is_active,
                v.lease_type,
                v.lease_type_description,
                v.purchase_price,
                v.residual_value,
                v.monthly_lease_amount,
                v.lease_duration_months,
                v.annual_km_allowance,
                v.current_odometer_km,
                v.last_odometer_date,
                v.lease_start_date,
                v.lease_end_date,
                v.expected_end_date,
                v.months_driven,
                v.months_remaining,
                v.days_to_contract_end,
                v.customer_id,
                v.customer_name,
                v.contract_position_number,
                d.driver_name,
                d.first_name AS driver_first_name,
                d.last_name AS driver_last_name,
                d.email_address AS driver_email,
                d.phone_mobile AS driver_mobile,
                ct.contract_number,
                ct.monthly_rate_total,
                ct.monthly_rate_depreciation,
                ct.monthly_rate_maintenance,
                ct.monthly_rate_insurance,
                ct.monthly_rate_fuel,
                ct.monthly_rate_tires,
                ct.monthly_rate_road_tax,
                ct.monthly_rate_admin,
                ct.monthly_rate_replacement_vehicle,
                ct.excess_km_rate,
                ct.interest_rate_percent,
                cust.customer_name AS company_name,
                cust.account_manager_name
            FROM dim_vehicle v
            LEFT JOIN dim_driver d
                ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
            LEFT JOIN dim_contract ct
                ON v.contract_position_number = ct.contract_position_number
            LEFT JOIN dim_customer cust
                ON v.customer_id = cust.customer_id
            WHERE v.vehicle_id = :vehicle_id
        """
        rows = await execute_raw_query(query, {"vehicle_id": vehicle_id})

        if not rows:
            return {"error": "Vehicle not found", "vehicle_id": vehicle_id}

        vehicle = dict(rows[0])

        # Get cost centre from staging_drivers
        cost_centre_query = """
            SELECT cost_center
            FROM staging_drivers
            WHERE object_no = :vehicle_id AND is_current = 1
            ORDER BY driver_no ASC
            LIMIT 1
        """
        cc_rows = await execute_raw_query(cost_centre_query, {"vehicle_id": vehicle_id})
        vehicle["cost_centre"] = cc_rows[0]["cost_center"] if cc_rows else None

        # Get start mileage from first odometer reading
        start_km_query = """
            SELECT reading_km
            FROM fact_odometer_reading
            WHERE vehicle_id = :vehicle_id
            ORDER BY reading_date ASC
            LIMIT 1
        """
        try:
            start_km_rows = await execute_raw_query(start_km_query, {"vehicle_id": vehicle_id})
            vehicle["start_mileage"] = start_km_rows[0]["reading_km"] if start_km_rows else None
        except Exception:
            vehicle["start_mileage"] = None

        # Get renewal order info
        renewal_query = """
            SELECT order_no, order_status
            FROM staging_orders
            WHERE previous_object_no = :vehicle_id
              AND order_status_code < 6
            ORDER BY order_no DESC
            LIMIT 1
        """
        renewal_rows = await execute_raw_query(renewal_query, {"vehicle_id": vehicle_id})
        if renewal_rows:
            vehicle["renewal_order_no"] = renewal_rows[0]["order_no"]
            vehicle["renewal_status"] = renewal_rows[0]["order_status"]
        else:
            vehicle["renewal_order_no"] = None
            vehicle["renewal_status"] = None

        # Compute gauge data
        duration_months = vehicle.get("lease_duration_months") or 0
        months_driven = vehicle.get("months_driven") or 0
        if duration_months > 0:
            duration_pct = round((months_driven / duration_months) * 100, 1)
        else:
            duration_pct = 0

        annual_km = vehicle.get("annual_km_allowance") or 0
        total_km_allowance = annual_km * (duration_months / 12) if duration_months > 0 else 0
        current_km = vehicle.get("current_odometer_km") or 0
        start_km = vehicle.get("start_mileage") or 0
        driven_km = current_km - start_km if current_km > start_km else current_km
        if total_km_allowance > 0:
            km_pct = round((driven_km / total_km_allowance) * 100, 1)
        else:
            km_pct = 0

        vehicle["gauge_duration"] = {
            "value": months_driven,
            "target": duration_months,
            "percentage": min(duration_pct, 150),
        }
        vehicle["gauge_km"] = {
            "value": driven_km,
            "target": int(total_km_allowance),
            "percentage": min(km_pct, 150),
        }

        return vehicle

    except Exception as e:
        logger.error(f"Error fetching vehicle details: {e}")
        raise


@router.get("/renewals/filters")
async def get_renewals_filter_options():
    """Get available filter options for the renewals page (makes, customers, etc.)."""
    try:
        # Get distinct makes from active vehicles
        makes_query = """
            SELECT DISTINCT make_name
            FROM dim_vehicle
            WHERE is_active = 1 AND make_name IS NOT NULL
            ORDER BY make_name
        """
        makes_result = await execute_raw_query(makes_query)
        makes = [r['make_name'] for r in makes_result] if makes_result else []

        # Get distinct customers with active vehicles
        customers_query = """
            SELECT DISTINCT customer_id, customer_name
            FROM dim_vehicle
            WHERE is_active = 1 AND customer_name IS NOT NULL
            ORDER BY customer_name
            LIMIT 100
        """
        customers_result = await execute_raw_query(customers_query)
        customers = [
            {"id": r['customer_id'], "name": r['customer_name']}
            for r in customers_result
        ] if customers_result else []

        return {
            "makes": makes,
            "customers": customers,
            "order_statuses": [
                {"code": 0, "label": "Created"},
                {"code": 1, "label": "Sent to Dealer"},
                {"code": 2, "label": "Delivery Confirmed"},
                {"code": 3, "label": "Insurance Arranged"},
                {"code": 4, "label": "Registration Arranged"},
                {"code": 5, "label": "Driver Info Prepared"},
            ],
            "renewal_statuses": [
                {"value": "Overdue", "label": "Overdue (past due)"},
                {"value": "Due Soon", "label": "Due Soon (0-30 days)"},
                {"value": "Due", "label": "Due (31-90 days)"},
                {"value": "Active", "label": "Active (90+ days)"},
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching renewals filter options: {e}")
        raise


@router.get("/renewals/kpis")
async def get_renewals_kpis():
    """
    Get KPIs for the Renewals & Orders page.

    KPI Definitions:
    - overdue_renewals_with_order: Active vehicles with days_to_contract_end < 0 AND has order
    - overdue_renewals_no_order: Active vehicles with days_to_contract_end < 0 AND no order
    - renewals_due_without_order: Active vehicles with days_to_contract_end BETWEEN 0 AND 90 AND no order
    - renewal_orders: Orders where previous_object_no IS NOT NULL AND order_status_code < 6
    - new_orders: Orders where previous_object_no IS NULL AND order_status_code < 6
    """
    try:
        # Get active order object_nos for comparison (orders not yet delivered)
        active_orders_query = """
            SELECT previous_object_no
            FROM staging_orders
            WHERE previous_object_no IS NOT NULL
              AND order_status_code < 6
        """
        active_orders = await execute_raw_query(active_orders_query)
        order_object_nos = {r['previous_object_no'] for r in active_orders} if active_orders else set()

        # Query 1: Overdue with order (days_to_contract_end < 0 AND has active order)
        overdue_with_order_query = """
            SELECT COUNT(*) as cnt
            FROM dim_vehicle
            WHERE is_active = 1
              AND days_to_contract_end < 0
              AND vehicle_id IN (
                  SELECT previous_object_no FROM staging_orders
                  WHERE previous_object_no IS NOT NULL AND order_status_code < 6
              )
        """
        result = await execute_raw_query(overdue_with_order_query)
        overdue_with_order = result[0]['cnt'] if result else 0

        # Query 2: Overdue without order (days_to_contract_end < 0 AND no active order)
        overdue_no_order_query = """
            SELECT COUNT(*) as cnt
            FROM dim_vehicle
            WHERE is_active = 1
              AND days_to_contract_end < 0
              AND vehicle_id NOT IN (
                  SELECT previous_object_no FROM staging_orders
                  WHERE previous_object_no IS NOT NULL AND order_status_code < 6
              )
        """
        result = await execute_raw_query(overdue_no_order_query)
        overdue_no_order = result[0]['cnt'] if result else 0

        # Query 3: Due without order (0-90 days AND no active order)
        due_no_order_query = """
            SELECT COUNT(*) as cnt
            FROM dim_vehicle
            WHERE is_active = 1
              AND days_to_contract_end BETWEEN 0 AND 90
              AND vehicle_id NOT IN (
                  SELECT previous_object_no FROM staging_orders
                  WHERE previous_object_no IS NOT NULL AND order_status_code < 6
              )
        """
        result = await execute_raw_query(due_no_order_query)
        due_no_order = result[0]['cnt'] if result else 0

        # Query 4: Renewal orders (has previous_object_no, not delivered)
        renewal_orders_query = """
            SELECT COUNT(*) as cnt
            FROM staging_orders
            WHERE previous_object_no IS NOT NULL
              AND order_status_code < 6
        """
        result = await execute_raw_query(renewal_orders_query)
        renewal_orders = result[0]['cnt'] if result else 0

        # Query 5: New orders (no previous_object_no, not delivered)
        new_orders_query = """
            SELECT COUNT(*) as cnt
            FROM staging_orders
            WHERE previous_object_no IS NULL
              AND order_status_code < 6
        """
        result = await execute_raw_query(new_orders_query)
        new_orders = result[0]['cnt'] if result else 0

        return {
            "overdue_renewals_with_order": overdue_with_order,
            "overdue_renewals_no_order": overdue_no_order,
            "renewals_due_without_order": due_no_order,
            "renewal_orders": renewal_orders,
            "new_orders": new_orders,
            "last_data_update": "2026-01-28T04:00:00Z",
            "last_monthly_closure": "December 2025"
        }

    except Exception as e:
        logger.error(f"Error fetching renewals KPIs: {e}")
        raise


# Sortable columns for renewals list
RENEWALS_SORTABLE_COLUMNS = {
    'registration_number': 'v.registration_number',
    'driver_name': 'd.driver_name',
    'make_and_model': 'v.make_and_model',
    'customer_name': 'v.customer_name',
    'vehicle_status': 'v.vehicle_status',
    'days_to_contract_end': 'v.days_to_contract_end',
    'expected_end_date': 'v.expected_end_date',
    'vehicle_id': 'v.vehicle_id',
    'order_no': 'o.order_no',
    'order_status': 'o.order_status',
    'order_date': 'o.order_date',
}


@router.get("/renewals/list")
async def get_renewals_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    filter_type: str = Query("all"),
    search: Optional[str] = Query(None),
    sort_by: str = Query("days_to_contract_end"),
    sort_order: str = Query("asc"),
    # Advanced filters
    customer_id: Optional[int] = Query(None),
    make: Optional[str] = Query(None),
    order_status_code: Optional[int] = Query(None),
    renewal_status: Optional[str] = Query(None),
):
    """
    Get paginated list of vehicles/orders for Renewals & Orders page.

    Filter types:
    - overdue_with_order: Overdue vehicles with active renewal order
    - overdue_no_order: Overdue vehicles without renewal order
    - due_no_order: Due (0-90 days) vehicles without renewal order
    - renewal_orders: Active renewal orders (orders replacing existing vehicles)
    - new_orders: Active new orders (orders for new vehicles)
    - all: All active vehicles due or overdue for renewal
    """
    try:
        # For order-focused filters, return order data
        if filter_type in ("renewal_orders", "new_orders"):
            return await _get_orders_list(
                page, page_size, filter_type, search, sort_by, sort_order,
                customer_id=customer_id, order_status_code=order_status_code
            )

        # For vehicle-focused filters, return vehicle data with optional order info
        return await _get_renewal_vehicles_list(
            page, page_size, filter_type, search, sort_by, sort_order,
            customer_id=customer_id, make=make, order_status_code=order_status_code,
            renewal_status=renewal_status
        )

    except Exception as e:
        logger.error(f"Error fetching renewals list: {e}")
        raise


async def _get_renewal_vehicles_list(
    page: int,
    page_size: int,
    filter_type: str,
    search: Optional[str],
    sort_by: str,
    sort_order: str,
    customer_id: Optional[int] = None,
    make: Optional[str] = None,
    order_status_code: Optional[int] = None,
    renewal_status: Optional[str] = None,
):
    """Get vehicle list for renewal-focused filters."""
    conditions = ["v.is_active = 1"]
    params = {}

    # Apply filter type conditions
    if filter_type == "overdue_with_order":
        conditions.append("v.days_to_contract_end < 0")
        conditions.append("""v.vehicle_id IN (
            SELECT previous_object_no FROM staging_orders
            WHERE previous_object_no IS NOT NULL AND order_status_code < 6
        )""")
    elif filter_type == "overdue_no_order":
        conditions.append("v.days_to_contract_end < 0")
        conditions.append("""v.vehicle_id NOT IN (
            SELECT previous_object_no FROM staging_orders
            WHERE previous_object_no IS NOT NULL AND order_status_code < 6
        )""")
    elif filter_type == "due_no_order":
        conditions.append("v.days_to_contract_end BETWEEN 0 AND 90")
        conditions.append("""v.vehicle_id NOT IN (
            SELECT previous_object_no FROM staging_orders
            WHERE previous_object_no IS NOT NULL AND order_status_code < 6
        )""")
    else:
        # "all" - show all vehicles due or overdue (within 90 days or overdue)
        conditions.append("v.days_to_contract_end <= 90")

    # Search filter
    if search:
        conditions.append(
            "(v.registration_number LIKE :search "
            "OR v.make_and_model LIKE :search "
            "OR d.driver_name LIKE :search)"
        )
        params["search"] = f"%{search}%"

    # Advanced filters
    if customer_id:
        conditions.append("v.customer_id = :customer_id")
        params["customer_id"] = customer_id

    if make:
        conditions.append("v.make_name = :make")
        params["make"] = make

    if order_status_code is not None:
        conditions.append("o.order_status_code = :order_status_code")
        params["order_status_code"] = order_status_code

    if renewal_status:
        # Filter by computed renewal_status
        if renewal_status == "Overdue":
            conditions.append("v.days_to_contract_end < 0")
        elif renewal_status == "Due Soon":
            conditions.append("v.days_to_contract_end BETWEEN 0 AND 30")
        elif renewal_status == "Due":
            conditions.append("v.days_to_contract_end BETWEEN 31 AND 90")
        elif renewal_status == "Active":
            conditions.append("v.days_to_contract_end > 90")

    where_clause = "WHERE " + " AND ".join(conditions)

    # Validate sort column
    sort_col = RENEWALS_SORTABLE_COLUMNS.get(sort_by, 'v.days_to_contract_end')
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

    # Fetch page with order info
    offset = (page - 1) * page_size
    data_query = f"""
        SELECT
            v.vehicle_id,
            v.vehicle_id as object_no,
            v.registration_number,
            v.vin_number,
            v.make_name,
            v.model_name,
            v.make_and_model,
            v.customer_name,
            v.vehicle_status,
            v.days_to_contract_end,
            v.expected_end_date,
            d.driver_name,
            o.order_no,
            o.order_status,
            o.order_date,
            CASE
                WHEN v.days_to_contract_end < 0 THEN 'Overdue'
                WHEN v.days_to_contract_end BETWEEN 0 AND 30 THEN 'Due Soon'
                WHEN v.days_to_contract_end BETWEEN 31 AND 90 THEN 'Due'
                ELSE 'Active'
            END as renewal_status
        FROM dim_vehicle v
        LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
        LEFT JOIN staging_orders o ON v.vehicle_id = o.previous_object_no AND o.order_status_code < 6
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
        "filter_type": filter_type
    }


async def _get_orders_list(
    page: int,
    page_size: int,
    filter_type: str,
    search: Optional[str],
    sort_by: str,
    sort_order: str,
    customer_id: Optional[int] = None,
    order_status_code: Optional[int] = None,
):
    """Get order list for order-focused filters."""
    conditions = ["o.order_status_code < 6"]  # Not yet delivered
    params = {}

    # Apply filter type conditions
    if filter_type == "renewal_orders":
        conditions.append("o.previous_object_no IS NOT NULL")
    elif filter_type == "new_orders":
        conditions.append("o.previous_object_no IS NULL")

    # Search filter
    if search:
        conditions.append(
            "(c.customer_name LIKE :search "
            "OR CAST(o.order_no AS TEXT) LIKE :search)"
        )
        params["search"] = f"%{search}%"

    # Advanced filters
    if customer_id:
        conditions.append("o.customer_no = :customer_id")
        params["customer_id"] = customer_id

    if order_status_code is not None:
        conditions.append("o.order_status_code = :order_status_code")
        params["order_status_code"] = order_status_code

    where_clause = "WHERE " + " AND ".join(conditions)

    # Sort handling for orders
    order_sort_cols = {
        'order_no': 'o.order_no',
        'order_status': 'o.order_status',
        'order_date': 'o.order_date',
        'customer_name': 'c.customer_name',
    }
    sort_col = order_sort_cols.get(sort_by, 'o.order_no')
    sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"

    # Count total
    count_query = f"""
        SELECT COUNT(*) as total
        FROM staging_orders o
        LEFT JOIN staging_customers c ON o.customer_no = c.customer_id
        {where_clause}
    """
    count_result = await execute_raw_query(count_query, params)
    total = count_result[0]['total'] if count_result else 0
    total_pages = math.ceil(total / page_size) if total > 0 else 1

    # Fetch page
    offset = (page - 1) * page_size
    data_query = f"""
        SELECT
            o.order_no,
            o.order_no as id,
            o.customer_no,
            c.customer_name,
            o.order_status_code,
            o.order_status,
            o.order_date,
            o.requested_delivery_date,
            o.confirmed_delivery_date,
            o.previous_object_no,
            o.previous_object_no as object_no,
            v.registration_number,
            v.make_and_model,
            d.driver_name,
            CASE WHEN o.previous_object_no IS NOT NULL THEN 1 ELSE 0 END as is_renewal,
            CASE WHEN o.previous_object_no IS NULL THEN 1 ELSE 0 END as is_new
        FROM staging_orders o
        LEFT JOIN staging_customers c ON o.customer_no = c.customer_id
        LEFT JOIN dim_vehicle v ON o.previous_object_no = v.vehicle_id
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
        "filter_type": filter_type
    }


@router.get("/renewals/export")
async def export_renewals_list(
    filter_type: str = Query("all"),
    search: Optional[str] = Query(None),
    customer_id: Optional[int] = Query(None),
    make: Optional[str] = Query(None),
    order_status_code: Optional[int] = Query(None),
    renewal_status: Optional[str] = Query(None),
    format: str = Query("csv"),
):
    """Export renewals list to CSV."""
    try:
        # Build conditions (same as _get_renewal_vehicles_list but no pagination)
        conditions = ["v.is_active = 1"]
        params = {}

        if filter_type == "overdue_with_order":
            conditions.append("v.days_to_contract_end < 0")
            conditions.append("""v.vehicle_id IN (
                SELECT previous_object_no FROM staging_orders
                WHERE previous_object_no IS NOT NULL AND order_status_code < 6
            )""")
        elif filter_type == "overdue_no_order":
            conditions.append("v.days_to_contract_end < 0")
            conditions.append("""v.vehicle_id NOT IN (
                SELECT previous_object_no FROM staging_orders
                WHERE previous_object_no IS NOT NULL AND order_status_code < 6
            )""")
        elif filter_type == "due_no_order":
            conditions.append("v.days_to_contract_end BETWEEN 0 AND 90")
            conditions.append("""v.vehicle_id NOT IN (
                SELECT previous_object_no FROM staging_orders
                WHERE previous_object_no IS NOT NULL AND order_status_code < 6
            )""")
        elif filter_type in ("renewal_orders", "new_orders"):
            # Export orders instead
            return await _export_orders(filter_type, search, customer_id, order_status_code)
        else:
            conditions.append("v.days_to_contract_end <= 90")

        if search:
            conditions.append(
                "(v.registration_number LIKE :search "
                "OR v.make_and_model LIKE :search "
                "OR d.driver_name LIKE :search)"
            )
            params["search"] = f"%{search}%"

        if customer_id:
            conditions.append("v.customer_id = :customer_id")
            params["customer_id"] = customer_id

        if make:
            conditions.append("v.make_name = :make")
            params["make"] = make

        if renewal_status:
            if renewal_status == "Overdue":
                conditions.append("v.days_to_contract_end < 0")
            elif renewal_status == "Due Soon":
                conditions.append("v.days_to_contract_end BETWEEN 0 AND 30")
            elif renewal_status == "Due":
                conditions.append("v.days_to_contract_end BETWEEN 31 AND 90")

        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT
                v.registration_number AS "License Plate",
                d.driver_name AS "Driver",
                CASE
                    WHEN v.days_to_contract_end < 0 THEN 'Overdue'
                    WHEN v.days_to_contract_end BETWEEN 0 AND 30 THEN 'Due Soon'
                    WHEN v.days_to_contract_end BETWEEN 31 AND 90 THEN 'Due'
                    ELSE 'Active'
                END AS "Renewal Status",
                v.vehicle_id AS "Object No",
                v.make_and_model AS "Make & Model",
                v.customer_name AS "Customer",
                v.days_to_contract_end AS "Days to End",
                v.expected_end_date AS "Expected End Date",
                o.order_status AS "Order Status",
                o.order_no AS "Order No"
            FROM dim_vehicle v
            LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
            LEFT JOIN staging_orders o ON v.vehicle_id = o.previous_object_no AND o.order_status_code < 6
            {where_clause}
            ORDER BY v.days_to_contract_end ASC
        """

        rows = await execute_raw_query(query, params)

        # Generate CSV
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        else:
            output.write("No data found")

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=renewals_export.csv"
            }
        )

    except Exception as e:
        logger.error(f"Error exporting renewals: {e}")
        raise


async def _export_orders(
    filter_type: str,
    search: Optional[str],
    customer_id: Optional[int],
    order_status_code: Optional[int],
):
    """Export orders to CSV."""
    conditions = ["o.order_status_code < 6"]
    params = {}

    if filter_type == "renewal_orders":
        conditions.append("o.previous_object_no IS NOT NULL")
    elif filter_type == "new_orders":
        conditions.append("o.previous_object_no IS NULL")

    if search:
        conditions.append(
            "(c.customer_name LIKE :search "
            "OR CAST(o.order_no AS TEXT) LIKE :search)"
        )
        params["search"] = f"%{search}%"

    if customer_id:
        conditions.append("o.customer_no = :customer_id")
        params["customer_id"] = customer_id

    if order_status_code is not None:
        conditions.append("o.order_status_code = :order_status_code")
        params["order_status_code"] = order_status_code

    where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT
            o.order_no AS "Order No",
            c.customer_name AS "Customer",
            o.order_status AS "Order Status",
            o.order_date AS "Order Date",
            o.requested_delivery_date AS "Requested Delivery",
            o.confirmed_delivery_date AS "Confirmed Delivery",
            v.registration_number AS "Replacing Vehicle",
            v.make_and_model AS "Make & Model",
            d.driver_name AS "Driver",
            CASE WHEN o.previous_object_no IS NOT NULL THEN 'Renewal' ELSE 'New' END AS "Order Type"
        FROM staging_orders o
        LEFT JOIN staging_customers c ON o.customer_no = c.customer_id
        LEFT JOIN dim_vehicle v ON o.previous_object_no = v.vehicle_id
        LEFT JOIN dim_driver d ON v.vehicle_id = d.vehicle_id AND d.is_primary_driver = 1
        {where_clause}
        ORDER BY o.order_no DESC
    """

    rows = await execute_raw_query(query, params)

    # Generate CSV
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    else:
        output.write("No data found")

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=orders_export.csv"
        }
    )
