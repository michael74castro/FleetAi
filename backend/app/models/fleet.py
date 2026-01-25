"""
FleetAI - Fleet Models
SQLAlchemy models for fleet data (read from reporting schema)
Note: These are read-only views of the reporting dimensional model
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Date, Numeric, ForeignKey
)
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.database import Base
from app.core.config import settings

# Use schema only for MSSQL, not SQLite
def get_table_args(schema_name='reporting'):
    if settings.DATABASE_TYPE == 'sqlite':
        return {}
    return {'schema': schema_name}

def fk(table_column: str) -> str:
    """Get foreign key reference, handling schema for MSSQL vs SQLite"""
    if settings.DATABASE_TYPE == 'sqlite':
        # Strip schema prefix for SQLite
        if '.' in table_column:
            parts = table_column.split('.')
            if len(parts) == 3:  # schema.table.column
                return f"{parts[1]}.{parts[2]}"
            return table_column
        return table_column
    return table_column


class DimCustomer(Base):
    """Customer dimension - read-only from reporting schema"""
    __tablename__ = 'dim_customer'
    __table_args__ = get_table_args()

    customer_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(20), nullable=False)
    customer_name: Mapped[str] = mapped_column(String(200), nullable=False)
    legal_name: Mapped[Optional[str]] = mapped_column(String(200))
    account_type: Mapped[Optional[str]] = mapped_column(String(50))
    parent_customer_id: Mapped[Optional[str]] = mapped_column(String(20))
    parent_customer_name: Mapped[Optional[str]] = mapped_column(String(200))
    tax_id: Mapped[Optional[str]] = mapped_column(String(30))
    industry: Mapped[Optional[str]] = mapped_column(String(100))
    employee_count_tier: Mapped[Optional[str]] = mapped_column(String(20))
    credit_rating: Mapped[Optional[str]] = mapped_column(String(20))
    payment_terms: Mapped[Optional[str]] = mapped_column(String(50))
    account_manager: Mapped[Optional[str]] = mapped_column(String(50))
    region: Mapped[Optional[str]] = mapped_column(String(50))
    territory: Mapped[Optional[str]] = mapped_column(String(50))
    billing_city: Mapped[Optional[str]] = mapped_column(String(100))
    billing_state: Mapped[Optional[str]] = mapped_column(String(50))
    billing_country: Mapped[Optional[str]] = mapped_column(String(50))
    status: Mapped[Optional[str]] = mapped_column(String(20))
    created_date: Mapped[Optional[date]] = mapped_column(Date)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

    def __repr__(self):
        return f"<DimCustomer(customer_id={self.customer_id}, name={self.customer_name})>"


class DimVehicle(Base):
    """Vehicle dimension - read-only from reporting schema"""
    __tablename__ = 'dim_vehicle'
    __table_args__ = get_table_args()

    vehicle_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    equipment_id: Mapped[str] = mapped_column(String(20), nullable=False)
    vin: Mapped[Optional[str]] = mapped_column(String(17))
    license_plate: Mapped[Optional[str]] = mapped_column(String(20))
    make: Mapped[Optional[str]] = mapped_column(String(50))
    model: Mapped[Optional[str]] = mapped_column(String(50))
    model_year: Mapped[Optional[int]] = mapped_column(Integer)
    color: Mapped[Optional[str]] = mapped_column(String(30))
    body_type: Mapped[Optional[str]] = mapped_column(String(30))
    engine_type: Mapped[Optional[str]] = mapped_column(String(30))
    fuel_type: Mapped[Optional[str]] = mapped_column(String(20))
    transmission: Mapped[Optional[str]] = mapped_column(String(20))
    acquisition_date: Mapped[Optional[date]] = mapped_column(Date)
    acquisition_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    residual_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    status: Mapped[Optional[str]] = mapped_column(String(20))
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

    @property
    def make_model(self) -> str:
        return f"{self.make or ''} {self.model or ''}".strip()

    def __repr__(self):
        return f"<DimVehicle(equipment_id={self.equipment_id}, make_model={self.make_model})>"


class DimDriver(Base):
    """Driver dimension - read-only from reporting schema"""
    __tablename__ = 'dim_driver'
    __table_args__ = get_table_args()

    driver_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    driver_id: Mapped[str] = mapped_column(String(20), nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(String(20))
    first_name: Mapped[Optional[str]] = mapped_column(String(50))
    last_name: Mapped[Optional[str]] = mapped_column(String(50))
    full_name: Mapped[Optional[str]] = mapped_column(String(101))
    email: Mapped[Optional[str]] = mapped_column(String(100))
    department: Mapped[Optional[str]] = mapped_column(String(100))
    cost_center: Mapped[Optional[str]] = mapped_column(String(50))
    license_state: Mapped[Optional[str]] = mapped_column(String(10))
    license_expiry_date: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[Optional[str]] = mapped_column(String(20))
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

    def __repr__(self):
        return f"<DimDriver(driver_id={self.driver_id}, name={self.full_name})>"


class DimContract(Base):
    """Contract dimension - read-only from reporting schema"""
    __tablename__ = 'dim_contract'
    __table_args__ = get_table_args()

    contract_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    contract_no: Mapped[str] = mapped_column(String(20), nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(String(20))
    contract_type: Mapped[Optional[str]] = mapped_column(String(50))
    start_date: Mapped[Optional[date]] = mapped_column(Date)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    term_months: Mapped[Optional[int]] = mapped_column(Integer)
    monthly_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    mileage_allowance: Mapped[Optional[int]] = mapped_column(Integer)
    excess_mileage_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    insurance_included: Mapped[Optional[bool]] = mapped_column(Boolean)
    maintenance_included: Mapped[Optional[bool]] = mapped_column(Boolean)
    status: Mapped[Optional[str]] = mapped_column(String(20))
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

    @property
    def total_contract_value(self) -> Optional[Decimal]:
        if self.monthly_rate and self.term_months:
            return self.monthly_rate * self.term_months
        return None

    def __repr__(self):
        return f"<DimContract(contract_no={self.contract_no}, status={self.status})>"


class DimDate(Base):
    """Date dimension - read-only from reporting schema"""
    __tablename__ = 'dim_date'
    __table_args__ = get_table_args()

    date_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_date: Mapped[date] = mapped_column(Date, nullable=False)
    day_of_week: Mapped[Optional[int]] = mapped_column(Integer)
    day_name: Mapped[Optional[str]] = mapped_column(String(10))
    day_of_month: Mapped[Optional[int]] = mapped_column(Integer)
    day_of_year: Mapped[Optional[int]] = mapped_column(Integer)
    week_of_year: Mapped[Optional[int]] = mapped_column(Integer)
    month_number: Mapped[Optional[int]] = mapped_column(Integer)
    month_name: Mapped[Optional[str]] = mapped_column(String(10))
    month_short: Mapped[Optional[str]] = mapped_column(String(3))
    quarter_number: Mapped[Optional[int]] = mapped_column(Integer)
    quarter_name: Mapped[Optional[str]] = mapped_column(String(6))
    year_number: Mapped[Optional[int]] = mapped_column(Integer)
    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer)
    is_weekend: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_holiday: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_business_day: Mapped[Optional[bool]] = mapped_column(Boolean)

    def __repr__(self):
        return f"<DimDate({self.full_date})>"


class FactInvoice(Base):
    """Invoice fact table - read-only from reporting schema"""
    __tablename__ = 'fact_invoices'
    __table_args__ = get_table_args()

    invoice_fact_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_date_key: Mapped[int] = mapped_column(Integer, ForeignKey('dim_date.date_key'))
    due_date_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_date.date_key'))
    customer_key: Mapped[int] = mapped_column(Integer, ForeignKey('dim_customer.customer_key'))
    contract_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_contract.contract_key'))
    invoice_no: Mapped[str] = mapped_column(String(30), nullable=False)
    subtotal: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    tax_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    total_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    paid_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    balance_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    line_item_count: Mapped[Optional[int]] = mapped_column(Integer)
    days_outstanding: Mapped[Optional[int]] = mapped_column(Integer)
    is_overdue: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Relationships
    invoice_date: Mapped[Optional["DimDate"]] = relationship("DimDate", foreign_keys=[invoice_date_key])
    customer: Mapped[Optional["DimCustomer"]] = relationship("DimCustomer")
    contract: Mapped[Optional["DimContract"]] = relationship("DimContract")

    def __repr__(self):
        return f"<FactInvoice(invoice_no={self.invoice_no}, total={self.total_amount})>"


class FactFuel(Base):
    """Fuel consumption fact table - read-only from reporting schema"""
    __tablename__ = 'fact_fuel'
    __table_args__ = get_table_args()

    fuel_fact_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transaction_date_key: Mapped[int] = mapped_column(Integer, ForeignKey('dim_date.date_key'))
    customer_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_customer.customer_key'))
    vehicle_key: Mapped[int] = mapped_column(Integer, ForeignKey('dim_vehicle.vehicle_key'))
    driver_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_driver.driver_key'))
    gallons: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    price_per_gallon: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    odometer: Mapped[Optional[int]] = mapped_column(Integer)
    miles_since_last_fill: Mapped[Optional[int]] = mapped_column(Integer)
    mpg: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    fuel_type: Mapped[Optional[str]] = mapped_column(String(20))
    product_code: Mapped[Optional[str]] = mapped_column(String(20))

    # Relationships
    transaction_date: Mapped[Optional["DimDate"]] = relationship("DimDate")
    customer: Mapped[Optional["DimCustomer"]] = relationship("DimCustomer")
    vehicle: Mapped[Optional["DimVehicle"]] = relationship("DimVehicle")
    driver: Mapped[Optional["DimDriver"]] = relationship("DimDriver")

    def __repr__(self):
        return f"<FactFuel(vehicle_key={self.vehicle_key}, gallons={self.gallons})>"


class FactMaintenance(Base):
    """Maintenance fact table - read-only from reporting schema"""
    __tablename__ = 'fact_maintenance'
    __table_args__ = get_table_args()

    maintenance_fact_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    completed_date_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_date.date_key'))
    scheduled_date_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_date.date_key'))
    customer_key: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dim_customer.customer_key'))
    vehicle_key: Mapped[int] = mapped_column(Integer, ForeignKey('dim_vehicle.vehicle_key'))
    work_order_no: Mapped[str] = mapped_column(String(30), nullable=False)
    order_type: Mapped[Optional[str]] = mapped_column(String(50))
    priority: Mapped[Optional[str]] = mapped_column(String(20))
    labor_hours: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    labor_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    parts_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    total_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    estimated_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    days_to_complete: Mapped[Optional[int]] = mapped_column(Integer)
    is_scheduled: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_completed: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Relationships
    completed_date: Mapped[Optional["DimDate"]] = relationship("DimDate", foreign_keys=[completed_date_key])
    customer: Mapped[Optional["DimCustomer"]] = relationship("DimCustomer")
    vehicle: Mapped[Optional["DimVehicle"]] = relationship("DimVehicle")

    def __repr__(self):
        return f"<FactMaintenance(work_order={self.work_order_no}, cost={self.total_cost})>"
