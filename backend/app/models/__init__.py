"""
FleetAI - SQLAlchemy Models
"""

from app.models.user import (
    User,
    Role,
    Permission,
    UserCustomerAccess,
    UserDriverLink,
    UserSession,
    AuditLog,
)

from app.models.dashboard import (
    Dashboard,
    DashboardWidget,
    DashboardAccess,
    WIDGET_TYPES,
)

from app.models.report import (
    Report,
    ReportAccess,
    ReportSchedule,
    ReportExecution,
    Dataset,
)

from app.models.ai import (
    AIConversation,
    AIMessage,
    AISQLAudit,
    VectorEmbedding,
    KnowledgeBase,
)

from app.models.fleet import (
    DimCustomer,
    DimVehicle,
    DimDriver,
    DimContract,
    DimDate,
    FactInvoice,
    FactFuel,
    FactMaintenance,
)


__all__ = [
    # User models
    "User",
    "Role",
    "Permission",
    "UserCustomerAccess",
    "UserDriverLink",
    "UserSession",
    "AuditLog",
    # Dashboard models
    "Dashboard",
    "DashboardWidget",
    "DashboardAccess",
    "WIDGET_TYPES",
    # Report models
    "Report",
    "ReportAccess",
    "ReportSchedule",
    "ReportExecution",
    "Dataset",
    # AI models
    "AIConversation",
    "AIMessage",
    "AISQLAudit",
    "VectorEmbedding",
    "KnowledgeBase",
    # Fleet models (read-only)
    "DimCustomer",
    "DimVehicle",
    "DimDriver",
    "DimContract",
    "DimDate",
    "FactInvoice",
    "FactFuel",
    "FactMaintenance",
]
