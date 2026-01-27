"""
FleetAI - AI Pydantic Schemas
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .common import FleetAIBaseModel


class MessageBase(BaseModel):
    """Base message schema"""
    role: str = Field(pattern="^(user|assistant|system)$")
    content: str


class MessageCreate(MessageBase):
    """Schema for creating a message"""
    metadata: Optional[Dict[str, Any]] = None


class MessageResponse(MessageBase, FleetAIBaseModel):
    """Schema for message response"""
    message_id: int
    conversation_id: str
    metadata: Optional[Dict[str, Any]]
    created_at: datetime


class ConversationBase(BaseModel):
    """Base conversation schema"""
    title: Optional[str] = Field(None, max_length=200)


class ConversationCreate(ConversationBase):
    """Schema for creating a conversation"""
    context: Optional[Dict[str, Any]] = None


class ConversationResponse(ConversationBase, FleetAIBaseModel):
    """Schema for conversation response"""
    conversation_id: str
    user_id: int
    context: Optional[Dict[str, Any]]
    started_at: datetime
    last_message_at: Optional[datetime]
    message_count: int
    is_archived: bool


class ConversationWithMessages(ConversationResponse):
    """Conversation with messages"""
    messages: List[MessageResponse] = []


class ConversationSummary(FleetAIBaseModel):
    """Conversation summary for list views"""
    conversation_id: str
    title: Optional[str]
    started_at: datetime
    last_message_at: Optional[datetime]
    message_count: int
    is_archived: bool


class ChatRequest(BaseModel):
    """Request to send a chat message"""
    message: str = Field(min_length=1, max_length=10000)
    conversation_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


class ChartConfig(BaseModel):
    """Chart visualization configuration for AI responses"""
    chart_type: str = Field(description="One of: bar, line, area, pie, donut")
    x_axis_key: Optional[str] = Field(None, description="Key for x-axis / category dimension")
    y_axis_keys: Optional[List[str]] = Field(None, description="Keys for y-axis / value metrics")
    title: Optional[str] = None


class ChatResponse(BaseModel):
    """Response from chat"""
    conversation_id: str
    message: MessageResponse
    data: Optional[Any] = None  # Query results, charts, etc.
    chart_config: Optional[ChartConfig] = None
    suggestions: Optional[List[str]] = None
    sources: Optional[List[Dict[str, Any]]] = None


class StreamChatResponse(BaseModel):
    """Streaming chat response chunk"""
    type: str  # 'text', 'data', 'complete', 'error'
    content: str
    metadata: Optional[Dict[str, Any]] = None


class SQLGenerationRequest(BaseModel):
    """Request to generate SQL from natural language"""
    query: str = Field(min_length=1, max_length=5000)
    context: Optional[Dict[str, Any]] = None
    execute: bool = False


class SQLGenerationResponse(BaseModel):
    """Response with generated SQL"""
    user_query: str
    generated_sql: str
    explanation: str
    is_safe: bool
    safety_notes: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[int] = None


class SQLAuditResponse(FleetAIBaseModel):
    """SQL audit entry response"""
    audit_id: int
    conversation_id: Optional[str]
    user_id: int
    user_query: Optional[str]
    generated_sql: Optional[str]
    was_executed: bool
    row_count: Optional[int]
    execution_time_ms: Optional[int]
    was_safe: Optional[bool]
    safety_notes: Optional[str]
    created_at: datetime


class InsightRequest(BaseModel):
    """Request for AI insights"""
    dataset: str
    metric: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    insight_type: str = "summary"  # summary, anomaly, trend, forecast


class InsightResponse(BaseModel):
    """AI-generated insight response"""
    insight_type: str
    title: str
    summary: str
    details: Optional[str] = None
    data: Optional[Any] = None
    confidence: Optional[float] = None
    recommendations: Optional[List[str]] = None


class KnowledgeBaseBase(BaseModel):
    """Base knowledge base schema"""
    category: Optional[str] = Field(None, max_length=100)
    question: str = Field(max_length=1000)
    answer: str
    keywords: Optional[str] = Field(None, max_length=500)
    is_active: bool = True


class KnowledgeBaseCreate(KnowledgeBaseBase):
    """Schema for creating a KB entry"""
    pass


class KnowledgeBaseUpdate(BaseModel):
    """Schema for updating a KB entry"""
    category: Optional[str] = None
    question: Optional[str] = None
    answer: Optional[str] = None
    keywords: Optional[str] = None
    is_active: Optional[bool] = None


class KnowledgeBaseResponse(KnowledgeBaseBase, FleetAIBaseModel):
    """Schema for KB response"""
    kb_id: int
    created_at: datetime
    updated_at: Optional[datetime]


class VectorSearchRequest(BaseModel):
    """Request for vector similarity search"""
    query: str = Field(min_length=1, max_length=5000)
    entity_types: Optional[List[str]] = None
    limit: int = Field(default=10, ge=1, le=100)


class VectorSearchResult(BaseModel):
    """Single vector search result"""
    entity_type: str
    entity_id: str
    content_text: Optional[str]
    similarity_score: float


class VectorSearchResponse(BaseModel):
    """Response with vector search results"""
    query: str
    results: List[VectorSearchResult]
    total_results: int


class DashboardSuggestionRequest(BaseModel):
    """Request for dashboard suggestions"""
    dataset: str
    description: Optional[str] = None


class DashboardSuggestion(BaseModel):
    """Suggested dashboard widget"""
    widget_type: str
    title: str
    description: str
    config: Dict[str, Any]
    relevance_score: float


class DashboardSuggestionResponse(BaseModel):
    """Response with dashboard suggestions"""
    suggestions: List[DashboardSuggestion]
    explanation: str


class ReportSuggestionRequest(BaseModel):
    """Request for report suggestions"""
    purpose: str
    dataset: Optional[str] = None


class ReportSuggestion(BaseModel):
    """Suggested report configuration"""
    name: str
    description: str
    columns: List[Dict[str, Any]]
    filters: List[Dict[str, Any]]
    relevance_score: float


class ReportSuggestionResponse(BaseModel):
    """Response with report suggestions"""
    suggestions: List[ReportSuggestion]
    explanation: str
