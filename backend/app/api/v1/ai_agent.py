"""
FleetAI - AI Agent API Routes
"""

from typing import List, Optional
import json
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import (
    AsyncDB, Pagination, UserAIAccess, ai_rate_limit
)
from app.models.ai import AIConversation, AIMessage, AISQLAudit, KnowledgeBase
from app.schemas.ai import (
    ConversationCreate, ConversationResponse, ConversationWithMessages,
    ConversationSummary, MessageResponse,
    ChatRequest, ChatResponse,
    SQLGenerationRequest, SQLGenerationResponse,
    InsightRequest, InsightResponse,
    KnowledgeBaseCreate, KnowledgeBaseUpdate, KnowledgeBaseResponse,
    VectorSearchRequest, VectorSearchResponse,
    DashboardSuggestionRequest, DashboardSuggestionResponse,
    ReportSuggestionRequest, ReportSuggestionResponse
)
from app.schemas.common import PaginatedResponse, SuccessResponse
from app.services.ai_service import AIService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    db: AsyncDB,
    user: UserAIAccess,
    _: None = Depends(ai_rate_limit)
):
    """
    Send a message to the AI assistant.
    Creates a new conversation if conversation_id is not provided.
    """
    ai_service = AIService(db)

    # Get or create conversation
    if request.conversation_id:
        result = await db.execute(
            select(AIConversation).where(
                AIConversation.conversation_id == request.conversation_id,
                AIConversation.user_id == user.user_id
            )
        )
        conversation = result.scalar_one_or_none()

        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")
    else:
        # Create new conversation
        conversation = AIConversation(
            user_id=user.user_id,
            title=request.message[:50] + "..." if len(request.message) > 50 else request.message,
            context=json.dumps(request.context) if request.context else None
        )
        db.add(conversation)
        await db.flush()

    # Get user context for AI - admins/super users have full access
    customer_ids = None
    if user.get_role_level() < 50:
        # Fetch customer IDs explicitly to avoid lazy loading
        from app.models.user import UserCustomerAccess
        result = await db.execute(
            select(UserCustomerAccess.customer_id).where(
                UserCustomerAccess.user_id == user.user_id
            )
        )
        customer_ids = [row[0] for row in result.fetchall()]

    # Save user message
    user_message = AIMessage(
        conversation_id=conversation.conversation_id,
        role="user",
        content=request.message
    )
    db.add(user_message)

    # Get conversation history
    result = await db.execute(
        select(AIMessage).where(
            AIMessage.conversation_id == conversation.conversation_id
        ).order_by(AIMessage.created_at.desc()).limit(10)
    )
    history = list(reversed(result.scalars().all()))

    # Generate AI response
    response = await ai_service.generate_response(
        user_message=request.message,
        conversation_history=history,
        user_context={
            "user_id": user.user_id,
            "role": user.get_role_name(),
            "customer_ids": customer_ids
        }
    )

    # Save assistant message
    assistant_message = AIMessage(
        conversation_id=conversation.conversation_id,
        role="assistant",
        content=response["message"],
        metadata=json.dumps(response.get("metadata", {}))
    )
    db.add(assistant_message)

    # Update conversation
    conversation.message_count += 2
    conversation.last_message_at = datetime.utcnow()

    await db.commit()
    await db.refresh(assistant_message)

    return ChatResponse(
        conversation_id=str(conversation.conversation_id),
        message=MessageResponse(
            message_id=assistant_message.message_id,
            conversation_id=str(assistant_message.conversation_id),
            role=assistant_message.role,
            content=assistant_message.content,
            metadata=json.loads(assistant_message.metadata) if assistant_message.metadata else None,
            created_at=assistant_message.created_at
        ),
        data=response.get("data"),
        chart_config=response.get("chart_config"),
        suggestions=response.get("suggestions"),
        sources=response.get("sources")
    )


@router.get("/conversations", response_model=PaginatedResponse[ConversationSummary])
async def list_conversations(
    db: AsyncDB,
    user: UserAIAccess,
    pagination: Pagination,
    is_archived: bool = False
):
    """List user's AI conversations"""
    query = select(AIConversation).where(
        AIConversation.user_id == user.user_id,
        AIConversation.is_archived == is_archived
    )

    # Count
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Paginate
    query = query.offset(pagination.offset).limit(pagination.page_size)
    query = query.order_by(AIConversation.last_message_at.desc().nullsfirst())

    result = await db.execute(query)
    conversations = result.scalars().all()

    summaries = [
        ConversationSummary(
            conversation_id=str(c.conversation_id),
            title=c.title,
            started_at=c.started_at,
            last_message_at=c.last_message_at,
            message_count=c.message_count,
            is_archived=c.is_archived
        )
        for c in conversations
    ]

    return PaginatedResponse.create(
        items=summaries,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.get("/conversations/{conversation_id}", response_model=ConversationWithMessages)
async def get_conversation(
    conversation_id: str,
    db: AsyncDB,
    user: UserAIAccess
):
    """Get conversation with messages"""
    result = await db.execute(
        select(AIConversation).where(
            AIConversation.conversation_id == conversation_id,
            AIConversation.user_id == user.user_id
        )
    )
    conversation = result.scalar_one_or_none()

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Get messages
    result = await db.execute(
        select(AIMessage).where(
            AIMessage.conversation_id == conversation_id
        ).order_by(AIMessage.created_at)
    )
    messages = result.scalars().all()

    message_responses = [
        MessageResponse(
            message_id=m.message_id,
            conversation_id=str(m.conversation_id),
            role=m.role,
            content=m.content,
            metadata=json.loads(m.metadata) if m.metadata else None,
            created_at=m.created_at
        )
        for m in messages
    ]

    return ConversationWithMessages(
        conversation_id=str(conversation.conversation_id),
        user_id=conversation.user_id,
        title=conversation.title,
        context=json.loads(conversation.context) if conversation.context else None,
        started_at=conversation.started_at,
        last_message_at=conversation.last_message_at,
        message_count=conversation.message_count,
        is_archived=conversation.is_archived,
        messages=message_responses
    )


@router.delete("/conversations/{conversation_id}", response_model=SuccessResponse)
async def delete_conversation(
    conversation_id: str,
    db: AsyncDB,
    user: UserAIAccess
):
    """Delete a conversation"""
    result = await db.execute(
        select(AIConversation).where(
            AIConversation.conversation_id == conversation_id,
            AIConversation.user_id == user.user_id
        )
    )
    conversation = result.scalar_one_or_none()

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    await db.delete(conversation)
    await db.commit()

    return SuccessResponse(message="Conversation deleted")


@router.post("/sql/generate", response_model=SQLGenerationResponse)
async def generate_sql(
    request: SQLGenerationRequest,
    db: AsyncDB,
    user: UserAIAccess,
    _: None = Depends(ai_rate_limit)
):
    """
    Generate SQL query from natural language.
    Optionally executes the query if safe.
    """
    ai_service = AIService(db)

    # Get user context - admins/super users have full access
    customer_ids = None
    if user.get_role_level() < 50:
        from app.models.user import UserCustomerAccess
        result = await db.execute(
            select(UserCustomerAccess.customer_id).where(
                UserCustomerAccess.user_id == user.user_id
            )
        )
        customer_ids = [row[0] for row in result.fetchall()]

    # Generate SQL
    sql_result = await ai_service.generate_sql(
        user_query=request.query,
        user_context={
            "user_id": user.user_id,
            "role": user.get_role_name(),
            "customer_ids": customer_ids
        }
    )

    # Log to audit
    audit = AISQLAudit(
        user_id=user.user_id,
        user_query=request.query,
        generated_sql=sql_result["sql"],
        was_safe=sql_result["is_safe"],
        safety_notes=sql_result.get("safety_notes")
    )

    # Execute if requested and safe
    query_results = None
    row_count = None
    execution_time = None

    if request.execute and sql_result["is_safe"]:
        start = datetime.utcnow()
        query_results, row_count = await ai_service.execute_safe_query(
            sql_result["sql"],
            customer_ids
        )
        execution_time = int((datetime.utcnow() - start).total_seconds() * 1000)

        audit.was_executed = True
        audit.row_count = row_count
        audit.execution_time_ms = execution_time

    db.add(audit)
    await db.commit()

    return SQLGenerationResponse(
        user_query=request.query,
        generated_sql=sql_result["sql"],
        explanation=sql_result["explanation"],
        is_safe=sql_result["is_safe"],
        safety_notes=sql_result.get("safety_notes"),
        results=query_results,
        row_count=row_count,
        execution_time_ms=execution_time
    )


@router.post("/insights", response_model=InsightResponse)
async def get_insights(
    request: InsightRequest,
    db: AsyncDB,
    user: UserAIAccess,
    _: None = Depends(ai_rate_limit)
):
    """Generate AI-powered insights for a dataset"""
    ai_service = AIService(db)

    # Get user context - admins/super users have full access
    customer_ids = None
    if user.get_role_level() < 50:
        from app.models.user import UserCustomerAccess
        result = await db.execute(
            select(UserCustomerAccess.customer_id).where(
                UserCustomerAccess.user_id == user.user_id
            )
        )
        customer_ids = [row[0] for row in result.fetchall()]

    insights_result = await ai_service.generate_insights(
        dataset=request.dataset,
        metric=request.metric,
        filters=request.filters,
        insight_type=request.insight_type,
        customer_ids=customer_ids
    )

    return InsightResponse(
        insight_type=request.insight_type,
        title=insights_result["title"],
        summary=insights_result["summary"],
        details=insights_result.get("details"),
        data=insights_result.get("data"),
        confidence=insights_result.get("confidence"),
        recommendations=insights_result.get("recommendations")
    )


@router.post("/suggest/dashboard", response_model=DashboardSuggestionResponse)
async def suggest_dashboard(
    request: DashboardSuggestionRequest,
    db: AsyncDB,
    user: UserAIAccess
):
    """Get AI suggestions for dashboard widgets"""
    ai_service = AIService(db)

    result = await ai_service.suggest_dashboard_widgets(
        dataset=request.dataset,
        description=request.description
    )

    return DashboardSuggestionResponse(
        suggestions=result["suggestions"],
        explanation=result["explanation"]
    )


@router.post("/suggest/report", response_model=ReportSuggestionResponse)
async def suggest_report(
    request: ReportSuggestionRequest,
    db: AsyncDB,
    user: UserAIAccess
):
    """Get AI suggestions for report configuration"""
    ai_service = AIService(db)

    result = await ai_service.suggest_report_config(
        purpose=request.purpose,
        dataset=request.dataset
    )

    return ReportSuggestionResponse(
        suggestions=result["suggestions"],
        explanation=result["explanation"]
    )


@router.post("/search", response_model=VectorSearchResponse)
async def semantic_search(
    request: VectorSearchRequest,
    db: AsyncDB,
    user: UserAIAccess
):
    """Perform semantic search across fleet data"""
    ai_service = AIService(db)

    results = await ai_service.vector_search(
        query=request.query,
        entity_types=request.entity_types,
        limit=request.limit
    )

    return VectorSearchResponse(
        query=request.query,
        results=results,
        total_results=len(results)
    )


# Knowledge Base endpoints (admin only)
@router.get("/knowledge", response_model=List[KnowledgeBaseResponse])
async def list_knowledge_base(
    db: AsyncDB,
    user: UserAIAccess,
    category: Optional[str] = None,
    is_active: bool = True
):
    """List knowledge base entries"""
    query = select(KnowledgeBase).where(KnowledgeBase.is_active == is_active)

    if category:
        query = query.where(KnowledgeBase.category == category)

    query = query.order_by(KnowledgeBase.category, KnowledgeBase.question)

    result = await db.execute(query)
    entries = result.scalars().all()

    return [
        KnowledgeBaseResponse(
            kb_id=e.kb_id,
            category=e.category,
            question=e.question,
            answer=e.answer,
            keywords=e.keywords,
            is_active=e.is_active,
            created_at=e.created_at,
            updated_at=e.updated_at
        )
        for e in entries
    ]
