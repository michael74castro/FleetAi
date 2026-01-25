"""
FleetAI - AI Models
SQLAlchemy models for AI conversations and embeddings
"""

from datetime import datetime
from typing import Optional, List
import uuid

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Text, BigInteger
)
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.database import Base
from app.core.config import settings

# Use UNIQUEIDENTIFIER for MSSQL, String(36) for SQLite
if settings.DATABASE_TYPE == 'sqlite':
    GUID_TYPE = String(36)
else:
    from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
    GUID_TYPE = UNIQUEIDENTIFIER


# Use schema only for MSSQL, not SQLite
def get_app_table_args():
    if settings.DATABASE_TYPE == 'sqlite':
        return {}
    return {'schema': 'app'}


class AIConversation(Base):
    """AI conversation sessions"""
    __tablename__ = 'ai_conversations'
    __table_args__ = get_app_table_args()

    conversation_id: Mapped[str] = mapped_column(
        GUID_TYPE,
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey('users.user_id'),
        nullable=False
    )
    title: Mapped[Optional[str]] = mapped_column(String(200))
    context: Mapped[Optional[str]] = mapped_column(Text)  # JSON context
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="conversations")
    messages: Mapped[List["AIMessage"]] = relationship(
        "AIMessage",
        back_populates="conversation",
        cascade="all, delete-orphan",
        order_by="AIMessage.created_at"
    )
    sql_audits: Mapped[List["AISQLAudit"]] = relationship(
        "AISQLAudit",
        back_populates="conversation"
    )

    def __repr__(self):
        return f"<AIConversation(user_id={self.user_id}, title={self.title})>"


class AIMessage(Base):
    """Individual messages in AI conversations"""
    __tablename__ = 'ai_messages'
    __table_args__ = get_app_table_args()

    message_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[str] = mapped_column(
        GUID_TYPE,
        ForeignKey('ai_conversations.conversation_id', ondelete='CASCADE'),
        nullable=False
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # user, assistant, system
    content: Mapped[str] = mapped_column(Text, nullable=False)
    message_metadata: Mapped[Optional[str]] = mapped_column(Text)  # JSON: tokens, model, sql_query, etc.
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    conversation: Mapped["AIConversation"] = relationship(
        "AIConversation",
        back_populates="messages"
    )

    def __repr__(self):
        return f"<AIMessage(role={self.role}, conv_id={self.conversation_id})>"


class AISQLAudit(Base):
    """Audit log for AI-generated SQL queries"""
    __tablename__ = 'ai_sql_audit'
    __table_args__ = get_app_table_args()

    audit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[Optional[str]] = mapped_column(
        GUID_TYPE,
        ForeignKey('ai_conversations.conversation_id')
    )
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey('users.user_id'),
        nullable=False
    )
    user_query: Mapped[Optional[str]] = mapped_column(Text)
    generated_sql: Mapped[Optional[str]] = mapped_column(Text)
    was_executed: Mapped[bool] = mapped_column(Boolean, default=False)
    row_count: Mapped[Optional[int]] = mapped_column(Integer)
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    was_safe: Mapped[Optional[bool]] = mapped_column(Boolean)
    safety_notes: Mapped[Optional[str]] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    conversation: Mapped[Optional["AIConversation"]] = relationship(
        "AIConversation",
        back_populates="sql_audits"
    )
    user: Mapped["User"] = relationship("User")

    def __repr__(self):
        return f"<AISQLAudit(user_id={self.user_id}, was_executed={self.was_executed})>"


class VectorEmbedding(Base):
    """Vector embeddings for semantic search"""
    __tablename__ = 'vector_embeddings'
    __table_args__ = get_app_table_args()

    embedding_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(50), nullable=False)  # vehicle, customer, document
    entity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    content_text: Mapped[Optional[str]] = mapped_column(Text)
    embedding_model: Mapped[Optional[str]] = mapped_column(String(100))
    embedding_json: Mapped[Optional[str]] = mapped_column(Text)  # JSON array of floats (fallback)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    def __repr__(self):
        return f"<VectorEmbedding(type={self.entity_type}, entity_id={self.entity_id})>"


class KnowledgeBase(Base):
    """FAQ/Knowledge base for AI context"""
    __tablename__ = 'knowledge_base'
    __table_args__ = get_app_table_args()

    kb_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[Optional[str]] = mapped_column(String(100))
    question: Mapped[Optional[str]] = mapped_column(String(1000))
    answer: Mapped[Optional[str]] = mapped_column(Text)
    keywords: Mapped[Optional[str]] = mapped_column(String(500))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    def __repr__(self):
        return f"<KnowledgeBase(category={self.category}, q={self.question[:50] if self.question else None})>"


# Forward references
from app.models.user import User  # noqa: E402
