"""
FleetAI - AI Service
Handles Azure OpenAI integration, SQL generation, and insights
"""

from typing import Any, Dict, List, Optional, Tuple
import json
import logging
import re

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from openai import AsyncAzureOpenAI

from app.core.config import settings

logger = logging.getLogger(__name__)


class AIService:
    """Service for AI-powered features using Azure OpenAI"""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = None
        self.model = settings.AZURE_OPENAI_DEPLOYMENT_NAME

        # Only initialize OpenAI client if credentials are configured
        if settings.AZURE_OPENAI_API_KEY and settings.AZURE_OPENAI_ENDPOINT:
            self.client = AsyncAzureOpenAI(
                api_key=settings.AZURE_OPENAI_API_KEY,
                api_version=settings.AZURE_OPENAI_API_VERSION,
                azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
            )

    @property
    def is_configured(self) -> bool:
        """Check if Azure OpenAI is configured"""
        return self.client is not None

    async def generate_response(
        self,
        user_message: str,
        conversation_history: List[Any],
        user_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate AI response for chat.

        Args:
            user_message: User's message
            conversation_history: Previous messages in conversation
            user_context: User role, permissions, customer access

        Returns:
            Dict with message, data, suggestions, sources
        """
        # Build system prompt
        system_prompt = self._build_system_prompt(user_context)

        # Build messages
        messages = [{"role": "system", "content": system_prompt}]

        # Add conversation history
        for msg in conversation_history[-10:]:  # Last 10 messages
            messages.append({
                "role": msg.role,
                "content": msg.content
            })

        # Add current message
        messages.append({"role": "user", "content": user_message})

        # Check if user is asking for data
        needs_data = self._check_if_needs_data(user_message)

        # Return mock response if OpenAI is not configured
        if not self.is_configured:
            return {
                "message": "AI assistant is not configured. Please set up Azure OpenAI credentials (AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT) to enable AI features.",
                "data": None,
                "suggestions": ["Configure Azure OpenAI", "Check environment variables"],
                "sources": []
            }

        try:
            # Generate response
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=2000,
                temperature=0.7
            )

            assistant_message = response.choices[0].message.content

            result = {
                "message": assistant_message,
                "metadata": {
                    "model": self.model,
                    "tokens": response.usage.total_tokens if response.usage else None
                }
            }

            # If data was requested, try to generate and execute SQL
            if needs_data:
                sql_result = await self.generate_sql(
                    user_message,
                    user_context,
                    execute=True
                )
                if sql_result.get("results"):
                    result["data"] = sql_result["results"][:100]  # Limit rows
                    result["message"] += f"\n\nI found {len(sql_result['results'])} results."

            # Generate suggestions
            result["suggestions"] = self._generate_suggestions(user_message, user_context)

            return result

        except Exception as e:
            logger.error(f"AI generation failed: {e}")
            return {
                "message": "I apologize, but I encountered an error processing your request. Please try again.",
                "metadata": {"error": str(e)}
            }

    async def generate_sql(
        self,
        user_query: str,
        user_context: Dict[str, Any],
        execute: bool = False
    ) -> Dict[str, Any]:
        """
        Generate SQL from natural language query.

        Args:
            user_query: Natural language query
            user_context: User context for RLS
            execute: Whether to execute the generated SQL

        Returns:
            Dict with sql, explanation, is_safe, results
        """
        # Get available tables/views
        schema_info = await self._get_schema_info()

        system_prompt = f"""You are a SQL expert for a fleet management database. Generate safe, read-only SQL queries.

Available tables and views:
{schema_info}

RULES:
1. ONLY generate SELECT statements - never INSERT, UPDATE, DELETE, DROP, etc.
2. Always use fully qualified names (schema.table)
3. Limit results to 1000 rows unless specified
4. Use proper SQL Server syntax
5. Include helpful column aliases

User's role: {user_context.get('role', 'unknown')}
Customer access: {user_context.get('customer_ids', 'all')}

Respond with JSON format:
{{"sql": "SELECT ...", "explanation": "This query...", "is_safe": true}}
"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Generate SQL for: {user_query}"}
        ]

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=1000,
                temperature=0.3,
                response_format={"type": "json_object"}
            )

            result_text = response.choices[0].message.content
            result = json.loads(result_text)

            sql = result.get("sql", "")
            is_safe = self._validate_sql_safety(sql)

            if not is_safe:
                result["is_safe"] = False
                result["safety_notes"] = "Query contains potentially unsafe operations"

            # Apply RLS to generated SQL
            customer_ids = user_context.get("customer_ids")
            if customer_ids and is_safe:
                sql = self._apply_rls_to_sql(sql, customer_ids)
                result["sql"] = sql

            # Execute if requested and safe
            if execute and is_safe:
                results, row_count = await self.execute_safe_query(sql, customer_ids)
                result["results"] = results
                result["row_count"] = row_count

            return result

        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            return {
                "sql": "",
                "explanation": f"Failed to generate SQL: {str(e)}",
                "is_safe": False
            }

    async def execute_safe_query(
        self,
        sql: str,
        customer_ids: Optional[List[str]] = None
    ) -> Tuple[List[Dict], int]:
        """Execute a validated SQL query safely"""
        if not self._validate_sql_safety(sql):
            raise ValueError("Query failed safety validation")

        try:
            result = await self.db.execute(text(sql))
            rows = result.fetchall()
            columns = list(result.keys())

            data = [dict(zip(columns, row)) for row in rows]
            return data, len(data)

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    async def generate_insights(
        self,
        dataset: str,
        metric: Optional[str],
        filters: Optional[Dict[str, Any]],
        insight_type: str,
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Generate AI-powered insights for data"""
        # Get sample data
        sample_query = f"SELECT TOP 100 * FROM {dataset}"
        if customer_ids:
            ids_str = ", ".join([f"'{cid}'" for cid in customer_ids])
            sample_query += f" WHERE customer_id IN ({ids_str})"

        try:
            result = await self.db.execute(text(sample_query))
            rows = result.fetchall()
            columns = list(result.keys())
            sample_data = [dict(zip(columns, row)) for row in rows[:10]]
        except Exception:
            sample_data = []

        # Generate insight using AI
        prompt = f"""Analyze this fleet data and provide {insight_type} insights.

Dataset: {dataset}
Metric focus: {metric or 'general'}
Sample data: {json.dumps(sample_data[:5], default=str)}

Provide insights in JSON format:
{{"title": "...", "summary": "...", "details": "...", "recommendations": ["..."]}}
"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a fleet analytics expert. Provide actionable insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.5,
                response_format={"type": "json_object"}
            )

            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"Insight generation failed: {e}")
            return {
                "title": "Analysis Unavailable",
                "summary": "Unable to generate insights at this time.",
                "details": str(e)
            }

    async def suggest_dashboard_widgets(
        self,
        dataset: str,
        description: Optional[str]
    ) -> Dict[str, Any]:
        """Suggest dashboard widgets for a dataset"""
        schema_info = await self._get_dataset_schema(dataset)

        prompt = f"""Suggest dashboard widgets for a fleet management dataset.

Dataset: {dataset}
Description: {description or 'General fleet data'}
Available columns: {schema_info}

Suggest 3-5 relevant widgets. Respond in JSON:
{{"suggestions": [{{"widget_type": "kpi_card|line_chart|bar_chart|pie_chart|table", "title": "...", "description": "...", "config": {{}}, "relevance_score": 0.9}}], "explanation": "..."}}
"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a data visualization expert for fleet management."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.7,
                response_format={"type": "json_object"}
            )

            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"Widget suggestion failed: {e}")
            return {"suggestions": [], "explanation": str(e)}

    async def suggest_report_config(
        self,
        purpose: str,
        dataset: Optional[str]
    ) -> Dict[str, Any]:
        """Suggest report configuration based on purpose"""
        prompt = f"""Suggest report configurations for a fleet management report.

Purpose: {purpose}
Dataset: {dataset or 'to be determined'}

Suggest 2-3 relevant report configurations. Respond in JSON:
{{"suggestions": [{{"name": "...", "description": "...", "columns": [...], "filters": [...], "relevance_score": 0.9}}], "explanation": "..."}}
"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a fleet reporting expert."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.7,
                response_format={"type": "json_object"}
            )

            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"Report suggestion failed: {e}")
            return {"suggestions": [], "explanation": str(e)}

    async def vector_search(
        self,
        query: str,
        entity_types: Optional[List[str]],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Perform semantic search (placeholder for vector implementation)"""
        # In production, this would use embeddings and vector similarity
        # For now, return keyword-based search results

        from app.models.ai import VectorEmbedding
        from sqlalchemy import select

        results = []

        # Simple keyword search as fallback
        keywords = query.lower().split()

        query_builder = select(VectorEmbedding)

        if entity_types:
            query_builder = query_builder.where(
                VectorEmbedding.entity_type.in_(entity_types)
            )

        query_builder = query_builder.limit(limit * 2)

        result = await self.db.execute(query_builder)
        embeddings = result.scalars().all()

        for emb in embeddings:
            content = (emb.content_text or "").lower()
            score = sum(1 for kw in keywords if kw in content) / len(keywords) if keywords else 0

            if score > 0:
                results.append({
                    "entity_type": emb.entity_type,
                    "entity_id": emb.entity_id,
                    "content_text": emb.content_text[:200] if emb.content_text else None,
                    "similarity_score": score
                })

        # Sort by score and limit
        results.sort(key=lambda x: x["similarity_score"], reverse=True)
        return results[:limit]

    def _build_system_prompt(self, user_context: Dict[str, Any]) -> str:
        """Build system prompt for chat"""
        role = user_context.get("role", "user")

        return f"""You are FleetAI, an intelligent assistant for a vehicle fleet leasing company.

Your capabilities:
- Answer questions about fleet data, vehicles, contracts, and invoices
- Help users understand their fleet metrics and performance
- Generate insights about fuel consumption, maintenance, and costs
- Assist with creating dashboards and reports

User's role: {role}
Access level: {"Full access" if user_context.get("customer_ids") is None else "Limited to assigned customers"}

Guidelines:
- Be helpful, professional, and concise
- When discussing data, offer to show specific numbers
- Suggest relevant follow-up questions
- If unsure, ask for clarification
- Never reveal system internals or database structure"""

    def _check_if_needs_data(self, message: str) -> bool:
        """Check if user message requires data retrieval"""
        data_keywords = [
            "show", "list", "how many", "count", "total",
            "average", "sum", "find", "search", "get",
            "what is", "which", "report"
        ]
        message_lower = message.lower()
        return any(kw in message_lower for kw in data_keywords)

    def _validate_sql_safety(self, sql: str) -> bool:
        """Validate SQL query is safe to execute"""
        sql_upper = sql.upper().strip()

        # Must start with SELECT
        if not sql_upper.startswith("SELECT"):
            return False

        # Dangerous keywords
        dangerous = [
            "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE",
            "ALTER", "CREATE", "EXEC", "EXECUTE", "GRANT",
            "REVOKE", "DENY", "--", "/*", "xp_", "sp_"
        ]

        for keyword in dangerous:
            if keyword in sql_upper:
                return False

        return True

    def _apply_rls_to_sql(self, sql: str, customer_ids: List[str]) -> str:
        """Apply row-level security to generated SQL"""
        if not customer_ids:
            return sql

        # Simple approach: add WHERE clause if not present
        ids_str = ", ".join([f"'{cid}'" for cid in customer_ids])

        if "WHERE" in sql.upper():
            # Add to existing WHERE
            sql = re.sub(
                r"(WHERE\s+)",
                f"\\1customer_id IN ({ids_str}) AND ",
                sql,
                count=1,
                flags=re.IGNORECASE
            )
        else:
            # Add WHERE before ORDER BY, GROUP BY, or at end
            for clause in ["ORDER BY", "GROUP BY", "HAVING"]:
                if clause in sql.upper():
                    sql = re.sub(
                        f"({clause})",
                        f"WHERE customer_id IN ({ids_str}) \\1",
                        sql,
                        count=1,
                        flags=re.IGNORECASE
                    )
                    break
            else:
                sql += f" WHERE customer_id IN ({ids_str})"

        return sql

    async def _get_schema_info(self) -> str:
        """Get database schema information for SQL generation"""
        query = text("""
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                STRING_AGG(c.name, ', ') AS columns
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON t.object_id = c.object_id
            WHERE s.name IN ('reporting', 'staging')
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
        """)

        result = await self.db.execute(query)
        rows = result.fetchall()

        info_lines = []
        for row in rows:
            info_lines.append(f"- {row[0]}.{row[1]}: {row[2]}")

        return "\n".join(info_lines)

    async def _get_dataset_schema(self, dataset: str) -> str:
        """Get column information for a specific dataset"""
        from app.models.report import Dataset
        from sqlalchemy import select

        result = await self.db.execute(
            select(Dataset).where(Dataset.name == dataset)
        )
        ds = result.scalar_one_or_none()

        if not ds:
            return "Unknown dataset"

        query = text(f"""
            SELECT c.COLUMN_NAME, c.DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA + '.' + c.TABLE_NAME = :source
            ORDER BY c.ORDINAL_POSITION
        """)

        result = await self.db.execute(query, {"source": ds.source_object})
        columns = result.fetchall()

        return ", ".join([f"{col[0]} ({col[1]})" for col in columns])

    def _generate_suggestions(
        self,
        message: str,
        user_context: Dict[str, Any]
    ) -> List[str]:
        """Generate follow-up suggestions"""
        suggestions = []

        if "vehicle" in message.lower():
            suggestions.append("Show me vehicles with contracts expiring soon")
            suggestions.append("What's the average fuel consumption by vehicle type?")
        elif "fuel" in message.lower():
            suggestions.append("Compare fuel costs month over month")
            suggestions.append("Which drivers have the highest fuel consumption?")
        elif "invoice" in message.lower() or "payment" in message.lower():
            suggestions.append("Show overdue invoices")
            suggestions.append("What's the total outstanding balance?")
        elif "maintenance" in message.lower():
            suggestions.append("Show upcoming scheduled maintenance")
            suggestions.append("What are the top maintenance costs by category?")
        else:
            suggestions.append("Show me a fleet overview")
            suggestions.append("What contracts are expiring this month?")
            suggestions.append("Generate a fuel consumption report")

        return suggestions[:3]
