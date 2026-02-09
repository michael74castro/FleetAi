"""
FleetAI - AI Service
Handles OpenAI / Azure OpenAI integration, SQL generation, and insights
"""

from typing import Any, Dict, List, Optional, Tuple
import json
import logging
import re
import time

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from openai import AsyncAzureOpenAI, AsyncOpenAI

from app.core.config import settings

logger = logging.getLogger(__name__)


class AIService:
    """Service for AI-powered features using OpenAI or Azure OpenAI"""

    # Class-level cache shared across all per-request instances
    _domain_cache: Dict[str, Any] = {}
    _cache_loaded_at: Optional[float] = None
    _CACHE_TTL_SECONDS: int = 3600  # 1 hour

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = None
        self.model = settings.AZURE_OPENAI_DEPLOYMENT_NAME

        # Try Azure OpenAI first, then regular OpenAI
        if settings.AZURE_OPENAI_API_KEY and settings.AZURE_OPENAI_ENDPOINT:
            self.client = AsyncAzureOpenAI(
                api_key=settings.AZURE_OPENAI_API_KEY,
                api_version=settings.AZURE_OPENAI_API_VERSION,
                azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
            )
            logger.info("AI Service: Using Azure OpenAI")
        elif settings.OPENAI_API_KEY:
            self.client = AsyncOpenAI(
                api_key=settings.OPENAI_API_KEY
            )
            self.model = settings.OPENAI_MODEL
            logger.info(f"AI Service: Using OpenAI ({self.model})")

    @property
    def is_configured(self) -> bool:
        """Check if an OpenAI provider is configured"""
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
        # Ensure domain knowledge is cached before building prompts
        await self._load_domain_knowledge(self.db)

        # Pre-check: Registration expiry queries (not available in database)
        query_lower = user_message.lower()
        registration_keywords = ['registration expir', 'license expir', 'licence expir',
                                  'plate expir', 'registration renewal', 'license renewal',
                                  'licence renewal', 'registration is expiring', 'plates expiring']
        if any(kw in query_lower for kw in registration_keywords):
            return {
                "message": (
                    "I noticed you're asking about vehicle registration expiry dates. "
                    "Unfortunately, vehicle registration renewal dates (government license plate renewal) "
                    "are not tracked in this system.\n\n"
                    "However, I can help you with **contract/lease expiration data**:\n"
                    "- 'Show me vehicles with contracts expiring next month'\n"
                    "- 'Which vehicles are ending their lease in the next 3 months?'\n"
                    "- 'List vehicles where months_remaining is less than 3'\n\n"
                    "Would you like me to show contract expiration data instead?"
                ),
                "data": None,
                "suggestions": [
                    "Show vehicles with contracts expiring next month",
                    "List vehicles ending lease in next 3 months",
                    "How many vehicles have contracts expiring this year?"
                ],
                "sources": None
            }

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
                "message": "AI assistant is not configured. Set OPENAI_API_KEY in backend/.env for regular OpenAI, or AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT for Azure OpenAI.",
                "data": None,
                "suggestions": ["Configure OpenAI API key", "Check environment variables"],
                "sources": []
            }

        try:
            # If data was requested, try to fetch data FIRST so we can include
            # actual results in the AI prompt (prevents hallucinated numbers)
            query_data = None
            chart_config = None

            if needs_data:
                # Check for common aggregation patterns and handle directly
                import re
                aggregation_result = await self._handle_aggregation_query(user_message.lower())
                if aggregation_result:
                    result = {
                        "message": aggregation_result["message"],
                        "data": aggregation_result["data"],
                        "metadata": {"model": self.model, "tokens": 0}
                    }
                    chart_config = self._detect_chart_config(result["data"], user_message)
                    if chart_config:
                        result["chart_config"] = chart_config
                    result["suggestions"] = self._generate_suggestions(user_message, user_context)
                    return result

                # Check for fleet-wide maintenance insights
                maintenance_insights_result = await self._handle_maintenance_insights_query(user_message.lower())
                if maintenance_insights_result:
                    result = {
                        "message": maintenance_insights_result["message"],
                        "data": maintenance_insights_result["data"],
                        "metadata": {"model": self.model, "tokens": 0}
                    }
                    chart_config = self._detect_chart_config(result["data"], user_message)
                    if chart_config:
                        result["chart_config"] = chart_config
                    result["suggestions"] = self._generate_suggestions(user_message, user_context)
                    return result

                # Check for service cost/invoice queries (maintenance, tyres, etc.)
                service_result = await self._handle_service_cost_query(user_message.lower())
                if service_result:
                    result = {
                        "message": service_result["message"],
                        "data": service_result["data"],
                        "metadata": {"model": self.model, "tokens": 0}
                    }
                    chart_config = self._detect_chart_config(result["data"], user_message)
                    if chart_config:
                        result["chart_config"] = chart_config
                    result["suggestions"] = self._generate_suggestions(user_message, user_context)
                    return result

                # Check for book value queries
                book_value_result = await self._handle_book_value_query(user_message.lower())
                if book_value_result:
                    result = {
                        "message": book_value_result["message"],
                        "data": book_value_result["data"],
                        "metadata": {"model": self.model, "tokens": 0}
                    }
                    chart_config = self._detect_chart_config(result["data"], user_message)
                    if chart_config:
                        result["chart_config"] = chart_config
                    result["suggestions"] = self._generate_suggestions(user_message, user_context)
                    return result

                # Check for contract expiry queries
                expiry_result = await self._handle_contract_expiry_query(user_message.lower())
                if expiry_result:
                    result = {
                        "message": expiry_result["message"],
                        "data": expiry_result["data"],
                        "metadata": {"model": self.model, "tokens": 0}
                    }
                    chart_config = self._detect_chart_config(result["data"], user_message)
                    if chart_config:
                        result["chart_config"] = chart_config
                    result["suggestions"] = self._generate_suggestions(user_message, user_context)
                    return result

                # General SQL path: generate and execute SQL to get data first
                conversation_context = "\n".join([
                    f"{msg.role}: {msg.content}"
                    for msg in conversation_history[-6:]
                ])

                sql_result = await self.generate_sql(
                    user_message,
                    user_context,
                    execute=True,
                    conversation_context=conversation_context
                )
                if sql_result.get("results"):
                    query_data = sql_result["results"][:100]

            # Now make a SINGLE AI call, including actual data if we have it
            if query_data is not None:
                data_preview = json.dumps(query_data[:20], default=str)
                messages.append({
                    "role": "system",
                    "content": (
                        f"IMPORTANT: A database query has already been executed for this question. "
                        f"The query returned {len(query_data)} row(s). Here are the actual results:\n"
                        f"{data_preview}\n\n"
                        f"You MUST use ONLY these actual numbers in your response. "
                        f"Do NOT invent, estimate, or guess any numbers. "
                        f"Cite the exact values from the data above."
                    )
                })
            elif needs_data:
                # Data was needed but query failed — tell the AI not to fabricate
                messages.append({
                    "role": "system",
                    "content": (
                        "IMPORTANT: The database query for this question failed or returned no results. "
                        "Do NOT fabricate or guess any numbers, names, or data. "
                        "Instead, let the user know you were unable to retrieve the data and suggest "
                        "they rephrase their question or try a simpler query."
                    )
                })

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=2000,
                temperature=0.2
            )

            assistant_message = response.choices[0].message.content

            result = {
                "message": assistant_message,
                "metadata": {
                    "model": self.model,
                    "tokens": response.usage.total_tokens if response.usage else None
                }
            }

            if query_data is not None:
                result["data"] = query_data
                chart_config = self._detect_chart_config(query_data, user_message)
                if chart_config:
                    result["chart_config"] = chart_config

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
        execute: bool = False,
        conversation_context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate SQL from natural language query.

        Args:
            user_query: Natural language query
            user_context: User context for RLS
            execute: Whether to execute the generated SQL
            conversation_context: Previous conversation messages for context

        Returns:
            Dict with sql, explanation, is_safe, results
        """
        # Ensure domain knowledge is cached
        await self._load_domain_knowledge(self.db)

        # Pre-check: Registration expiry queries (not available in database)
        query_lower = user_query.lower()
        registration_keywords = ['registration expir', 'license expir', 'licence expir',
                                  'plate expir', 'registration renewal', 'license renewal',
                                  'licence renewal', 'registration is expiring', 'plates expiring']
        if any(kw in query_lower for kw in registration_keywords):
            return {
                "user_query": user_query,
                "sql": None,
                "explanation": (
                    "Vehicle registration expiry dates (government license plate renewal) "
                    "are not tracked in this system. The database contains contract/lease "
                    "expiration data instead. Would you like to see vehicles with contracts "
                    "expiring soon? Try asking: 'Show me vehicles with contracts expiring next month' "
                    "or 'Which vehicles are ending their lease in the next 3 months?'"
                ),
                "is_safe": True,
                "safety_notes": None,
                "results": None,
                "row_count": None,
                "execution_time_ms": None
            }

        # Get available tables/views (structural reference)
        schema_info = await self._get_schema_info()

        # Get semantic domain block (dynamic from cache, or static fallback)
        domain_block = self._domain_cache.get("sql_prompt_domain_block") or self._get_static_fallback_sql_domain()

        # Detect aggregation patterns and directly generate SQL for common patterns
        import re
        aggregation_patterns = [
            (r'(?:show|get|list|display)?\s*(?:me\s+)?(?:the\s+)?(?:number|count)?\s*(?:of\s+)?vehicles?\s+by\s+(\w+)', 'vehicles', 'dim_vehicle'),
            (r'(?:show|get|list|display)?\s*(?:me\s+)?(?:the\s+)?vehicles?\s+(?:breakdown\s+)?by\s+(\w+)', 'vehicles', 'dim_vehicle'),
            (r'(?:show|get|list|display)?\s*(?:me\s+)?(?:the\s+)?(?:number|count)?\s*(?:of\s+)?contracts?\s+by\s+(\w+)', 'contracts', 'dim_contract'),
        ]

        # Map common dimension names to actual column names
        dim_mapping = {
            'status': 'vehicle_status',
            'make': 'make_name',
            'manufacturer': 'make_name',
            'brand': 'make_name',
            'model': 'model_name',
            'customer': 'customer_name',
            'type': 'body_type',
            'year': 'model_year',
            'fuel': 'fuel_type',
        }

        for pattern, entity, table in aggregation_patterns:
            match = re.search(pattern, query_lower, re.IGNORECASE)
            if match:
                dimension = match.group(1).lower()
                col_name = dim_mapping.get(dimension, dimension)

                # Check if user is asking for "active" vehicles
                is_active_filter = 'active' in query_lower and table == 'dim_vehicle'
                where_clause = "WHERE is_active = 1 " if is_active_filter else ""
                active_desc = "active " if is_active_filter else ""

                # Directly generate the aggregation SQL without calling the AI
                direct_sql = f"SELECT {col_name}, COUNT(*) as {entity}_count FROM {table} {where_clause}GROUP BY {col_name} ORDER BY {entity}_count DESC"

                result = {
                    "sql": direct_sql,
                    "explanation": f"This query counts {active_desc}{entity} grouped by {dimension}.",
                    "is_safe": True,
                    "results": None,
                    "row_count": None
                }

                # Execute if requested
                if execute:
                    try:
                        results, row_count = await self.execute_safe_query(direct_sql, user_context.get("customer_ids"))
                        result["results"] = results
                        result["row_count"] = row_count
                    except Exception as e:
                        logger.error(f"Direct aggregation query execution failed: {e}")

                return result

        # Intercept service cost/invoice queries (maintenance, tyres, fuel, etc.)
        service_result = await self._handle_service_cost_query(query_lower)
        if service_result:
            return {
                "sql": "(handled by service cost handler)",
                "explanation": service_result.get("message", ""),
                "is_safe": True,
                "results": service_result.get("data"),
                "row_count": len(service_result.get("data", []))
            }

        aggregation_hint = ""

        db_type = "SQLite" if settings.DATABASE_TYPE == "sqlite" else "SQL Server"
        system_prompt = f"""You are a SQL expert for a fleet management database (vehicle leasing company). Generate safe, read-only SQL queries.

Database type: {db_type}

Available tables and views (structural reference):
{schema_info}

{domain_block}

RULES:
1. ONLY generate SELECT statements - never INSERT, UPDATE, DELETE, DROP, etc.
2. {"Use plain table names (no schema prefix)" if settings.DATABASE_TYPE == "sqlite" else "Always use fully qualified names (schema.table)"}
3. Limit results to 1000 rows unless specified
4. Use proper {db_type} syntax
5. Include helpful column aliases
6. For status filtering, use the status codes and is_active flag defined above
7. For date comparisons in SQLite, use date() function e.g. date('now', '-12 months')
8. Prefer pre-built views over complex JOINs when they answer the question
9. Use is_active = 1 / is_active = 0 for active/terminated vehicle filtering
10. Follow the defined relationships for JOINs between tables
11. For contract expiration/ending/remaining questions: ALWAYS query dim_vehicle and use expected_end_date or months_remaining. NEVER use lease_end_date (stale source field that will return wrong results). NEVER query dim_contract for expiration (its date columns may be NULL). Example: "vehicles expiring in 3 months" = SELECT COUNT(*) FROM dim_vehicle WHERE months_remaining BETWEEN 0 AND 3
12. CRITICAL - "Registration expiry" vs "Contract expiry" are DIFFERENT: Vehicle registration expiry (government license plate renewal) is NOT tracked in this database. Contract/lease expiry (expected_end_date) IS available. If user asks about "registration expiring" or "license plate renewal", respond with JSON {{"sql": null, "explanation": "Vehicle registration expiry dates (government license plate renewal) are not tracked in this system. However, I can show you contract/lease expiration data. Would you like to see vehicles with contracts expiring soon instead?", "is_safe": true}}
13. CRITICAL - For "by" or "per" or "breakdown" questions (e.g., "vehicles by status", "count by make", "breakdown by customer"), ALWAYS use GROUP BY and COUNT/SUM aggregations. Example: "vehicles by status" = SELECT vehicle_status, COUNT(*) as vehicle_count FROM dim_vehicle GROUP BY vehicle_status ORDER BY vehicle_count DESC
14. CRITICAL - For visualization/chart queries, return aggregated data with clear dimension and measure columns. The result should have a categorical column (for x-axis) and numeric columns (for y-axis). Never return raw detail rows for chart queries.
15. CRITICAL - Column names: Use EXACT column names from the schema. The column for vehicle make/manufacturer is "make_name" (NOT "manufacturer" or "make"). The column for vehicle model is "model_name" (NOT "model"). Example: "vehicles by make" = SELECT make_name, COUNT(*) as vehicle_count FROM dim_vehicle WHERE is_active = 1 GROUP BY make_name ORDER BY vehicle_count DESC
16. CRITICAL - When using JOINs, ALWAYS qualify column names with table aliases to avoid ambiguous column errors. Example: SELECT c.customer_name, COUNT(*) as vehicle_count FROM dim_vehicle v JOIN dim_customer c ON v.customer_id = c.customer_id GROUP BY c.customer_name
17. CRITICAL - For maintenance cost, budget, or invoice queries: ALWAYS use fact_exploitation_services with service_code=580 for maintenance. Use total_monthly_cost for actual cost and total_monthly_invoice for budgeted/invoiced amount. NEVER use fact_car_reports for maintenance cost analysis (its maintenance columns are per-km rates, NOT AED amounts).

User's role: {user_context.get('role', 'unknown')}
Customer access: {user_context.get('customer_ids', 'all')}

{f'''CONVERSATION CONTEXT (use this to understand what the user is referring to):
{conversation_context}
''' if conversation_context else ''}
{aggregation_hint}

User's query: {user_query}

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
        if settings.DATABASE_TYPE == "sqlite":
            sample_query = f"SELECT * FROM {dataset} LIMIT 100"
        else:
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

    @classmethod
    async def _load_domain_knowledge(cls, db: AsyncSession) -> None:
        """Load domain knowledge from semantic catalog tables into class-level cache."""
        now = time.time()
        if cls._cache_loaded_at and (now - cls._cache_loaded_at) < cls._CACHE_TTL_SECONDS:
            return  # Cache is fresh

        try:
            # Load all 4 catalog tables
            table_rows = (await db.execute(
                text("SELECT table_name, display_name, description, business_domain, grain, typical_questions FROM semantic_table_catalog")
            )).fetchall()

            column_rows = (await db.execute(
                text("SELECT table_name, column_name, display_name, description, data_type, example_values, is_measure, is_key FROM semantic_column_catalog")
            )).fetchall()

            rel_rows = (await db.execute(
                text("SELECT from_table, from_column, to_table, to_column, relationship_type, description FROM semantic_relationships")
            )).fetchall()

            glossary_rows = (await db.execute(
                text("SELECT term, definition, synonyms, related_tables, calculation_formula FROM semantic_business_glossary")
            )).fetchall()

            if not table_rows:
                logger.warning("Semantic catalog tables are empty — using static fallback")
                cls._domain_cache = {}
                cls._cache_loaded_at = now
                return

            cls._domain_cache["sql_prompt_domain_block"] = cls._format_sql_domain_block(
                table_rows, column_rows, rel_rows
            )
            cls._domain_cache["chat_prompt_domain_block"] = cls._format_chat_domain_block(
                glossary_rows, table_rows
            )
            cls._cache_loaded_at = now

            logger.info(
                f"Domain knowledge loaded: {len(table_rows)} tables, "
                f"{len(column_rows)} columns, {len(rel_rows)} relationships, "
                f"{len(glossary_rows)} glossary terms"
            )
        except Exception as e:
            logger.error(f"Failed to load domain knowledge: {e}")
            # Leave cache empty; callers will use static fallback

    @staticmethod
    def _format_sql_domain_block(table_rows, column_rows, rel_rows) -> str:
        """Build a compact prompt block for SQL generation from catalog data."""
        lines: List[str] = []

        # --- Table catalog ---
        lines.append("=== TABLE CATALOG ===")
        for r in table_rows:
            table_name, display_name, description, domain, grain, _ = r
            lines.append(f"- {table_name} ({display_name}): {description}  [grain: {grain}]")

        # --- Key columns grouped by table ---
        lines.append("\n=== KEY COLUMNS ===")
        cols_by_table: Dict[str, list] = {}
        for r in column_rows:
            tbl = r[0]
            cols_by_table.setdefault(tbl, []).append(r)
        for tbl, cols in cols_by_table.items():
            col_parts = []
            for _, col_name, display_name, desc, dtype, examples, is_measure, is_key in cols:
                flags = []
                if is_key:
                    flags.append("KEY")
                if is_measure:
                    flags.append("MEASURE")
                flag_str = f" [{','.join(flags)}]" if flags else ""
                ex_str = f" (e.g. {examples})" if examples else ""
                col_parts.append(f"  {col_name}{flag_str}: {desc}{ex_str}")
            lines.append(f"{tbl}:")
            lines.extend(col_parts)

        # --- Relationships ---
        lines.append("\n=== RELATIONSHIPS (JOIN guidance) ===")
        for r in rel_rows:
            from_tbl, from_col, to_tbl, to_col, rel_type, desc = r
            lines.append(f"- {from_tbl}.{from_col} -> {to_tbl}.{to_col} ({rel_type}) — {desc}")

        # --- Business rules (hardcoded, critical for SQL accuracy) ---
        lines.append("\n=== BUSINESS RULES ===")
        lines.append("""Vehicle Status Codes (dim_vehicle.vehicle_status_code):
  0 = Created (new, not yet active)
  1 = Active (on lease)
  2 = Terminated - Invoicing Stopped
  3 = Terminated - Invoice Adjustment Made
  4 = Terminated - Mileage Adjustment Made
  5 = Terminated - De-investment Made
  8 = Terminated - Ready for Settlement
  9 = Terminated - Final Settlement Made
  Active vehicles: is_active = 1 (status_code IN (0, 1))
  Terminated vehicles: is_active = 0 (status_code >= 2)

Order Status Codes (ref_order_status.status_code):
  Order Phase: 0=Created, 1=Sent to Dealer, 2=Delivery Confirmed
  Delivery Phase: 3=Insurance, 4=Registration, 5=Driver Pack, 6=Delivered, 7=Lease Schedule
  9=Cancelled

Lease Types (dim_vehicle.lease_type / dim_contract.lease_type):
  O = Operational Lease (full service)
  F = Financial Lease
  L = Lease

Driver Rules:
  Primary driver: driver_sequence = 1 or is_primary_driver = 1
  Active driver: is_active = 1

Contract Rules:
  Active contract: is_active = 1 or contract_status = 'Active'
  Monthly rate components: depreciation + interest + maintenance + insurance + fuel + tires + road_tax + admin + replacement_vehicle

Date Handling (SQLite):
  Dates stored as TEXT in YYYY-MM-DD format
  Use date('now'), date('now', '-12 months'), etc.
  Compare with >= / <= operators on TEXT columns

Maintenance / Service Records (fact_odometer_reading):
  source_type = 'Service' filters to maintenance/service records only
  source_type = 'Manual Entry' for manual odometer readings
  source_type = 'Fuel Transaction' for fuel-related readings
  transaction_description contains free-text repair/service details (e.g. '10000km service done')
  transaction_amount holds the cost of the service
  Use LIKE '%keyword%' on transaction_description to search for specific services
  JOIN to dim_vehicle via vehicle_id for vehicle/customer context

Fuel Codes (dim_vehicle.fuel_code / CCOB_OBFUCD_Fuel_Code):
  1 = Petrol - Unleaded 91 E-Plus
  2 = Petrol - Unleaded 95 Special
  3 = Diesel
  4 = LPG
  6 = Petrol - Unleaded 98 Super
  7 = Electric - Full Electric Vehicle (BEV)
  8 = Electric - Plugin Hybrid Electric Vehicle (PHEV)
  9 = Electric - Hybrid Electric Vehicle (HEV)
  Note: Code 5 is not in use.
  Fuel type groupings:
    Petrol: fuel_code IN (1, 2, 6)
    Diesel: fuel_code = 3
    LPG: fuel_code = 4
    Electric (all): fuel_code IN (7, 8, 9)
    Full Electric only (BEV): fuel_code = 7
    Hybrid (PHEV + HEV): fuel_code IN (8, 9)

Contract Duration & Termination:
  IMPORTANT: For any question about expiring, ending, or remaining time, always use expected_end_date
  and months_remaining. NEVER use lease_end_date for expiration queries — it is a raw source-system
  field that may be stale or empty.
  expected_end_date = lease_start_date + lease_duration_months (pre-computed in dim_vehicle and dim_contract)
  months_driven = months from lease_start_date / contract_start_date to today
  months_remaining = months from today to expected_end_date / contract_end_date
  Negative months_remaining means the contract is overdue / past expected end.
  "Vehicles expiring in 3 months" or "ending in 3 months":
    SELECT COUNT(*) FROM dim_vehicle WHERE months_remaining BETWEEN 0 AND 3
  "Contracts expiring this year":
    SELECT COUNT(*) FROM dim_vehicle WHERE expected_end_date BETWEEN date('now') AND date('now','+12 months')
  Vehicle and contract are interchangeable — every vehicle has a contract.
  dim_vehicle has: expected_end_date, months_driven, months_remaining (USE THESE for expiration queries)
  dim_contract has: contract_start_date, contract_end_date, months_driven, months_remaining
  lease_end_date: Raw source-system end date — DO NOT use for expiration analysis, use expected_end_date instead

CRITICAL - Registration vs Contract Expiry (DIFFERENT CONCEPTS):
  "Vehicle registration expiry" = government license plate renewal date (NOT AVAILABLE IN DATABASE)
  "Contract/lease expiry" = when the lease agreement with the fleet company ends (AVAILABLE via expected_end_date, months_remaining)
  The database does NOT contain vehicle registration renewal dates.
  If the user asks about "registration expiring", "registration renewal", or "license plate expiry":
    - Explain that vehicle registration expiry dates are not tracked in this system
    - Offer to show contract/lease expiration data instead using expected_end_date
    - Do NOT silently substitute contract data for registration queries

Renewal Intelligence (days_to_contract_end and Orders):
  days_to_contract_end = number of days from today to expected_end_date (in dim_vehicle)
    Negative value = vehicle is OVERDUE (past expected end date)
    0-90 = vehicle is DUE FOR RENEWAL (within 90 days)
    > 90 = vehicle not yet due

  Linking Vehicles to Renewal Orders:
    staging_orders.previous_object_no = the vehicle_id being REPLACED by this order
    If previous_object_no IS NOT NULL: this is a RENEWAL ORDER (replacing an existing vehicle)
    If previous_object_no IS NULL: this is a NEW ORDER (new vehicle, not a replacement)
    Active orders (not yet delivered): order_status_code < 6
    Delivered orders: order_status_code >= 6

  Common Renewal Queries:
    "Overdue vehicles with an order":
      SELECT v.* FROM dim_vehicle v
      WHERE v.is_active = 1 AND v.days_to_contract_end < 0
        AND v.vehicle_id IN (SELECT previous_object_no FROM staging_orders WHERE order_status_code < 6)

    "Overdue vehicles without an order":
      SELECT v.* FROM dim_vehicle v
      WHERE v.is_active = 1 AND v.days_to_contract_end < 0
        AND v.vehicle_id NOT IN (SELECT previous_object_no FROM staging_orders WHERE previous_object_no IS NOT NULL AND order_status_code < 6)

    "Overdue vehicles with order, grouped by order status":
      SELECT o.order_status, COUNT(*) as count
      FROM dim_vehicle v
      JOIN staging_orders o ON v.vehicle_id = o.previous_object_no
      WHERE v.is_active = 1 AND v.days_to_contract_end < 0 AND o.order_status_code < 6
      GROUP BY o.order_status

    "Vehicles due for renewal (0-90 days) without order":
      SELECT v.* FROM dim_vehicle v
      WHERE v.is_active = 1 AND v.days_to_contract_end BETWEEN 0 AND 90
        AND v.vehicle_id NOT IN (SELECT previous_object_no FROM staging_orders WHERE previous_object_no IS NOT NULL AND order_status_code < 6)

Reporting Period Format (fact_car_reports, fact_exploitation_services, fact_maintenance_approvals):
  CRITICAL: reporting_period is a YYYYMM integer (e.g. 202601 = January 2026).
  Do NOT join reporting_period to dim_date.date_key (different format).
  For date filtering, compare directly: reporting_period >= 202502 (for months after Feb 2025)
  To get last 12 months: reporting_period >= CAST(strftime('%Y%m', date('now', '-12 months')) AS INTEGER)
  IMPORTANT: When querying by registration_number, always add v.is_active = 1 to filter for the active vehicle
    (vehicles may be re-leased with the same registration but different object numbers).

Monthly Service Costs and Invoices (fact_exploitation_services):
  CRITICAL: For ANY question about cost, invoice, or spending per month by service type,
  ALWAYS use fact_exploitation_services. This is THE table for monthly financial data.
    vehicle_id = object number (join to dim_vehicle.vehicle_id)
    total_monthly_cost = actual cost in AED for that month
    total_monthly_invoice = actual invoiced amount in AED for that month
    service_code = exploitation/service type (integer)
    reporting_period = YYYYMM integer (e.g. 202501 = January 2025)

  Common exploitation/service codes (from ref_domain_translation domain_id=5):
    11 = Depreciation, 100 = Comprehensive insurance, 140 = Third party insurance
    170 = Insurance (general), 580 = Maintenance and repairs, 581 = Tyres
    582 = Tyre repair, 583 = Miscellaneous, 585 = Wheel alignment
    600 = Fuel, 620 = Replacement vehicle, 640 = Security Pass Certificate Fee
    650 = Licensing, 660 = Roadside assistance, 680 = Traffic fines
    700 = Fee, 711 = Finance administration fee, 999 = Interest

  To look up service code descriptions:
    JOIN ref_domain_translation dt ON CAST(dt.domain_value AS INTEGER) = es.service_code
      AND dt.domain_id = 5 AND dt.language_code = 'E'

  Join to vehicle: fact_exploitation_services.vehicle_id = dim_vehicle.vehicle_id

  Example: "Maintenance invoice for registration 3946615 for January 2025":
    SELECT es.reporting_period, es.total_monthly_cost as cost_aed, es.total_monthly_invoice as invoice_aed
    FROM fact_exploitation_services es
    JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
    WHERE v.registration_number = '3946615' AND v.is_active = 1
      AND es.service_code = 580
      AND es.reporting_period = 202501

  Example: "Monthly maintenance cost and invoice for last 12 months":
    SELECT es.reporting_period, SUM(es.total_monthly_cost) as maintenance_cost, SUM(es.total_monthly_invoice) as maintenance_invoice
    FROM fact_exploitation_services es
    JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
    WHERE v.registration_number = '3946615' AND v.is_active = 1
      AND es.service_code = 580
      AND es.reporting_period >= CAST(strftime('%Y%m', date('now', '-12 months')) AS INTEGER)
    GROUP BY es.reporting_period ORDER BY es.reporting_period

  Example: "All service costs for a vehicle by service type":
    SELECT dt.domain_text as service_description, es.service_code,
           SUM(es.total_monthly_cost) as total_cost, SUM(es.total_monthly_invoice) as total_invoice
    FROM fact_exploitation_services es
    JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
    LEFT JOIN ref_domain_translation dt ON CAST(dt.domain_value AS INTEGER) = es.service_code
      AND dt.domain_id = 5 AND dt.language_code = 'E'
    WHERE v.registration_number = '3946615' AND v.is_active = 1
    GROUP BY es.service_code, dt.domain_text ORDER BY total_invoice DESC

Vehicle Cost Reports (fact_car_reports):
  Monthly cost snapshot per vehicle. Join: fact_car_reports.vehicle_id = dim_vehicle.vehicle_id
  IMPORTANT: The cost breakdown columns (fuel_cost_total, maintenance_cost_total, tyre_cost_total,
  replacement_car_cost_total) are PER-KM RATES, not AED amounts. Multiply by km_driven to get AED.
  Only use total_cost (cumulative AED), total_invoiced (cumulative AED), and cost_per_km directly.
  For per-month cost/invoice breakdowns, prefer fact_exploitation_services instead.

Maintenance Approvals (fact_maintenance_approvals):
  Individual maintenance events with approval_date, amount (AED), supplier_name, description.
  Use for detailed maintenance history (what work was done, which supplier, cost per event).
  Join: fact_maintenance_approvals.vehicle_id = dim_vehicle.vehicle_id""")

        # --- Pre-built views ---
        lines.append("\n=== PRE-BUILT VIEWS (prefer these over complex JOINs) ===")
        view_rows = [r for r in table_rows if r[0].startswith("view_")]
        for r in view_rows:
            table_name, display_name, description, _, _, typical_q = r
            lines.append(f"- {table_name}: {description}")
            if typical_q:
                lines.append(f"  Use for: {typical_q}")

        return "\n".join(lines)

    @staticmethod
    def _format_chat_domain_block(glossary_rows, table_rows) -> str:
        """Build a prompt block for general chat from glossary and table catalog."""
        lines: List[str] = []

        lines.append("=== BUSINESS GLOSSARY ===")
        for r in glossary_rows:
            term, definition, synonyms, related_tables, formula = r
            entry = f"- {term}: {definition}"
            if synonyms:
                entry += f"  (also called: {synonyms})"
            if formula:
                entry += f"  [Formula: {formula}]"
            lines.append(entry)

        lines.append("\n=== AVAILABLE DATA ===")
        for r in table_rows:
            table_name, display_name, description, domain, _, typical_q = r
            lines.append(f"- {display_name} ({table_name}): {description}")
            if typical_q:
                lines.append(f"  Typical questions: {typical_q}")

        return "\n".join(lines)

    @staticmethod
    def _get_static_fallback_sql_domain() -> str:
        """Hardcoded fallback domain knowledge when catalog tables are empty."""
        return """=== TABLE CATALOG ===
- dim_customer (Customers): Master list of all customers who lease vehicles  [grain: One row per customer]
- dim_vehicle (Vehicles): All vehicles with specs, status, and lease info  [grain: One row per vehicle]
- dim_driver (Drivers): People assigned to drive vehicles  [grain: One row per driver-vehicle assignment]
- dim_contract (Contracts): Lease contracts with pricing and rate breakdowns  [grain: One row per contract position]
- dim_group (Customer Groups): Logical groupings of customers  [grain: One row per group]
- dim_make_model (Makes and Models): Vehicle manufacturers and models  [grain: One row per make-model]
- dim_date (Calendar Dates): Date dimension for time analysis  [grain: One row per date]
- ref_vehicle_status (Vehicle Statuses): Status code reference  [grain: One row per status code]
- ref_order_status (Order Statuses): Order status reference  [grain: One row per status code]
- ref_fuel_code (Fuel Codes): Fuel/energy type reference  [grain: One row per fuel code]
- fact_odometer_reading (Odometer & Service Records): Mileage readings AND maintenance/service records. source_type = 'Service' for maintenance. transaction_description has repair details.  [grain: One row per reading/service event]
- fact_billing (Billing Records): Billing transactions  [grain: One row per billing record]

=== CRITICAL COLUMN NAMES (use EXACTLY these names) ===
dim_vehicle columns:
  - vehicle_id (PRIMARY KEY)
  - registration_number (license plate)
  - make_name (vehicle manufacturer - e.g. Toyota, Ford) - NEVER use "manufacturer" or "make", ALWAYS use "make_name"
  - model_name (vehicle model - e.g. Camry, F-150) - NEVER use "model", ALWAYS use "model_name"
  - customer_id, customer_name
  - vehicle_status, vehicle_status_code, is_active
  - fuel_code, lease_type
  - expected_end_date, months_remaining, days_to_contract_end
dim_customer columns:
  - customer_id (PRIMARY KEY)
  - customer_name
  - account_manager

=== KEY RELATIONSHIPS ===
- dim_vehicle.customer_id -> dim_customer.customer_id (many-to-one)
- dim_driver.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- dim_contract.customer_id -> dim_customer.customer_id (many-to-one)
- dim_vehicle.contract_position_number -> dim_contract.contract_position_number (many-to-one)
- fact_odometer_reading.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- fact_billing.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- fact_billing.customer_id -> dim_customer.customer_id (many-to-one)
- fact_odometer_reading.reading_date_key -> dim_date.date_key (many-to-one)
- dim_vehicle.vehicle_status_code -> ref_vehicle_status.status_code (many-to-one)
- dim_group.customer_id -> dim_customer.customer_id (many-to-one)

=== BUSINESS RULES ===
Vehicle Status Codes (dim_vehicle.vehicle_status_code):
  0 = Created, 1 = Active, 2 = Terminated - Invoicing Stopped
  3 = Terminated - Invoice Adjustment, 4 = Terminated - Mileage Adjustment
  5 = Terminated - De-investment, 8 = Terminated - Ready for Settlement
  9 = Terminated - Final Settlement
  Active vehicles: is_active = 1 (status IN (0,1))
  Terminated: is_active = 0 (status >= 2)

Fuel Codes (dim_vehicle.fuel_code):
  1 = Petrol - Unleaded 91 E-Plus, 2 = Petrol - Unleaded 95 Special
  3 = Diesel, 4 = LPG, 6 = Petrol - Unleaded 98 Super
  7 = Electric - Full Electric (BEV), 8 = Electric - Plugin Hybrid (PHEV), 9 = Electric - Hybrid (HEV)
  Petrol: fuel_code IN (1,2,6), Diesel: fuel_code=3, Electric (all): fuel_code IN (7,8,9)

Lease Types: O = Operational, F = Financial, L = Lease
Primary driver: driver_sequence = 1 or is_primary_driver = 1
Active contract: is_active = 1 or contract_status = 'Active'
Dates: TEXT YYYY-MM-DD, use date('now'), date('now', '-12 months')
Maintenance: fact_odometer_reading with source_type = 'Service'. transaction_description has details. Use LIKE for search.
Contract Duration & Expiration:
  IMPORTANT: For expiring/ending questions, ALWAYS use expected_end_date and months_remaining. NEVER use lease_end_date (stale source data).
  expected_end_date = start + duration months. months_driven = months since start. months_remaining = months to end (negative = overdue).
  "Vehicles expiring in 3 months": SELECT COUNT(*) FROM dim_vehicle WHERE months_remaining BETWEEN 0 AND 3
  Vehicle and contract are interchangeable — every vehicle has a contract.
  dim_vehicle: expected_end_date, months_driven, months_remaining (USE THESE)
  dim_contract: contract_start_date, contract_end_date, months_driven, months_remaining
  lease_end_date: Raw source field — DO NOT use for expiration queries
CRITICAL - Registration vs Contract Expiry:
  "Vehicle registration expiry" (government license plate renewal) is NOT tracked in this system.
  "Contract/lease expiry" (expected_end_date, months_remaining) IS available.
  If user asks about "registration expiring" or "license plate expiry", explain this distinction.

=== PRE-BUILT VIEWS (prefer over complex JOINs) ===
- view_fleet_overview: Vehicles with customer and primary driver info
- view_customer_fleet_summary: Fleet size and value by customer
- view_vehicle_details: Full vehicle details with contract and customer
- view_contract_summary: All contracts with computed total value
- view_customer_contracts: Contract totals by customer
- view_make_model_distribution: Vehicle counts by make/model
- view_mileage_analysis: Odometer readings and mileage by vehicle
- view_customer_billing_summary: Billing totals by customer
- view_driver_directory: All drivers with vehicle and customer info
- view_account_manager_portfolio: Customers and vehicles per account manager
- view_maintenance_analysis: Maintenance events with vehicle and supplier details
- view_vehicle_cost_analysis: Vehicle cost breakdown from monthly car reports
- view_supplier_summary: Supplier statistics including total spending and vehicles serviced

Reporting Period: reporting_period is YYYYMM integer (e.g. 202601). Do NOT join to dim_date.
  Filter: reporting_period >= CAST(strftime('%Y%m', date('now', '-12 months')) AS INTEGER)
  When filtering by registration_number, always add v.is_active = 1

Monthly Costs/Invoices (fact_exploitation_services):
  CRITICAL: For ANY question about cost, invoice, or spending per month by service type,
  ALWAYS use fact_exploitation_services:
    total_monthly_cost = actual AED cost per month
    total_monthly_invoice = actual AED invoiced per month
    service_code: 11=Depreciation, 100=Insurance, 580=Maintenance, 581=Tyres, 600=Fuel, 620=Replacement vehicle, 650=Licensing, 660=Roadside assistance, 700=Fee, 711=Finance admin fee, 999=Interest
  Join: fact_exploitation_services.vehicle_id = dim_vehicle.vehicle_id
  Service code descriptions: JOIN ref_domain_translation dt ON CAST(dt.domain_value AS INTEGER) = es.service_code AND dt.domain_id = 5 AND dt.language_code = 'E'
  CRITICAL - Budget vs Cost terminology:
    "Budgeted amount" / "budget" / "invoiced amount" = total_monthly_invoice (the invoiced amount billed to the customer)
    "Actual cost" / "real cost" / "maintenance cost" = total_monthly_cost (the actual cost incurred)
    When a user asks about vehicles "exceeding budget" or "over budget" or "cost over invoiced amount", compare total_monthly_cost > total_monthly_invoice
    Example: "top 10 vehicles exceeding maintenance cost over invoiced amount":
      SELECT v.vehicle_id, v.registration_number, v.make_and_model,
             SUM(es.total_monthly_cost) AS total_maintenance_cost,
             SUM(es.total_monthly_invoice) AS total_invoiced_amount,
             ROUND(SUM(es.total_monthly_cost) - SUM(es.total_monthly_invoice), 2) AS over_budget
      FROM fact_exploitation_services es
      JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
      WHERE es.service_code = 580 AND v.is_active = 1
      GROUP BY v.vehicle_id, v.registration_number, v.make_and_model
      HAVING total_maintenance_cost > total_invoiced_amount
      ORDER BY over_budget DESC LIMIT 10

Vehicle Cost Reports (fact_car_reports):
  Monthly snapshot per vehicle. Only total_cost and total_invoiced are in AED.
  Cost breakdown columns (maintenance_cost_total etc.) are PER-KM RATES, not AED amounts.
  CRITICAL: NEVER use fact_car_reports for maintenance cost queries — the maintenance columns are per-km rates, NOT AED amounts.
  ALWAYS use fact_exploitation_services (service_code=580) for maintenance cost analysis.
  For per-month breakdowns, use fact_exploitation_services instead."""

    def _build_system_prompt(self, user_context: Dict[str, Any]) -> str:
        """Build system prompt for chat"""
        role = user_context.get("role", "user")
        domain_block = self._domain_cache.get("chat_prompt_domain_block", "")

        return f"""You are FleetAI, an intelligent assistant for a vehicle fleet leasing company.

Your capabilities:
- Answer questions about fleet data, vehicles, contracts, drivers, billing, and invoices
- Help users understand their fleet metrics and performance
- Explain fleet terminology (lease types, vehicle statuses, contract components)
- Generate insights about mileage, costs, and fleet composition
- Assist with creating dashboards and reports

{domain_block}

User's role: {role}
Access level: {"Full access" if user_context.get("customer_ids") is None else "Limited to assigned customers"}

Guidelines:
- Be helpful, professional, and concise
- When discussing data, offer to show specific numbers
- IMPORTANT: All monetary amounts must be displayed in UAE Dirhams (AED). Always use AED as the currency symbol regardless of what currency appears in the data. Never use NOK, USD, EUR or other currencies - convert all displays to AED, e.g. AED 121,581.41
- IMPORTANT: When querying vehicles from dim_vehicle, ALWAYS include "is_active = 1" filter unless the user specifically asks for inactive/terminated vehicles. This ensures only active fleet vehicles are counted.
- IMPORTANT: After presenting data results, ALWAYS add a "**Notable:**" section with business insights. Highlight trends, anomalies, peaks, or actionable observations. Examples:
  * "**Notable:** Month 3-4 has the highest volume with 506 vehicles expiring - prioritize renewal planning for this period."
  * "**Notable:** Maintenance costs increased 25% compared to previous period - investigate potential causes."
  * "**Notable:** Toyota vehicles represent 45% of the fleet - consider diversification."
- Suggest relevant follow-up questions
- If unsure, ask for clarification
- Never reveal system internals or database structure
- IMPORTANT: Always consider the full conversation context - if the user asks follow-up questions like "show me that as a chart", "break it down by status", or "filter that by Toyota", understand they are referring to the previous query/topic
- When the user says "it", "that", "those", "the same", etc., refer back to the previous messages to understand what they mean"""

    def _detect_chart_config(self, data: list, user_message: str) -> dict | None:
        """Auto-detect appropriate chart type from data shape and query"""
        if not data or len(data) == 0:
            return None

        first_row = data[0]
        keys = list(first_row.keys())

        # Patterns for numeric keys that are actually identifiers/dimensions, not measures
        id_patterns = ['_id', 'period', 'year', 'month', 'week', 'code', 'number']

        numeric_keys = []
        categorical_keys = []
        date_keys = []

        for key in keys:
            sample = next((row[key] for row in data if row.get(key) is not None), None)
            if sample is None:
                categorical_keys.append(key)
                continue
            if isinstance(sample, (int, float)):
                # Check if this numeric key is actually an identifier/dimension
                key_lower = key.lower()
                if any(p in key_lower for p in id_patterns):
                    categorical_keys.append(key)
                else:
                    numeric_keys.append(key)
            elif isinstance(sample, str):
                if any(p in key.lower() for p in ['date', 'month', 'year', 'week', 'period', 'time']):
                    date_keys.append(key)
                else:
                    categorical_keys.append(key)
            else:
                categorical_keys.append(key)

        row_count = len(data)

        # Single numeric value — no chart needed
        if row_count == 1 and len(numeric_keys) == 1 and len(categorical_keys) == 0:
            return None

        # Date/time dimension + numeric → line chart
        if date_keys and numeric_keys:
            y_keys = [k for k in numeric_keys if k != date_keys[0]][:3]
            if y_keys:
                return {
                    "chart_type": "line",
                    "x_axis_key": date_keys[0],
                    "y_axis_keys": y_keys,
                }

        # Few categories (≤8) + single numeric + distribution keywords → pie chart
        if categorical_keys and numeric_keys and row_count <= 8 and len(numeric_keys) == 1:
            dist_keywords = ['by', 'per', 'breakdown', 'distribution', 'status', 'type', 'category', 'split']
            if any(kw in user_message.lower() for kw in dist_keywords):
                return {
                    "chart_type": "pie",
                    "x_axis_key": categorical_keys[0],
                    "y_axis_keys": [numeric_keys[0]],
                }

        # Categorical + numeric(s) → bar chart
        if categorical_keys and numeric_keys:
            x_key = categorical_keys[0]
            y_keys = [k for k in numeric_keys if k != x_key][:4]
            if y_keys:
                return {
                    "chart_type": "bar",
                    "x_axis_key": x_key,
                    "y_axis_keys": y_keys,
                }

        # Only numerics, multiple rows → line chart
        if numeric_keys and row_count > 2:
            x_key = keys[0]
            y_keys = [k for k in numeric_keys if k != x_key][:3]
            if y_keys:
                return {
                    "chart_type": "line",
                    "x_axis_key": x_key,
                    "y_axis_keys": y_keys,
                }

        return None

    def _check_if_needs_data(self, message: str) -> bool:
        """Check if user message requires data retrieval"""
        data_keywords = [
            "show", "list", "how many", "count", "total",
            "average", "sum", "find", "search", "get",
            "what is", "which", "report", "breakdown",
            "by status", "by make", "by customer", "by type",
            "per month", "per year", "distribution", "top",
            "chart", "graph", "visualize", "display",
            "insight", "insights", "analyze", "analysis", "generate", "overview", "summary", "trend"
        ]
        message_lower = message.lower()
        return any(kw in message_lower for kw in data_keywords)

    async def _handle_aggregation_query(self, query_lower: str) -> dict | None:
        """Handle common aggregation queries directly without AI"""

        # Common patterns: "vehicles by X", "show vehicles by X", "show me vehicles by X", etc.
        # More flexible patterns to catch various phrasings
        dim_choices = r'status|make|manufacturer|brand|customer|type|model|fuel'
        patterns = [
            (rf'(?:show|get|list|display|give)?\s*(?:me\s+)?(?:the\s+)?(?:all\s+)?(?:total\s+)?(?:number\s+)?(?:of\s+)?vehicles?\s+(?:breakdown\s+)?by\s+({dim_choices})', 'dim_vehicle', 'vehicles'),
            (rf'(?:show|get|list|display|give)?\s*(?:me\s+)?(?:the\s+)?(?:number|count)?\s*(?:of\s+)?vehicles?\s+by\s+({dim_choices})', 'dim_vehicle', 'vehicles'),
            (rf'(?:breakdown|distribution)\s+(?:of\s+)?vehicles?\s+by\s+({dim_choices})', 'dim_vehicle', 'vehicles'),
            (rf'how\s+many\s+vehicles?\s+(?:per|by|for\s+each)\s+({dim_choices})', 'dim_vehicle', 'vehicles'),
            (rf'vehicles?\s+(?:per|by)\s+({dim_choices})', 'dim_vehicle', 'vehicles'),
            (r'(?:show|get|list|display|give)?\s*(?:me\s+)?(?:the\s+)?contracts?\s+by\s+(status|customer)', 'dim_contract', 'contracts'),
        ]

        dim_mapping = {
            'status': 'vehicle_status',
            'make': 'make_name',
            'manufacturer': 'make_name',
            'brand': 'make_name',
            'model': 'model_name',
            'customer': 'customer_name',
            'type': 'body_type',
            'fuel': 'fuel_type',
        }

        for pattern, table, entity in patterns:
            match = re.search(pattern, query_lower)
            if match:
                dimension = match.group(1).lower()
                col_name = dim_mapping.get(dimension, dimension)
                count_col = f"{entity}_count"

                # Check if user is asking for "active" vehicles
                is_active_filter = 'active' in query_lower and table == 'dim_vehicle'
                where_clause = "WHERE is_active = 1 " if is_active_filter else ""
                active_desc = "active " if is_active_filter else ""

                sql = f"SELECT {col_name}, COUNT(*) as {count_col} FROM {table} {where_clause}GROUP BY {col_name} ORDER BY {count_col} DESC"

                logger.info(f"Aggregation handler matched pattern for '{dimension}', executing: {sql}")

                try:
                    result = await self.db.execute(text(sql))
                    columns = list(result.keys())
                    results = [dict(zip(columns, row)) for row in result.fetchall()]
                    if results:
                        # Build a nice summary message
                        summary_lines = []
                        for r in results[:10]:
                            value = r.get(col_name) or "Unknown"
                            count = r.get(count_col, 0)
                            summary_lines.append(f"- **{value}**: {count:,} {entity}")

                        message = f"Here's the breakdown of {active_desc}{entity} by {dimension}:\n\n" + "\n".join(summary_lines)
                        if len(results) > 10:
                            message += f"\n\n...and {len(results) - 10} more categories."

                        logger.info(f"Aggregation handler returning {len(results)} results")
                        return {
                            "data": results,
                            "message": message
                        }
                except Exception as e:
                    logger.error(f"Aggregation query failed: {e}")
                    return None

        return None

    async def _handle_service_cost_query(self, query_lower: str) -> dict | None:
        """Handle service cost/invoice queries using fact_exploitation_services.

        The AI model often picks wrong tables for these queries, so we intercept
        and generate correct SQL directly.
        """

        # Detect service cost/invoice queries
        service_keywords = ['maintenance', 'tyre', 'tire', 'insurance', 'fuel',
                            'replacement', 'licensing', 'roadside', 'service',
                            'cost', 'invoice', 'invoiced', 'spending', 'expense']
        monthly_keywords = ['month', 'monthly', 'per month', 'last 12', 'last 6',
                            'this year', 'last year', 'trend', 'history']

        has_service = any(kw in query_lower for kw in service_keywords)
        has_monthly = any(kw in query_lower for kw in monthly_keywords)

        if not (has_service and has_monthly):
            return None

        # Extract vehicle identifier (registration number or object number)
        vehicle_match = re.search(r'(?:vehicle|registration|reg|car|object)[\s#:]*(?:number\s+)?(\d{4,8})', query_lower)
        if not vehicle_match:
            # Try standalone number patterns (e.g. "for 3946615")
            vehicle_match = re.search(r'(?:for|of)\s+(\d{5,8})', query_lower)
        if not vehicle_match:
            return None

        vehicle_id_str = vehicle_match.group(1)

        # Determine service code filter
        service_code_filter = None
        service_label = "all services"
        if 'maintenance' in query_lower or 'repair' in query_lower:
            service_code_filter = 580
            service_label = "Maintenance and repairs"
        elif 'tyre' in query_lower or 'tire' in query_lower:
            service_code_filter = 581
            service_label = "Tyres"
        elif 'insurance' in query_lower:
            service_code_filter = 100
            service_label = "Comprehensive insurance"
        elif 'fuel' in query_lower:
            service_code_filter = 600
            service_label = "Fuel"
        elif 'replacement' in query_lower:
            service_code_filter = 620
            service_label = "Replacement vehicle"
        elif 'licensing' in query_lower:
            service_code_filter = 650
            service_label = "Licensing"
        elif 'roadside' in query_lower:
            service_code_filter = 660
            service_label = "Roadside assistance"

        # Determine time range (number of months back)
        months_back = 12  # default
        m = re.search(r'last\s+(\d+)\s+month', query_lower)
        if m:
            months_back = int(m.group(1))
        elif 'last year' in query_lower:
            months_back = 12
        elif 'this year' in query_lower:
            months_back = 12
        elif 'last 6' in query_lower:
            months_back = 6
        elif 'last 3' in query_lower:
            months_back = 3
        elif 'last 24' in query_lower:
            months_back = 24

        # Build SQL - try registration number first (most common user input)
        service_filter = f"AND es.service_code = {service_code_filter}" if service_code_filter else ""

        sql = f"""SELECT es.reporting_period,
       SUM(es.total_monthly_cost) as cost_aed,
       SUM(es.total_monthly_invoice) as invoice_aed
FROM fact_exploitation_services es
JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
WHERE v.registration_number = '{vehicle_id_str}' AND v.is_active = 1
  {service_filter}
  AND es.reporting_period >= CAST(strftime('%Y%m', date('now', '-{months_back} months')) AS INTEGER)
GROUP BY es.reporting_period
ORDER BY es.reporting_period"""

        logger.info(f"Service cost handler: vehicle={vehicle_id_str}, service={service_label}, months={months_back}")
        logger.info(f"Service cost handler SQL: {sql}")

        try:
            result = await self.db.execute(text(sql))
            columns = list(result.keys())
            results = [dict(zip(columns, row)) for row in result.fetchall()]

            # If no results with registration_number, try as direct vehicle_id
            if not results:
                sql_direct = f"""SELECT es.reporting_period,
       SUM(es.total_monthly_cost) as cost_aed,
       SUM(es.total_monthly_invoice) as invoice_aed
FROM fact_exploitation_services es
WHERE es.vehicle_id = {vehicle_id_str}
  {service_filter}
  AND es.reporting_period >= CAST(strftime('%Y%m', date('now', '-{months_back} months')) AS INTEGER)
GROUP BY es.reporting_period
ORDER BY es.reporting_period"""
                logger.info(f"Service cost handler: retrying with direct vehicle_id")
                result = await self.db.execute(text(sql_direct))
                columns = list(result.keys())
                results = [dict(zip(columns, row)) for row in result.fetchall()]

            if results:
                # Format period for display
                month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                              'July', 'August', 'September', 'October', 'November', 'December']
                for row in results:
                    period = row.get('reporting_period', 0)
                    if period:
                        year = period // 100
                        month = period % 100
                        row['month'] = f"{month_names[month]} {year}"

                # Build summary
                total_cost = sum(r.get('cost_aed', 0) or 0 for r in results)
                total_invoice = sum(r.get('invoice_aed', 0) or 0 for r in results)

                summary_lines = []
                for r in results:
                    month_str = r.get('month', str(r.get('reporting_period', '')))
                    cost = r.get('cost_aed', 0) or 0
                    invoice = r.get('invoice_aed', 0) or 0
                    summary_lines.append(f"- **{month_str}**: Cost AED {cost:,.2f} | Invoice AED {invoice:,.2f}")

                message = (
                    f"**{service_label}** for vehicle **{vehicle_id_str}** "
                    f"(last {months_back} months):\n\n"
                    + "\n".join(summary_lines)
                    + f"\n\n**Totals**: Cost AED {total_cost:,.2f} | Invoice AED {total_invoice:,.2f}"
                )

                logger.info(f"Service cost handler returning {len(results)} months of data")
                return {
                    "data": results,
                    "message": message
                }
            else:
                return {
                    "data": [],
                    "message": f"No {service_label.lower()} data found for vehicle {vehicle_id_str} in the last {months_back} months."
                }

        except Exception as e:
            logger.error(f"Service cost query failed: {e}")
            return None

    async def _handle_book_value_query(self, query_lower: str) -> dict | None:
        """Handle book value queries using fact_car_reports and fact_exploitation_services.

        Book value is stored in fact_car_reports.current_book_value for each reporting period.
        Monthly depreciation comes from fact_exploitation_services with service_code = 11.
        Future book value = current_book_value - (months_ahead * monthly_depreciation)
        """

        # Detect book value queries
        book_value_keywords = ['book value', 'bookvalue', 'nbv', 'net book value',
                               'depreciation', 'depreciate', 'asset value', 'carrying value']

        has_book_value = any(kw in query_lower for kw in book_value_keywords)
        if not has_book_value:
            return None

        # Extract vehicle identifier
        vehicle_match = re.search(r'(?:vehicle|object|reg|car)[\s#:]*(?:number\s+)?(\d{4,8})', query_lower)
        if not vehicle_match:
            vehicle_match = re.search(r'(?:for|of)\s+(\d{5,8})', query_lower)
        if not vehicle_match:
            # Try standalone number
            vehicle_match = re.search(r'\b(\d{6,8})\b', query_lower)
        if not vehicle_match:
            return None

        vehicle_id_str = vehicle_match.group(1)

        # Check for future projection request
        is_future = any(kw in query_lower for kw in ['future', 'project', 'forecast', 'next', 'will be', 'in 12 months', 'in 6 months', 'end of'])

        # Determine number of months for future projection
        future_months = 12  # default
        m = re.search(r'(?:next|in)\s+(\d+)\s+month', query_lower)
        if m:
            future_months = int(m.group(1))
        elif 'end of year' in query_lower or 'year end' in query_lower:
            from datetime import datetime
            current_month = datetime.now().month
            future_months = 12 - current_month + 1
        elif 'end of contract' in query_lower or 'lease end' in query_lower:
            future_months = None  # Will calculate from contract end date

        try:
            # Get current book value history (no currency_code - we always use AED)
            sql = f"""
            SELECT
                cr.reporting_period,
                cr.current_book_value as book_value
            FROM fact_car_reports cr
            WHERE cr.vehicle_id = {vehicle_id_str}
              AND cr.current_book_value IS NOT NULL
              AND cr.current_book_value > 0
            ORDER BY cr.reporting_period DESC
            LIMIT 12
            """

            result = await self.db.execute(text(sql))
            columns = list(result.keys())
            history = [dict(zip(columns, row)) for row in result.fetchall()]

            if not history:
                # Try with registration number lookup
                sql_lookup = f"""
                SELECT
                    cr.reporting_period,
                    cr.current_book_value as book_value
                FROM fact_car_reports cr
                JOIN dim_vehicle v ON cr.vehicle_id = v.vehicle_id
                WHERE v.registration_number = '{vehicle_id_str}'
                  AND cr.current_book_value IS NOT NULL
                  AND cr.current_book_value > 0
                ORDER BY cr.reporting_period DESC
                LIMIT 12
                """
                result = await self.db.execute(text(sql_lookup))
                columns = list(result.keys())
                history = [dict(zip(columns, row)) for row in result.fetchall()]

            if not history:
                return {
                    "data": [],
                    "message": f"No book value data found for vehicle {vehicle_id_str}."
                }

            latest_book_value = history[0]['book_value']
            latest_period = history[0]['reporting_period']
            currency = 'AED'  # Always use AED for display

            # Get monthly depreciation from exploitation service code 11
            sql_dep = f"""
            SELECT total_monthly_cost as monthly_depreciation
            FROM fact_exploitation_services
            WHERE vehicle_id = {vehicle_id_str}
              AND service_code = 11
            ORDER BY reporting_period DESC
            LIMIT 1
            """
            dep_result = await self.db.execute(text(sql_dep))
            dep_row = dep_result.fetchone()
            monthly_depreciation = dep_row[0] if dep_row else 0

            # Format history for display and chart
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            chart_data = []
            for row in history:
                period = row.get('reporting_period', 0)
                if period:
                    year = period // 100
                    month = period % 100
                    month_str = f"{month_names[month]} {year}"
                    # Create clean data for chart (only month and book_value)
                    chart_data.append({
                        'month': month_str,
                        'book_value': row.get('book_value', 0)
                    })
                    row['month'] = month_str

            # Build response message
            summary_lines = []
            for r in history[:6]:  # Show last 6 months
                month_str = r.get('month', str(r.get('reporting_period', '')))
                bv = r.get('book_value', 0) or 0
                summary_lines.append(f"- **{month_str}**: {currency} {bv:,.2f}")

            # Format latest period for display
            latest_year = latest_period // 100
            latest_month = latest_period % 100
            latest_period_str = f"{month_names[latest_month]} {latest_year}"

            message = f"**Book Value** for vehicle **{vehicle_id_str}**:\n\n"
            message += f"**Latest Book Value** ({latest_period_str}): **{currency} {latest_book_value:,.2f}**\n"
            message += f"**Monthly Depreciation**: {currency} {monthly_depreciation:,.2f}\n\n"
            message += "**Recent History:**\n" + "\n".join(summary_lines)

            # Add future projection if requested
            if is_future and monthly_depreciation > 0:
                if future_months:
                    future_book_value = latest_book_value - (future_months * monthly_depreciation)
                    future_book_value = max(0, future_book_value)  # Can't go negative
                    message += f"\n\n**Future Projection** ({future_months} months ahead):\n"
                    message += f"- Projected Book Value: **{currency} {future_book_value:,.2f}**\n"
                    message += f"- Total Depreciation: {currency} {future_months * monthly_depreciation:,.2f}"

            logger.info(f"Book value handler returning {len(chart_data)} months of data for vehicle {vehicle_id_str}")
            return {
                "data": list(reversed(chart_data)),  # Chronological order for charts
                "message": message
            }

        except Exception as e:
            logger.error(f"Book value query failed: {e}")
            return None

    async def _handle_contract_expiry_query(self, query_lower: str) -> dict | None:
        """Handle contract expiry/renewal queries.

        Returns vehicles expiring in the next N months with proper is_active filter.
        Uses actual month names (e.g., "February 2026") starting from current month.
        """
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        # Detect expiry/renewal queries
        # Use word-boundary regex to avoid false matches (e.g. "exceeding" matching "ending")
        expiry_patterns = [
            r'expir', r'renew', r'\bending\b', r'contract\s+end', r'lease\s+end',
            r'due\s+for\s+renewal', r'coming\s+up\s+for\s+renewal', r'contracts?\s+ending'
        ]

        has_expiry = any(re.search(p, query_lower) for p in expiry_patterns)
        if not has_expiry:
            return None

        # Determine number of months (default 6)
        months_ahead = 6
        m = re.search(r'(?:next|in|within)\s+(\d+)\s+month', query_lower)
        if m:
            months_ahead = int(m.group(1))
        elif '12 month' in query_lower or 'year' in query_lower:
            months_ahead = 12
        elif '3 month' in query_lower:
            months_ahead = 3

        try:
            # Get current date for month calculations
            today = datetime.now()
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']

            # Build month labels dynamically starting from current month
            month_labels = []
            for i in range(months_ahead):
                future_date = today + relativedelta(months=i)
                month_labels.append(f"{month_names[future_date.month]} {future_date.year}")

            # Query vehicles expiring in the next N months (active only)
            # Group by month of expected_end_date
            sql = f"""
            SELECT
                strftime('%Y-%m', expected_end_date) as month_key,
                COUNT(*) as vehicle_count
            FROM dim_vehicle
            WHERE days_to_contract_end BETWEEN 0 AND {months_ahead * 30}
              AND is_active = 1
              AND expected_end_date IS NOT NULL
            GROUP BY month_key
            ORDER BY month_key
            """

            result = await self.db.execute(text(sql))
            columns = list(result.keys())
            raw_results = [dict(zip(columns, row)) for row in result.fetchall() if row[0] is not None]

            if not raw_results:
                return {
                    "data": [],
                    "message": f"No vehicles found expiring in the next {months_ahead} months."
                }

            # Convert month_key (YYYY-MM) to readable format (Month Year)
            results = []
            for r in raw_results:
                month_key = r.get('month_key', '')
                if month_key:
                    year, month = month_key.split('-')
                    month_name = f"{month_names[int(month)]} {year}"
                    results.append({
                        'month': month_name,
                        'vehicle_count': r.get('vehicle_count', 0)
                    })

            # Calculate total
            total = sum(r.get('vehicle_count', 0) for r in results)

            # Find the peak period
            peak = max(results, key=lambda x: x.get('vehicle_count', 0))
            peak_period = peak.get('month', '')
            peak_count = peak.get('vehicle_count', 0)

            # Build message
            message = f"**Vehicles Expiring in the Next {months_ahead} Months:**\n\n"
            for r in results:
                month = r.get('month', '')
                count = r.get('vehicle_count', 0)
                message += f"- **{month}**: {count:,} vehicles\n"

            message += f"\n**Total: {total:,} vehicles**"

            # Add Notable insight
            message += f"\n\n**Notable:** **{peak_period}** has the highest volume with **{peak_count:,} vehicles** expiring"
            if peak_count > total * 0.3:
                message += " - this represents over 30% of expiring contracts. Prioritize renewal planning for this period."
            else:
                message += " - consider proactive renewal outreach for this period."

            logger.info(f"Contract expiry handler returning {len(results)} periods, {total} total vehicles")
            return {
                "data": results,
                "message": message
            }

        except Exception as e:
            logger.error(f"Contract expiry query failed: {e}")
            return None

    async def _handle_maintenance_insights_query(self, query_lower: str) -> dict | None:
        """Handle fleet-wide maintenance cost insights queries.

        Returns maintenance cost breakdown by month and by service type for the entire fleet.
        """
        from datetime import datetime

        # Detect maintenance insights queries (fleet-wide, no specific vehicle)
        insight_keywords = ['insight', 'analyze', 'analysis', 'overview', 'summary', 'trend', 'generate']
        maintenance_keywords = ['maintenance', 'repair', 'service cost', 'servicing', 'cost']

        has_insight = any(kw in query_lower for kw in insight_keywords)
        has_maintenance = any(kw in query_lower for kw in maintenance_keywords)

        # Don't trigger if there's a specific vehicle number (let the other handler deal with it)
        has_vehicle = re.search(r'\d{5,8}', query_lower) is not None

        logger.info(f"Maintenance insights check: insight={has_insight}, maintenance={has_maintenance}, vehicle={has_vehicle}, query={query_lower[:50]}")

        if not (has_insight and has_maintenance) or has_vehicle:
            return None

        logger.info("Maintenance insights handler triggered")

        try:
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']

            # Query 1: Monthly maintenance costs for last 12 months
            sql_monthly = """
            SELECT
                es.reporting_period,
                SUM(es.total_monthly_cost) as total_cost,
                SUM(es.total_monthly_invoice) as total_invoice,
                COUNT(DISTINCT es.vehicle_id) as vehicles_serviced
            FROM fact_exploitation_services es
            WHERE es.service_code = 580
              AND es.reporting_period >= CAST(strftime('%Y%m', date('now', '-12 months')) AS INTEGER)
            GROUP BY es.reporting_period
            ORDER BY es.reporting_period
            """

            result = await self.db.execute(text(sql_monthly))
            columns = list(result.keys())
            monthly_data = [dict(zip(columns, row)) for row in result.fetchall()]

            # Query 2: Top customers by maintenance cost
            sql_top_customers = """
            SELECT
                v.customer_name,
                SUM(es.total_monthly_cost) as total_cost,
                COUNT(DISTINCT es.vehicle_id) as vehicle_count
            FROM fact_exploitation_services es
            JOIN dim_vehicle v ON es.vehicle_id = v.vehicle_id
            WHERE es.service_code = 580
              AND es.reporting_period >= CAST(strftime('%Y%m', date('now', '-12 months')) AS INTEGER)
            GROUP BY v.customer_name
            ORDER BY total_cost DESC
            LIMIT 5
            """

            result2 = await self.db.execute(text(sql_top_customers))
            columns2 = list(result2.keys())
            top_customers = [dict(zip(columns2, row)) for row in result2.fetchall()]

            if not monthly_data:
                return {
                    "data": [],
                    "message": "No maintenance cost data found for the last 12 months."
                }

            # Format monthly data for display
            chart_data = []
            for row in monthly_data:
                period = row.get('reporting_period', 0)
                if period:
                    year = period // 100
                    month = period % 100
                    row['month'] = f"{month_names[month]} {year}"
                    chart_data.append({
                        'month': row['month'],
                        'total_cost': row.get('total_cost', 0) or 0,
                        'vehicles_serviced': row.get('vehicles_serviced', 0) or 0
                    })

            # Calculate totals and insights
            total_cost = sum(r.get('total_cost', 0) or 0 for r in monthly_data)
            total_invoice = sum(r.get('total_invoice', 0) or 0 for r in monthly_data)
            total_vehicles = sum(r.get('vehicles_serviced', 0) or 0 for r in monthly_data)
            avg_monthly_cost = total_cost / len(monthly_data) if monthly_data else 0

            # Find peak month
            peak_month = max(monthly_data, key=lambda x: x.get('total_cost', 0) or 0)
            peak_period = peak_month.get('reporting_period', 0)
            peak_year = peak_period // 100
            peak_mon = peak_period % 100
            peak_month_name = f"{month_names[peak_mon]} {peak_year}"
            peak_cost = peak_month.get('total_cost', 0) or 0

            # Find trend (compare last 3 months to previous 3 months)
            if len(monthly_data) >= 6:
                recent_3 = sum(r.get('total_cost', 0) or 0 for r in monthly_data[-3:])
                previous_3 = sum(r.get('total_cost', 0) or 0 for r in monthly_data[-6:-3])
                if previous_3 > 0:
                    trend_pct = ((recent_3 - previous_3) / previous_3) * 100
                    trend_direction = "increased" if trend_pct > 0 else "decreased"
                else:
                    trend_pct = 0
                    trend_direction = "stable"
            else:
                trend_pct = 0
                trend_direction = "insufficient data"

            # Build message
            message = "**Maintenance Cost Insights (Last 12 Months)**\n\n"

            message += "**Summary:**\n"
            message += f"- **Total Maintenance Cost**: AED {total_cost:,.2f}\n"
            message += f"- **Total Invoiced**: AED {total_invoice:,.2f}\n"
            message += f"- **Average Monthly Cost**: AED {avg_monthly_cost:,.2f}\n"
            message += f"- **Vehicles Serviced**: {total_vehicles:,} service events\n\n"

            message += "**Monthly Breakdown:**\n"
            for row in monthly_data[-6:]:  # Last 6 months
                month_str = row.get('month', '')
                cost = row.get('total_cost', 0) or 0
                vehicles = row.get('vehicles_serviced', 0) or 0
                message += f"- **{month_str}**: AED {cost:,.2f} ({vehicles} vehicles)\n"

            if top_customers:
                message += "\n**Top 5 Customers by Maintenance Cost:**\n"
                for cust in top_customers:
                    name = cust.get('customer_name', 'Unknown')
                    cost = cust.get('total_cost', 0) or 0
                    count = cust.get('vehicle_count', 0) or 0
                    message += f"- **{name}**: AED {cost:,.2f} ({count} vehicles)\n"

            # Notable insight
            message += f"\n**Notable:** **{peak_month_name}** had the highest maintenance costs at **AED {peak_cost:,.2f}**."
            if trend_direction != "insufficient data":
                message += f" Recent trend shows costs have **{trend_direction}** by **{abs(trend_pct):.1f}%** compared to the previous quarter."
                if trend_pct > 15:
                    message += " Consider reviewing maintenance contracts or investigating potential issues."
                elif trend_pct < -15:
                    message += " Good progress on cost optimization."

            logger.info(f"Maintenance insights handler returning {len(chart_data)} months of data")
            return {
                "data": chart_data,
                "message": message
            }

        except Exception as e:
            logger.error(f"Maintenance insights query failed: {e}")
            return None

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
        """Get database schema information for SQL generation.

        Only includes semantic-layer tables (dim_*, fact_*, view_*, ref_*, agg_*)
        to keep the prompt focused and avoid confusion with raw landing/staging tables.
        """
        if settings.DATABASE_TYPE == "sqlite":
            # Only show semantic-layer tables/views — not landing_ or staging_ tables
            result = await self.db.execute(
                text("""SELECT name, type FROM sqlite_master
                        WHERE (type='table' OR type='view')
                          AND (name LIKE 'dim_%' OR name LIKE 'fact_%' OR name LIKE 'view_%'
                               OR name LIKE 'ref_%' OR name LIKE 'agg_%' OR name LIKE 'semantic_%')
                        ORDER BY name""")
            )
            tables = result.fetchall()

            info_lines = []
            for table_name, _ in tables:
                col_result = await self.db.execute(text(f"PRAGMA table_info('{table_name}')"))
                columns = col_result.fetchall()
                col_names = ", ".join(col[1] for col in columns)
                info_lines.append(f"- {table_name}: {col_names}")

            return "\n".join(info_lines)
        else:
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
        if settings.DATABASE_TYPE == "sqlite":
            # In SQLite, try to get columns directly from the table name
            try:
                result = await self.db.execute(text(f"PRAGMA table_info('{dataset}')"))
                columns = result.fetchall()
                if columns:
                    return ", ".join([f"{col[1]} ({col[2]})" for col in columns])
            except Exception:
                pass
            return "Unknown dataset"
        else:
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
