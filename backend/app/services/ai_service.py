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
                    # Auto-detect chart visualization
                    chart_config = self._detect_chart_config(result["data"], user_message)
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
        # Ensure domain knowledge is cached
        await self._load_domain_knowledge(self.db)

        # Get available tables/views (structural reference)
        schema_info = await self._get_schema_info()

        # Get semantic domain block (dynamic from cache, or static fallback)
        domain_block = self._domain_cache.get("sql_prompt_domain_block") or self._get_static_fallback_sql_domain()

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
  Compare with >= / <= operators on TEXT columns""")

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
- fact_odometer_reading (Odometer Readings): Historical mileage readings  [grain: One row per reading]
- fact_billing (Billing Records): Billing transactions  [grain: One row per billing record]

=== KEY RELATIONSHIPS ===
- dim_vehicle.customer_id -> dim_customer.customer_id (many-to-one)
- dim_driver.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- dim_contract.customer_id -> dim_customer.customer_id (many-to-one)
- dim_vehicle.contract_position_number -> dim_contract.contract_position_number (many-to-one)
- fact_odometer_reading.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- fact_billing.vehicle_id -> dim_vehicle.vehicle_id (many-to-one)
- fact_billing.customer_id -> dim_customer.customer_id (many-to-one)

=== BUSINESS RULES ===
Vehicle Status Codes (dim_vehicle.vehicle_status_code):
  0 = Created, 1 = Active, 2 = Terminated - Invoicing Stopped
  3 = Terminated - Invoice Adjustment, 4 = Terminated - Mileage Adjustment
  5 = Terminated - De-investment, 8 = Terminated - Ready for Settlement
  9 = Terminated - Final Settlement
  Active vehicles: is_active = 1 (status IN (0,1))
  Terminated: is_active = 0 (status >= 2)

Lease Types: O = Operational, F = Financial, L = Lease
Primary driver: driver_sequence = 1 or is_primary_driver = 1
Active contract: is_active = 1 or contract_status = 'Active'
Dates: TEXT YYYY-MM-DD, use date('now'), date('now', '-12 months')

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
- view_account_manager_portfolio: Customers and vehicles per account manager"""

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
- Suggest relevant follow-up questions
- If unsure, ask for clarification
- Never reveal system internals or database structure"""

    def _detect_chart_config(self, data: list, user_message: str) -> dict | None:
        """Auto-detect appropriate chart type from data shape and query"""
        if not data or len(data) == 0:
            return None

        first_row = data[0]
        keys = list(first_row.keys())

        numeric_keys = []
        categorical_keys = []
        date_keys = []

        for key in keys:
            sample = next((row[key] for row in data if row.get(key) is not None), None)
            if sample is None:
                categorical_keys.append(key)
                continue
            if isinstance(sample, (int, float)):
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
            return {
                "chart_type": "line",
                "x_axis_key": date_keys[0],
                "y_axis_keys": numeric_keys[:3],
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
            return {
                "chart_type": "bar",
                "x_axis_key": categorical_keys[0],
                "y_axis_keys": numeric_keys[:4],
            }

        # Only numerics, multiple rows → line chart
        if numeric_keys and row_count > 2:
            return {
                "chart_type": "line",
                "x_axis_key": keys[0],
                "y_axis_keys": numeric_keys[:3],
            }

        return None

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
        if settings.DATABASE_TYPE == "sqlite":
            result = await self.db.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            )
            tables = result.fetchall()

            info_lines = []
            for (table_name,) in tables:
                if table_name.startswith('sqlite_'):
                    continue
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
