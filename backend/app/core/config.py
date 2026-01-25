"""
FleetAI - Application Configuration
Centralized configuration management using Pydantic Settings
"""

from functools import lru_cache
from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "FleetAI"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"  # development, staging, production

    # API
    API_V1_PREFIX: str = "/api/v1"
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:3003", "http://localhost:5173"]

    # Azure AD Authentication (optional for local testing)
    AZURE_TENANT_ID: Optional[str] = Field(default=None, description="Azure AD Tenant ID")
    AZURE_CLIENT_ID: Optional[str] = Field(default=None, description="Azure AD Client ID")
    AZURE_CLIENT_SECRET: Optional[str] = Field(default=None, description="Azure AD Client Secret")
    AZURE_OPENAPI_CLIENT_ID: Optional[str] = Field(default=None, description="OpenAPI Swagger Client ID")

    # Database Configuration
    # Use DATABASE_TYPE=sqlite for local testing, DATABASE_TYPE=mssql for production
    DATABASE_TYPE: str = Field(default="sqlite", description="Database type: sqlite or mssql")
    SQLITE_PATH: str = Field(default="./fleetai.db", description="SQLite database file path")

    # MSSQL Settings (required only if DATABASE_TYPE=mssql)
    MSSQL_SERVER: Optional[str] = Field(default=None, description="MSSQL Server hostname")
    MSSQL_DATABASE: str = Field(default="FleetAI", description="MSSQL Database name")
    MSSQL_USERNAME: Optional[str] = Field(default=None, description="MSSQL Username")
    MSSQL_PASSWORD: Optional[str] = Field(default=None, description="MSSQL Password")
    MSSQL_DRIVER: str = Field(default="ODBC Driver 18 for SQL Server")
    MSSQL_POOL_SIZE: int = Field(default=10, ge=1, le=50)
    MSSQL_MAX_OVERFLOW: int = Field(default=20, ge=0, le=100)

    # Azure OpenAI (optional for local testing)
    AZURE_OPENAI_ENDPOINT: Optional[str] = Field(default=None, description="Azure OpenAI Endpoint URL")
    AZURE_OPENAI_API_KEY: Optional[str] = Field(default=None, description="Azure OpenAI API Key")
    AZURE_OPENAI_API_VERSION: str = Field(default="2024-02-15-preview")
    AZURE_OPENAI_DEPLOYMENT_NAME: str = Field(default="gpt-4")
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = Field(default="text-embedding-ada-002")

    # Session & Security
    SECRET_KEY: str = Field(..., min_length=32, description="Secret key for JWT signing")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=480)  # 8 hours
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=7)

    # File Storage
    UPLOAD_DIR: str = Field(default="./uploads")
    EXPORT_DIR: str = Field(default="./exports")
    MAX_UPLOAD_SIZE_MB: int = Field(default=50)

    # Email (for report scheduling)
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = Field(default=587)
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SMTP_FROM_EMAIL: str = Field(default="noreply@fleetai.local")
    SMTP_USE_TLS: bool = Field(default=True)

    # Logging
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FORMAT: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Rate Limiting
    RATE_LIMIT_REQUESTS: int = Field(default=100)
    RATE_LIMIT_PERIOD_SECONDS: int = Field(default=60)

    @property
    def database_url(self) -> str:
        """Get database URL based on DATABASE_TYPE"""
        if self.DATABASE_TYPE == "sqlite":
            return f"sqlite:///{self.SQLITE_PATH}"
        return self.mssql_connection_string

    @property
    def async_database_url(self) -> str:
        """Get async database URL based on DATABASE_TYPE"""
        if self.DATABASE_TYPE == "sqlite":
            return f"sqlite+aiosqlite:///{self.SQLITE_PATH}"
        return self.async_mssql_connection_string

    @property
    def mssql_connection_string(self) -> str:
        """Build MSSQL connection string for SQLAlchemy"""
        if not all([self.MSSQL_SERVER, self.MSSQL_USERNAME, self.MSSQL_PASSWORD]):
            raise ValueError("MSSQL settings required when DATABASE_TYPE=mssql")
        return (
            f"mssql+pyodbc://{self.MSSQL_USERNAME}:{self.MSSQL_PASSWORD}"
            f"@{self.MSSQL_SERVER}/{self.MSSQL_DATABASE}"
            f"?driver={self.MSSQL_DRIVER.replace(' ', '+')}"
            f"&TrustServerCertificate=yes"
        )

    @property
    def async_mssql_connection_string(self) -> str:
        """Build async MSSQL connection string"""
        if not all([self.MSSQL_SERVER, self.MSSQL_USERNAME, self.MSSQL_PASSWORD]):
            raise ValueError("MSSQL settings required when DATABASE_TYPE=mssql")
        return (
            f"mssql+aioodbc://{self.MSSQL_USERNAME}:{self.MSSQL_PASSWORD}"
            f"@{self.MSSQL_SERVER}/{self.MSSQL_DATABASE}"
            f"?driver={self.MSSQL_DRIVER.replace(' ', '+')}"
            f"&TrustServerCertificate=yes"
        )

    @field_validator('CORS_ORIGINS', mode='before')
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(',')]
        return v

    @field_validator('LOG_LEVEL')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of {valid_levels}')
        return v.upper()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class TestSettings(Settings):
    """Test environment settings"""
    ENVIRONMENT: str = "test"
    DEBUG: bool = True
    MSSQL_DATABASE: str = "FleetAI_Test"

    class Config:
        env_file = ".env.test"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Export singleton
settings = get_settings()
