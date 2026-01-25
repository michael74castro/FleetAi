"""
FleetAI - Security utilities
JWT handling, password hashing, and security helpers
"""

from datetime import datetime, timedelta
from typing import Any, Optional
import hashlib
import secrets

from jose import JWTError, jwt
import hashlib as _hashlib
from pydantic import BaseModel

from .config import settings


# Simple password hashing for development (use bcrypt in production)
class _SimplePwdContext:
    """Simple password context using hashlib for development"""
    def hash(self, password: str) -> str:
        return _hashlib.sha256(password.encode()).hexdigest()

    def verify(self, plain_password: str, hashed_password: str) -> bool:
        return _hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password

pwd_context = _SimplePwdContext()

# JWT Configuration
ALGORITHM = "HS256"
SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES


class TokenPayload(BaseModel):
    """JWT Token payload structure"""
    sub: str  # User ID or Azure AD Object ID
    email: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    customer_ids: Optional[list[str]] = None
    exp: Optional[datetime] = None
    iat: Optional[datetime] = None
    type: str = "access"  # access or refresh


class TokenData(BaseModel):
    """Decoded token data"""
    user_id: str
    email: Optional[str] = None
    role: Optional[str] = None
    customer_ids: list[str] = []


def create_access_token(
    data: dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT access token.

    Args:
        data: Payload data to encode
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "access"
    })

    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(
    data: dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT refresh token.

    Args:
        data: Payload data to encode
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT refresh token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "refresh"
    })

    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[TokenPayload]:
    """
    Decode and validate a JWT token.

    Args:
        token: JWT token string

    Returns:
        TokenPayload if valid, None otherwise
    """
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        return TokenPayload(**payload)
    except JWTError:
        return None


def verify_token(token: str, token_type: str = "access") -> Optional[TokenData]:
    """
    Verify a token and extract user data.

    Args:
        token: JWT token string
        token_type: Expected token type (access or refresh)

    Returns:
        TokenData if valid, None otherwise
    """
    payload = decode_token(token)
    if payload is None:
        return None

    if payload.type != token_type:
        return None

    if payload.exp and datetime.utcnow() > payload.exp:
        return None

    return TokenData(
        user_id=payload.sub,
        email=payload.email,
        role=payload.role,
        customer_ids=payload.customer_ids or []
    )


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)


def generate_token_hash(token: str) -> str:
    """Generate a hash of a token for storage"""
    return hashlib.sha256(token.encode()).hexdigest()


def generate_secure_random_string(length: int = 32) -> str:
    """Generate a cryptographically secure random string"""
    return secrets.token_urlsafe(length)


def generate_api_key() -> tuple[str, str]:
    """
    Generate an API key and its hash.

    Returns:
        Tuple of (api_key, api_key_hash)
    """
    api_key = f"fleetai_{secrets.token_urlsafe(32)}"
    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    return api_key, api_key_hash


def verify_api_key(api_key: str, api_key_hash: str) -> bool:
    """Verify an API key against its stored hash"""
    return hashlib.sha256(api_key.encode()).hexdigest() == api_key_hash


class RateLimiter:
    """Simple in-memory rate limiter"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: dict[str, list[datetime]] = {}

    def is_allowed(self, key: str) -> bool:
        """Check if a request is allowed for the given key"""
        now = datetime.utcnow()
        window_start = now - timedelta(seconds=self.window_seconds)

        if key not in self._requests:
            self._requests[key] = []

        # Clean old requests
        self._requests[key] = [
            req_time for req_time in self._requests[key]
            if req_time > window_start
        ]

        # Check limit
        if len(self._requests[key]) >= self.max_requests:
            return False

        # Record this request
        self._requests[key].append(now)
        return True

    def reset(self, key: str) -> None:
        """Reset rate limit for a key"""
        if key in self._requests:
            del self._requests[key]


# Global rate limiter instance
rate_limiter = RateLimiter(
    max_requests=settings.RATE_LIMIT_REQUESTS,
    window_seconds=settings.RATE_LIMIT_PERIOD_SECONDS
)
