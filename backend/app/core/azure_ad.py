"""
FleetAI - Azure AD / Entra ID Authentication
Handles Azure AD OAuth2 authentication and token validation
"""

from typing import Optional
import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2AuthorizationCodeBearer
from jose import JWTError, jwt
from pydantic import BaseModel
import httpx

from .config import settings

logger = logging.getLogger(__name__)


class AzureADUser(BaseModel):
    """Azure AD user information from token"""
    oid: str  # Object ID
    sub: str  # Subject
    name: Optional[str] = None
    preferred_username: Optional[str] = None
    email: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    roles: list[str] = []
    groups: list[str] = []


class AzureADConfig:
    """Azure AD configuration and endpoints"""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: Optional[str] = None
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        # Azure AD endpoints
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.authorization_url = f"{self.authority}/oauth2/v2.0/authorize"
        self.token_url = f"{self.authority}/oauth2/v2.0/token"
        self.jwks_url = f"{self.authority}/discovery/v2.0/keys"
        self.issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"

        # Cache for JWKS
        self._jwks_cache: Optional[dict] = None

    async def get_jwks(self) -> dict:
        """Fetch JSON Web Key Set from Azure AD"""
        if self._jwks_cache is None:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.jwks_url)
                response.raise_for_status()
                self._jwks_cache = response.json()
        return self._jwks_cache

    def clear_jwks_cache(self):
        """Clear JWKS cache to force refresh"""
        self._jwks_cache = None


# Global Azure AD configuration
azure_ad_config = AzureADConfig(
    tenant_id=settings.AZURE_TENANT_ID,
    client_id=settings.AZURE_CLIENT_ID,
    client_secret=settings.AZURE_CLIENT_SECRET
)

# OAuth2 scheme for Swagger UI
oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl=azure_ad_config.authorization_url,
    tokenUrl=azure_ad_config.token_url,
    scopes={
        f"api://{settings.AZURE_CLIENT_ID}/FleetAI.Read": "Read access to FleetAI API",
        f"api://{settings.AZURE_CLIENT_ID}/FleetAI.Write": "Write access to FleetAI API",
    }
)


async def validate_azure_ad_token(token: str) -> AzureADUser:
    """
    Validate an Azure AD access token and extract user information.

    Args:
        token: JWT access token from Azure AD

    Returns:
        AzureADUser with validated user information

    Raises:
        HTTPException: If token is invalid
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        # Decode token header to get key ID
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        if not kid:
            logger.warning("Token missing key ID (kid)")
            raise credentials_exception

        # Get signing keys from Azure AD
        jwks = await azure_ad_config.get_jwks()
        signing_key = None

        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                signing_key = key
                break

        if not signing_key:
            # Refresh JWKS cache and try again
            azure_ad_config.clear_jwks_cache()
            jwks = await azure_ad_config.get_jwks()

            for key in jwks.get("keys", []):
                if key.get("kid") == kid:
                    signing_key = key
                    break

        if not signing_key:
            logger.warning(f"Signing key not found for kid: {kid}")
            raise credentials_exception

        # Decode and validate token
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=settings.AZURE_CLIENT_ID,
            issuer=azure_ad_config.issuer,
            options={
                "verify_exp": True,
                "verify_aud": True,
                "verify_iss": True,
            }
        )

        # Extract user information
        user = AzureADUser(
            oid=payload.get("oid", ""),
            sub=payload.get("sub", ""),
            name=payload.get("name"),
            preferred_username=payload.get("preferred_username"),
            email=payload.get("email") or payload.get("preferred_username"),
            given_name=payload.get("given_name"),
            family_name=payload.get("family_name"),
            roles=payload.get("roles", []),
            groups=payload.get("groups", []),
        )

        return user

    except JWTError as e:
        logger.warning(f"JWT validation error: {e}")
        raise credentials_exception
    except httpx.HTTPError as e:
        logger.error(f"Error fetching JWKS: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable"
        )


async def get_current_user(
    token: str = Depends(oauth2_scheme)
) -> AzureADUser:
    """
    Dependency to get current authenticated user from Azure AD token.

    Usage:
        @app.get("/protected")
        async def protected_route(user: AzureADUser = Depends(get_current_user)):
            return {"user": user.email}
    """
    return await validate_azure_ad_token(token)


async def get_optional_user(
    token: Optional[str] = Depends(oauth2_scheme)
) -> Optional[AzureADUser]:
    """
    Dependency to get optional user (allows anonymous access).

    Usage:
        @app.get("/semi-protected")
        async def semi_protected(user: Optional[AzureADUser] = Depends(get_optional_user)):
            if user:
                return {"user": user.email}
            return {"user": "anonymous"}
    """
    if not token:
        return None
    try:
        return await validate_azure_ad_token(token)
    except HTTPException:
        return None


class RoleChecker:
    """
    Dependency class to check if user has required roles.

    Usage:
        admin_required = RoleChecker(["admin"])

        @app.get("/admin-only")
        async def admin_route(user: AzureADUser = Depends(admin_required)):
            return {"admin": True}
    """

    def __init__(self, required_roles: list[str]):
        self.required_roles = required_roles

    async def __call__(
        self,
        user: AzureADUser = Depends(get_current_user)
    ) -> AzureADUser:
        # Check if user has any of the required roles
        if not any(role in user.roles for role in self.required_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"User does not have required role. Required: {self.required_roles}"
            )
        return user


# Pre-configured role checkers
require_admin = RoleChecker(["admin", "Admin", "FleetAI.Admin"])
require_super_user = RoleChecker(["admin", "super_user", "FleetAI.SuperUser"])
require_account_manager = RoleChecker(["admin", "super_user", "account_manager", "FleetAI.AccountManager"])


async def get_user_permissions(user: AzureADUser) -> list[str]:
    """
    Get list of permissions based on user's Azure AD roles.

    This maps Azure AD roles to application permissions.
    """
    role_permissions = {
        "FleetAI.Admin": [
            "admin:users", "admin:roles", "admin:system",
            "dashboard:*", "report:*", "data:view_all", "ai:*"
        ],
        "FleetAI.SuperUser": [
            "dashboard:*", "report:*", "data:view_all", "ai:*"
        ],
        "FleetAI.AccountManager": [
            "dashboard:view", "dashboard:create", "dashboard:share",
            "report:view", "report:create", "report:export", "report:schedule",
            "data:view_assigned", "ai:assistant", "ai:analytics"
        ],
        "FleetAI.FleetAdmin": [
            "dashboard:view", "dashboard:create",
            "report:view", "report:create", "report:export",
            "data:view_company", "ai:assistant"
        ],
        "FleetAI.ClientContact": [
            "dashboard:view", "report:view", "report:export",
            "data:view_company", "ai:assistant"
        ],
        "FleetAI.Driver": [
            "dashboard:view", "report:view", "data:view_own"
        ],
    }

    permissions = set()
    for role in user.roles:
        if role in role_permissions:
            for perm in role_permissions[role]:
                if perm.endswith(":*"):
                    # Wildcard permission
                    base = perm[:-2]
                    permissions.update([
                        f"{base}:view", f"{base}:create",
                        f"{base}:edit", f"{base}:delete", f"{base}:share"
                    ])
                else:
                    permissions.add(perm)

    return list(permissions)


class PermissionChecker:
    """
    Dependency class to check if user has required permissions.

    Usage:
        can_create_dashboard = PermissionChecker("dashboard:create")

        @app.post("/dashboards")
        async def create_dashboard(user: AzureADUser = Depends(can_create_dashboard)):
            return {"created": True}
    """

    def __init__(self, required_permission: str):
        self.required_permission = required_permission

    async def __call__(
        self,
        user: AzureADUser = Depends(get_current_user)
    ) -> AzureADUser:
        permissions = await get_user_permissions(user)

        if self.required_permission not in permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied. Required: {self.required_permission}"
            )
        return user
