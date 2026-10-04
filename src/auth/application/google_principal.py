from src.auth.domain.entities import Principal
from src.auth.domain.interfaces import UserIdentityRepository
from src.auth.exceptions import AuthError, InvalidTokenError
from src.auth.infrastructure.google_oidc import GoogleIdTokenVerifier
from src.config.settings import settings


class GooglePrincipalResolver:
    '''
    Validates Google ID tokens and maps verified users to application principals.
    '''

    def __init__(
        self,
        verifier: GoogleIdTokenVerifier,
        user_repository: UserIdentityRepository,
    ) -> None:
        '''
        Args:
            verifier (GoogleIdTokenVerifier): Google JWKS ID token verifier.
            user_repository (UserIdentityRepository): Application user lookup.
        '''
        self._verifier = verifier
        self._users = user_repository
        self._client_id = settings.GOOGLE_OAUTH_CLIENT_ID

    def from_id_token(self, id_token: str) -> Principal:
        '''
        Verify Google ID token and build tenant-scoped principal.

        Args:
            id_token (str): Google OIDC ID token (Bearer value).

        Returns:
            Principal: Authenticated principal for the request.

        Raises:
            InvalidTokenError: If the Google token is invalid.
            AuthError: If the user is not registered.
        '''
        claims = self._verifier.verify(id_token)
        email = str(claims["email"])
        user = self._users.get_by_email(email)
        if user is None:
            raise AuthError("User is not registered in this application")
        return Principal(
            sub=user.email,
            tenant_id=user.tenant_id,
            client_id=self._client_id,
            role=user.role,
        )
