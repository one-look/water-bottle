class AuthError(Exception):
    '''
    Base exception for OAuth2/OIDC failures in the auth module.
    '''


class InvalidTokenError(AuthError):
    '''
    Raised when a Google OIDC ID token fails validation.
    '''
