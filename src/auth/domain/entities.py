from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class UserIdentity:
    '''
    Application user record resolved from the identity store.
    '''

    email: str
    tenant_id: str
    role: str


@dataclass(frozen=True)
class Principal:
    '''
    Authenticated request principal derived from a verified Google ID token.
    '''

    sub: str
    tenant_id: str
    client_id: str
    role: Optional[str] = None
