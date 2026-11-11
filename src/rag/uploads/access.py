'''Role checks for chat document upload.'''

from typing import Literal

from fastapi import HTTPException, status

from src.auth.domain.entities import Principal

UploadScope = Literal["tenant", "session"]

_EMPLOYEE_ROLES = frozenset({"employee", "office", "staff"})


def upload_scope_for_principal(principal: Principal | None) -> UploadScope:
    '''
    Resolve ingest scope from authenticated role.

    Raises:
        HTTPException: Not signed in or role cannot upload.
    '''
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sign in required to upload documents",
            headers={"WWW-Authenticate": "Bearer"},
        )
    role = (principal.role or "").strip().lower()
    if role == "teacher":
        return "session"
    if role in _EMPLOYEE_ROLES:
        return "tenant"
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only teachers and employees can upload documents",
    )
