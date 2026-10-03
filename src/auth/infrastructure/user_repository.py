from typing import Optional

from src.auth.domain.entities import UserIdentity
from src.auth.domain.interfaces import UserIdentityRepository
from src.database.dbfile import USERS_DB


class DbFileUserRepository(UserIdentityRepository):
    '''
    User identity repository backed by the temporary dbfile dictionary.
    '''

    def get_by_email(self, email: str) -> Optional[UserIdentity]:
        '''
        Args:
            email (str): User email address.

        Returns:
            Optional[UserIdentity]: User record if found, else None.
        '''
        candidate = email.strip()
        row = USERS_DB.get(candidate)
        matched_email = candidate
        if row is None:
            for key, value in USERS_DB.items():
                if key.lower() == candidate.lower():
                    row = value
                    matched_email = key
                    break
        if row is None:
            return None
        return UserIdentity(
            email=matched_email,
            tenant_id=row["tenant_id"],
            role=row["role"],
        )
