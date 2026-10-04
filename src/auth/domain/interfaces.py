from abc import ABC, abstractmethod
from typing import Optional

from src.auth.domain.entities import UserIdentity


class UserIdentityRepository(ABC):
    '''
    Port for loading user identity (tenant and role) by email.
    '''

    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserIdentity]:
        '''
        Look up a user by email address.

        Args:
            email (str): User email from the Google ID token.

        Returns:
            Optional[UserIdentity]: User record if registered, else None.
        '''
        pass
