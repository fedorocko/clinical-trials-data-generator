"""
Manages Snowflake Users.

Example:
    >>> user = User("test_user")
    >>> created_user = root.users.create(user)
    >>> root.users["test_user"].fetch()
    >>> root.users["test_user"].delete()
"""

from public import public

from ._generated.models import ContainingScope as ContainingScope
from ._user import Grant, Securable, User, UserCollection, UserResource


public(
    User=User,
    UserCollection=UserCollection,
    UserResource=UserResource,
    Securable=Securable,
    Grant=Grant,
    ContainingScope=ContainingScope,
)
