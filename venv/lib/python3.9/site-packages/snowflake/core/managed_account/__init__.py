"""Manages Snowflake ManagedAccounts.

Example:
    >>> managed_account_collection = root.managed_accounts
    >>> managed_account = ManagedAccount(
    ...     name="managed_account_name",
    ...     admin_name = "admin"
    ...     admin_password = 'TestPassword1'
    ...     account_type = "READER"
    ...  )
    >>> managed_account_collection.create(managed_account)

Refer to :class:`snowflake.core.Root` to create the ``root``.
"""

from public import public

from ._managed_account import ManagedAccount, ManagedAccountCollection, ManagedAccountResource


public(
    ManagedAccount=ManagedAccount,
    ManagedAccountCollection=ManagedAccountCollection,
    ManagedAccountResource=ManagedAccountResource,
)
