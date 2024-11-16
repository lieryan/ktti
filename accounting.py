from typing import Optional, NewType
from db import connect
from decimal import Decimal


db = connect()

AccountId = NewType("AccountId", str)
TransactionId = NewType("TransactionId", str)
Money = NewType("Money", Decimal)

class Transaction:
    pass


## API


def get_or_create_account(
    name: str,
) -> AccountId:
    return AccountId("")


def create_pending_transaction(
    tx_id: TransactionId,
    account_id: AccountId,
    amount: Money,
    last_tx_id: Optional[TransactionId],
):
    pass


def settle_pending_transaction(
    tx_id: TransactionId,
):
    """
    Reflect the transaction amount to the current balance, if tx_id is already
    a settled transaction, do nothing.
    """
    pass


def refund_pending_transaction(
    tx_id: TransactionId,
    amount: Optional[Money],
):
    """When `amount` is provided, do a partial refund."""
    pass


def get_current_balance(
    account_id: AccountId,
) -> Money:
    return Money(Decimal())


def get_available_balance(
    account_id: AccountId,
) -> Money:
    return Money(Decimal())


def list_transactions(
    account_id: AccountId,
) -> Transaction:
    return Transaction()


### UI

def print_transaction(
    account_id: AccountId,
) -> Transaction:
    return Transaction()
