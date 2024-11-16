from decimal import Decimal
from typing import Optional, NewType
from uuid import uuid4, UUID
from db import Account

from sqlalchemy.orm import Session


AccountId = NewType("AccountId", UUID)
TransactionId = NewType("TransactionId", str)
Money = NewType("Money", Decimal)

class Transaction:
    pass


class AutocommitSessionTransaction:
    """
    A mixin class that handles dealing opening and automatically committing
    database transaction when using an instance of the class as a context
    manager.
    """
    def __enter__(self):
        return self.session.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.session.commit()
        self.session.__exit__(exc_type, exc_val, exc_tb)


## API


class Ledger(AutocommitSessionTransaction):
    def __init__(self, engine):
        self.engine = engine
        self.session = Session(self.engine)

    def create_account(
        self,
        name: str,
    ) -> AccountId:
        obj = Account(name=name)
        with self as session_transaction:
            self.session.add(obj)
            self.session.flush()
            return AccountId(obj.id)


    def create_pending_transaction(
        self,
        tx_id: TransactionId,
        account_id: AccountId,
        amount: Money,
        last_tx_id: Optional[TransactionId],
    ):
        pass


    def settle_pending_transaction(
        self,
        tx_id: TransactionId,
    ):
        """
        Reflect the transaction amount to the current balance, if tx_id is already
        a settled transaction, do nothing.
        """
        pass


    def refund_pending_transaction(
        self,
        tx_id: TransactionId,
        amount: Optional[Money],
    ):
        """When `amount` is provided, do a partial refund."""
        pass


    def get_current_balance(
        self,
        account_id: AccountId,
    ) -> Money:
        return Money(Decimal())


    def get_available_balance(
        self,
        account_id: AccountId,
    ) -> Money:
        return Money(Decimal())


    def list_transactions(
        self,
        account_id: AccountId,
    ) -> Transaction:
        return Transaction()


    ### UI

    def print_transaction(
        self,
        account_id: AccountId,
    ) -> Transaction:
        return Transaction()
