from decimal import Decimal
from typing import Optional, NewType

from sqlalchemy.orm import Session


AccountId = NewType("AccountId", str)
TransactionId = NewType("TransactionId", str)
Money = NewType("Money", Decimal)

class Transaction:
    pass


## API


class Ledger:
    def __init__(self, engine):
        self.engine = engine
        self.session = Session(self.engine)

    def __enter__(self):
        return self.session.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.commit()
        self.session.__exit__(exc_type, exc_val, exc_tb)

    def create_account(
        self,
        name: str,
    ) -> AccountId:
        return AccountId("")


    def get_or_create_account(
        self,
        name: str,
    ) -> AccountId:
        return AccountId("")


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
