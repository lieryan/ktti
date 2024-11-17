from decimal import Decimal
from typing import Optional, NewType
from uuid import UUID, uuid4
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from db import Account, Tx, TxType


AccountId = NewType("AccountId", UUID)
TransactionId = NewType("TransactionId", bytes)
Money = NewType("Money", Decimal)


@dataclass
class Balance:
    # Amount of money owned by the account
    current: Money

    # Amount of money that is available to use (available = current - pending)
    available: Money


class AutocommitSessionTransaction:
    """
    A mixin class that handles dealing opening and automatically committing
    database transaction when using an instance of the class as a context
    manager.
    """

    def __init__(self, engine):
        self.engine = engine
        self.session = Session(self.engine)

    def __enter__(self):
        return self.session.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.session.commit()
        else:
            self.session.rollback()
        self.session.__exit__(exc_type, exc_val, exc_tb)


## API


class Ledger(AutocommitSessionTransaction):
    def create_account(
        self,
        name: str,
    ) -> tuple[AccountId, TransactionId]:
        with self:
            obj = Account(name=name)

            self.session.add(obj)
            self.session.flush()

            new_account_tx = Tx(
                idempotency_key=uuid4(),
                account_id=obj.id,
                type=TxType.NEW_ACCOUNT,
                amount=Money(Decimal(0)),
                prev_tx_id=None,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            new_account_tx._set_transaction_hash()
            self.session.add(new_account_tx)

            return AccountId(obj.id), TransactionId(new_account_tx.id)

    def create_pending_transaction(
        self,
        idempotency_key: TransactionId,
        account_id: AccountId,
        amount: Money,
        prev_tx_id: Optional[TransactionId] = None,
    ):
        obj = Tx(
            idempotency_key=idempotency_key,
            account_id=account_id,
            type=TxType.PENDING,
            amount=amount,

            prev_tx_id=prev_tx_id,
            prev_current_balance=Money(Decimal(0)),
            prev_available_balance=Money(Decimal(0)),
            current_balance=Money(Decimal(0)),
            available_balance=Money(Decimal(0)),
        )
        obj._set_transaction_hash()

        with self:
            self.session.add(obj)
            self.session.flush()
            return TransactionId(obj.id)

    def settle_transaction(
        self,
        idempotency_key: TransactionId,
        pending_tx_id: TransactionId,
        prev_tx_id: Optional[TransactionId] = None,
    ):
        """
        Reflect the transaction amount to the current balance, if
        pending_tx_id is already a settled transaction, do nothing.
        """
        with self:
            pending_tx = self.session.get(Tx, pending_tx_id)
            settled_amount = pending_tx.amount  # TODO: calculate settled amount
            obj = Tx(
                idempotency_key=idempotency_key,
                account_id=pending_tx.account_id,
                type=TxType.SETTLEMENT,
                amount=settled_amount,

                prev_tx_id=prev_tx_id,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            obj._set_transaction_hash()

            self.session.add(obj)
            self.session.flush()
            return TransactionId(obj.id)

    def refund_pending_transaction(
        self,
        tx_id: TransactionId,
        amount: Optional[Money],
    ):
        """When `amount` is provided, do a partial refund."""
        pass

    def get_balance(
        self,
        account_id: AccountId,
    ) -> Balance:
        return Balance(
            current=Money(Decimal()),
            available=Money(Decimal()),
        )

    def list_transactions(
        self,
        account_id: AccountId,
    ) -> list[Tx]:
        return list(
            self.session.execute(
                select(Tx).where(Tx.account_id == account_id)
            ).scalars()
        )

    ### UI

    def print_transactions(
        self,
        account_id: AccountId,
    ) -> None:
        pass
