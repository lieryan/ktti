from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, NewType, Any, Iterator
from uuid import UUID, uuid4

import sqlalchemy
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

    def __init__(self, engine: sqlalchemy.Engine):
        self.engine = engine
        self.session = Session(self.engine)

    def __enter__(self) -> sqlalchemy.orm.SessionTransaction:
        return self.session.begin()

    def __exit__(self, exc_type: Exception, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise
        else:
            self.session.rollback()
        self.session.__exit__(exc_type, exc_val, exc_tb)


## API


def ensure_idempotency_key(idempotency_key: Optional[UUID]) -> UUID:
    if idempotency_key is None:
        return uuid4()
    else:
        return idempotency_key


class Ledger(AutocommitSessionTransaction):
    def create_account(
        self,
        name: str,
        *,
        idempotency_key: Optional[UUID] = None,
    ) -> tuple[AccountId, TransactionId]:
        idempotency_key = ensure_idempotency_key(idempotency_key)
        with self:
            obj = Account(name=name)

            self.session.add(obj)
            self.session.flush()

            new_account_tx = Tx(
                idempotency_key=idempotency_key,
                account_id=obj.id,
                type=TxType.NEW_ACCOUNT,
                amount=0,
                pending_amount=0,
                group_tx_id=None,
                group_prev_tx_id=None,
                group_prev_pending_amount=0,
                prev_tx_id=None,
                prev_current_balance=0,
                prev_available_balance=0,
                current_balance=0,
                available_balance=0,
            )
            new_account_tx._set_transaction_hash()
            self.session.add(new_account_tx)

            return AccountId(obj.id), TransactionId(new_account_tx.id)

    def create_pending_transaction(
        self,
        account_id: AccountId,
        amount: Money,
        *,
        idempotency_key: Optional[UUID] = None,
        prev_tx_id: Optional[TransactionId] = None,
    ) -> TransactionId:
        idempotency_key = ensure_idempotency_key(idempotency_key)
        with self:
            obj = Tx(
                idempotency_key=idempotency_key,
                account_id=account_id,
                type=TxType.PENDING,
                amount=amount,
                pending_amount=amount,
            )
            obj._set_prev_tx(self.session.get(Tx, prev_tx_id))
            if obj.is_credit:
                obj.available_balance += amount
            obj._set_transaction_hash()
            obj._set_group_tx_root()

            self.session.add(obj)
            self.session.flush()
            return TransactionId(obj.id)

    def settle_transaction(
        self,
        group_tx_id: TransactionId,
        *,
        idempotency_key: Optional[UUID] = None,
        prev_tx_id: Optional[TransactionId] = None,
    ) -> TransactionId:
        """
        Reflect the transaction amount to the current balance, if
        group_tx_id already have a settled Tx, do nothing.
        """
        idempotency_key = ensure_idempotency_key(idempotency_key)
        with self:
            group_tx = self._get_group_tx(group_tx_id)
            obj = Tx(
                idempotency_key=idempotency_key,
                account_id=group_tx.account_id,
                type=TxType.SETTLEMENT,
            )
            obj._set_prev_tx(self.session.get(Tx, prev_tx_id))
            self._add_to_group(obj, group_tx)
            group_latest_tx_id = self.get_latest_group_transaction(group_tx)
            group_latest_tx = self.session.get(Tx, group_latest_tx_id)
            assert group_latest_tx is not None
            settled_amount = group_latest_tx.pending_amount
            obj.amount = settled_amount
            obj.pending_amount = settled_amount
            obj.current_balance += settled_amount
            if group_tx.is_debit:
                obj.available_balance += settled_amount
            obj._set_transaction_hash()

            self.session.add(obj)
            self.session.flush()
            return TransactionId(obj.id)

    def refund_pending_transaction(
        self,
        group_tx_id: TransactionId,
        amount: Optional[Money],
        *,
        idempotency_key: Optional[UUID] = None,
        prev_tx_id: Optional[TransactionId] = None,
    ) -> TransactionId:
        """If `amount` is provided, do a partial refund."""
        idempotency_key = ensure_idempotency_key(idempotency_key)
        assert amount, "automatic determination of amount is not yet supported"
        with self:
            group_tx = self._get_group_tx(group_tx_id)
            if not group_tx.is_credit:
                raise ValueError("Can only refund credit transaction.")

            obj = Tx(
                idempotency_key=idempotency_key,
                account_id=group_tx.account_id,
                type=TxType.REFUND,
                amount=amount,
            )
            obj._set_prev_tx(self.session.get(Tx, prev_tx_id))
            self._add_to_group(obj, group_tx)
            obj.pending_amount = obj.group_prev_pending_amount + amount
            obj.available_balance += amount
            obj._set_transaction_hash()

            self.session.add(obj)
            self.session.flush()
            return TransactionId(obj.id)


    def get_balance(
        self,
        account_id: AccountId,
    ) -> Balance:
        tx = self.session.get(Tx, self.get_latest_transaction(account_id))
        assert tx is not None
        return Balance(
            current=Money(tx.current_balance),
            available=Money(tx.available_balance),
        )

    def list_transactions(
        self,
        account_id: AccountId,
    ) -> list[Tx]:
        results = list(
            self.session.execute(
                select(Tx).where(Tx.account_id == account_id)
            ).scalars()
        )
        new_account_tx, = (tx for tx in results if tx.type == TxType.NEW_ACCOUNT)

        def iterate_sorted_chain(start: Tx) -> Iterator[Tx]:
            it: Optional[Tx] = start
            while it:
                yield it
                it = it.next_tx

        return list(iterate_sorted_chain(new_account_tx))

    def _get_group_tx(self, group_tx_id: TransactionId) -> Tx:
        group_tx = self.session.get(Tx, group_tx_id)
        if group_tx is None:
            raise ValueError(f"Transaction group {group_tx_id!r} does not exist.")
        if group_tx.type != TxType.PENDING:
            raise ValueError(f"Transaction {group_tx_id!r} is not a Group ID.")
        return group_tx

    def _add_to_group(self, obj: Tx, group_tx: Tx) -> None:
        if group_tx.type != TxType.PENDING:
            raise ValueError("group_tx must be the pending transaction of the group")

        group_latest_tx_id = self.get_latest_group_transaction(group_tx)
        group_latest_tx = self.session.get(Tx, group_latest_tx_id)
        assert group_latest_tx is not None
        obj.group_tx_id = group_tx.id
        obj.group_prev_tx_id = group_latest_tx_id
        obj.group_prev_pending_amount = group_latest_tx.pending_amount

    def get_latest_transaction(
        self,
        account_id: AccountId,
    ) -> TransactionId:
        """Find the Tx that is never referenced by other Tx.prev_id, this is always the latest Tx"""
        # FIXME: Calculating the head of the transactions in this way can be
        #        slow if there's a lot of transactions in the account.
        #        We would need to record the head of the log in the accounts
        #        table to optimize this lookup.
        tx_ids = set(self.session.execute(select(Tx.id).where(Tx.account_id == account_id)).scalars())
        prev_tx_ids = set(self.session.execute(select(Tx.prev_tx_id).where(Tx.account_id == account_id)).scalars())
        latest_tx, = tx_ids - prev_tx_ids
        return TransactionId(latest_tx)

    def get_latest_group_transaction(
        self,
        group_tx: Tx,
    ) -> TransactionId:
        """Find the Tx in the Tx group that is never referenced by other
        Tx.group_prev_tx_id, this is always the latest Tx for that group"""
        # FIXME: Calculating the head of the transactions in this way can be
        #        slow if there's a lot of transactions in the account.
        #        We would need to record the head of the log in the accounts
        #        table to optimize this lookup.
        assert group_tx is not None and group_tx.type == TxType.PENDING
        tx_ids = set(self.session.execute(select(Tx.id).where(Tx.group_tx_id == group_tx.id)).scalars())
        prev_tx_ids = set(self.session.execute(select(Tx.group_prev_tx_id).where(Tx.group_tx_id == group_tx.id)).scalars())
        latest_tx, = tx_ids - prev_tx_ids
        return TransactionId(latest_tx)

    ### UI

    def print_transactions(
        self,
        account_id: AccountId,
    ) -> None:
        pass
