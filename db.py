from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Optional
from uuid import UUID, uuid4

import sqlalchemy
from sqlalchemy import (
    create_engine,
    String,
    ForeignKey,
    ForeignKeyConstraint,
    UniqueConstraint,
    CheckConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "account"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(30), unique=True)

    def __repr__(self) -> str:
        return f"<Account {self.id} name={self.name}>"


class TxType(Enum):
    NEW_ACCOUNT = "n"
    PENDING = "p"
    REFUND = "r"
    SETTLEMENT = "s"


class Tx(Base):
    __tablename__ = "tx"
    __table_args__ = (
        # prev_tx_id forms a chain of Tx that are in the order of their
        # transaction requests received or processed by the ledger.
        # all Tx related through prev_tx_id chain must belong to the same
        # account
        ForeignKeyConstraint(
            [
                "account_id",
                "prev_tx_id",
            ],
            [
                "tx.account_id",
                "tx.id",
            ],
        ),
        UniqueConstraint(
            "account_id",
            "id",
        ),

        # Tx with the same group_tx_id forms a Tx group that consists of
        #
        # - exactly one pending transaction
        # - one or more refund transactions
        # - at most one settlement transaction
        #
        # The settlement tx of a group closes the group from further
        # alterations.
        #
        # All Tx in the group must belong to the same account.
        ForeignKeyConstraint(
            [
                "account_id",
                "group_tx_id",
                "group_prev_tx_id",
            ],
            [
                "tx.account_id",
                "tx.group_tx_id",
                "tx.id",
            ],
        ),
        UniqueConstraint(
            "account_id",
            "group_tx_id",
            "id",
        ),
        # TODO: add a constraint/trigger to check that the original tx must be
        #       a pending Tx

        # enforce that original_tx_pending_amount are correctly
        # denormalized/duplicated through group_tx_id chain
        ForeignKeyConstraint(
            [
                "group_prev_tx_id",
                "group_prev_pending_amount",
            ],
            [
                "tx.id",
                "tx.pending_amount",
            ],
        ),
        UniqueConstraint("id", "pending_amount"),

        # enforce that prev_current_balance and prev_available_balance are
        # denormalized/duplicated correctly from their prev_tx these are done
        # so we can let the database enforce check constraint against the
        # previous tx balances
        ForeignKeyConstraint(
            [
                "prev_tx_id",
                "prev_current_balance",
                "prev_available_balance",
            ],
            [
                "tx.id",
                "tx.current_balance",
                "tx.available_balance",
            ],
        ),
        UniqueConstraint("id", "current_balance", "available_balance"),

        # don't allow more than one one NEW_ACCOUNT transaction for each Account
        Index("tx_only_one_new_account_tx_per_account_id", "account_id", unique=True, postgresql_where="type = 'NEW_ACCOUNT'"),
        # don't allow more than one one SETTLEMENT transaction for each PENDING transaction
        Index("tx_only_one_settlement_per_pending", "group_tx_id", unique=True, postgresql_where="type = 'SETTLEMENT'"),

        # only NEW_ACCOUNT transaction can have empty prev_tx_id
        CheckConstraint("type = 'NEW_ACCOUNT' OR prev_tx_id IS NOT NULL", name="tx_require_prev_tx_id"),

        # only NEW_ACCOUNT and PENDING transaction can have empty group_prev_tx_id
        CheckConstraint("type = 'NEW_ACCOUNT' OR type = 'PENDING' OR group_prev_tx_id IS NOT NULL", name="tx_require_group_prev_tx_id"),

        # only NEW_ACCOUNT can have empty group_tx_id
        CheckConstraint("type = 'NEW_ACCOUNT' OR group_tx_id IS NOT NULL", name="tx_require_group_tx_id"),

        # balances should never go negative, the prev_* balance does not
        # require their own constraint since they are always checked against by
        # foreign key constraint
        CheckConstraint("current_balance >= 0", name="tx_positive_current_balance"),
        CheckConstraint("available_balance >= 0", name="tx_positive_available_balance"),
        CheckConstraint("available_balance <= current_balance", name="tx_available_always_lt_current"),

        # The expression `NOT (X) OR (Y)` is basically `IF (X) THEN (Y)`
        CheckConstraint("NOT (type = 'NEW_ACCOUNT') OR (amount = 0 AND pending_amount = 0 AND current_balance = 0 AND available_balance = 0 AND prev_current_balance = 0 AND prev_available_balance = 0)", name="tx_new_account_starts_with_zero_balance"),
        CheckConstraint("NOT (type = 'SETTLEMENT') OR (amount = pending_amount)", name="tx_pending_amount_equals_amount"),
        CheckConstraint("NOT (type = 'PENDING' AND amount > 0) OR (pending_amount = amount AND current_balance = prev_current_balance AND available_balance = prev_available_balance)", name="tx_pending_debit_does_not_change_balance"),
        CheckConstraint("NOT (type = 'PENDING' AND amount < 0) OR (pending_amount = amount AND current_balance = prev_current_balance AND available_balance = prev_available_balance + amount)", name="tx_pending_credit_reduces_available_balance"),
        CheckConstraint("NOT (type = 'SETTLEMENT' AND amount > 0) OR (current_balance = prev_current_balance + pending_amount AND available_balance = prev_available_balance + pending_amount)", name="tx_refund_debit_increases_balances"),
        CheckConstraint("NOT (type = 'SETTLEMENT' AND amount < 0) OR (current_balance = prev_current_balance + pending_amount AND available_balance = prev_available_balance)", name="tx_refund_credit_reduces_current_balance"),
        CheckConstraint("NOT (type = 'REFUND') OR (amount > 0 AND pending_amount <= 0 AND pending_amount = group_prev_pending_amount + amount)", name="tx_refund_reduces_pending_amount"),
    )

    id: Mapped[bytes] = mapped_column(BYTEA(32), primary_key=True)
    idempotency_key: Mapped[UUID] = mapped_column(unique=True)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("account.id"))
    account: Mapped[Account] = relationship()
    type: Mapped[TxType]
    amount: Mapped[Decimal]
    pending_amount: Mapped[Decimal]

    # For refunds and settlements, the group_tx_id points to the pending
    # transaction event
    group_tx_id: Mapped[Optional[bytes]] = mapped_column(
        BYTEA(32),
        nullable=True,
    )
    group_prev_tx_id: Mapped[Optional[bytes]] = mapped_column(
        BYTEA(32),
        nullable=True,
    )
    group_next_tx: Mapped[Optional["Tx"]] = relationship(
        back_populates="group_prev_tx",
        viewonly=True,
        foreign_keys=[account_id, group_tx_id, group_prev_tx_id],
    )
    group_prev_tx: Mapped[Optional["Tx"]] = relationship(
        back_populates="group_next_tx",
        viewonly=True,
        remote_side=[id],
        foreign_keys=[account_id, group_tx_id, group_prev_tx_id],
    )
    group_prev_pending_amount: Mapped[Decimal]

    # `prev_tx_id` causes Tx to form a linked list chain that defines the
    # logical sequence of the transactions
    prev_tx_id: Mapped[Optional[bytes]] = mapped_column(
        BYTEA(32),
        nullable=True,
        unique=True,
    )
    prev_current_balance: Mapped[Decimal]
    prev_available_balance: Mapped[Decimal]
    current_balance: Mapped[Decimal]
    available_balance: Mapped[Decimal]

    next_tx: Mapped[Optional["Tx"]] = relationship(
        back_populates="prev_tx",
        viewonly=True,
        foreign_keys=[prev_tx_id, account_id],
    )
    prev_tx: Mapped[Optional["Tx"]] = relationship(
        back_populates="next_tx",
        viewonly=True,
        remote_side=[id],
        foreign_keys=[prev_tx_id, account_id],
    )

    def _set_transaction_hash(self) -> None:
        assert self.id is None
        self.id = self.tx_hash

    def _set_group_tx_root(self) -> None:
        self.group_tx_id = self.id
        self.group_prev_tx_id = None
        self.group_prev_pending_amount = Decimal("0")
        
    @property
    def tx_hash(self) -> bytes:
        # adding prev_tx_id into the hashed data means that alterations to
        # previous transaction entries would cause the hashes to become
        # invalid.
        data: dict[str, str] = dict(
            idempotency_key=str(self.idempotency_key),
            account_id=str(self.account_id),
            type=str(self.type),
            amount=str(Decimal(self.amount).normalize()),
            pending_amount=str(Decimal(self.pending_amount).normalize()),
            prev_tx_id=(self.prev_tx_id or b'').hex(),
            group_prev_tx_id=(self.group_prev_tx_id or b'').hex(),
            prev_current_balance=str(Decimal(self.prev_current_balance).normalize()),
            prev_available_balance=str(Decimal(self.prev_available_balance).normalize()),
            current_balance=str(Decimal(self.current_balance).normalize()),
            available_balance=str(Decimal(self.available_balance).normalize()),
        )
        if self.type not in (TxType.NEW_ACCOUNT, TxType.PENDING):
            group_tx_id=(self.group_tx_id or b'').hex(),
        serialized = "\n".join([f"{key}={value}" for key, value in sorted(data.items())])
        tx_hash = sha256(serialized.encode("ascii")).digest()
        if self.id is not None:
            assert self.id == tx_hash
        return tx_hash

    @property
    def is_debit(self) -> bool:
        assert self.type == TxType.PENDING
        return self.amount > 0

    @property
    def is_credit(self) -> bool:
        assert self.type == TxType.PENDING
        return self.amount < 0

    def __repr__(self) -> str:
        group_tx_short = (self.group_tx_id or b"").hex()[:10]
        tx_hash_short = self.tx_hash.hex()[:10]
        tx_type = self.type.name
        account_name = self.account.name if self.account else self.account_id
        return f"<Tx {group_tx_short}:{tx_hash_short} {tx_type} account={account_name} amount={self.amount} pending_amount={self.pending_amount} balances={self.current_balance},{self.available_balance},{self.prev_current_balance},{self.prev_available_balance}>"


def connect() -> sqlalchemy.Engine:
    return create_engine("postgresql+psycopg://postgres:password@localhost:5432/postgres")


def create_tables(conn: sqlalchemy.Connection) -> None:
    Base.metadata.create_all(conn)
