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
    SETTLEMENT = "s"


class Tx(Base):
    __tablename__ = "tx"
    __table_args__ = (
        # this constraint enforces that all linked relationship belongs to the same account
        ForeignKeyConstraint(
            [
                "prev_tx_id",
                "account_id",
            ],
            [
                "tx.id",
                "tx.account_id",
            ],
        ),
        UniqueConstraint("id", "account_id"),

        # this constraint enforces that prev_current_balance and
        # prev_available_balance are copied exactly from their prev_tx
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

        # this unique index/constraints that you can only have one NEW_ACCOUNT
        # transaction for each account
        Index("tx_only_one_new_account_tx_per_account_id", "account_id", unique=True, postgresql_where="type = 'NEW_ACCOUNT'"),

        # only NEW_ACCOUNT transaction can have empty prev_tx_id
        CheckConstraint("type = 'NEW_ACCOUNT' OR prev_tx_id IS NOT NULL", name="tx_require_prev_tx_id"),

        # these constraints ensures that balances never go negative
        CheckConstraint("current_balance >= 0", name="tx_positive_current_balance"),
        CheckConstraint("available_balance >= 0", name="tx_positive_available_balance"),
    )

    id: Mapped[bytes] = mapped_column(BYTEA(32), primary_key=True)
    idempotency_key: Mapped[UUID] = mapped_column(unique=True)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("account.id"))
    account: Mapped[Account] = relationship()
    type: Mapped[TxType]
    amount: Mapped[Decimal]

    # For refunds and settlements, the original_tx_id points to the pending
    # transaction event
    original_tx_id: Mapped[Optional[bytes]] = mapped_column(
        BYTEA(32),
        nullable=True,
    )

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

    @property
    def tx_hash(self) -> bytes:
        # adding prev_tx_id into the hashed data means that alterations to
        # previous transaction entries would cause the hashes to become
        # invalid.
        data = f"{(self.prev_tx_id or b'').hex()}|{self.idempotency_key}|{self.account_id}|{self.type}|{self.amount.normalize()}"
        tx_hash = sha256(data.encode("ascii")).digest()
        assert self.id is None or self.id == tx_hash
        return tx_hash

    def __repr__(self) -> str:
        return f"<Tx {self.tx_hash.hex()} {self.type.name} account={self.account.name if self.account else self.account_id} amount={self.amount}>"


def connect():
    return create_engine("")


def create_tables(conn: sqlalchemy.Connection) -> None:
    Base.metadata.create_all(conn)
