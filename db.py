from decimal import Decimal
from enum import Enum
from hashlib import sha256
from uuid import UUID, uuid4
from typing import Optional

import sqlalchemy
from sqlalchemy import (
    create_engine,
    String,
    ForeignKey,
    ForeignKeyConstraint,
    UniqueConstraint,
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
    )

    id: Mapped[bytes] = mapped_column(BYTEA(32), primary_key=True)
    idempotency_key: Mapped[UUID] = mapped_column(unique=True)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("account.id"))
    account: Mapped[Account] = relationship()
    type: Mapped[TxType]
    amount: Mapped[Decimal]

    prev_tx_id: Mapped[Optional[bytes]] = mapped_column(
        BYTEA(32), nullable=True, unique=True
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
        data = f"{self.idempotency_key}|{self.account_id}|{self.type}|{self.amount.normalize()}"
        tx_hash = sha256(data.encode("ascii")).digest()
        assert self.id is None or self.id == tx_hash
        return tx_hash

    def __repr__(self) -> str:
        return f"<Tx {self.tx_hash.hex()} {self.type.name} account={self.account.name if self.account else self.account_id} amount={self.amount}>"


def connect():
    return create_engine("")


def create_tables(conn: sqlalchemy.Connection) -> None:
    Base.metadata.create_all(conn)
