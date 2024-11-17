from decimal import Decimal
from enum import Enum
from hashlib import sha256
from uuid import UUID, uuid4

import sqlalchemy
from sqlalchemy import create_engine, String, ForeignKey, BINARY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "account"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(30), unique=True)

    def __repr__(self) -> str:
        return (
            f"<Account {self.id} name={self.name}>"
        )

class TxType(Enum):
    PENDING = "p"
    SETTLEMENT = "s"


class Tx(Base):
    __tablename__ = "tx"

    id: Mapped[bytes] = mapped_column(BINARY(32), primary_key=True)
    idempotency_key: Mapped[UUID] = mapped_column(unique=True)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("account.id"))
    account: Mapped[Account] = relationship()
    type: Mapped[TxType]
    amount: Mapped[Decimal]

    def _set_transaction_hash(self) -> None:
        assert self.id is None
        self.id = self.tx_hash

    @property
    def tx_hash(self) -> bytes:
        data = f"{self.idempotency_key}|{self.account_id}|{self.type}|{self.amount}"
        tx_hash = sha256(data.encode("ascii")).digest()
        assert self.id is None or self.id == tx_hash
        return tx_hash

    def __repr__(self) -> str:
        return (
            f"<Tx {self.tx_hash.hex()} {self.type.name} account={self.account.name if self.account else self.account_id} amount={self.amount}>"
        )


def connect():
    return create_engine("sqlite:///accounting.db")


def create_tables(conn: sqlalchemy.Connection) -> None:
    Base.metadata.create_all(conn)
