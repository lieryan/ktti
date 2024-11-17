from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

import sqlalchemy
from sqlalchemy import create_engine, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "account"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(30), unique=True)

    def __repr__(self):
        return (
            f"<Account {self.id} name={self.name}>"
        )

class TxType(Enum):
    PENDING = "p"
    SETTLEMENT = "s"


class Tx(Base):
    __tablename__ = "tx"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    idempotency_key: Mapped[UUID] = mapped_column(unique=True)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("account.id"))
    account: Mapped[Account] = relationship()
    type: Mapped[TxType]
    amount: Mapped[Decimal]

    def __repr__(self):
        return (
            f"<Tx {self.id} {self.type.name} account={self.account.name} amount={self.amount}>"
        )


def connect():
    return create_engine("sqlite:///accounting.db")


def create_tables(conn: sqlalchemy.Connection):
    Base.metadata.create_all(conn)
