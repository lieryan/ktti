import sqlalchemy
from sqlalchemy import create_engine, text, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "account"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))


def connect():
    return create_engine("sqlite:///accounting.db")


def create_tables(conn: sqlalchemy.Connection):
    Base.metadata.create_all(conn)
