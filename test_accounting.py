import sqlalchemy
from uuid import UUID
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session

from pytest import fixture, raises

from db import create_tables, Account
import accounting


@fixture
def engine():
    return create_engine("sqlite://")


@fixture
def conn(engine):
    with engine.begin() as conn:
        yield conn


@fixture
def ledger(engine):
    return accounting.Ledger(engine)


@fixture
def db(conn):
    create_tables(conn)
    return conn


def table_exists(conn, tablename):
    try:
        conn.execute(text(f"SELECT * FROM {tablename}")).fetchall() == []
    except sqlalchemy.exc.OperationalError as e:
        assert "no such table" in str(e)
        return False
    else:
        return True


def test_create_tables(conn):
    assert not table_exists(conn, "account")

    create_tables(conn)

    assert table_exists(conn, "account")


def test_create_account(ledger, db):
    ledger.create_account("andy")

    account = ledger.session.execute(select(Account)).scalar()
    assert isinstance(account.id, UUID)
    assert account.name == "andy"


def test_cannot_create_two_accounts_with_the_same_name(ledger, db):
    ledger.create_account("andy")
    with raises(sqlalchemy.exc.IntegrityError):
        ledger.create_account("andy")
