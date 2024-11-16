import sqlalchemy
from sqlalchemy import create_engine, text

from pytest import fixture

from db import create_tables
import accounting


@fixture
def engine():
    return create_engine("sqlite://")


@fixture
def conn(engine):
    with engine.begin() as conn:
        yield conn


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


def test_create_account(db):
    ledger = accounting.Ledger(db)
    ledger.create_account("hello")
