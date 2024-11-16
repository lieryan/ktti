import sqlite3

from pytest import fixture

from db import create_tables


@fixture
def conn():
    return sqlite3.connect(":memory:")


@fixture
def db(conn):
    return conn


def table_exists(conn, tablename):
    try:
        conn.execute(f"SELECT * FROM {tablename}").fetchall() == []
    except sqlite3.OperationalError as e:
        return False
    else:
        return True


def test_create_tables(conn):
    assert not table_exists(conn, "account")

    create_tables(conn)

    assert table_exists(conn, "account")
