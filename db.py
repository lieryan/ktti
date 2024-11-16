import sqlalchemy
from sqlalchemy import create_engine, text


def connect():
    return create_engine("sqlite:///accounting.db")


def create_tables(conn: sqlalchemy.Connection):
    conn.execute(text("CREATE TABLE IF NOT EXISTS account (id PRIMARY KEY, name)"))
