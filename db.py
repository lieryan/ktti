import sqlite3


def connect():
    return sqlite3.connect("accounting.db")


def create_tables(db: sqlite3.Connection):
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS account (account_id, name)")
