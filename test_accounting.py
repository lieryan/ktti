from decimal import Decimal
from uuid import UUID, uuid4

import sqlalchemy
from pytest import fixture, raises
from sqlalchemy import create_engine, text, select

import accounting
from accounting import Money
from db import create_tables, Account, Tx, TxType


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


def test_create_pending_transaction(ledger, db):
    andy = ledger.create_account("andy")

    idempotency_key = uuid4()
    transaction = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    obj = ledger.session.execute(select(Tx)).scalar()
    assert isinstance(obj.id, UUID)
    assert obj.idempotency_key == idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("50"))


def test_settle_pending_transaction(ledger, db):
    andy = ledger.create_account("andy")

    pending_transaction_idempotency_key = uuid4()
    pending_transaction = ledger.create_pending_transaction(
        idempotency_key=pending_transaction_idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    settlement_transaction_idempotency_key = uuid4()
    settlement_transaction = ledger.settle_pending_transaction(
        idempotency_key=settlement_transaction_idempotency_key,
        pending_tx_id=pending_transaction,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.id == settlement_transaction)).scalar()
    assert isinstance(obj.id, UUID)
    assert obj.idempotency_key == settlement_transaction_idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.SETTLEMENT
    assert obj.amount == Money(Decimal("50"))
