from decimal import Decimal
from typing import Any
from contextlib import contextmanager
from uuid import UUID, uuid4

import sqlalchemy
from pytest import fixture, raises
from sqlalchemy import create_engine, text, select, func

import accounting
from accounting import Money
from db import create_tables, Account, Tx, TxType


@fixture
def engine():
    return create_engine(
        "postgresql+psycopg://postgres:password@localhost:5432/postgres"
    )


@fixture
def ledger(engine):
    return accounting.Ledger(engine)


@fixture
def db(engine):
    create_tables(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM tx"))
        conn.execute(text("DELETE FROM account"))
        conn.commit()
    return engine


def is_sha256_bytes(value: Any) -> bool:
    return isinstance(value, bytes) and len(value) == 32


@contextmanager
def assert_does_not_create_any_new_tx(ledger):
    with ledger:
        start_count = ledger.session.execute(select(func.count(Tx.id))).scalar_one()
    yield
    with ledger:
        end_count = ledger.session.execute(select(func.count(Tx.id))).scalar_one()
    assert start_count == end_count


def assert_tx_balances(
    new_account_tx,
    *,
    current_balance,
    available_balance,
    prev_current_balance,
    prev_available_balance,
):
    assert new_account_tx.current_balance == current_balance
    assert new_account_tx.available_balance == available_balance
    assert new_account_tx.prev_current_balance == prev_current_balance
    assert new_account_tx.prev_available_balance == prev_available_balance


def test_create_account(ledger, db):
    andy, prev_tx_id = ledger.create_account("andy")

    account = ledger.session.execute(select(Account)).scalar()
    assert isinstance(account.id, UUID)
    assert account.name == "andy"

    new_account_tx = ledger.session.execute(select(Tx)).scalar()
    assert is_sha256_bytes(new_account_tx.id)
    assert new_account_tx.account_id == andy
    assert new_account_tx.type == TxType.NEW_ACCOUNT
    assert new_account_tx.amount == Money(Decimal(0))
    assert_tx_balances(
        new_account_tx,
        current_balance=Money(Decimal(0)),
        available_balance=Money(Decimal(0)),
        prev_current_balance=Money(Decimal(0)),
        prev_available_balance=Money(Decimal(0)),
    )


def test_cannot_create_two_accounts_with_the_same_name(ledger, db):
    andy, prev_tx_id = ledger.create_account("andy")

    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError):
        ledger.create_account("andy")


def test_create_pending_transaction(ledger, db):
    andy, prev_tx_id = ledger.create_account("andy")

    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=prev_tx_id,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.type == TxType.PENDING)).scalar()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("50"))

    assert obj.prev_tx_id == prev_tx_id
    assert_tx_balances(
        obj,
        current_balance=Money(Decimal(0)),
        available_balance=Money(Decimal(0)),
        prev_current_balance=Money(Decimal(0)),
        prev_available_balance=Money(Decimal(0)),
    )


def test_cannot_create_transaction_with_duplicate_idempotency_key(ledger, db):
    andy, prev_tx_id = ledger.create_account("andy")

    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=prev_tx_id,
    )
    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError):
        tx = ledger.create_pending_transaction(
            idempotency_key=idempotency_key,
            account_id=andy,
            amount=Money(Decimal("50")),
            prev_tx_id=prev_tx_id,
        )


def test_next_tx_prev_relationships_tx_are_correctly_linked(ledger, db):
    andy, new_account_tx_id = ledger.create_account("andy")

    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=new_account_tx_id,
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=tx1,
    )

    new_account_tx = ledger.session.get(Tx, new_account_tx_id)
    t1 = ledger.session.get(Tx, tx1)
    t2 = ledger.session.get(Tx, tx2)

    assert t1.prev_tx == new_account_tx
    assert t1.prev_tx_id == new_account_tx_id

    assert t1.next_tx == t2


def test_cannot_create_pending_transaction_if_prev_tx_id_does_not_match_the_account_id(ledger, db):
    andy, andy_prev_tx_id = ledger.create_account("andy")
    bill, bill_prev_tx_id = ledger.create_account("bill")

    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError):
        tx = ledger.create_pending_transaction(
            idempotency_key=uuid4(),
            account_id=bill,
            amount=Money(Decimal("50")),
            prev_tx_id=andy_prev_tx_id,
        )


def test_settle_transaction(ledger, db):
    andy, prev_tx_id = ledger.create_account("andy")

    pending_tx_idempotency_key = uuid4()
    pending_tx = ledger.create_pending_transaction(
        idempotency_key=pending_tx_idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=prev_tx_id,
    )

    settlement_tx_idempotency_key = uuid4()
    settlement_tx = ledger.settle_transaction(
        idempotency_key=settlement_tx_idempotency_key,
        pending_tx_id=pending_tx,
        prev_tx_id=pending_tx,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.id == settlement_tx)).scalar()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == settlement_tx_idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.SETTLEMENT
    assert obj.amount == Money(Decimal("50"))

    assert obj.prev_tx_id == pending_tx
    assert_tx_balances(
        obj,
        prev_current_balance=Money(Decimal("0")),
        prev_available_balance=Money(Decimal("0")),
        current_balance=Money(Decimal("50")),
        available_balance=Money(Decimal("50")),
    )



def test_list_transactions(ledger, db):
    andy, andy_prev_tx_id = ledger.create_account("andy")
    bill, bill_prev_tx_id = ledger.create_account("bill")

    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_prev_tx_id,
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("60")),
        prev_tx_id=tx1,
    )

    settlement_tx = ledger.settle_transaction(
        idempotency_key=uuid4(),
        pending_tx_id=tx1,
        prev_tx_id=tx2,
    )

    tx_on_other_account = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=bill,
        amount=Money(Decimal("70")),
        prev_tx_id=bill_prev_tx_id,
    )

    txs = ledger.list_transactions(account_id=andy)
    assert len(txs) == 4
    assert all(tx.account_id == andy for tx in txs)

    assert txs[0].type == TxType.NEW_ACCOUNT

    assert txs[1].type == TxType.PENDING
    assert txs[1].amount == Money(Decimal("50"))

    assert txs[2].type == TxType.PENDING
    assert txs[2].amount == Money(Decimal("60"))

    assert txs[3].type == TxType.SETTLEMENT
    assert txs[3].amount == Money(Decimal("50"))
