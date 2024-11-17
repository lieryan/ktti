from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy
from pytest import fixture, raises
from sqlalchemy import create_engine, text, select

import accounting
from accounting import Money
from db import create_tables, Account, Tx, TxType


@fixture
def engine():
    return create_engine("postgresql+psycopg://postgres:password@localhost:5432/postgres")


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


def test_create_account(ledger, db):
    andy = ledger.create_account("andy")

    account = ledger.session.execute(select(Account)).scalar()
    assert isinstance(account.id, UUID)
    assert account.name == "andy"

    new_account_tx = ledger.session.execute(select(Tx)).scalar()
    assert is_sha256_bytes(new_account_tx.id)
    assert new_account_tx.account_id == andy
    assert new_account_tx.type == TxType.NEW_ACCOUNT
    assert new_account_tx.amount == Money(Decimal(0))
    assert new_account_tx.current_balance == Money(Decimal(0))
    assert new_account_tx.available_balance == Money(Decimal(0))

def test_cannot_create_two_accounts_with_the_same_name(ledger, db):
    andy = ledger.create_account("andy")

    with raises(sqlalchemy.exc.IntegrityError):
        ledger.create_account("andy")


def test_create_pending_transaction(ledger, db):
    andy = ledger.create_account("andy")

    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    obj = ledger.session.execute(select(Tx)).scalar()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("50"))


def test_cannot_create_transaction_with_duplicate_idempotency_key(ledger, db):
    andy = ledger.create_account("andy")

    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )
    with raises(sqlalchemy.exc.IntegrityError):
        tx = ledger.create_pending_transaction(
            idempotency_key=idempotency_key,
            account_id=andy,
            amount=Money(Decimal("50")),
        )


def test_settle_pending_transaction(ledger, db):
    andy = ledger.create_account("andy")

    pending_tx_idempotency_key = uuid4()
    pending_tx = ledger.create_pending_transaction(
        idempotency_key=pending_tx_idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    settlement_tx_idempotency_key = uuid4()
    settlement_tx = ledger.settle_transaction(
        idempotency_key=settlement_tx_idempotency_key,
        pending_tx_id=pending_tx,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.id == settlement_tx)).scalar()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == settlement_tx_idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.SETTLEMENT
    assert obj.amount == Money(Decimal("50"))


def test_list_transactions(ledger, db):
    andy = ledger.create_account("andy")
    bill = ledger.create_account("bill")

    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("60")),
    )

    settlement_tx = ledger.settle_transaction(
        idempotency_key=uuid4(),
        pending_tx_id=tx1,
    )

    tx_on_other_account = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=bill,
        amount=Money(Decimal("70")),
    )

    txs = ledger.list_transactions(account_id=andy)
    assert len(txs) == 3
    assert all(tx.account_id == andy for tx in txs)

    assert txs[0].type == TxType.PENDING
    assert txs[0].amount == Money(Decimal("50"))

    assert txs[1].type == TxType.PENDING
    assert txs[1].amount == Money(Decimal("60"))

    assert txs[2].type == TxType.SETTLEMENT
    assert txs[2].amount == Money(Decimal("50"))
