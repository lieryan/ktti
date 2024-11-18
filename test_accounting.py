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


@fixture
def _andy(ledger):
    andy, prev_tx_id = ledger.create_account("andy")
    return andy, prev_tx_id


@fixture
def andy(_andy):
    return _andy[0]


@fixture
def andy_new_account_tx_id(_andy):
    return _andy[1]


@fixture
def andy_account_balance_is_100(ledger, db, andy, andy_new_account_tx_id):
    debit_tx_id = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("100")),
        prev_tx_id=andy_new_account_tx_id,
    )
    settlement_tx_id = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=debit_tx_id,
        prev_tx_id=debit_tx_id,
    )

    with ledger:
        settlement_tx = ledger.session.get(Tx, settlement_tx_id)
        assert settlement_tx.current_balance == Decimal(100)
        assert settlement_tx.available_balance == Decimal(100)

    return settlement_tx_id


@fixture
def _bill(ledger):
    bill, prev_tx_id = ledger.create_account("bill")
    return bill, prev_tx_id


@fixture
def bill(_bill):
    return _bill[0]


@fixture
def bill_new_account_tx_id(_bill):
    return _bill[1]


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
    andy, new_account_tx_id = ledger.create_account("andy")

    account = ledger.session.execute(select(Account)).scalar_one()
    assert isinstance(account.id, UUID)
    assert account.name == "andy"

    new_account_tx = ledger.session.execute(select(Tx)).scalar_one()
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
    andy, new_account_tx_id = ledger.create_account("andy")
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match="Key \\(name\\)=\\(andy\\) already exists.",
            ) as e:
        ledger.create_account("andy")


def test_cannot_create_new_account_transaction_for_the_same_account(ledger, db):
    andy, new_account_tx_id = ledger.create_account("andy")
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match="Key \\(account_id\\)=\\(........-....-....-....-............\\) already exists.",
            ) as e:
        with ledger:
            new_account_tx = Tx(
                idempotency_key=uuid4(),
                account_id=andy,
                type=TxType.NEW_ACCOUNT,
                amount=Money(Decimal(0)),
                group_tx_id=None,
                prev_tx_id=None,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            new_account_tx._set_transaction_hash()
            ledger.session.add(new_account_tx)


def test_create_pending_transaction_debit(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
):
    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.type == TxType.PENDING)).scalar_one()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("50"))

    assert obj.prev_tx_id == andy_new_account_tx_id
    assert_tx_balances(
        obj,
        current_balance=Money(Decimal(0)),
        available_balance=Money(Decimal(0)),
        prev_current_balance=Money(Decimal(0)),
        prev_available_balance=Money(Decimal(0)),
    )

    assert tx is not None and obj.group_tx_id == tx, "The group_tx of the pending Tx is the pending Tx itself"
    assert obj.group_prev_tx_id is None


def test_create_pending_transaction_credit(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
    andy_account_balance_is_100,
):
    idempotency_key = uuid4()
    pending_tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("-50")),
        prev_tx_id=andy_account_balance_is_100,
    )

    obj = ledger.session.get(Tx, pending_tx)
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == idempotency_key
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("-50"))

    assert obj.prev_tx_id == andy_account_balance_is_100
    assert_tx_balances(
        obj,
        current_balance=Money(Decimal(100)),
        available_balance=Money(Decimal(50)), # pending credit transaction reduces available balance
        prev_current_balance=Money(Decimal(100)),
        prev_available_balance=Money(Decimal(100)),
    )

    assert pending_tx is not None and obj.group_tx_id == pending_tx, "The group_tx of the pending Tx is the pending Tx itself"
    assert obj.group_prev_tx_id is None


def test_create_pending_transaction_credit_insufficient_fund(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
    andy_account_balance_is_100,
):
    idempotency_key = uuid4()
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match='violates check constraint "tx_positive_available_balance"',
            ):
        pending_tx = ledger.create_pending_transaction(
            idempotency_key=idempotency_key,
            account_id=andy,
            amount=Money(Decimal("-150")),
            prev_tx_id=andy_account_balance_is_100,
        )


def test_cannot_create_transaction_with_duplicate_idempotency_key(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
):
    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match='duplicate key value violates unique constraint "tx_pkey"',
            ):
        tx = ledger.create_pending_transaction(
            idempotency_key=idempotency_key,
            account_id=andy,
            amount=Money(Decimal("50")),
            prev_tx_id=andy_new_account_tx_id,
        )


def test_next_tx_prev_tx_relationships_are_correctly_linked(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
):
    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=tx1,
    )

    andy_new_account_tx = ledger.session.get(Tx, andy_new_account_tx_id)
    t1 = ledger.session.get(Tx, tx1)
    t2 = ledger.session.get(Tx, tx2)

    assert t1.prev_tx == andy_new_account_tx
    assert t1.prev_tx_id == andy_new_account_tx_id

    assert t1.next_tx == t2


def test_group_next_tx_group_prev_tx_relationships_are_correctly_linked(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
    andy_account_balance_is_100,
):
    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("-50")),
        prev_tx_id=andy_account_balance_is_100,
    )

    tx2 = ledger.refund_pending_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        amount=Money(Decimal("10")),
        prev_tx_id=tx1,
    )

    tx3 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("20")),
        prev_tx_id=tx2,
    )

    tx4 = ledger.refund_pending_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        amount=Money(Decimal("30")),
        prev_tx_id=tx3,
    )

    tx5 = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        prev_tx_id=tx4,
    )

    andy_new_account_tx = ledger.session.get(Tx, andy_new_account_tx_id)
    t1 = ledger.session.get(Tx, tx1)
    t2 = ledger.session.get(Tx, tx2)
    t3 = ledger.session.get(Tx, tx3)
    t4 = ledger.session.get(Tx, tx4)
    t5 = ledger.session.get(Tx, tx5)

    assert t2.prev_tx == t1
    assert t2.prev_tx_id == tx1

    assert t2.next_tx == t3

    assert t1.group_tx_id == tx1
    assert t2.group_tx_id == tx1
    assert t3.group_tx_id == t3.id
    assert t4.group_tx_id == tx1

    assert t2.group_prev_tx == t1
    assert t2.group_prev_tx_id == tx1

    assert t2.group_next_tx == t4


def test_cannot_create_pending_transaction_if_prev_tx_id_does_not_match_the_account_id(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
    bill,
):
    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='violates foreign key constraint "tx_account_id_prev_tx_id_fkey"'):
        tx = ledger.create_pending_transaction(
            idempotency_key=uuid4(),
            account_id=bill,
            amount=Money(Decimal("50")),
            prev_tx_id=andy_new_account_tx_id,
        )


def test_prev_tx_id_cannot_be_empty_except_for_new_account_transaction(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
):
    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='new row for relation "tx" violates check constraint "tx_require_prev_tx_id"'):
        with ledger:
            new_tx = Tx(
                idempotency_key=uuid4(),
                account_id=andy,
                type=TxType.PENDING,
                amount=Money(Decimal(0)),
                prev_tx_id=None,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            new_tx._set_transaction_hash()
            ledger.session.add(new_tx)


def test_settle_transaction(ledger, db, andy, andy_new_account_tx_id):
    pending_tx_idempotency_key = uuid4()
    pending_tx = ledger.create_pending_transaction(
        idempotency_key=pending_tx_idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    settlement_tx_idempotency_key = uuid4()
    settlement_tx = ledger.settle_transaction(
        idempotency_key=settlement_tx_idempotency_key,
        group_tx_id=pending_tx,
        prev_tx_id=pending_tx,
    )

    obj = ledger.session.execute(select(Tx).where(Tx.id == settlement_tx)).scalar_one()
    assert is_sha256_bytes(obj.id)
    assert obj.idempotency_key == settlement_tx_idempotency_key
    assert obj.account.id == andy
    assert obj.group_tx_id == pending_tx
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


def test_setting_prev_tx_balances_when_creating_and_settling_transactions(ledger, db, andy, andy_new_account_tx_id):
    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    tx2 = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        prev_tx_id=tx1,
    )

    tx3 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("30")),
        prev_tx_id=tx2,
    )

    tx4 = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx3,
        prev_tx_id=tx3,
    )

    assert_tx_balances(
        ledger.session.get(Tx, tx1),
        prev_current_balance=Decimal("0"),
        prev_available_balance=Decimal("0"),
        current_balance=Decimal("0"),
        available_balance=Decimal("0"),
    )

    assert_tx_balances(
        ledger.session.get(Tx, tx2),
        prev_current_balance=Decimal("0"),
        prev_available_balance=Decimal("0"),
        current_balance=Decimal("50"),
        available_balance=Decimal("50"),
    )

    assert_tx_balances(
        ledger.session.get(Tx, tx3),
        prev_current_balance=Decimal("50"),
        prev_available_balance=Decimal("50"),
        current_balance=Decimal("50"),
        available_balance=Decimal("50"),
    )

    assert_tx_balances(
        ledger.session.get(Tx, tx4),
        prev_current_balance=Decimal("50"),
        prev_available_balance=Decimal("50"),
        current_balance=Decimal("80"),
        available_balance=Decimal("80"),
    )


def test_refund_pending_debit_transaction(
    ledger: accounting.Ledger,
    db,
    andy: accounting.AccountId,
    andy_new_account_tx_id: accounting.TransactionId,
    andy_account_balance_is_100,
) -> None:
    debit_tx = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_account_balance_is_100,
    )

    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="Can only refund credit transaction."):
        refund_tx = ledger.refund_pending_transaction(
            idempotency_key=uuid4(),
            group_tx_id=debit_tx,
            amount=Money(Decimal("20")),
        )

def test_refund_pending_credit_transaction(
    ledger: accounting.Ledger,
    db,
    andy: accounting.AccountId,
    andy_new_account_tx_id: accounting.TransactionId,
    andy_account_balance_is_100,
) -> None:
    credit_tx = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("-50")),
        prev_tx_id=andy_account_balance_is_100,
    )

    refund_tx = ledger.refund_pending_transaction(
        idempotency_key=uuid4(),
        group_tx_id=credit_tx,
        amount=Money(Decimal("20")),
        prev_tx_id=credit_tx,
    )

    settlement_tx = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=credit_tx,
        prev_tx_id=refund_tx,
    )

    assert_tx_balances(
        ledger.session.get(Tx, credit_tx),
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("100"),
        current_balance=Decimal("100"),
        available_balance=Decimal("50"),
    )

    assert_tx_balances(
        ledger.session.get(Tx, refund_tx),
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("50"),
        current_balance=Decimal("100"),
        available_balance=Decimal("70"),
    )

    assert_tx_balances(
        ledger.session.get(Tx, settlement_tx),
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("70"),
        current_balance=Decimal("70"),
        available_balance=Decimal("70"),
    )


def test_list_transactions(
    ledger,
    db,
    andy, andy_new_account_tx_id,
    bill, bill_new_account_tx_id,
):
    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("60")),
        prev_tx_id=tx1,
    )

    settlement_tx = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        prev_tx_id=tx2,
    )

    tx_on_other_account = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=bill,
        amount=Money(Decimal("70")),
        prev_tx_id=bill_new_account_tx_id,
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


def test_get_latest_transaction(
    ledger,
    db,
    andy,
    andy_new_account_tx_id,
):
    tx1 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=andy_new_account_tx_id,
    )

    tx2 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=tx1,
    )

    tx3 = ledger.settle_transaction(
        idempotency_key=uuid4(),
        group_tx_id=tx1,
        prev_tx_id=tx2,
    )

    tx4 = ledger.create_pending_transaction(
        idempotency_key=uuid4(),
        account_id=andy,
        amount=Money(Decimal("50")),
        prev_tx_id=tx3,
    )

    latest_tx = ledger.get_latest_transaction(andy)

    assert latest_tx == tx4
