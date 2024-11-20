from decimal import Decimal
from typing import Any, Optional, Iterator
from contextlib import contextmanager
from uuid import UUID, uuid4

import sqlalchemy
from pytest import fixture, raises
from sqlalchemy import create_engine, text, select, func

import accounting
from accounting import Money
from db import create_tables, Account, Tx, TxType


@fixture
def engine() -> sqlalchemy.Engine:
    return create_engine(
        "postgresql+psycopg://postgres:password@localhost:5432/postgres"
    )


@fixture
def ledger(engine: sqlalchemy.Engine) -> accounting.Ledger:
    return accounting.Ledger(engine)


@fixture(autouse=True)
def db(engine: sqlalchemy.Engine) -> sqlalchemy.Engine:
    """Re-initialize the database"""
    create_tables(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM tx"))
        conn.execute(text("DELETE FROM account"))
        conn.commit()
    return engine


def is_sha256_bytes(value: Any) -> bool:
    return isinstance(value, bytes) and len(value) == 32


@fixture
def _andy(ledger):  # type: ignore[no-untyped-def]
    andy, prev_tx_id = ledger.create_account("andy")
    return andy, prev_tx_id


@fixture
def andy(_andy) -> accounting.AccountId:  # type: ignore[no-untyped-def]
    return _andy[0]  # type: ignore[no-any-return]


@fixture
def andy_new_account_tx_id(_andy) -> accounting.TransactionId:  # type: ignore[no-untyped-def]
    return _andy[1]  # type: ignore[no-any-return]


@fixture
def given_andy_account_balance_is_100(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> accounting.TransactionId:
    debit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("100")),
    )
    settlement_tx_id = ledger.settle_transaction(
        group_tx_id=debit_tx_id,
    )

    with ledger:
        settlement_tx = ledger.session.get(Tx, settlement_tx_id)
        assert settlement_tx is not None
        assert settlement_tx.current_balance == Decimal(100)
        assert settlement_tx.available_balance == Decimal(100)

    return settlement_tx_id


@fixture
def given_andy_has_pending_debit_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> accounting.TransactionId:
    debit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("30")),
    )

    with ledger:
        settlement_tx = ledger.session.get(Tx, debit_tx_id)
        assert settlement_tx is not None
        assert settlement_tx.current_balance == Decimal(100)
        assert settlement_tx.available_balance == Decimal(100)
    return debit_tx_id


@fixture
def given_andy_has_pending_credit_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> accounting.TransactionId:
    credit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-30")),
    )

    with ledger:
        settlement_tx = ledger.session.get(Tx, credit_tx_id)
        assert settlement_tx is not None
        assert settlement_tx.current_balance == Decimal(100)
        assert settlement_tx.available_balance == Decimal(70)
    return credit_tx_id


@fixture
def given_andy_has_settled_credit_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_pending_credit_transaction: accounting.TransactionId,
) -> accounting.TransactionId:
    settlement_tx_id = ledger.settle_transaction(
        group_tx_id=given_andy_has_pending_credit_transaction,
    )

    with ledger:
        settlement_tx = ledger.session.get(Tx, settlement_tx_id)
        assert settlement_tx is not None
        assert settlement_tx.current_balance == Decimal(70)
        assert settlement_tx.available_balance == Decimal(70)
    return settlement_tx_id


@fixture
def _bill(ledger):  # type: ignore[no-untyped-def]
    bill, prev_tx_id = ledger.create_account("bill")
    return bill, prev_tx_id


@fixture
def bill(_bill) -> accounting.AccountId:  # type: ignore[no-untyped-def]
    return _bill[0]  # type: ignore[no-any-return]


@fixture
def bill_new_account_tx_id(_bill) -> accounting.TransactionId:  # type: ignore[no-untyped-def]
    return _bill[1]  # type: ignore[no-any-return]


@contextmanager
def assert_does_not_create_any_new_tx(
    ledger: accounting.Ledger,
) -> Iterator[None]:
    with ledger:
        start_count = ledger.session.execute(select(func.count(Tx.id))).scalar_one()
    yield
    with ledger:
        end_count = ledger.session.execute(select(func.count(Tx.id))).scalar_one()
    assert start_count == end_count


def assert_tx_balances(
    tx: Optional[Tx],
    *,
    current_balance: Decimal,
    available_balance: Decimal,
    prev_current_balance: Decimal,
    prev_available_balance: Decimal,
) -> None:
    assert tx is not None
    assert tx.current_balance == current_balance
    assert tx.available_balance == available_balance
    assert tx.prev_current_balance == prev_current_balance
    assert tx.prev_available_balance == prev_available_balance


def test_create_account(
    ledger: accounting.Ledger,
) -> None:
    andy, new_account_tx_id = ledger.create_account("andy")

    account = ledger.session.execute(select(Account)).scalar_one()
    assert isinstance(account.id, UUID)
    assert account.name == "andy"

    new_account_tx = ledger.session.execute(select(Tx)).scalar_one()
    assert is_sha256_bytes(new_account_tx.id)
    assert new_account_tx.account_id == andy
    assert new_account_tx.type == TxType.NEW_ACCOUNT
    assert new_account_tx.amount == Money(Decimal(0))
    assert new_account_tx.pending_amount == Money(Decimal(0))
    assert new_account_tx.group_prev_pending_amount == Money(Decimal(0))
    assert_tx_balances(
        new_account_tx,
        current_balance=Money(Decimal(0)),
        available_balance=Money(Decimal(0)),
        prev_current_balance=Money(Decimal(0)),
        prev_available_balance=Money(Decimal(0)),
    )


def test_cannot_create_two_accounts_with_the_same_name(
    ledger: accounting.Ledger,
) -> None:
    andy, new_account_tx_id = ledger.create_account("andy")
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match="Key \\(name\\)=\\(andy\\) already exists.",
            ) as e:
        ledger.create_account("andy")


def test_cannot_create_new_account_transaction_for_the_same_account(
    ledger: accounting.Ledger,
) -> None:
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
                pending_amount=Money(Decimal(0)),
                group_tx_id=None,
                group_prev_pending_amount=Money(Decimal(0)),
                prev_tx_id=None,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            new_account_tx._set_transaction_hash()
            ledger.session.add(new_account_tx)


def test_create_pending_transaction_debit(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    andy_new_account_tx_id: accounting.TransactionId,
) -> None:
    tx = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    obj = ledger.session.execute(select(Tx).where(Tx.type == TxType.PENDING)).scalar_one()
    assert is_sha256_bytes(obj.id)
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("50"))
    assert obj.pending_amount == Money(Decimal("50"))

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
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    pending_tx = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    obj = ledger.session.get(Tx, pending_tx)
    assert obj is not None
    assert is_sha256_bytes(obj.id)
    assert obj.account.id == andy
    assert obj.type == TxType.PENDING
    assert obj.amount == Money(Decimal("-50"))
    assert obj.pending_amount == Money(Decimal("-50"))

    assert obj.prev_tx_id == given_andy_account_balance_is_100
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
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match='violates check constraint "tx_positive_available_balance"',
            ):
        pending_tx = ledger.create_pending_transaction(
            account_id=andy,
            amount=Money(Decimal("-150")),
        )


def test_create_pending_transaction_with_explicit_idempotency_key(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    explicit_idempotency_key = uuid4()
    pending_tx = ledger.create_pending_transaction(
        idempotency_key=explicit_idempotency_key,
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    obj = ledger.session.get(Tx, pending_tx)
    assert obj is not None
    assert obj.idempotency_key == explicit_idempotency_key


def test_cannot_create_transaction_with_duplicate_idempotency_key(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    idempotency_key = uuid4()
    tx = ledger.create_pending_transaction(
        idempotency_key=idempotency_key,
        account_id=andy,
        amount=Money(Decimal("50")),
    )
    with assert_does_not_create_any_new_tx(ledger), \
            raises(
                sqlalchemy.exc.IntegrityError,
                match='duplicate key value violates unique constraint "tx_idempotency_key_key"',
            ):
        tx = ledger.create_pending_transaction(
            idempotency_key=idempotency_key,
            account_id=andy,
            amount=Money(Decimal("20")),
        )


def test_next_tx_prev_tx_relationships_are_correctly_linked(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    andy_new_account_tx_id: accounting.TransactionId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx2 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    andy_new_account_tx = ledger.session.get(Tx, andy_new_account_tx_id)
    t1 = ledger.session.get(Tx, tx1)
    t2 = ledger.session.get(Tx, tx2)
    assert t1 is not None and t2 is not None
    assert t1.prev_tx == andy_new_account_tx
    assert t1.prev_tx_id == andy_new_account_tx_id

    assert t1.next_tx == t2


def test_group_next_tx_group_prev_tx_relationships_are_correctly_linked(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    tx2 = ledger.refund_pending_transaction(
        group_tx_id=tx1,
        amount=Money(Decimal("10")),
    )

    tx3 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("20")),
    )

    tx4 = ledger.refund_pending_transaction(
        group_tx_id=tx1,
        amount=Money(Decimal("30")),
    )

    tx5 = ledger.settle_transaction(
        group_tx_id=tx1,
    )

    t1 = ledger.session.get(Tx, tx1)
    t2 = ledger.session.get(Tx, tx2)
    t3 = ledger.session.get(Tx, tx3)
    t4 = ledger.session.get(Tx, tx4)
    t5 = ledger.session.get(Tx, tx5)

    assert t1 and t2 and t3 and t4 and t5
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
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    andy_new_account_tx_id: accounting.TransactionId,
    bill: accounting.AccountId,
) -> None:
    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='violates foreign key constraint "tx_account_id_prev_tx_id_fkey"'):
        tx = ledger.create_pending_transaction(
            account_id=bill,
            amount=Money(Decimal("50")),
            prev_tx_id=andy_new_account_tx_id,
        )


def test_prev_tx_id_cannot_be_empty_except_for_new_account_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='new row for relation "tx" violates check constraint "tx_require_prev_tx_id"'):
        with ledger:
            new_tx = Tx(
                idempotency_key=uuid4(),
                account_id=andy,
                type=TxType.PENDING,
                amount=Money(Decimal(100)),
                pending_amount=Money(Decimal(100)),
                group_prev_pending_amount=Money(Decimal(0)),
                prev_tx_id=None,
                prev_current_balance=Money(Decimal(0)),
                prev_available_balance=Money(Decimal(0)),
                current_balance=Money(Decimal(0)),
                available_balance=Money(Decimal(0)),
            )
            new_tx._set_transaction_hash()
            new_tx._set_group_tx_root()
            ledger.session.add(new_tx)


def test_group_prev_tx_id_cannot_be_empty_except_for_pending_and_new_account_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='new row for relation "tx" violates check constraint "tx_require_group_prev_tx_id"'):
        with ledger:
            new_tx = Tx(
                idempotency_key=uuid4(),
                account_id=andy,
                type=TxType.SETTLEMENT,
                amount=Money(Decimal("-50")),
                pending_amount=Money(Decimal("-50")),
                group_prev_pending_amount=Money(Decimal("-50")),
                group_prev_tx_id=None,
                prev_current_balance=Money(Decimal("100")),
                prev_available_balance=Money(Decimal("50")),
                current_balance=Money(Decimal("50")),
                available_balance=Money(Decimal("50")),
            )
            new_tx._set_transaction_hash()
            ledger.session.add(new_tx)


def test_settle_transaction_debit(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_pending_debit_transaction: accounting.TransactionId,
) -> None:
    settlement_tx_id = ledger.settle_transaction(
        group_tx_id=given_andy_has_pending_debit_transaction,
    )

    settlement_tx = ledger.session.get(Tx, settlement_tx_id)
    assert settlement_tx is not None
    assert is_sha256_bytes(settlement_tx.id)
    assert settlement_tx.account.id == andy
    assert settlement_tx.group_tx_id == given_andy_has_pending_debit_transaction
    assert settlement_tx.type == TxType.SETTLEMENT
    assert settlement_tx.amount == Money(Decimal("30"))
    assert settlement_tx.pending_amount == Money(Decimal("30"))
    assert settlement_tx.group_prev_pending_amount == Money(Decimal("30"))

    assert settlement_tx.prev_tx_id == given_andy_has_pending_debit_transaction
    assert_tx_balances(
        settlement_tx,
        prev_current_balance=Money(Decimal("100")),
        prev_available_balance=Money(Decimal("100")),
        current_balance=Money(Decimal("130")),
        available_balance=Money(Decimal("130")),
    )


def test_settle_transaction_credit(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_pending_credit_transaction: accounting.TransactionId,
) -> None:
    settlement_tx_id = ledger.settle_transaction(
        group_tx_id=given_andy_has_pending_credit_transaction,
    )

    settlement_tx = ledger.session.get(Tx, settlement_tx_id)
    assert settlement_tx is not None
    assert is_sha256_bytes(settlement_tx.id)
    assert settlement_tx.account.id == andy
    assert settlement_tx.group_tx_id == given_andy_has_pending_credit_transaction
    assert settlement_tx.type == TxType.SETTLEMENT
    assert settlement_tx.amount == Money(Decimal("-30"))
    assert settlement_tx.pending_amount == Money(Decimal("-30"))
    assert settlement_tx.group_prev_pending_amount == Money(Decimal("-30"))

    assert settlement_tx.prev_tx_id == given_andy_has_pending_credit_transaction
    assert_tx_balances(
        settlement_tx,
        prev_current_balance=Money(Decimal("100")),
        prev_available_balance=Money(Decimal("70")),
        current_balance=Money(Decimal("70")),
        available_balance=Money(Decimal("70")),
    )


def test_settle_transaction_with_explicit_idempotency_key(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    pending_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("30")),
    )

    explicit_idempotency_key = uuid4()
    settlement_tx_id = ledger.settle_transaction(
        idempotency_key=explicit_idempotency_key,
        group_tx_id=pending_tx_id,
    )

    settlement_tx = ledger.session.get(Tx, settlement_tx_id)
    assert settlement_tx is not None
    assert settlement_tx.idempotency_key == explicit_idempotency_key


def test_settle_transaction_with_nonexistent_group_tx(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    nonexistent_tx_id = accounting.TransactionId(b"nonexistent")
    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="Transaction group .* does not exist."):
        settlement_tx_id = ledger.settle_transaction(
            group_tx_id=nonexistent_tx_id,
        )


def test_settle_non_group_tx(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_settled_credit_transaction: accounting.TransactionId,
) -> None:
    with ledger:
        settlement_tx = ledger.session.get(Tx, given_andy_has_settled_credit_transaction)
        assert settlement_tx is not None
        assert settlement_tx.type == TxType.SETTLEMENT

    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="is not a Group ID."):
        refund_tx = ledger.settle_transaction(
            group_tx_id=given_andy_has_settled_credit_transaction,
        )



def test_setting_prev_tx_balances_when_creating_and_settling_transactions(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx2 = ledger.settle_transaction(
        group_tx_id=tx1,
    )

    tx3 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("30")),
    )

    tx4 = ledger.settle_transaction(
        group_tx_id=tx3,
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
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    debit_tx = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="Can only refund credit transaction."):
        refund_tx = ledger.refund_pending_transaction(
            group_tx_id=debit_tx,
            amount=Money(Decimal("20")),
        )


def test_refund_with_nonexistent_group_tx(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_pending_credit_transaction: accounting.TransactionId,
) -> None:
    nonexistent_tx_id = accounting.TransactionId(b"nonexistent")
    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="Transaction group .* does not exist."):
        settlement_tx_id = ledger.refund_pending_transaction(
            group_tx_id=nonexistent_tx_id,
            amount=Money(Decimal("20")),
        )


def test_refund_non_group_tx(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_settled_credit_transaction: accounting.TransactionId,
) -> None:
    with ledger:
        settlement_tx = ledger.session.get(Tx, given_andy_has_settled_credit_transaction)
        assert settlement_tx is not None
        assert settlement_tx.type == TxType.SETTLEMENT

    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="is not a Group ID."):
        refund_tx = ledger.refund_pending_transaction(
            group_tx_id=given_andy_has_settled_credit_transaction,
            amount=Money(Decimal("20")),
        )


def test_refund_pending_credit_transaction(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    credit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    refund_tx_id = ledger.refund_pending_transaction(
        group_tx_id=credit_tx_id,
        amount=Money(Decimal("20")),
    )

    refund2_tx_id = ledger.refund_pending_transaction(
        group_tx_id=credit_tx_id,
        amount=Money(Decimal("12")),
    )

    settlement_tx_id = ledger.settle_transaction(
        group_tx_id=credit_tx_id,
    )

    credit_tx = ledger.session.get(Tx, credit_tx_id)
    assert credit_tx is not None
    assert credit_tx.amount == Money(Decimal("-50"))
    assert credit_tx.pending_amount == Money(Decimal("-50"))
    assert_tx_balances(
        credit_tx,
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("100"),
        current_balance=Decimal("100"),
        available_balance=Decimal("50"),
    )

    refund_tx = ledger.session.get(Tx, refund_tx_id)
    assert refund_tx is not None
    assert refund_tx.amount == Money(Decimal("20"))
    assert refund_tx.pending_amount == Money(Decimal("-30"))
    assert_tx_balances(
        refund_tx,
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("50"),
        current_balance=Decimal("100"),
        available_balance=Decimal("70"),
    )

    refund2_tx = ledger.session.get(Tx, refund2_tx_id)
    assert refund2_tx is not None
    assert refund2_tx.amount == Money(Decimal("12"))
    assert refund2_tx.pending_amount == Money(Decimal("-18"))
    assert_tx_balances(
        refund2_tx,
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("70"),
        current_balance=Decimal("100"),
        available_balance=Decimal("82"),
    )

    settlement_tx = ledger.session.get(Tx, settlement_tx_id)
    assert settlement_tx is not None
    assert settlement_tx.amount == Money(Decimal("-18"))
    assert settlement_tx.pending_amount == Money(Decimal("-18"))
    assert_tx_balances(
        settlement_tx,
        prev_current_balance=Decimal("100"),
        prev_available_balance=Decimal("82"),
        current_balance=Decimal("82"),
        available_balance=Decimal("82"),
    )


def test_cannot_refund_negative_amount(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    credit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    with assert_does_not_create_any_new_tx(ledger), \
            raises(ValueError, match="Refund amount must be positive"):
        refund_tx_id = ledger.refund_pending_transaction(
            group_tx_id=credit_tx_id,
            amount=Money(Decimal("-20")),
        )

def test_cannot_refund_more_than_pending_amount(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_account_balance_is_100: accounting.TransactionId,
) -> None:
    credit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    unrelated_credit_tx_id = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("-50")),
    )

    refund_tx_id = ledger.refund_pending_transaction(
        group_tx_id=credit_tx_id,
        amount=Money(Decimal("20")),
    )

    refund2_tx_id = ledger.refund_pending_transaction(
        group_tx_id=credit_tx_id,
        amount=Money(Decimal("10")),
    )

    with assert_does_not_create_any_new_tx(ledger), \
            raises(sqlalchemy.exc.IntegrityError, match='violates check constraint "tx_refund_reduces_pending_amount"'):
        refund3_tx_id = ledger.refund_pending_transaction(
            group_tx_id=credit_tx_id,
            amount=Money(Decimal("30")),
        )


def test_list_transactions(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    bill: accounting.AccountId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx2 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("60")),
    )

    settlement_tx = ledger.settle_transaction(
        group_tx_id=tx1,
    )

    tx_on_other_account = ledger.create_pending_transaction(
        account_id=bill,
        amount=Money(Decimal("70")),
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
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
) -> None:
    tx1 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx2 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    tx3 = ledger.settle_transaction(
        group_tx_id=tx1,
    )

    tx4 = ledger.create_pending_transaction(
        account_id=andy,
        amount=Money(Decimal("50")),
    )

    latest_tx = ledger.get_latest_transaction(andy)

    assert isinstance(latest_tx, Tx)
    assert latest_tx.id == tx4


def test_get_balance(
    ledger: accounting.Ledger,
    andy: accounting.AccountId,
    given_andy_has_pending_credit_transaction: accounting.TransactionId,
) -> None:
    balance = ledger.get_balance(andy)
    assert balance == accounting.Balance(
        current=Money(Decimal("100")),
        available=Money(Decimal("70")),
    )
