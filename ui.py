import accounting
from typing import Callable, Any, TypeVar
from functools import wraps
import db
from decimal import Decimal
from sqlalchemy import select, func
import IPython

ledger = accounting.Ledger(db.connect())

BLUE = "\033[94m"
CLEAR = "\033[0m"
help_text = BLUE + '''

To create an account:

    In [1]: create_account("jim")
    Out[1]: UUID('58c16883-5acb-401d-910d-682d331a2cea')

To set the account for the current UI session:

    In [2]: activate_account("jim")
    Working on jim accounts. jim has 1 transaction(s).
    Current balance: $0   Available balance: $0

To create and settle a debit transaction:

    In [3]: create_debit_transaction(100)
    Working on jim accounts. jim has 2 transaction(s).
    Current balance: $0   Available balance: $0
    Out[3]: 'fa5102cf6da4915133b5d32571ac8b2c5e4e4608d3e9a54d4989e09f954286b9'

    In [4]: settle_transaction("fa5102cf6da4915133b5d32571ac8b2c5e4e4608d3e9a54d4989e09f954286b9")
    Working on jim accounts. jim has 3 transaction(s).
    Current balance: $100   Available balance: $100

To create and partially refund a credit transaction:

    In [5]: tx1 = create_credit_transaction(30)
    Working on jim accounts. jim has 4 transaction(s).
    Current balance: $100   Available balance: $70

    In [6]: refund_transaction(tx1, 10)
    Working on jim accounts. jim has 5 transaction(s).
    Current balance: $100   Available balance: $80

    In [7]: refund_transaction(tx1, 5)
    Working on jim accounts. jim has 6 transaction(s).
    Current balance: $100   Available balance: $85

    In [8]: settle_transaction(tx1)
    Working on jim accounts. jim has 7 transaction(s).
    Current balance: $85   Available balance: $85
''' + CLEAR

class _Usage:
    def __repr__(self) -> str:
        return help_text


usage = _Usage()

header = f'''\
Ledger CLI

Connected to {ledger.engine.url}
Type `usage` and press enter for help with the Ledger CLI.
'''

def create_account(name: str) -> accounting.AccountId:
    return ledger.create_account(name)


T = TypeVar('T', bound=Callable[..., Any])
def catch_exception(wrapped: T) -> T:
    @wraps(wrapped)
    def _func(*args: Any, **kwargs: Any) -> Any:
        try:
            return wrapped(*args, **kwargs)
        except Exception as e:
            print("ERROR: " + str(e))
    return _func  # type: ignore[return-value]


def _must_have_active_account() -> None:
    if active_account_id is None:
        raise Exception('No active account. You need to activate the account for the UI session by calling `activate_account("jack")`')


def _validate_group_tx_id(group_tx_id_hex: str) -> accounting.TransactionId:
    with ledger:
        group_tx_id = accounting.TransactionId(bytes.fromhex(group_tx_id_hex))
        tx = ledger.session.get(db.Tx, group_tx_id)
        assert tx is not None
        if tx.account_id != active_account_id:
            active_account = ledger.session.get(db.Account, active_account_id)
            assert active_account is not None
            raise Exception(f"Tx {group_tx_id_hex} belongs to account {tx.account.name}, but currently active account is {active_account.name}. Activate {active_account.name} and retry again to proceed.")
        return group_tx_id


def activate_account(name: str) -> None:
    global active_account_id
    with ledger:
        account_id = ledger.session.scalar(select(db.Account.id).where(db.Account.name == name))
        assert account_id is not None
        active_account_id = accounting.AccountId(account_id)
    print_account_summmary()


def _create_pending_transaction(amount: Decimal | float | int, **kwargs: Any) -> str:
    _must_have_active_account()
    amount = accounting.Money(Decimal(amount))
    tx_id = ledger.create_pending_transaction(account_id=active_account_id, amount=amount, **kwargs)
    print_account_summmary()
    return tx_id.hex()


@catch_exception
def create_debit_transaction(amount: Decimal | float | int, **kwargs: Any) -> str:
    return _create_pending_transaction(amount, **kwargs)


@catch_exception
def create_credit_transaction(amount: Decimal | float | int, **kwargs: Any) -> str:
    return _create_pending_transaction(-amount, **kwargs)


@catch_exception
def settle_transaction(group_tx_id_hex: str, **kwargs: Any) -> None:
    _must_have_active_account()
    group_tx_id = _validate_group_tx_id(group_tx_id_hex)
    ledger.settle_transaction(group_tx_id, **kwargs)
    print_account_summmary()


@catch_exception
def refund_transaction(group_tx_id_hex: str, amount: Decimal | float | int, **kwargs: Any) -> None:
    _must_have_active_account()
    amount = accounting.Money(Decimal(amount))
    group_tx_id = _validate_group_tx_id(group_tx_id_hex)
    ledger.refund_pending_transaction(group_tx_id, amount)
    print_account_summmary()


@catch_exception
def print_account_summmary() -> None:
    with ledger:
        account = ledger.session.get(db.Account, active_account_id)
        assert account is not None
        count_tx = ledger.session.scalar(select(func.count(db.Tx.id)).where(db.Tx.account_id == active_account_id))
        balance = ledger.get_balance(active_account_id)
        print(f"Working on {account.name} accounts. {account.name} has {count_tx} transaction(s).")
        print(f"Current balance: ${balance.current}   Available balance: ${balance.available}")


@catch_exception
def print_transactions() -> None:
    _must_have_active_account()
    with ledger:
        for tx in ledger.list_transactions(account_id=active_account_id):
            print(tx)


def create_database() -> None:
    with ledger.engine.begin() as conn:
        db.create_tables(conn)


def reset_database() -> None:
    with ledger.engine.begin() as conn:
        db.Base.metadata.drop_all(conn)
    create_database()


active_account_id = None

ip = IPython.terminal.embed.InteractiveShellEmbed()  # type: ignore[no-untyped-call]
print(header)
ip.mainloop()  # type: ignore[no-untyped-call]
