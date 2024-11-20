## Prerequisites

1. Python 3.12 or higher
2. venv
3. Docker and Docker Compose


## Setup

1. Create a virtual environment: python -m venv venv
2. Activate the virtual environment: source venv/bin/activate
3. Install dependencies: pip install -r requirements.txt
4. Run databases: sudo docker compose up -d


## Design rationale

1. Some ledgers designs records transaction status transitions in a separate
   event table, we have implemented a ledger system that uses only a single
   immutable Tx table to record transaction events and the transaction
   themselves. In general, having a separate events table is more flexible but 
   it isn't actually necessary for the complexity of the ledger required for
   this assignment.

2. I've decided to model debit and credit transactions as positive and negative
   value in the amount fields for simplicity. This may not always be the best
   way to model debit and credit in a ledger, but it's good enough for this
   purpose.

3. Money type with Decimal. In production application, I'd have used a Money
   class to handle currencies explicitly in the system.

4. The exercise does not require cryptographic tamperproofing of the
   transaction log, but I thought it'd be of interest to implement this anyway
   due to our discussion.

5. Use of requirements.txt. Not the most modern practice, I would have used
   poetry for actual projects, but it's simplest for the purpose of this
   assignment.

6. It's a bit of a strange requirement from the prompt to require that refund
   is intended to work on a pending transaction. Normally, pending transaction
   is meant for payments in the middle of processing; and you only want to
   refund payments after the initial processing have finished (e.g. a couple
   months after a purchase, a customer demanded a warranty refund). If you want
   to change payments while the process pending payment processing has not
   completed, you'd it'd have made more sense to just cancel the pending
   payment and make a new payment session instead of keeping the payment
   pending for months in case a customer want a refund.

6. Tx.idempotency_key is used to detect duplicate/repeated transactions if the
   sender retried a request with identical idempotency key, they are considered
   the same transaction. I don't have the time to implement this mechanism
   fully, but the intent here is that when there's unique constraint violation
   on idempotency_key, the ledger should not insert new Tx but just returned a
   response as if it actually did. A more sophisticated implementation might
   even detect when the repeat request doesn't come with the same data as the
   original request and raise that as a different error to the client.

7. The ledger supports optional optimistic locking with prev_tx_id. The client
   can pass the ID of the last transaction it knew about to ensure that the
   request is only processed if there wasn't any other concurrent transactions.

8. In production scenario, you would have wanted to version tx_hash so that you
   can still verify hashes of old transactions if their calculation logic
   changes.


## Caveats

There's no database migration facilities. Call db.create_tables() to create
table, but you'll need to destroy the database and their existing tables if
schema changed.
