import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys

from ..constants import Constants

# -----------------------------
# Project configuration
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = Decimal("1000.00")
CURRENCY = "USD"

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

DEFAULT_LOOP = True
SLEEP_SECONDS = 2

parser = argparse.ArgumentParser()
parser.add_argument("--once", action="store_true")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

fake = Faker()


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(random.uniform(float(min_val), float(max_val)))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# -----------------------------
# DB Connection
# -----------------------------
try:
    conn = psycopg2.connect(
        host=Constants.POSTGRES_HOST,
        port=Constants.POSTGRES_PORT,
        dbname=Constants.POSTGRES_DB,
        user=Constants.POSTGRES_USER,
        password=Constants.POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)


# -----------------------------
# Core generation logic
# -----------------------------
def run_iteration():
    customers = []

    # 1. Customers
    for _ in range(NUM_CUSTOMERS):
        try:
            email = f"{fake.user_name()}{random.randint(100000,999999)}@example.com"

            cur.execute(
                """
                INSERT INTO customers (firstname, lastname, email)
                VALUES (%s, %s, %s)
                ON CONFLICT (email) DO NOTHING
                RETURNING id
                """,
                (fake.first_name(), fake.last_name(), email),
            )

            row = cur.fetchone()
            if not row:
                continue

            customer_id = row[0]
            customers.append(customer_id)

        except Exception as e:
            print(f"❌ Error inserting customer: {e}")
            raise

    # 2. Accounts
    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            cur.execute(
                """
                INSERT INTO accounts (customer_id, account_type, balance, currency)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (
                    customer_id,
                    random.choice(["savings", "current"]),
                    random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
                    CURRENCY,
                ),
            )
            accounts.append(cur.fetchone()[0])

    # 3. Transactions
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(["cr", "db"])
        amount = random_money(Decimal("1.00"), MAX_TXN_AMOUNT)

        related_account = None
        if len(accounts) > 1 and random.random() < 0.3:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            """
            INSERT INTO transactions (account_id, txn_type, amount, related_account, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (account_id, txn_type, amount, related_account, "completed"),
        )

    print(
        f"✅ {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions generated."
    )


# -----------------------------
# Main Loop
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")

        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\n🛑 Interrupted. Shutting down.")

finally:
    cur.close()
    conn.close()
    sys.exit(0)
