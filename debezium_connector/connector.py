import json
import requests
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from constants import Constants


connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": Constants.DEBEZIUM_HOST,
        "database.port": Constants.POSTGRES_PORT,
        "database.user": Constants.POSTGRES_USER,
        "database.password": Constants.POSTGRES_PASSWORD,
        "database.dbname": Constants.POSTGRES_DB,
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}


def get_debezium_connector():
    url = f"http://localhost:{Constants.DEBEZIUM_PORT}/connectors"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, headers=headers, data=json.dumps(connector_config)
        )
        response.raise_for_status()
        if response.status_code == 201:
            print("✅ Connector created successfully!")

    except requests.HTTPError as e:
        if e.response.status_code == 409:
            print("⚠️ Connector already exists.")
        else:
            print(
                f"❌ Failed to create connector ({e.response.status_code}): {e.response.text}"
            )


if __name__ == "__main__":
    get_debezium_connector()
