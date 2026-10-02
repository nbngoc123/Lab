import requests
import json
import time

def setup_connector():
    connector_config = {
        "name": "test-postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": "postgres-cdc",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "testdb",
            "topic.prefix": "pg",
            "plugin.name": "pgoutput"
        }
    }
    
    print("Registering Debezium Connector...")
    try:
        response = requests.post(
            'http://localhost:8083/connectors',
            headers={'Content-Type': 'application/json'},
            json=connector_config
        )
        print("Status:", response.status_code)
        print("Response:", response.text)
    except Exception as e:
        print("Error registering connector:", e)

if __name__ == "__main__":
    setup_connector()
