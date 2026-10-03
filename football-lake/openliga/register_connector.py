"""Đăng ký (hoặc cập nhật) Debezium connector đọc football_source -> Kafka."""
import os
import sys

import requests

from . import config

CONNECT_URL = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
NAME = "football-source-connector"
TABLES = ["sports", "result_types", "leagues", "league_result_infos", "teams", "locations",
          "groups", "matches", "match_results", "goals"]


def connector_config():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        # bên trong docker network: host = postgres-cdc, port 5432
        "database.hostname": os.getenv("DBZ_PG_HOST", "postgres-cdc"),
        "database.port": os.getenv("DBZ_PG_PORT", "5432"),
        "database.user": config.PG_USER,
        "database.password": config.PG_PASSWORD,
        "database.dbname": config.PG_DB,
        "topic.prefix": "football",                      # topic: football.public.matches ...
        "slot.name": "football_slot",
        "publication.name": "football_pub",
        "publication.autocreate.mode": "disabled",       # publication do schema.sql quản lý
        "table.include.list": ",".join(f"public.{t}" for t in TABLES),
        "snapshot.mode": "initial",                      # đọc lại toàn bộ bảng đã backfill vào Kafka
        "tombstones.on.delete": "true",
        "heartbeat.interval.ms": "60000",                # giúp slot tiến lên khi DB ít ghi
    }


def main():
    cfg = connector_config()
    r = requests.get(f"{CONNECT_URL}/connectors/{NAME}", timeout=10)
    if r.status_code == 200:
        r = requests.put(f"{CONNECT_URL}/connectors/{NAME}/config", json=cfg, timeout=30)
    else:
        r = requests.post(f"{CONNECT_URL}/connectors", json={"name": NAME, "config": cfg}, timeout=30)
    print(r.status_code, r.text[:500])
    return 0 if r.status_code < 300 else 1


if __name__ == "__main__":
    sys.exit(main())
