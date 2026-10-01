#!/bin/bash
echo "1. Khởi động container Superset..."
docker compose up -d

echo "2. Chờ 5 giây cho container sẵn sàng..."
sleep 5

echo "3. Cài đặt Driver DuckDB để đọc file football_lake.duckdb..."
docker exec -i --user root superset pip install duckdb-engine

echo "4. Khởi tạo Admin (Tài khoản đăng nhập Superset)..."
docker exec -i superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

echo "5. Cập nhật Database Metadata cho Superset..."
docker exec -i superset superset db upgrade

echo "6. Tạo Roles và Permissions mặc định..."
docker exec -i superset superset init

echo "7. Khởi động lại Superset để nhận Driver DuckDB..."
docker restart superset

echo "=============================================================="
echo "✅ Cài đặt hoàn tất! Bạn có thể truy cập Superset tại: http://localhost:8088"
echo "👤 Username: admin"
echo "🔑 Password: admin"
echo "=============================================================="
