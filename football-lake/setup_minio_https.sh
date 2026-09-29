#!/bin/bash
# Script cài đặt Nginx và cấu hình HTTPS (Let's Encrypt) cho MinIO trên Azure VM
# Chạy script này trên máy ảo Azure của bạn (SSH vào: ssh azureuser@20.41.113.183)

set -e

# 1. Khai báo domain (Sử dụng dịch vụ nip.io để biến IP thành Domain miễn phí)
IP="20.41.113.183"
DOMAIN="${IP}.nip.io"
EMAIL="admin@${DOMAIN}" # Email dùng để đăng ký Let's Encrypt

echo "Bắt đầu cấu hình HTTPS cho MinIO với domain: $DOMAIN"

# 2. Cài đặt Nginx và Certbot
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx

# 3. Tạo cấu hình Nginx Reverse Proxy cho MinIO (Port 80 tạm thời để Certbot xác thực)
cat <<EOF | sudo tee /etc/nginx/sites-available/minio
server {
    listen 80;
    server_name $DOMAIN;

    # Cấu hình proxy cho S3 API của MinIO
    location / {
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_connect_timeout 300;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        chunked_transfer_encoding off;

        # Chỏ vào MinIO đang chạy ở cổng 9000
        proxy_pass http://localhost:9000;
    }
}
EOF

# Kích hoạt cấu hình và khởi động lại Nginx
sudo ln -sf /etc/nginx/sites-available/minio /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo systemctl restart nginx

# 4. Xin cấp chứng chỉ HTTPS (SSL) bằng Certbot
echo "Đang lấy chứng chỉ SSL từ Let's Encrypt..."
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos -m $EMAIL

echo "--------------------------------------------------------"
echo "✅ HOÀN TẤT! Cấu hình Nginx HTTPS cho MinIO đã xong."
echo "--------------------------------------------------------"
echo "Vui lòng lên Azure Portal -> Network Security Group (NSG) của máy ảo này:"
echo "Mở Port 80 (HTTP) và Port 443 (HTTPS) cho phép truy cập Inbound."
echo "--------------------------------------------------------"
echo "Trong file snowflake_setup.sql, bạn đổi ENDPOINT thành:"
echo "ENDPOINT = '$DOMAIN'"
echo "--------------------------------------------------------"
