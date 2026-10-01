FROM apache/superset:latest

# Chuyển sang quyền root để cài đặt thư viện
USER root

# Cài đặt duckdb-engine thẳng vào môi trường ảo của Superset
RUN /app/.venv/bin/pip install duckdb-engine==0.13.0 duckdb==1.1.2

# Trả lại quyền cho user superset
USER superset
