# 🔎 Thông tin máy
import platform, shutil, os
print("OS      :", platform.platform())
print("CPU     :", platform.processor() or platform.machine())
print("Python  :", platform.python_version())
total, used, free = shutil.disk_usage(os.getcwd())
print(f"Đĩa trống: {free/1e9:.1f} GB")

# 🔎 Kiểm tra Docker đã cài & đang chạy
import subprocess, shutil
def run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception as e:
        return None
if shutil.which("docker") is None:
    print("❌ Chưa tìm thấy 'docker'. Hãy cài Docker Desktop và mở nó lên.")
else:
    v = run(["docker","--version"]);  print("✅ Docker:", v.stdout.strip() if v else "?")
    info = run(["docker","info"])
    if info and info.returncode==0: print("✅ Docker Engine đang chạy.")
    else: print("❌ Docker chưa chạy — hãy MỞ Docker Desktop và chờ Engine xanh.")

# (tuỳ chọn) Tự khởi động hạ tầng — bỏ comment 2 dòng dưới nếu muốn chạy từ notebook
# import subprocess
# print(subprocess.run(["docker","compose","up","-d"], capture_output=True, text=True).stdout)

# 🔎 Kiểm tra container đang chạy
import subprocess, shutil
if shutil.which("docker"):
    r = subprocess.run(["docker","compose","ps"], capture_output=True, text=True)
    print(r.stdout or r.stderr)
    print("→ Tất cả STATUS nên là 'running'/'healthy'. Nếu trống: chạy 'docker compose up -d' trong thư mục ThucHanh.")

# 🔎 Kiểm tra thư viện Python bắt buộc
import importlib
libs = ["duckdb","pandas","pyarrow","s3fs","sqlalchemy","psycopg2","sklearn","matplotlib","requests"]
missing=[]
for m in libs:
    try:
        importlib.import_module(m); print(f"✅ {m}")
    except Exception:
        missing.append(m); print(f"❌ {m}  (thiếu)")
if missing:
    print("\n→ Cài bổ sung:  pip install -r requirements.txt")
else:
    print("\n✅ Đủ thư viện.")

# ✅ KIỂM TRA SẴN SÀNG THỰC HÀNH
CFG = {"s3_endpoint":"localhost:9000","s3_key":"minioadmin","s3_secret":"minioadmin123",
       "pg_dwh":"postgresql+psycopg2://dataeng:dataeng123@localhost:5432/dwh"}
result = {}

# 1) MinIO
try:
    import s3fs
    fs = s3fs.S3FileSystem(key=CFG["s3_key"], secret=CFG["s3_secret"],
                           client_kwargs={"endpoint_url":"http://"+CFG["s3_endpoint"]})
    buckets = [b.split("/")[0] for b in fs.ls("")]
    have = [b for b in ["raw","staging","curated"] if b in buckets]
    result["MinIO (:9000)"] = ("✅", f"bucket: {have or 'chưa có → tạo raw/staging/curated'}")
except Exception as e:
    result["MinIO (:9000)"] = ("❌", str(e)[:70])

# 2) Postgres
try:
    from sqlalchemy import create_engine, text
    with create_engine(CFG["pg_dwh"]).connect() as con:
        con.execute(text("select 1"))
    result["PostgreSQL (:5432)"] = ("✅", "kết nối OK")
except Exception as e:
    result["PostgreSQL (:5432)"] = ("❌", str(e)[:70])

# 3) DuckDB + httpfs
try:
    import duckdb
    d = duckdb.connect(); d.execute("INSTALL httpfs; LOAD httpfs;")
    result["DuckDB + httpfs"] = ("✅", duckdb.__version__)
except Exception as e:
    result["DuckDB + httpfs"] = ("❌", str(e)[:70])

# 4) Superset (cổng mở?)
import socket
def port_open(host, port):
    s=socket.socket(); s.settimeout(2)
    try: s.connect((host,port)); return True
    except Exception: return False
    finally: s.close()
result["Superset (:8088)"] = ("✅","cổng mở") if port_open("localhost",8088) else ("⚠️","chưa mở (chờ init hoặc chưa bật)")

print("="*56)
for k,(icon,msg) in result.items():
    print(f"{icon}  {k:<22} {msg}")
print("="*56)
ok = all(v[0]=="✅" for k,v in result.items() if k!="Superset (:8088)")
print("\n🎉 SẴN SÀNG! Mở Bai1_LamQuen_Ingestion.ipynb" if ok
      else "\n⚠️ Còn mục ❌ — xem Mục 7 Troubleshooting bên dưới.")

