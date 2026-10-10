"""Chạy dbt (staging -> core -> mart) trên 'lake' cục bộ. Dùng đúng file trong repo; chỉ thay kho S3 bằng thư mục cục bộ."""
import os, re, shutil, subprocess, tempfile

REPO_DBT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "dbt"))


def build(lake_dir, dwh_dir, workdir=None):
    work = workdir or tempfile.mkdtemp(prefix="mart_dbt_")
    proj = os.path.join(work, "dbt")
    shutil.copytree(REPO_DBT, proj, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb", "dbt_packages"))
    shutil.copytree(os.path.join(REPO_DBT, "..", "seed"), os.path.join(work, "seed"))      # seed-paths: ../seed
    pk = os.environ.get("MART_TEST_DBT_PACKAGES")
    if pk:
        shutil.copytree(pk, os.path.join(proj, "dbt_packages"))
    p = os.path.join(proj, "macros", "lake.sql")
    s = open(p, encoding="utf-8").read()
    s2 = re.sub(r"'s3://[^']*\{\{ suffix \}\}'", f"'{lake_dir}/{{{{ suffix }}}}'", s)
    assert s2 != s, "không tìm thấy lake_path để thay"
    open(p, "w", encoding="utf-8").write(s2)
    open(os.path.join(proj, "profiles.yml"), "w").write(
        f"football_lake:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: ':memory:'\n"
        f"      threads: 4\n      external_root: {dwh_dir}\n")
    os.makedirs(os.path.join(dwh_dir, "staging"), exist_ok=True)
    env = {**os.environ, "DBT_TARGET_PATH": os.path.join(work, "t"), "DBT_LOG_PATH": os.path.join(work, "l")}
    if not pk:
        subprocess.run(["dbt", "deps", "--profiles-dir", "."], cwd=proj, env=env, check=True, capture_output=True)
    for sel in ("path:models/staging", "team_alias path:models/core", "team_alias int_team_alias path:models/mart"):
        r = subprocess.run(["dbt", "build", "--select", *sel.split(), "--profiles-dir", "."], cwd=proj, env=env,
                           capture_output=True, text=True)
        out = re.sub(r"\x1b\[[0-9;]*m", "", r.stdout)
        assert r.returncode == 0 and re.search(r"ERROR=0", out), f"dbt build {sel} lỗi:\n{out[-3000:]}"
    return dwh_dir
