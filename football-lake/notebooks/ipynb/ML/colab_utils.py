"""
Utilities cho Google Colab: Tailscale, MLflow và tải model PyTorch đã log.

Ví dụ:
    from colab_utils import setup_tailscale, setup_mlflow, load_trained_model

    setup_tailscale()  # đọc TAILSCALE_AUTH_KEY từ Colab Secrets
    setup_mlflow("http://100.116.34.36:8080")
    model = load_trained_model("resnet50")

Lưu ý:
- Cài Tailscale binary một lần cho mỗi Colab runtime bằng install_tailscale().
- Sau Restart session, thường cần gọi lại setup_tailscale() và setup_mlflow().
- Đừng ghi auth key trực tiếp vào notebook/module; hãy dùng Colab Secrets.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional
import zipfile
import requests

TAILSCALE_SOCKS_PORT = 1055
TAILSCALE_HTTP_PROXY_PORT = 1056
DEFAULT_HOSTNAME = "google-colab"
DEFAULT_EXPERIMENT = "Chest-Xray-CNN-LIME"
DEFAULT_MODEL_FILES = [
    "MLmodel",
    "data/pickle_module_info.txt",
    "data/model.pth",
]


def get_secret(name: str) -> Optional[str]:
    """Đọc secret từ Colab Secrets; trả về None nếu không tồn tại/không ở Colab."""
    try:
        from google.colab import userdata
        return userdata.get(name)
    except Exception:
        return None


def _run(command, check=True, capture_output=True, text=True):
    """Chạy lệnh hệ thống và đưa lỗi dễ đọc nếu lệnh thất bại."""
    return subprocess.run(
        command, check=check, capture_output=capture_output, text=text
    )


def install_tailscale():
    """Cài Tailscale binary trong runtime Colab hiện tại (cần internet/sudo)."""
    if shutil.which("tailscale") and shutil.which("tailscaled"):
        print("Đã tìm thấy tailscale và tailscaled.")
        return

    print("Đang cài Tailscale...")
    script = _run(
        ["bash", "-lc", "curl -fsSL https://tailscale.com/install.sh | sh"],
        check=False,
    )
    if script.returncode != 0:
        raise RuntimeError(
            "Không cài được Tailscale. Kiểm tra internet/quyền sudo.\n"
            + (script.stderr or script.stdout or "")
        )
    if not (shutil.which("tailscale") and shutil.which("tailscaled")):
        raise RuntimeError("Cài đặt xong nhưng không tìm thấy binary tailscale/tailscaled.")
    print("Cài Tailscale thành công.")


def _tailscale_status():
    if not shutil.which("tailscale"):
        return ""
    result = _run(["tailscale", "status"], check=False)
    return (result.stdout or "") + (result.stderr or "")


def setup_tailscale(
    auth_key: Optional[str] = None,
    hostname: str = DEFAULT_HOSTNAME,
    force_reconnect: bool = False,
    start_wait: int = 3,
    install_if_missing: bool = False,
):
    """
    Khởi động userspace Tailscale proxy và kết nối tailnet.

    auth_key mặc định được đọc từ Colab Secrets: TAILSCALE_AUTH_KEY.
    Gọi lại hàm an toàn hơn so với khởi chạy tailscaled vô điều kiện:
    hàm kiểm tra tiến trình/status trước, tránh tạo nhiều daemon.
    """
    if install_if_missing:
        install_tailscale()

    if not shutil.which("tailscale") or not shutil.which("tailscaled"):
        raise RuntimeError(
            "Chưa có Tailscale binary. Chạy install_tailscale() "
            "hoặc setup_tailscale(install_if_missing=True)."
        )

    key = auth_key or get_secret("TAILSCALE_AUTH_KEY")
    if not key:
        raise RuntimeError(
            "Thiếu TAILSCALE_AUTH_KEY. Hãy thêm key vào Colab Secrets "
            "với đúng tên TAILSCALE_AUTH_KEY."
        )

    # Kiểm tra daemon đã hoạt động chưa; nếu chưa mới khởi chạy.
    status = _tailscale_status()
    daemon = None
    if "Logged out" not in status and ("100." in status or "Running" in status):
        if not force_reconnect:
            print("Tailscale có vẻ đã kết nối; bỏ qua khởi động lại.")
            return status

    # Tránh để lại nhiều tailscaled trong cùng runtime.
    subprocess.run(["pkill", "-f", "tailscaled.*--tun=userspace-networking"],
                   check=False, capture_output=True, text=True)
    time.sleep(1)

    log_path = "/tmp/tailscaled.log"
    log_file = open(log_path, "a", buffering=1)
    daemon = subprocess.Popen(
        [
            "tailscaled",
            "--tun=userspace-networking",
            f"--socks5-server=localhost:{TAILSCALE_SOCKS_PORT}",
            f"--outbound-http-proxy-listen=localhost:{TAILSCALE_HTTP_PROXY_PORT}",
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(max(1, start_wait))

    # Dùng argv thay vì shell interpolation để tránh lỗi ký tự đặc biệt trong key.
    up = _run(
        ["tailscale", "up", f"--authkey={key}", f"--hostname={hostname}"],
        check=False,
    )
    if up.returncode != 0:
        # Nếu daemon đã có state, thử kết nối lại mà không thay đổi auth key.
        retry = _run(["tailscale", "up", f"--hostname={hostname}"], check=False)
        if retry.returncode != 0:
            raise RuntimeError(
                "tailscale up thất bại.\n"
                f"tailscale up: {up.stderr or up.stdout}\n"
                f"retry: {retry.stderr or retry.stdout}\n"
                f"Xem log daemon: {log_path}"
            )

    time.sleep(2)
    status = _tailscale_status()
    print(status.strip() or "Đã chạy tailscale status.")
    print(f"HTTP proxy: http://localhost:{TAILSCALE_HTTP_PROXY_PORT}")
    print(f"SOCKS5 proxy: socks5://localhost:{TAILSCALE_SOCKS_PORT}")
    return status


def tailscale_is_connected() -> bool:
    """Kiểm tra nhanh Tailscale có IP 100.x trong status hay không."""
    status = _tailscale_status()
    return any(line.strip().startswith("100.") for line in status.splitlines())


def setup_mlflow(
    server_uri: str = "http://100.116.34.36:8080",
    experiment: str = DEFAULT_EXPERIMENT,
    proxy_url: str = f"http://localhost:{TAILSCALE_HTTP_PROXY_PORT}",
    set_proxy_env: bool = True,
    verify: bool = True,
):
    """Cấu hình proxy và MLflow Tracking URI; có thể kiểm tra kết nối server."""
    import mlflow

    if set_proxy_env:
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["ALL_PROXY"] = proxy_url
        # requests trên Linux có thể đọc biến chữ thường.
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        os.environ["all_proxy"] = proxy_url

    mlflow.set_tracking_uri(server_uri.rstrip("/"))
    mlflow.set_experiment(experiment)

    if verify:
        try:
            response = requests.get(
                server_uri.rstrip("/") + "/health",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=15,
            )
            response.raise_for_status()
            print(f"MLflow server phản hồi HTTP {response.status_code}: {server_uri}")
        except Exception as exc:
            raise RuntimeError(
                "Đã cấu hình MLflow nhưng chưa xác minh được kết nối. "
                "Kiểm tra Tailscale, IP/port server và proxy localhost:1056. "
                f"Chi tiết: {exc}"
            ) from exc

    print(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
    print(f"Experiment: {experiment}")
    return mlflow.get_tracking_uri()


class MLflowModelLoader:
    """
    Tải logged model theo tên run và cache trong bộ nhớ.

    Yêu cầu MLflow server có endpoint logged-model artifacts mà notebook đang dùng.
    Với MLflow server/phiên bản khác, endpoint API có thể cần điều chỉnh.
    """

    def __init__(
        self,
        experiment: str = DEFAULT_EXPERIMENT,
        proxy_url: str = f"http://localhost:{TAILSCALE_HTTP_PROXY_PORT}",
        model_files=None,
    ):
        import mlflow
        from mlflow.tracking import MlflowClient

        self.mlflow = mlflow
        self.client = MlflowClient()
        self.experiment = experiment
        self.proxy_url = proxy_url
        self.model_files = list(model_files or DEFAULT_MODEL_FILES)
        self.trained_models = {}
        self.model_ids = {}

    def find_model_id(self, name: str) -> str:
        """Tìm logged model mới nhất thuộc run FINISHED có tên đúng bằng name."""
        if name in self.model_ids:
            return self.model_ids[name]

        df = self.mlflow.search_runs(
            experiment_names=[self.experiment],
            filter_string=(
                f"attributes.run_name = '{name}' "
                "and attributes.status = 'FINISHED'"
            ),
            order_by=["attributes.start_time DESC"],
            max_results=1,
        )
        if df.empty:
            raise RuntimeError(
                f"Không tìm thấy run '{name}' trong experiment '{self.experiment}'."
            )

        run_id = df.iloc[0]["run_id"]
        exp_id = df.iloc[0]["experiment_id"]
        models = [
            m for m in self.client.search_logged_models(
                experiment_ids=[exp_id], max_results=100
            )
            if m.source_run_id == run_id
        ]
        if not models:
            raise RuntimeError(
                f"Run '{name}' ({run_id[:8]}) không có logged model."
            )

        latest = max(models, key=lambda m: m.creation_timestamp)
        self.model_ids[name] = latest.model_id
        print(f"[{name}] run {run_id[:8]} -> model {latest.model_id}")
        return latest.model_id

    def _fetch_file(self, model_id: str, relative_path: str, local_path: str):
        server = self.mlflow.get_tracking_uri().rstrip("/")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        url = f"{server}/ajax-api/2.0/mlflow/logged-models/{model_id}/artifacts/files"

        with requests.get(
            url,
            params={"artifact_file_path": relative_path},
            proxies={"http": self.proxy_url, "https": self.proxy_url},
            stream=True,
            timeout=600,
        ) as response:
            response.raise_for_status()
            with open(local_path, "wb") as output:
                for chunk in response.iter_content(1 << 20):
                    if chunk:
                        output.write(chunk)

    def load(self, name: str, force_reload: bool = False):
        """Trả model đã cache; force_reload=True để tải lại."""
        import torch
        import mlflow.pytorch

        if name in self.trained_models and not force_reload:
            return self.trained_models[name]

        model_id = self.find_model_id(name)
        model_dir = tempfile.mkdtemp(prefix=f"{name}_")
        for relative_path in self.model_files:
            started = time.time()
            local_path = os.path.join(model_dir, relative_path)
            self._fetch_file(model_id, relative_path, local_path)
            print(
                f"[{name}] tải {relative_path}: "
                f"{os.path.getsize(local_path):,} bytes ({time.time()-started:.1f}s)"
            )

        try:
            model = mlflow.pytorch.load_model(
                model_dir, map_location="cpu", weights_only=False
            )
        except Exception as exc:
            model_path = os.path.join(model_dir, "data", "model.pth")
            print(f"mlflow.pytorch.load_model lỗi: {exc!r}; thử torch.load.")
            model = torch.load(
                model_path, map_location="cpu", weights_only=False
            )

        model.eval()
        self.trained_models[name] = model
        print(f"[{name}] nạp OK: {type(model).__name__}")
        return model


_default_loader = None


def load_trained_model(
    name: str,
    experiment: str = DEFAULT_EXPERIMENT,
    proxy_url: str = f"http://localhost:{TAILSCALE_HTTP_PROXY_PORT}",
    force_reload: bool = False,
):
    """
    Hàm tiện lợi dùng trực tiếp trong notebook.
    Cache model tồn tại trong runtime Python hiện tại.
    """
    global _default_loader
    if (
        _default_loader is None
        or _default_loader.experiment != experiment
        or _default_loader.proxy_url != proxy_url
    ):
        _default_loader = MLflowModelLoader(
            experiment=experiment, proxy_url=proxy_url
        )
    return _default_loader.load(name, force_reload=force_reload)


def clear_model_cache():
    """Xóa cache model trong module; không xóa file đã tải trong /tmp."""
    global _default_loader
    if _default_loader is not None:
        _default_loader.trained_models.clear()
        _default_loader.model_ids.clear()
    _default_loader = None


def install_colab_dependencies(mlflow_version="3.16.1"):
    """Cài các package còn thiếu trong notebook."""
    import sys
    subprocess.run([
        sys.executable, "-m", "pip", "install", "-q",
        f"mlflow=={mlflow_version}",
        "pysocks",
        "kaggle",
    ], check=True)
    print("Đã cài MLflow, PySocks và Kaggle CLI.")


def download_kaggle_dataset(
    dataset: str,
    data_dir: str,
    force: bool = False,
    keep_zip: bool = False,
    proxy: bool = False,
):
    """
    Tải và giải nén dataset từ Kaggle.

    Parameters
    ----------
    dataset:
        ID Kaggle, ví dụ 'paultimothymooney/chest-xray-pneumonia'.
    data_dir:
        Thư mục lưu dữ liệu đã giải nén.
    force:
        True = xóa dữ liệu cũ và tải lại.
    keep_zip:
        True = giữ file ZIP sau khi giải nén.
    proxy:
        False = bỏ các biến proxy khi tải Kaggle.

    Returns
    -------
    pathlib.Path: thư mục chứa dữ liệu.
    """
    import sys

    data_dir = Path(data_dir).expanduser()
    download_dir = data_dir.parent / f"{data_dir.name}_download"
    zip_path = download_dir / f"{dataset.split('/')[-1]}.zip"

    # 1. Nếu dữ liệu đã tồn tại thì bỏ qua
    if (
        not force
        and data_dir.is_dir()
        and any(data_dir.iterdir())
    ):
        print(f"[Kaggle] Đã có dữ liệu: {data_dir}")
        print("[Kaggle] Bỏ qua tải xuống.")
        return data_dir

    # 2. Lấy token từ Colab Secrets nếu chưa có trong môi trường
    if not os.environ.get("KAGGLE_API_TOKEN"):
        token = get_secret("KAGGLE_API_TOKEN")
        if token:
            os.environ["KAGGLE_API_TOKEN"] = token

    if not os.environ.get("KAGGLE_API_TOKEN"):
        raise RuntimeError(
            "Thiếu Kaggle API token. Hãy thêm KAGGLE_API_TOKEN "
            "vào Colab Secrets hoặc cấu hình token trong môi trường."
        )

    # 3. Cài Kaggle CLI nếu chưa có
    if shutil.which("kaggle") is None:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "kaggle"],
            check=True,
        )

    # 4. Chuẩn bị thư mục
    if force:
        if data_dir.exists():
            shutil.rmtree(data_dir)
        if download_dir.exists():
            shutil.rmtree(download_dir)

    data_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    # 5. Tải dataset
    env = os.environ.copy()

    if not proxy:
        for key in (
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
            "http_proxy", "https_proxy", "all_proxy",
        ):
            env.pop(key, None)

    command = [
        "kaggle", "datasets", "download",
        "-d", dataset,
        "-p", str(download_dir),
    ]

    print(f"[Kaggle] Đang tải: {dataset}")
    subprocess.run(command, env=env, check=True)

    # 6. Giải nén các file ZIP
    zip_files = list(download_dir.glob("*.zip"))

    if not zip_files:
        raise FileNotFoundError(
            f"Không tìm thấy file ZIP trong {download_dir}"
        )

    root = data_dir.resolve()

    for archive in zip_files:
        print(f"[Kaggle] Giải nén: {archive.name}")

        with zipfile.ZipFile(archive, "r") as zf:
            bad_file = zf.testzip()
            if bad_file:
                raise RuntimeError(f"ZIP bị lỗi: {bad_file}")

            # Ngăn đường dẫn trong ZIP thoát khỏi thư mục đích
            for member in zf.infolist():
                target = (data_dir / member.filename).resolve()
                if not target.is_relative_to(root):
                    raise ValueError(
                        f"Đường dẫn không an toàn: {member.filename}"
                    )

            zf.extractall(data_dir)

        if not keep_zip:
            archive.unlink()

    print(f"[Kaggle] Hoàn tất: {data_dir}")
    return data_dir



# ============================================================
# MLflow utilities
# ============================================================

DEFAULT_EXPERIMENT = "Colab_Tailscale_Test"
def start_run(
    run_name: str,
    experiment: str = DEFAULT_EXPERIMENT,
    tags: dict | None = None,
    nested: bool = False,
):
    import mlflow

    mlflow.set_experiment(experiment)

    # Chỉ truyền các tags mà người dùng cung cấp.
    run_tags = dict(tags or {})

    return mlflow.start_run(
        run_name=run_name,
        tags=run_tags or None,
        nested=nested,
    )



def log_training_info(
    params: dict | None = None,
    metrics: dict | None = None,
    step: int | None = None,
    tags: dict | None = None,
):
    """Log hyperparameters, metrics và tags vào active run."""
    import mlflow

    if mlflow.active_run() is None:
        raise RuntimeError("Hãy gọi start_run() trước.")

    if params:
        mlflow.log_params({
            str(k): str(v) if isinstance(v, (dict, list, tuple)) else v
            for k, v in params.items()
        })

    if metrics:
        valid_metrics = {
            k: float(v)
            for k, v in metrics.items()
            if v is not None
        }
        mlflow.log_metrics(valid_metrics, step=step)

    if tags:
        mlflow.set_tags(tags)


def log_dataset_info(
    data_dir: str,
    dataset_name: str,
    version: str | None = None,
    extra_info: dict | None = None,
):
    """
    Ghi nhận metadata dataset vào active run.
    Không upload toàn bộ dataset lên MLflow.
    """
    import mlflow

    path = Path(data_dir).expanduser().resolve()

    if not path.is_dir():
        raise FileNotFoundError(f"Không tìm thấy dataset: {path}")

    file_count = sum(1 for p in path.rglob("*") if p.is_file())
    total_bytes = sum(
        p.stat().st_size for p in path.rglob("*") if p.is_file()
    )

    info = {
        "dataset_name": dataset_name,
        "dataset_path": str(path),
        "dataset_file_count": file_count,
        "dataset_size_bytes": total_bytes,
    }

    if version:
        info["dataset_version"] = version

    if extra_info:
        info.update(extra_info)

    if mlflow.active_run() is None:
        raise RuntimeError("Hãy gọi start_run() trước.")

    mlflow.set_tags({
        f"dataset.{k}": str(v)
        for k, v in info.items()
    })

    print(f"Dataset: {dataset_name}")
    print(f"Số file: {file_count:,}")
    print(f"Dung lượng: {total_bytes / (1024**2):.2f} MB")

    return info



def log_model(
    model,
    artifact_path="model",
    registered_model_name=None,
    input_example=None,
):
    import mlflow
    import xgboost as xgb
    import torch

    if mlflow.active_run() is None:
        raise RuntimeError("Hãy gọi start_run() trước.")

    kwargs = {"name": artifact_path}

    if registered_model_name:
        kwargs["registered_model_name"] = registered_model_name
    if input_example is not None:
        kwargs["input_example"] = input_example

    if isinstance(model, xgb.XGBModel):
        import mlflow.xgboost
        return mlflow.xgboost.log_model(xgb_model=model, **kwargs)

    if isinstance(model, torch.nn.Module):
        import mlflow.pytorch
        return mlflow.pytorch.log_model(pytorch_model=model, **kwargs)

    raise TypeError(f"Model chưa hỗ trợ: {type(model).__name__}")


def log_artifact(
    path: str,
    artifact_path: str | None = "artifacts",
):
    """Upload một file hoặc cả thư mục lên active run."""
    import mlflow

    if mlflow.active_run() is None:
        raise RuntimeError("Hãy gọi start_run() trước.")

    path = Path(path).expanduser()

    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy artifact: {path}")

    if path.is_dir():
        mlflow.log_artifacts(
            str(path),
            artifact_path=artifact_path,
        )
    else:
        mlflow.log_artifact(
            str(path),
            artifact_path=artifact_path,
        )



def get_best_run(
    metric="val_accuracy",
    experiment=DEFAULT_EXPERIMENT,
    ascending=False,
):
    import mlflow
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    exp = client.get_experiment_by_name(experiment)

    if exp is None:
        raise ValueError(f"Không tìm thấy experiment: {experiment}")

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="attributes.status = 'FINISHED'",
        max_results=1000,
    )

    # Chỉ lấy những run thực sự có metric cần tìm
    valid_runs = [
        run for run in runs
        if metric in run.data.metrics
    ]

    if not valid_runs:
        available_metrics = sorted({
            name
            for run in runs
            for name in run.data.metrics
        })

        raise RuntimeError(
            f"Không tìm thấy run có metric '{metric}'. "
            f"Các metric hiện có: {available_metrics}. "
            "Hãy log metric trong quá trình huấn luyện."
        )

    best_run = sorted(
        valid_runs,
        key=lambda run: run.data.metrics[metric],
        reverse=not ascending,
    )[0]

    print(f"Run tốt nhất: {best_run.info.run_name}")
    print(f"Run ID: {best_run.info.run_id}")
    print(f"{metric}: {best_run.data.metrics[metric]}")

    return best_run



# ============================================================
# Disk cache và file utilities
# ============================================================

def load_model_with_disk_cache(
    model_uri: str,
    cache_dir: str = "/content/mlflow_model_cache",
    force_reload: bool = False,
):
    """
    Tải model MLflow về ổ đĩa cục bộ rồi load.
    Nếu cache đã tồn tại, không tải lại.

    model_uri có thể là URI tương thích với MLflow,
    ví dụ runs:/<run_id>/model.
    """
    import mlflow.pytorch

    cache_root = Path(cache_dir).expanduser()
    cache_root.mkdir(parents=True, exist_ok=True)

    # Mỗi model URI có thư mục cache riêng.
    import hashlib
    cache_key = hashlib.sha256(model_uri.encode()).hexdigest()[:20]
    local_model_dir = cache_root / cache_key
    marker = local_model_dir / ".download_complete"

    if force_reload and local_model_dir.exists():
        shutil.rmtree(local_model_dir)

    if not marker.is_file():
        if local_model_dir.exists():
            shutil.rmtree(local_model_dir)

        local_model_dir.mkdir(parents=True)

        try:
            downloaded_path = mlflow.artifacts.download_artifacts(
                artifact_uri=model_uri,
                dst_path=str(local_model_dir),
            )

            # Nếu API tạo thêm thư mục con, giữ đường dẫn model thực.
            downloaded_path = Path(downloaded_path)

            if not (downloaded_path / "MLmodel").is_file():
                raise RuntimeError(
                    f"Không tìm thấy MLmodel trong {downloaded_path}"
                )

            marker.write_text(model_uri, encoding="utf-8")

            # Lưu đường dẫn model để lần sau tìm đúng thư mục.
            (local_model_dir / ".model_path").write_text(
                str(downloaded_path), encoding="utf-8"
            )
        except Exception:
            shutil.rmtree(local_model_dir, ignore_errors=True)
            raise

    model_path_file = local_model_dir / ".model_path"
    model_path = (
        Path(model_path_file.read_text(encoding="utf-8"))
        if model_path_file.exists()
        else local_model_dir
    )

    model = mlflow.pytorch.load_model(str(model_path), map_location="cpu")
    model.eval()

    print(f"Model cache: {model_path}")
    return model


def cleanup_temp_files(
    paths: list[str],
    older_than_seconds: float | None = None,
):
    """
    Xóa file/thư mục tạm được chỉ định.
    Có thể chỉ xóa các mục cũ hơn một khoảng thời gian.
    """
    now = time.time()
    removed = []

    for item in paths:
        path = Path(item).expanduser()

        if not path.exists():
            continue

        if older_than_seconds is not None:
            age = now - path.stat().st_mtime
            if age < older_than_seconds:
                continue

        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

        removed.append(str(path))

    print(f"Đã xóa {len(removed)} mục.")
    return removed


def get_project_paths(
    base_dir: str = "/content/chest_xray_project",
    create: bool = True,
):
    """Tạo và trả về các thư mục dùng chung của project."""
    root = Path(base_dir).expanduser()

    paths = {
        "root": root,
        "data": root / "data",
        "models": root / "models",
        "checkpoints": root / "checkpoints",
        "logs": root / "logs",
        "figures": root / "figures",
        "reports": root / "reports",
        "cache": root / "cache",
        "temp": root / "temp",
    }

    if create:
        for path in paths.values():
            path.mkdir(parents=True, exist_ok=True)

    return paths
