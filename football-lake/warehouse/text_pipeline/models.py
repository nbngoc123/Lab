import json
import logging
import urllib.request
import urllib.parse
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)


def _load_from_skops_file(file_path: Path):
    """Nạp an toàn model scikit-learn từ file skops bằng trusted types."""
    import skops.io as sio
    untrusted = sio.get_untrusted_types(file=file_path)
    return sio.load(file_path, trusted=untrusted)


def _resolve_model_download_url(tracking_uri: str, model_uri: str, timeout: int = 5) -> str:
    """Phân giải 100% ĐỘNG URL tải model từ MLflow Model Registry hoặc Runs.
    Tuyệt đối không hardcode ID hay đường dẫn.
    Hỗ trợ:
      - models:/<name>@<alias> (ví dụ models:/football-text-topic@champion)
      - models:/<name>/<version>
      - runs:/<run_id>/<artifact_path>
    """
    if not tracking_uri or not model_uri:
        return ""
        
    base = tracking_uri.rstrip("/")

    try:
        # TH1: URI đăng ký trong Model Registry (models:/...)
        if model_uri.startswith("models:/"):
            target = model_uri.replace("models:/", "").strip("/")
            source = ""
            
            if "@" in target:
                model_name, alias = target.split("@", 1)
                api_url = f"{base}/api/2.0/mlflow/registered-models/alias?name={urllib.parse.quote(model_name)}&alias={urllib.parse.quote(alias)}"
                req = urllib.request.Request(api_url)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    source = data.get("model_version", {}).get("source", "")
            elif "/" in target:
                model_name, version = target.split("/", 1)
                api_url = f"{base}/api/2.0/mlflow/model-versions/get?name={urllib.parse.quote(model_name)}&version={urllib.parse.quote(version)}"
                req = urllib.request.Request(api_url)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    source = data.get("model_version", {}).get("source", "")
            else:
                api_url = f"{base}/api/2.0/mlflow/registered-models/get?name={urllib.parse.quote(target)}"
                req = urllib.request.Request(api_url)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    versions = data.get("registered_model", {}).get("latest_versions", [])
                    source = versions[0].get("source", "") if versions else ""

            # Nếu source trỏ tới logged model models:/m-...
            if source and "models/" in source:
                m_id = source.split("models/")[-1]
                lm_url = f"{base}/api/2.0/mlflow/logged-models/{m_id}"
                req = urllib.request.Request(lm_url)
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    lm_data = json.loads(r.read().decode("utf-8"))
                    art_uri = lm_data.get("model", {}).get("info", {}).get("artifact_uri", "")
                    if art_uri:
                        clean_path = art_uri.replace("mlflow-artifacts:/", "").strip("/")
                        return f"{base}/api/2.0/mlflow-artifacts/artifacts/{clean_path}/model.skops"

        # TH2: URI trực tiếp từ Run (runs:/<run_id>/<artifact_path>)
        elif model_uri.startswith("runs:/"):
            parts = model_uri.replace("runs:/", "").strip("/").split("/", 1)
            run_id = parts[0]
            artifact_path = parts[1] if len(parts) > 1 else ""
            return f"{base}/get-artifact?path={artifact_path}/model.skops&run_uuid={run_id}"

    except Exception as e:
        logger.warning("Không thể phân giải động URL cho model URI '%s': %s", model_uri, e)

    return ""


def _download_and_cache_model(download_url: str, dest_path: Path, timeout: int = 20):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Đang tải model artifact động từ MLflow: %s", download_url)
    req = urllib.request.Request(download_url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        dest_path.write_bytes(resp.read())
    logger.info("✓ Đã lưu model vào cache: %s (%d bytes)", dest_path, dest_path.stat().st_size)


def load_models(cfg):
    """Nạp 3 model: topic, sentiment, anomaly.
    Quy trình phân giải ĐỘNG 100%:
      1. Kiểm tra bộ đệm cục bộ (Local Cache) để chạy siêu tốc.
      2. Nếu chưa có, truy vấn MLflow Server động theo URI (alias/version) để lấy URL tải artifact.
      3. Fallback dùng mlflow.sklearn.load_model.
    """
    cache_dir = Path(getattr(cfg, "cache_dir", "warehouse/model_cache/text"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    out = {}
    for name, uri, required in [
        ('topic', cfg.topic_uri, True),
        ('sentiment', cfg.sentiment_uri, True),
        ('anomaly', cfg.anomaly_uri, False)
    ]:
        cached_file = cache_dir / f"{name}_model.skops"
        model = None

        # 1. Ưu tiên nạp từ cache skops
        if cached_file.exists():
            try:
                model = _load_from_skops_file(cached_file)
                logger.info("✓ [Cache Hit] Đã nạp %s model từ: %s", name, cached_file)
            except Exception as e:
                logger.warning("Lỗi đọc cache skops %s: %s", name, e)

        # 2. Phân giải động qua MLflow REST API nếu chưa có cache
        if model is None and cfg.tracking_uri and uri:
            dl_url = _resolve_model_download_url(cfg.tracking_uri, uri)
            if dl_url:
                try:
                    _download_and_cache_model(dl_url, cached_file)
                    model = _load_from_skops_file(cached_file)
                    logger.info("✓ [Dynamic Resolve] Tải & nạp thành công %s model từ MLflow!", name)
                except Exception as de:
                    logger.warning("Lỗi khi tải động %s: %s", name, de)

        # 3. Fallback dùng mlflow.sklearn.load_model nếu có URI
        if model is None and uri:
            try:
                import mlflow
                import mlflow.sklearn
                mlflow.set_tracking_uri(cfg.tracking_uri)
                model = mlflow.sklearn.load_model(uri)
                logger.info("✓ [MLflow SDK] Nạp thành công %s qua URI: %s", name, uri)
            except Exception as exc:
                if required:
                    raise RuntimeError(f"Không thể nạp model {name} từ {uri}: {exc}") from exc
                logger.warning("Bỏ qua anomaly model do không nạp được: %s", exc)

        if model is None and required:
            raise RuntimeError(f"Thiếu model bắt buộc '{name}' ({uri})! Không thể tiếp tục.")
        
        out[name] = model

    return out


def topic_names(model):
    """Trích xuất tên chủ đề từ tâm cụm TF-IDF (centroid) của MiniBatchKMeans."""
    terms = np.asarray(model.named_steps['tfidf'].get_feature_names_out())
    centers = model.named_steps['cluster'].cluster_centers_
    return {
        int(i): f"Topic {i}: " + ", ".join(terms[np.argsort(center)[::-1][:3]])
        for i, center in enumerate(centers)
    }


def predict(docs, models, cfg):
    frame = docs.copy()
    names = topic_names(models['topic'])
    topic = []
    sent = []
    confidence = []

    for start in range(0, len(frame), cfg.chunk):
        batch = frame.iloc[start:start+cfg.chunk]
        # Topic inference
        topic.extend(np.asarray(models['topic'].predict(batch.topic_input.fillna('').tolist())).astype(int).tolist())
        
        # Sentiment inference
        sentiment = models['sentiment']
        values = batch.sentiment_input.fillna('').tolist()
        labels = sentiment.predict(values)
        sent.extend([str(v) for v in labels])
        if hasattr(sentiment, 'predict_proba'):
            confidence.extend(np.max(sentiment.predict_proba(values), axis=1).astype(float).tolist())
        else:
            confidence.extend([float('nan')] * len(values))

    frame['topic_id'] = topic
    frame['topic_name'] = frame.topic_id.map(names)
    frame['sentiment_label'] = sent
    frame['sentiment_confidence'] = confidence

    if not frame.sentiment_label.isin(['negative', 'neutral', 'positive']).all():
        raise ValueError('Sentiment model classes must be negative/neutral/positive')
    frame['sentiment_method'] = 'mlflow_registered_student'
    return frame
