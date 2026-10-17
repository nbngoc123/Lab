import json
import logging
import urllib.request
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

# Ánh xạ model ID trên MLflow để tải trực tiếp nếu MLflow client gặp sự cố chunk timeout
MODEL_ID_MAP = {
    "topic": "m-72eab58c8b704463935f1f736ad7a75b",
    "sentiment": "m-d34db090a51941fb968bcf3e4c095a83",
    "anomaly": "m-4b6bbde3bafd4933bf6b6806aa9c955e",
}


def _load_from_skops_file(file_path: Path):
    import skops.io as sio
    untrusted = sio.get_untrusted_types(file=file_path)
    return sio.load(file_path, trusted=untrusted)


def _download_model_artifact(tracking_uri: str, model_id: str, dest_path: Path, timeout: int = 15):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    base = tracking_uri.rstrip("/")
    url = f"{base}/api/2.0/mlflow-artifacts/artifacts/8/models/{model_id}/artifacts/model.skops"
    logger.info("Đang tải model artifact trực tiếp từ: %s", url)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        dest_path.write_bytes(resp.read())
    logger.info("✓ Đã lưu model vào cache: %s (%d bytes)", dest_path, dest_path.stat().st_size)


def load_models(cfg):
    """Nạp 3 model: topic, sentiment, anomaly. Ưu tiên Local Cache -> MLflow HTTP -> MLflow SDK."""
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
                logger.info("✓ Đã nạp %s model từ cache: %s", name, cached_file)
            except Exception as e:
                logger.warning("Lỗi khi đọc %s từ cache skops: %s", name, e)

        # 2. Nếu chưa có trong cache, tải trực tiếp qua HTTP từ MLflow Server
        if model is None and cfg.tracking_uri and name in MODEL_ID_MAP:
            try:
                _download_model_artifact(cfg.tracking_uri, MODEL_ID_MAP[name], cached_file)
                model = _load_from_skops_file(cached_file)
                logger.info("✓ Đã tải và nạp thành công %s model từ MLflow server!", name)
            except Exception as e:
                logger.warning("Không tải được trực tiếp %s từ MLflow: %s", name, e)

        # 3. Fallback dùng mlflow.sklearn.load_model nếu có URI
        if model is None and uri:
            try:
                import mlflow
                import mlflow.sklearn
                mlflow.set_tracking_uri(cfg.tracking_uri)
                model = mlflow.sklearn.load_model(uri)
                logger.info("✓ Nạp thành công %s qua MLflow URI: %s", name, uri)
            except Exception as exc:
                if required:
                    raise RuntimeError(f"Không thể nạp model {name} từ {uri}: {exc}") from exc
                logger.warning("Bỏ qua anomaly model do không nạp được: %s", exc)

        if model is None and required:
            raise RuntimeError(f"Thiếu model bắt buộc '{name}'! Vui lòng kiểm tra cache hoặc MLflow.")
        
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
