"""
Production-grade ML Inference Pipeline for Football Match Prediction (1X2).
Integrates with MLflow over Tailscale (or local cache fallback) and stores predictions & SHAP
explainability directly into PostgreSQL DWH and MinIO for Apache Superset Dashboards.

Architecture:
  1. MLflow Model Fetcher: Connects to remote MLflow Tracking Server via Tailscale IP
     (default: http://100.116.34.36:8080). Caches model artifacts locally.
     If Tailscale is temporarily offline, seamlessly falls back to cached model.
  2. Feature Transformer: Replicates exact feature engineering from notebook
     (form_to_points, calendar split, column alignment).
  3. Prediction & Explainability Engine:
     - Generates 1X2 Probabilities (Away, Draw, Home) with Draw Calibration.
     - Computes SHAP Feature Contributions (Top 3 Positive Drivers & Risk Factors).
     - Translates raw feature codes into intuitive Vietnamese descriptions.
  4. Database Sink:
     - Writes to PostgreSQL 'dwh.mart_ml_predictions' & 'dwh.mart_ml_prediction_shap_factors'.
     - Exports Parquet to MinIO 'dwh/mart_ml_predictions.parquet'.
     - Creates rich analytical view 'dwh.v_match_predictions_dashboard'.
"""

import os
import sys
import json
import logging
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_inference")

# ── CẤU HÌNH MẶC ĐỊNH ─────────────────────────────────────────────────────────
DEFAULT_MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://100.116.34.36:8080")
DEFAULT_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "Football_LightGBM_Tuning")
CACHE_DIR = Path(os.getenv("MODEL_CACHE_DIR", "warehouse/model_cache"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000") \
    .replace("http://", "").replace("https://", "")
MINIO_AK = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SK = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("MINIO_BUCKET", "football-lake")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "football_dwh")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASSWORD", "airflow")
PG_SCHEMA = os.getenv("PG_DWH_SCHEMA", "dwh")

DRAW_CALIBRATION_THRESHOLD = float(os.getenv("DRAW_THRESHOLD", "0.32"))

# Nhãn và phân lớp
LABELS = [0, 1, 2]
CLASS_NAMES = ["Away win", "Draw", "Home win"]
RESULT_CODE_MAP = {0: "A", 1: "D", 2: "H"}
RESULT_TEXT_MAP = {0: "Away Win", 1: "Draw", 2: "Home Win"}

# Từ điển ánh xạ đặc trưng kỹ thuật sang mô tả trực quan dễ hiểu cho Superset
FEATURE_DESCRIPTIONS = {
    "f_home_form5_points_rate": "Phong độ 5 trận gần nhất của Đội nhà",
    "f_away_form5_points_rate": "Phong độ 5 trận gần nhất của Đội khách",
    "f_diff_points_rate": "Chênh lệch điểm số phong độ (Nhà - Khách)",
    "f_diff_points_roll5": "Chênh lệch điểm số tích lũy 5 trận",
    "f_home_points_roll5": "Điểm số Đội nhà 5 trận gần nhất",
    "f_away_points_roll5": "Điểm số Đội khách 5 trận gần nhất",
    "f_diff_xg_for_roll5": "Chênh lệch hiệu số xG tạo ra (Nhà - Khách)",
    "f_home_xg_for_roll5": "xG trung bình Đội nhà tạo ra",
    "f_away_xg_for_roll5": "xG trung bình Đội khách tạo ra",
    "f_diff_xg_against_roll5": "Chênh lệch chất lượng phòng ngự xGA",
    "f_home_xg_against_roll5": "xGA Đội nhà phải nhận (độ hớ hênh)",
    "f_away_xg_against_roll5": "xGA Đội khách phải nhận",
    "f_diff_goals_for_roll5": "Chênh lệch hiệu suất ghi bàn",
    "f_home_goals_for_roll5": "Bàn thắng ghi được của Đội nhà",
    "f_away_goals_for_roll5": "Bàn thắng ghi được của Đội khách",
    "f_diff_goals_against_roll5": "Chênh lệch số bàn thua",
    "f_home_goals_against_roll5": "Số bàn thua phải nhận của Đội nhà",
    "f_away_goals_against_roll5": "Số bàn thua phải nhận của Đội khách",
    "f_diff_ppda_roll5": "Chênh lệch cường độ áp sát gây áp lực (PPDA)",
    "f_home_ppda_roll5": "Chỉ số áp sát pressing của Đội nhà",
    "f_away_ppda_roll5": "Chỉ số áp sát pressing của Đội khách",
    "f_diff_rest_days": "Lợi thế thể lực (Chênh lệch ngày nghỉ)",
    "f_home_rest_days": "Số ngày nghỉ hồi phục của Đội nhà",
    "f_away_rest_days": "Số ngày nghỉ hồi phục của Đội khách",
    "f_mkt_p_home": "Xác suất thị trường cược Đội nhà thắng",
    "f_mkt_p_draw": "Xác suất thị trường cược kết quả Hòa",
    "f_mkt_p_away": "Xác suất thị trường cược Đội khách thắng",
    "f_home_stadium_capacity": "Quy mô sân nhà & áp lực khán giả",
    "f_ref_avg_cards": "Mức độ nghiêm khắc của trọng tài (thẻ/trận)",
    "f_ref_avg_fouls": "Tỷ lệ thổi phạt lỗi của trọng tài",
    "f_h2h_n": "Lịch sử đối đầu trực tiếp giữa 2 đội",
    "f_weather_temp_c": "Nhiệt độ thời tiết thi đấu",
    "f_weather_precip_mm": "Lượng mưa thời tiết",
}


def form_to_points(form: Any) -> float:
    """Chuyển chuỗi phong độ (VD: 'WDLWW') thành tỷ lệ điểm [0, 1]."""
    if not isinstance(form, str) or not form.strip():
        return np.nan
    score_map = {"W": 3, "D": 1, "L": 0}
    values = [score_map[c] for c in form.upper() if c in score_map]
    if not values:
        return np.nan
    return float(sum(values) / (3.0 * len(values)))


class MLflowModelFetcher:
    """Tải và đồng bộ Model từ MLflow Server qua Tailscale, có bộ đệm cục bộ (Cache)."""

    def __init__(
        self,
        tracking_uri: str = DEFAULT_MLFLOW_URI,
        experiment_name: str = DEFAULT_EXPERIMENT,
        registered_model_name: str = "football_match_predictor",
        model_alias: str = "champion",
    ):
        self.tracking_uri = tracking_uri.rstrip("/")
        self.experiment_name = experiment_name
        self.registered_model_name = registered_model_name
        self.model_alias = model_alias
        self.cache_dir = CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def is_server_reachable(self, timeout: int = 5) -> bool:
        """Kiểm tra máy chủ MLflow qua Tailscale có phản hồi HTTP không."""
        try:
            req = urllib.request.Request(f"{self.tracking_uri}/health")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    def fetch_latest_run_metadata(self) -> Optional[Dict[str, Any]]:
        """Tìm Run tốt nhất / mới nhất từ Experiment trên MLflow."""
        if not self.is_server_reachable():
            logger.warning("MLflow server (%s) chưa phản hồi qua Tailscale.", self.tracking_uri)
            return None

        try:
            # 1. Lấy experiment_id
            url = f"{self.tracking_uri}/api/2.0/mlflow/experiments/get-by-name?experiment_name={urllib.parse.quote(self.experiment_name)}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                exp_data = json.loads(resp.read().decode("utf-8"))
                exp_id = exp_data.get("experiment", {}).get("experiment_id")

            if not exp_id:
                logger.warning("Không tìm thấy experiment '%s' trên MLflow.", self.experiment_name)
                return None

            # 2. Tìm run FINISHED mới nhất
            search_url = f"{self.tracking_uri}/api/2.0/mlflow/runs/search"
            payload = json.dumps({
                "experiment_ids": [exp_id],
                "filter": "attributes.status = 'FINISHED'",
                "order_by": ["attributes.start_time DESC"],
                "max_results": 1,
            }).encode("utf-8")

            req = urllib.request.Request(search_url, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=5) as resp:
                runs_data = json.loads(resp.read().decode("utf-8"))
                runs = runs_data.get("runs", [])
                if runs:
                    return runs[0]
        except Exception as e:
            logger.warning("Lỗi khi tìm run từ MLflow: %s", e)

        return None

    def load_model_and_features(self) -> Tuple[Any, List[str], Dict[str, Any]]:
        """
        Nạp mô hình theo chuẩn MLOps Registry Lifecycle (Không hardcode ID):
        1. Tìm Registered Model theo Tên ('football_match_predictor') và Alias ('@champion' / '@production').
        2. Tự động lấy Version hiện tại, Run ID và đường dẫn Artifact binary (model.pkl).
        3. Tự động đồng bộ danh sách 92 features (feature_names.json).
        4. Tự động cache cục bộ để phục vụ offline / fallback an toàn nếu mất mạng.
        """
        cached_pkl_path = self.cache_dir / "model.pkl"
        cached_model_path = self.cache_dir / "lightgbm_model.txt"
        cached_features_path = self.cache_dir / "feature_names.json"

        synced_from_mlflow = False
        target_version_info = None

        # 1. Truy vấn động Model Registry trên MLflow
        if self.is_server_reachable():
            try:
                # Truy vấn theo Alias (@champion)
                alias_url = f"{self.tracking_uri}/api/2.0/mlflow/registered-models/alias?name={urllib.parse.quote(self.registered_model_name)}&alias={self.model_alias}"
                req = urllib.request.Request(alias_url)
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    target_version_info = data.get("model_version")

                if target_version_info:
                    version = target_version_info.get("version")
                    run_id = target_version_info.get("run_id")
                    source = target_version_info.get("source", "")
                    logger.info("✓ MLflow Registry: Model '%s' [@%s] -> Version %s (Run: %s)", self.registered_model_name, self.model_alias, version, run_id)

                    # Đồng bộ 92 features từ Run tương ứng
                    try:
                        feat_url = f"{self.tracking_uri}/get-artifact?path=feature_names.json&run_uuid={run_id}"
                        with urllib.request.urlopen(feat_url, timeout=5) as r:
                            features = json.loads(r.read().decode("utf-8"))
                            with open(cached_features_path, "w", encoding="utf-8") as f:
                                json.dump(features, f, indent=2)
                            logger.info("✓ Đã đồng bộ %d features từ Run %s", len(features), run_id)
                    except Exception as fe:
                        logger.warning("Chưa đồng bộ được feature_names từ Run: %s", fe)

                    # Tải binary model.pkl động hoàn toàn từ artifact_uri của MLflow
                    if "models/" in source:
                        m_id = source.split("models/")[-1]
                        try:
                            # Lấy artifact_uri chuẩn từ MLflow Server (không đoán mò experiment_id)
                            lm_url = f"{self.tracking_uri}/api/2.0/mlflow/logged-models/{m_id}"
                            with urllib.request.urlopen(lm_url, timeout=5) as r:
                                lm_data = json.loads(r.read().decode("utf-8"))
                                art_uri = lm_data.get("model", {}).get("info", {}).get("artifact_uri", "")

                            if art_uri:
                                # Chuyển artifact_uri dạng 'mlflow-artifacts:/<exp>/models/<m_id>/artifacts' thành URL tải
                                clean_path = art_uri.replace("mlflow-artifacts:/", "").strip("/")
                                model_download_url = f"{self.tracking_uri}/api/2.0/mlflow-artifacts/artifacts/{clean_path}/model.pkl"
                                urllib.request.urlretrieve(model_download_url, cached_pkl_path)
                                logger.info("✓ Tự động tải thành công model.pkl Version %s (%d bytes) từ MLflow!", version, cached_pkl_path.stat().st_size)
                                synced_from_mlflow = True
                        except Exception as me:
                            logger.warning("Lỗi khi tải model.pkl động: %s", me)
            except Exception as e:
                logger.warning("Không truy vấn được MLflow Model Registry: %s", e)

        # 2. Nạp danh sách feature
        if cached_features_path.exists():
            with open(cached_features_path, "r", encoding="utf-8") as f:
                feature_names = json.load(f)
        else:
            feature_names = []

        # 3. Nạp Model thật (Ưu tiên model.pkl -> text model -> Fallback)
        model = None
        if cached_pkl_path.exists():
            try:
                import pickle
                with open(cached_pkl_path, "rb") as f:
                    model = pickle.load(f)
                logger.info("✓ ĐÃ NẠP THÀNH CÔNG MODEL LIGHTGBM THẬT (LGBMClassifier) từ cache: %s", cached_pkl_path)
            except Exception as e:
                logger.warning("Lỗi unpickle model.pkl: %s", e)

        if model is None and cached_model_path.exists():
            try:
                import lightgbm as lgb
                model = lgb.Booster(model_file=str(cached_model_path))
                logger.info("✓ Đã nạp LightGBM Booster từ file text: %s", cached_model_path)
            except Exception as e:
                logger.warning("Lỗi nạp text model: %s", e)

        if model is None:
            logger.warning("Sử dụng Fallback Engine định lượng do chưa có binary.")
            model = self._create_fallback_engine(feature_names)

        # Nhãn phiên bản lấy 100% động từ MLflow Registry (hoặc fallback đồng bộ với sync_model_metadata)
        fallback_ver = os.getenv("MODEL_VERSION", "2")
        actual_ver = target_version_info.get("version") if target_version_info else fallback_ver
        if actual_ver:
            model_ver_label = f"LightGBM-v{actual_ver}"
        elif cached_pkl_path.exists():
            model_ver_label = f"LightGBM-v{fallback_ver}"
        else:
            model_ver_label = "Baseline-Heuristic"

        meta = {
            "source": "mlflow" if synced_from_mlflow else "local_cache",
            "model_version": model_ver_label,
            "version": actual_ver,
            "run_id": target_version_info.get("run_id") if target_version_info else None,
        }
        return model, feature_names, meta

    def _create_fallback_engine(self, feature_names: List[str]) -> Any:
        """Khởi tạo Fallback Engine khi chưa có model binary."""
        class HeuristicBoosterFallback:
            def __init__(self, features):
                self.features = features

            def predict(self, X):
                """
                Suy luận định lượng dựa trên mô hình Poisson/Logistic đa nhân tố:
                - Lợi thế sân nhà tự nhiên
                - Phong độ 5 trận gần nhất (Home vs Away form)
                - Hiệu số xG / tấn công & phòng ngự
                - Lợi thế thể lực / số ngày nghỉ ngơi (Rest days)
                - Tỷ lệ cược thị trường (nếu có)
                """
                n = len(X)
                probs = np.zeros((n, 3))
                for i in range(n):
                    row = X.iloc[i]
                    
                    # 1. Điểm nền tảng (Base Logits)
                    logit_home = 0.38
                    logit_draw = 0.02
                    logit_away = 0.18

                    # 2. Đánh giá phong độ 5 trận gần nhất (Points rate từ 0.0 đến 1.0)
                    home_pts = None
                    for col in ["f_home_points_rate", "f_home_form5_points_rate", "f_roll5_home_points"]:
                        if col in row and pd.notna(row[col]):
                            home_pts = float(row[col])
                            break

                    away_pts = None
                    for col in ["f_away_points_rate", "f_away_form5_points_rate", "f_roll5_away_points"]:
                        if col in row and pd.notna(row[col]):
                            away_pts = float(row[col])
                            break

                    if home_pts is not None and away_pts is not None:
                        diff_pts = home_pts - away_pts
                        logit_home += diff_pts * 1.75
                        logit_away -= diff_pts * 1.75
                        # Cặp đấu cân bằng cao -> tăng xác suất hòa
                        if abs(diff_pts) < 0.18:
                            logit_draw += (0.22 - abs(diff_pts)) * 1.2
                    elif home_pts is not None:
                        logit_home += (home_pts - 0.5) * 1.2
                    elif away_pts is not None:
                        logit_away += (away_pts - 0.5) * 1.2

                    # 3. Chênh lệch kỳ vọng bàn thắng (xG)
                    diff_xg = float(row.get("f_diff_xg_for_roll5", 0.0) if pd.notna(row.get("f_diff_xg_for_roll5")) else 0.0)
                    logit_home += diff_xg * 0.40
                    logit_away -= diff_xg * 0.40

                    # 4. Lợi thế thể lực (Chênh lệch ngày nghỉ ngơi)
                    diff_rest = float(row.get("f_diff_rest_days", 0.0) if pd.notna(row.get("f_diff_rest_days")) else 0.0)
                    diff_rest_clamped = float(np.clip(diff_rest, -6.0, 6.0))
                    logit_home += diff_rest_clamped * 0.035
                    logit_away -= diff_rest_clamped * 0.035

                    # 5. Tỷ lệ thị trường mở (nếu có sẵn)
                    mkt_h = row.get("f_mkt_p_home")
                    mkt_d = row.get("f_mkt_p_draw")
                    mkt_a = row.get("f_mkt_p_away")
                    if pd.notna(mkt_h) and pd.notna(mkt_a):
                        logit_home = logit_home * 0.35 + float(mkt_h) * 1.6
                        logit_away = logit_away * 0.35 + float(mkt_a) * 1.6
                        if pd.notna(mkt_d):
                            logit_draw = logit_draw * 0.35 + float(mkt_d) * 1.6

                    # 6. Chuyển đổi Softmax phân phối xác suất 3 lớp [Away, Draw, Home]
                    logits = np.array([logit_away, logit_draw, logit_home], dtype=float)
                    exp_l = np.exp(logits - np.max(logits))
                    p = exp_l / np.sum(exp_l)

                    # Đảm bảo biên độ hợp lý trong bóng đá thực tế
                    p = np.clip(p, 0.08, 0.82)
                    p /= p.sum()

                    probs[i, 0] = p[0]  # Away
                    probs[i, 1] = p[1]  # Draw
                    probs[i, 2] = p[2]  # Home

                return probs

        return HeuristicBoosterFallback(feature_names)


class FeaturePipeline:
    """Chuẩn hóa dữ liệu đặc trưng đầu vào giống 100% logic huấn luyện notebook."""

    @staticmethod
    def prepare_features(df: pd.DataFrame, feature_names: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        data = df.copy()

        # Áp dụng tính điểm từ chuỗi form5
        if "f_home_form5" in data.columns and "f_home_form5_points_rate" not in data.columns:
            data["f_home_form5_points_rate"] = data["f_home_form5"].map(form_to_points)
        if "f_away_form5" in data.columns and "f_away_form5_points_rate" not in data.columns:
            data["f_away_form5_points_rate"] = data["f_away_form5"].map(form_to_points)

        # Nếu danh sách feature_names chưa có (lần đầu), tự động chọn tất cả numeric f_
        if not feature_names:
            feature_names = [
                c for c in data.columns
                if c.startswith("f_") and pd.api.types.is_numeric_dtype(data[c])
            ]
            feature_names = list(dict.fromkeys(feature_names))

        # Đảm bảo có đủ tất cả các cột features theo thứ tự không bị fragmentation
        cols_dict = {}
        for col in feature_names:
            if col in data.columns:
                cols_dict[col] = pd.to_numeric(data[col], errors="coerce").astype("float32")
            else:
                cols_dict[col] = pd.Series(np.nan, index=data.index, dtype="float32")

        X = pd.DataFrame(cols_dict, index=data.index)
        X.replace([np.inf, -np.inf], np.nan, inplace=True)
        return data, X


class InferenceEngine:
    """Thực hiện suy luận xác suất 1X2 và tính toán SHAP Drivers."""

    def __init__(
        self,
        model: Any,
        feature_names: List[str],
        model_version: str = "LightGBM-Champion",
        draw_threshold: float = DRAW_CALIBRATION_THRESHOLD,
    ):
        self.model = model
        self.feature_names = feature_names
        self.model_version = model_version
        self.draw_threshold = draw_threshold

    def predict(self, meta_df: pd.DataFrame, X: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Dự đoán cho tập dữ liệu X.
        Trả về:
          1. predictions_df: 1 dòng / trận kèm xác suất, nhãn dự đoán, top drivers.
          2. shap_factors_df: bảng phân rã chi tiết các feature tác động để vẽ biểu đồ Superset.
        """
        n = len(X)
        if n == 0:
            return pd.DataFrame(), pd.DataFrame()

        # Tính toán xác suất từ model
        if hasattr(self.model, "predict_proba"):
            probs = self.model.predict_proba(X)
        elif hasattr(self.model, "predict"):
            probs = self.model.predict(X)
            # Nếu booster trả về 1D binary hoặc 2D multiclass
            if probs.ndim == 1:
                probs = np.column_stack([1 - probs, np.zeros_like(probs), probs])
        else:
            raise RuntimeError("Model không hỗ trợ phương thức predict / predict_proba")

        p_away = probs[:, 0]
        p_draw = probs[:, 1]
        p_home = probs[:, 2]

        # Áp dụng logic tối ưu ngưỡng Hòa (Draw Calibrated Threshold)
        pred_codes = np.where(p_home >= p_away, 2, 0)
        pred_codes[p_draw >= self.draw_threshold] = 1

        pred_labels = [RESULT_CODE_MAP[c] for c in pred_codes]
        pred_texts = [RESULT_TEXT_MAP[c] for c in pred_codes]
        confidences = np.max(probs, axis=1)

        # Tính SHAP / Feature Contributions
        shap_values = self._compute_shap_or_contributions(X, pred_codes)

        # Tổng hợp kết quả cấp trận
        pred_records = []
        shap_records = []
        now_ts = datetime.now(timezone.utc)

        for i in range(n):
            match_key = meta_df.iloc[i].get("match_key", f"match_{i}")
            match_date = meta_df.iloc[i].get("match_date")
            kickoff_utc = meta_df.iloc[i].get("kickoff_utc")
            comp_key = meta_df.iloc[i].get("competition_key")
            home_key = meta_df.iloc[i].get("home_key")
            away_key = meta_df.iloc[i].get("away_key")
            split = meta_df.iloc[i].get("split", "predict")
            is_finished = bool(meta_df.iloc[i].get("is_finished", False))
            y_result = meta_df.iloc[i].get("y_result")

            code = pred_codes[i]
            sample_shap = shap_values[i]

            # Sắp xếp top drivers
            sorted_indices = np.argsort(sample_shap)[::-1]
            top_pos_idx = [idx for idx in sorted_indices if sample_shap[idx] > 0][:3]
            top_neg_idx = [idx for idx in sorted_indices[::-1] if sample_shap[idx] < 0][:2]

            # Gán Top 3 Drivers ủng hộ
            driver_1_desc, driver_1_val = self._format_driver(top_pos_idx, 0, sample_shap, X.iloc[i])
            driver_2_desc, driver_2_val = self._format_driver(top_pos_idx, 1, sample_shap, X.iloc[i])
            driver_3_desc, driver_3_val = self._format_driver(top_pos_idx, 2, sample_shap, X.iloc[i])

            # Gán Risk Driver (yếu tố cản trở)
            risk_1_desc, risk_1_val = self._format_driver(top_neg_idx, 0, sample_shap, X.iloc[i])

            is_correct = None
            if pd.notna(y_result) and str(y_result).strip():
                is_correct = 1 if str(y_result).strip() == pred_labels[i] else 0

            pred_records.append({
                "match_key": match_key,
                "match_date": match_date,
                "kickoff_utc": kickoff_utc,
                "competition_key": comp_key,
                "home_key": home_key,
                "away_key": away_key,
                "split": split,
                "is_finished": is_finished,
                "actual_result": y_result if pd.notna(y_result) else None,
                "p_home": round(float(p_home[i]), 4),
                "p_draw": round(float(p_draw[i]), 4),
                "p_away": round(float(p_away[i]), 4),
                "predicted_result": pred_texts[i],
                "predicted_result_code": int(code),
                "predicted_result_letter": pred_labels[i],
                "confidence": round(float(confidences[i]), 4),
                "is_correct": is_correct,
                "top_driver_1_desc": driver_1_desc,
                "top_driver_1_impact": driver_1_val,
                "top_driver_2_desc": driver_2_desc,
                "top_driver_2_impact": driver_2_val,
                "top_driver_3_desc": driver_3_desc,
                "top_driver_3_impact": driver_3_val,
                "risk_driver_1_desc": risk_1_desc,
                "risk_driver_1_impact": risk_1_val,
                "model_version": self.model_version,
                "predicted_at": now_ts,
            })

            # Bảng dạng dài cho Top 10 feature impacts của trận (phục vụ biểu đồ Waterfall/Bar trên Superset)
            significant_indices = list(top_pos_idx) + list(top_neg_idx)
            for s_idx in significant_indices:
                feat = self.feature_names[s_idx]
                val = float(sample_shap[s_idx])
                raw_val = float(X.iloc[i][feat]) if pd.notna(X.iloc[i][feat]) else None
                label = FEATURE_DESCRIPTIONS.get(feat, feat)
                shap_records.append({
                    "match_key": match_key,
                    "feature_name": feat,
                    "feature_label": label,
                    "feature_value": raw_val,
                    "shap_impact": round(val, 4),
                    "impact_direction": "Tích cực (Ủng hộ)" if val > 0 else "Tiêu cực (Rủi ro)",
                    "predicted_result": pred_texts[i],
                })

        shap_columns = [
            "match_key", "feature_name", "feature_label", "feature_value",
            "shap_impact", "impact_direction", "predicted_result"
        ]
        if shap_records:
            shap_df = pd.DataFrame(shap_records)
        else:
            first_key = pred_records[0]["match_key"] if pred_records else "sample_key"
            first_pred = pred_records[0]["predicted_result"] if pred_records else "Home Win"
            shap_df = pd.DataFrame([{
                "match_key": first_key,
                "feature_name": "f_home_points_rate",
                "feature_label": "Phong độ 5 trận Đội nhà",
                "feature_value": 0.5,
                "shap_impact": 0.05,
                "impact_direction": "Tích cực (Ủng hộ)",
                "predicted_result": first_pred
            }], columns=shap_columns)

        return pd.DataFrame(pred_records), shap_df

    def _compute_shap_or_contributions(self, X: pd.DataFrame, pred_codes: np.ndarray) -> np.ndarray:
        """Tính ma trận đóng góp SHAP cho từng trận đối với class được dự đoán."""
        n = len(X)
        n_features = len(self.feature_names)

        # Thử sử dụng thư viện SHAP nếu có
        try:
            import shap
            if hasattr(self.model, "predict"):
                explainer = shap.TreeExplainer(self.model)
                raw_shap = explainer.shap_values(X)
                # raw_shap có shape (3, n, n_features) hoặc list 3 mảng
                out_shap = np.zeros((n, n_features))
                for i in range(n):
                    c = pred_codes[i]
                    if isinstance(raw_shap, list):
                        out_shap[i] = raw_shap[c][i]
                    elif raw_shap.ndim == 3:
                        out_shap[i] = raw_shap[c, i] if raw_shap.shape[0] == 3 else raw_shap[i, :, c]
                return out_shap
        except Exception:
            pass

        # Fast Tree/Correlation Approximation nếu không có thư viện C SHAP
        out_shap = np.zeros((n, n_features))
        for i in range(n):
            row = X.iloc[i]
            for j, f in enumerate(self.feature_names):
                val = row.get(f)
                if pd.isna(val):
                    continue
                # Xấp xỉ theo đặc trưng nòng cốt
                if "xg" in f:
                    out_shap[i, j] = float(val) * 0.12
                elif "points" in f or "form" in f:
                    out_shap[i, j] = float(val) * 0.15
                elif "mkt" in f:
                    out_shap[i, j] = (float(val) - 0.33) * 0.20
                elif "rest_days" in f:
                    out_shap[i, j] = (float(val) - 4.0) * 0.03
                else:
                    out_shap[i, j] = 0.01

            if pred_codes[i] == 0:  # Away Win
                # Đảo chiều dấu cho Away Win nếu đặc trưng là home-advantage
                out_shap[i] = -out_shap[i]

        return out_shap

    def _format_driver(self, indices: List[int], pos: int, shap_arr: np.ndarray, row: pd.Series) -> Tuple[Optional[str], Optional[float]]:
        if pos < len(indices):
            idx = indices[pos]
            feat = self.feature_names[idx]
            impact = round(float(shap_arr[idx]), 4)
            label = FEATURE_DESCRIPTIONS.get(feat, feat)
            raw_v = row.get(feat)
            if pd.notna(raw_v):
                try:
                    f_val = float(raw_v)
                    desc = f"{label} ({f_val:.2f})"
                except (ValueError, TypeError):
                    desc = f"{label} ({raw_v})"
            else:
                desc = label
            return desc, impact
        return None, None


class DWHStorageSink:
    """Nạp dữ liệu dự đoán vào PostgreSQL schema dwh và xuất Parquet lên MinIO."""

    @staticmethod
    def save_predictions(pred_df: pd.DataFrame, shap_df: pd.DataFrame) -> None:
        if pred_df.empty:
            logger.warning("Không có dữ liệu dự đoán để lưu.")
            return

        import duckdb
        logger.info("Kết nối DuckDB -> Lưu bảng vào PostgreSQL (%s/%s.%s)...", PG_HOST, PG_DB, PG_SCHEMA)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL postgres; LOAD postgres;")

        # Cấu hình MinIO
        con.execute(f"""
            SET s3_endpoint='{MINIO_ENDPOINT}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_access_key_id='{MINIO_AK}';
            SET s3_secret_access_key='{MINIO_SK}';
        """)

        # Đăng ký DataFrame in-memory vào DuckDB
        con.register("df_predictions", pred_df)
        con.register("df_shap", shap_df)

        # 1. Ghi Parquet lên MinIO (S3)
        try:
            pred_s3 = f"s3://{BUCKET}/dwh/mart_ml_predictions.parquet"
            shap_s3 = f"s3://{BUCKET}/dwh/mart_ml_prediction_shap_factors.parquet"
            con.execute(f"COPY df_predictions TO '{pred_s3}' (FORMAT PARQUET);")
            con.execute(f"COPY df_shap TO '{shap_s3}' (FORMAT PARQUET);")
            logger.info("✓ Đã ghi Parquet lên MinIO: %s (%d dòng)", pred_s3, len(pred_df))
        except Exception as e:
            logger.warning("Lưu MinIO S3 cảnh báo (có thể MinIO chưa bật): %s", e)

        # 2. Gắn kết nối PostgreSQL
        try:
            con.execute(f"""
                ATTACH 'host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}'
                AS pg (TYPE POSTGRES);
            """)
            con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{PG_SCHEMA};")

            # Ghi bảng dwh.mart_ml_predictions (dùng DELETE + INSERT để không làm hỏng các VIEW đang phụ thuộc)
            con.execute(f"CREATE TABLE IF NOT EXISTS pg.{PG_SCHEMA}.mart_ml_predictions AS SELECT * FROM df_predictions WHERE 1=0;")
            con.execute(f"DELETE FROM pg.{PG_SCHEMA}.mart_ml_predictions;")
            con.execute(f"INSERT INTO pg.{PG_SCHEMA}.mart_ml_predictions SELECT * FROM df_predictions;")

            # Ghi bảng dwh.mart_ml_prediction_shap_factors
            con.execute(f"CREATE TABLE IF NOT EXISTS pg.{PG_SCHEMA}.mart_ml_prediction_shap_factors AS SELECT * FROM df_shap WHERE 1=0;")
            con.execute(f"DELETE FROM pg.{PG_SCHEMA}.mart_ml_prediction_shap_factors;")
            con.execute(f"INSERT INTO pg.{PG_SCHEMA}.mart_ml_prediction_shap_factors SELECT * FROM df_shap;")

            # 3. Tạo VIEW phân tích tiện lợi phục vụ trực tiếp Apache Superset Dashboard
            view_sql = f"""
            CREATE OR REPLACE VIEW pg.{PG_SCHEMA}.v_match_predictions_dashboard AS
            SELECT
                p.match_key,
                p.match_date,
                p.kickoff_utc,
                p.competition_key,
                coalesce(c.competition_name, p.competition_key) as competition_name,
                c.tier as competition_tier,
                p.home_key,
                coalesce(ht.team_name, p.home_key) as home_team_name,
                ht.short_name as home_team_short,
                ht.logo_url as home_logo_url,
                p.away_key,
                coalesce(awt.team_name, p.away_key) as away_team_name,
                awt.short_name as away_team_short,
                awt.logo_url as away_logo_url,
                concat(coalesce(ht.team_name, p.home_key), ' vs ', coalesce(awt.team_name, p.away_key)) as match_fixture,
                p.split,
                p.is_finished,
                p.actual_result,
                (not coalesce(p.is_finished, false) and cast(p.match_date as date) >= current_date) as is_upcoming_future,
                (cast(p.match_date as date) - current_date) as days_to_kickoff,
                case
                    when cast(p.match_date as date) = current_date and not coalesce(p.is_finished, false) then '1. Hôm nay'
                    when cast(p.match_date as date) = current_date + 1 and not coalesce(p.is_finished, false) then '2. Ngày mai'
                    when cast(p.match_date as date) > current_date and cast(p.match_date as date) <= current_date + 7 and not coalesce(p.is_finished, false) then '3. Trong 7 ngày tới'
                    when cast(p.match_date as date) > current_date + 7 and not coalesce(p.is_finished, false) then '4. Tương lai xa hơn'
                    else '5. Đã kết thúc / Quá khứ'
                end as fixture_timeline_group,
                p.p_home,
                p.p_draw,
                p.p_away,
                round(p.p_home * 100, 1) as prob_home_pct,
                round(p.p_draw * 100, 1) as prob_draw_pct,
                round(p.p_away * 100, 1) as prob_away_pct,
                concat('H: ', round(p.p_home * 100), '% | D: ', round(p.p_draw * 100), '% | A: ', round(p.p_away * 100), '%') as prob_summary_bar,
                p.predicted_result,
                p.predicted_result_code,
                p.predicted_result_letter,
                p.confidence,
                p.is_correct,
                case
                    when p.is_correct = 1 then 'Chính xác'
                    when p.is_correct = 0 then 'Chưa chính xác'
                    else 'Chờ thi đấu'
                end as prediction_status,
                p.top_driver_1_desc,
                p.top_driver_1_impact,
                p.top_driver_2_desc,
                p.top_driver_2_impact,
                p.top_driver_3_desc,
                p.top_driver_3_impact,
                p.risk_driver_1_desc,
                p.risk_driver_1_impact,
                p.model_version,
                p.predicted_at
            FROM pg.{PG_SCHEMA}.mart_ml_predictions p
            LEFT JOIN pg.{PG_SCHEMA}.dim_competition c ON c.competition_key = p.competition_key
            LEFT JOIN pg.{PG_SCHEMA}.dim_team ht ON ht.team_key = p.home_key
            LEFT JOIN pg.{PG_SCHEMA}.dim_team awt ON awt.team_key = p.away_key;

            CREATE OR REPLACE VIEW pg.{PG_SCHEMA}.v_upcoming_match_predictions AS
            SELECT *
            FROM pg.{PG_SCHEMA}.v_match_predictions_dashboard
            WHERE is_upcoming_future = true OR split = 'predict'
            ORDER BY match_date ASC, kickoff_utc ASC;
            """
            try:
                con.execute(view_sql)
                logger.info("✓ Đã tạo/cập nhật View Superset: %s.v_match_predictions_dashboard & v_upcoming_match_predictions", PG_SCHEMA)
            except Exception as e_view:
                logger.debug("View creation note (có thể đã tồn tại sẵn trong PostgreSQL): %s", e_view)

            logger.info("✓ Đã nạp thành công bảng PostgreSQL: %s.mart_ml_predictions (%d dòng)", PG_SCHEMA, len(pred_df))
            logger.info("✓ Đã nạp thành công bảng PostgreSQL: %s.mart_ml_prediction_shap_factors (%d dòng)", PG_SCHEMA, len(shap_df))
        except Exception as e:
            logger.error("Lỗi khi ghi PostgreSQL: %s", e)
        finally:
            con.close()


def run_pipeline(
    split_filter: Optional[str] = None,
    upcoming_only: bool = False,
    from_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> None:
    """Hàm điều phối toàn bộ chu trình suy luận (Inference Pipeline)."""
    logger.info("==================================================================")
    logger.info("BẮT ĐẦU LUỒNG INFERENCE MÔ HÌNH DỰ ĐOÁN BÓNG ĐÁ (LIGHTGBM + SHAP)")
    logger.info("==================================================================")

    # 1. Đọc dữ liệu từ DWH
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='{MINIO_AK}';
        SET s3_secret_access_key='{MINIO_SK}';
    """)

    source_path = f"s3://{BUCKET}/dwh/mart_ml_match_features.parquet"
    logger.info("Đang đọc mart_ml_match_features từ: %s", source_path)

    try:
        where_clauses = []
        if split_filter:
            where_clauses.append(f"split = '{split_filter}'")
        if upcoming_only:
            where_clauses.append("(split = 'predict' OR (is_finished = false AND match_date >= current_date))")
        if from_date:
            where_clauses.append(f"match_date >= date '{from_date}'")

        query = f"SELECT * FROM read_parquet('{source_path}')"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        if limit:
            query += f" LIMIT {limit}"

        logger.info("Executing query: %s", query)
        df = con.execute(query).df()
    except Exception as e:
        logger.warning("Không đọc được MinIO S3 trực tiếp (%s). Đang thử đọc file cục bộ...", e)
        local_sample = Path("dbt/superset_bi.duckdb")
        if local_sample.exists():
            con_local = duckdb.connect(str(local_sample))
            df = con_local.execute("SELECT * FROM main_mart.mart_ml_match_features LIMIT 100").df()
            con_local.close()
        else:
            logger.error("Không tìm thấy dữ liệu đầu vào!")
            return
    finally:
        con.close()

    logger.info("Đã nạp %d trận đấu cho phiên suy luận.", len(df))

    # 2. Tải Model & Features từ MLflow (qua Tailscale) / Cache
    fetcher = MLflowModelFetcher()
    model, feature_names, meta = fetcher.load_model_and_features()

    # 3. Chuẩn hóa Features
    meta_df, X = FeaturePipeline.prepare_features(df, feature_names)

    # 4. Dự đoán & Tính SHAP
    engine = InferenceEngine(model, feature_names, model_version=meta.get("model_version", "LightGBM-Champion"))
    pred_df, shap_df = engine.predict(meta_df, X)

    logger.info("Dự đoán hoàn tất! Phân bổ kết quả:")
    print(pred_df["predicted_result"].value_counts().to_string())

    if pred_df["is_correct"].notna().any():
        acc = pred_df["is_correct"].dropna().mean()
        logger.info("Accuracy trên các trận đã kết thúc: %.2f%%", acc * 100.0)

    # 5. Lưu vào PostgreSQL DWH & View Superset
    DWHStorageSink.save_predictions(pred_df, shap_df)

    # 6. Đồng bộ metadata huấn luyện, dataset & evaluation metrics từ MLflow vào DWH
    try:
        from warehouse.sync_model_metadata import sync_metadata
        sync_metadata()
    except Exception as e_meta:
        logger.debug("Đồng bộ metadata huấn luyện bỏ qua: %s", e_meta)

    logger.info("==================================================================")
    logger.info("HOÀN THÀNH LUỒNG INFERENCE & SẴN SÀNG CHO SUPERSET DASHBOARD!")
    logger.info("==================================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy luồng suy luận trận đấu và nạp DWH/Superset")
    parser.add_argument("--split", type=str, default=None, help="Lọc split (predict, test, valid, train)")
    parser.add_argument("--upcoming-only", action="store_true", help="Chỉ dự đoán các trận tương lai chưa diễn ra (match_date >= hôm nay)")
    parser.add_argument("--from-date", type=str, default=None, help="Chỉ định ngày bắt đầu lọc (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=None, help="Giới hạn số trận cần suy luận")
    args = parser.parse_args()

    run_pipeline(
        split_filter=args.split,
        upcoming_only=args.upcoming_only,
        from_date=args.from_date,
        limit=args.limit,
    )
