"""Module tích hợp luồng Text Intelligence (Inference -> DWH -> Superset) vào Football Lakehouse.
Chạy trực tiếp: python warehouse/text_inference.py
Chạy qua Airflow: dag_text_inference.py gọi hàm run_text_inference()
"""
import sys
import logging
from pathlib import Path

# Đảm bảo import được module trong project
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from warehouse.text_pipeline.config import Settings
from warehouse.text_pipeline.run import execute

logger = logging.getLogger("text_inference")


def run_text_inference(publish: bool = True):
    """Hàm entrypoint để Airflow hoặc Scheduler gọi."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    cfg = Settings.from_env()
    cfg.publish = publish
    logger.info("Khởi động phiên suy luận Text Intelligence (publish=%s)...", publish)
    return execute(cfg)


if __name__ == "__main__":
    run_text_inference(publish=True)
