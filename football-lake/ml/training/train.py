import os
import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import xgboost as xgb
from sklearn.metrics import accuracy_score, log_loss, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# 1. Load môi trường
env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(env_path)

def load_data_from_lake():
    print("[1/5] Khởi tạo kết nối DuckDB tới MinIO...")
    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000').replace('http://', '').replace('https://', '')
    minio_ak = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_sk = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    
    con.execute(f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{minio_ak}',
            SECRET '{minio_sk}',
            ENDPOINT '{minio_endpoint}',
            URL_STYLE 'path',
            USE_SSL 'false'
        );
    """)
    
    path = os.getenv('ML_FEATURES_PATH', 's3://football-lake/dwh/mart_ml_match_features.parquet')
    print(f"[2/5] Đang tải bảng đặc trưng từ mart: {path}")
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
        print(f"  -> Tải thành công {len(df):,} dòng.")
        return df
    except Exception as e:
        print(f"[!] Lỗi khi tải dữ liệu: {e}")
        exit(1)

def select_features(df):
    """Đặc trưng = cột SỐ bắt đầu bằng 'f_' (mart_ml_match_features đảm bảo f_* chỉ dùng thông tin trước giờ đá).
    KHÔNG chọn theo kiểu "mọi cột trừ khoá": một cột metadata/hậu-trận lọt vào sẽ âm thầm thành đặc trưng (rò rỉ nhãn).
    ML_DROP_PREFIXES="f_mkt_" để huấn luyện mô hình thuần phong độ, không dùng tỉ lệ cược."""
    drop = tuple(p.strip() for p in os.getenv('ML_DROP_PREFIXES', '').split(',') if p.strip())
    feats = [c for c in df.columns
             if c.startswith('f_') and pd.api.types.is_numeric_dtype(df[c]) and not (drop and c.startswith(drop))]
    bad = [c for c in feats if c.startswith('y_')]
    assert not bad, f"đặc trưng chứa cột nhãn: {bad}"
    return feats


def preprocess_data(df):
    print("[3/5] Tiền xử lý dữ liệu...")
    # Chỉ lấy trận đã có nhãn và đủ lịch sử (is_warm); trận 'predict' (chưa đá) dùng cho suy luận, không huấn luyện
    df = df[df['is_warm'].astype(bool) & df['is_labeled'].astype(bool)].copy()

    # Nhãn: Away win=0, Draw=1, Home win=2 (đã mã hoá sẵn trong mart)
    df['target_encoded'] = df['y_result_code'].astype(int)
    features = select_features(df)

    # Chia Train / Valid / Test theo cột 'split' do mart tạo (chia THEO THỜI GIAN, không ngẫu nhiên)
    train_df = df[df['split'] == 'train']
    valid_df = df[df['split'] == 'valid']
    test_df = df[df['split'] == 'test']

    print(f"  -> Tập Train: {len(train_df)} trận")
    print(f"  -> Tập Valid: {len(valid_df)} trận")
    print(f"  -> Tập Test:  {len(test_df)} trận")
    print(f"  -> Số lượng Features: {len(features)}")

    return train_df, valid_df, test_df, features

def train_xgboost(train_df, valid_df, features):
    print("[4/5] Bắt đầu huấn luyện mô hình XGBoost...")
    
    X_train, y_train = train_df[features], train_df['target_encoded']
    X_valid, y_valid = valid_df[features], valid_df['target_encoded']
    
    # Cấu hình tham số XGBoost cho bài toán Multi-class
    params = {
        'objective': 'multi:softprob',
        'num_class': 3,
        'eval_metric': 'mlogloss',
        'learning_rate': 0.05,
        'max_depth': 4,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'n_estimators': 500,
        'early_stopping_rounds': 30,   # dừng khi log-loss tập valid không cải thiện 30 vòng (tránh overfit: không có thì 500 cây xấu đi sau ~100)
        'random_state': 42
    }
    
    model = xgb.XGBClassifier(**params)
    
    # Huấn luyện mô hình và sử dụng Early Stopping
    model.fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],   # early stopping dùng eval_set cuối => chỉ tập valid
        verbose=50
    )
    
    print("  -> Huấn luyện xong!")
    return model

def evaluate_and_save(model, test_df, features):
    print("[5/5] Đánh giá mô hình trên tập Test...")
    
    X_test, y_test = test_df[features], test_df['target_encoded']
    
    # Dự đoán xác suất và nhãn
    y_pred_proba = model.predict_proba(X_test)
    y_pred = model.predict(X_test)
    
    # Tính các chỉ số
    acc = accuracy_score(y_test, y_pred)
    loss = log_loss(y_test, y_pred_proba)
    
    print(f"\n=== KẾT QUẢ ĐÁNH GIÁ (TEST SET) ===")
    print(f"Accuracy : {acc:.4f}")
    print(f"Log Loss : {loss:.4f}")
    print("\nChi tiết (0: Khách Thắng, 1: Hòa, 2: Chủ Thắng):")
    print(classification_report(y_test, y_pred))
    
    # Vẽ Feature Importance
    print("[*] Đang lưu biểu đồ mức độ quan trọng của Features...")
    importances = model.feature_importances_
    indices = np.argsort(importances)[-20:] # Lấy top 20 features
    
    plt.figure(figsize=(10, 8))
    plt.title('Top 20 Features Quan Trọng Nhất')
    plt.barh(range(len(indices)), importances[indices], color='b', align='center')
    plt.yticks(range(len(indices)), [features[i] for i in indices])
    plt.xlabel('Độ quan trọng (Relative Importance)')
    
    output_dir = os.path.dirname(__file__)
    plt.savefig(os.path.join(output_dir, 'feature_importance.png'), bbox_inches='tight')
    
    # Lưu Model
    model_path = os.path.join(output_dir, 'xgboost_match_predictor.pkl')
    joblib.dump(model, model_path)
    print(f"[*] Đã lưu mô hình tại: {model_path}")
    print(f"[*] Đã lưu biểu đồ tại: {os.path.join(output_dir, 'feature_importance.png')}")

if __name__ == "__main__":
    df = load_data_from_lake()
    train_df, valid_df, test_df, features = preprocess_data(df)
    model = train_xgboost(train_df, valid_df, features)
    evaluate_and_save(model, test_df, features)
