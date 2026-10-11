import nbformat as nbf
import os

def create_nb0_eda():
    nb = nbf.v4.new_notebook()
    nb.cells = [
        nbf.v4.new_markdown_cell("# Bài toán 0: Khai phá dữ liệu chuyên sâu (EDA) & Xử lý khuyết thiếu"),
        nbf.v4.new_code_cell("!pip install -q duckdb pandas matplotlib seaborn missingno"),
        nbf.v4.new_code_cell("""\
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno
from google.colab import userdata

# Kết nối MinIO
con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f\"\"\"
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3, KEY_ID '{userdata.get('MINIO_ACCESS_KEY')}', SECRET '{userdata.get('MINIO_SECRET_KEY')}',
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
\"\"\")

# Đọc toàn bộ bảng Match Features
print("Đang tải dữ liệu...")
df = con.execute("SELECT * FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet')").df()
print(f"Tổng số trận đấu: {len(df)}")
"""),
        nbf.v4.new_code_cell("""\
# 1. PHÂN TÍCH TỶ LỆ KHUYẾT THIẾU (MISSING VALUES)
print("--- TỶ LỆ DỮ LIỆU BỊ THIẾU TỪNG CỘT ---")
missing_pct = (df.isnull().sum() / len(df)) * 100
missing_pct = missing_pct[missing_pct > 0].sort_values(ascending=False)
display(missing_pct.to_frame("Missing %"))

# Trực quan hóa
plt.figure(figsize=(12, 6))
msno.matrix(df[missing_pct.index.tolist()[:20]])
plt.title("Ma trận khuyết thiếu 20 cột tệ nhất")
plt.show()
"""),
        nbf.v4.new_code_cell("""\
# 2. KIỂM TRA NGUỒN OPENLIGA VS API-FOOTBALL (Tiers)
# OpenLiga thường là tier thấp (BL2, BL3) và chỉ có dữ liệu phong độ, không có odds, xG
print("--- PHÂN BỐ GIẢI ĐẤU ---")
print(df['competition_key'].value_counts().head(10))

print("\\n--- TỶ LỆ CÓ TỶ LỆ CƯỢC (ODDS) THEO TIER ---")
# mkt_p_home là xác suất thị trường (từ Odds)
df['has_odds'] = df['f_mkt_p_home'].notnull()
odds_by_tier = df.groupby('competition_key')['has_odds'].mean().sort_values() * 100
print(odds_by_tier)
"""),
        nbf.v4.new_code_cell("""\
# 3. QUYẾT ĐỊNH XỬ LÝ (KHẮC PHỤC DỮ LIỆU)
# - Cột tuổi cầu thủ (f_age_...) hoặc lượt xem Wiki (f_wiki_...) nếu missing > 70% có thể phải drop hoặc train model riêng.
# - Với XGBoost, thuật toán tự xử lý được NaN (Sparsity Aware), nhưng nếu 9000 trận OpenLiga KHÔNG CÓ tỷ lệ cược (mkt_p), 
#   XGBoost sẽ gộp các trận OpenLiga vào chung 1 nhánh.
# => Khắc phục: Ta có thể tạo 2 Model riêng:
#    + Model 1 (Premium): Chỉ train trên các giải Top (có odds, có thời tiết, xG).
#    + Model 2 (Basic): Train trên toàn bộ (kể cả OpenLiga), chỉ dùng features phong độ.

# Hàm tạo tập dữ liệu Premium (Chỉ lấy các trận có Odds)
df_premium = df[df['has_odds'] == True].copy()
features_premium = [c for c in df_premium.columns if c.startswith('f_')]

print(f"Số trận Premium (Đầy đủ dữ liệu): {len(df_premium)}")
print(f"Số trận Basic (Chỉ có phong độ): {len(df)}")
""")
    ]
    with open('00_detailed_eda_and_data_cleaning.ipynb', 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

def create_nb1():
    nb = nbf.v4.new_notebook()
    nb.cells = [
        nbf.v4.new_markdown_cell("# Bài toán 1: Dự đoán kết quả trận đấu (Phân tách Premium vs Basic Model)"),
        nbf.v4.new_markdown_cell("Sau khi EDA, ta nhận thấy dữ liệu OpenLiga bị thiếu Odds và xG. Ta sẽ chia ra huấn luyện 2 mô hình khác nhau để tối ưu hóa đặc trưng thay vì nhét chung tất cả vào 1 mô hình XGBoost (dù XGBoost xử lý được NaN nhưng sẽ làm loãng trọng số của Odds)."),
        nbf.v4.new_code_cell("!pip install -q mlflow==3.16.1 pysocks kaggle xgboost shap duckdb"),
        nbf.v4.new_code_cell("""\
# Upload file colab_utils.py từ máy tính
from google.colab import files
uploaded = files.upload()
print("Uploaded:", list(uploaded.keys()))

import importlib
import colab_utils
importlib.invalidate_caches()
importlib.reload(colab_utils)
print("File đang dùng:", colab_utils.__file__)
"""),
        nbf.v4.new_code_cell("""\
import duckdb
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score
import colab_utils
from google.colab import userdata

colab_utils.install_tailscale()
colab_utils.setup_tailscale(auth_key=userdata.get('TAILSCALE_AUTH_KEY'))
colab_utils.setup_mlflow("http://20.41.113.183:8080", experiment="Match-Prediction-DualModels")

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f\"\"\"
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3, KEY_ID '{userdata.get('MINIO_ACCESS_KEY')}', SECRET '{userdata.get('MINIO_SECRET_KEY')}',
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
\"\"\")

# Đọc kết hợp 2 bảng Match Features
df = con.execute(\"\"\"
    SELECT m.*, t.f_form5 
    FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet') m
    LEFT JOIN read_parquet('s3://football-lake/dwh/mart_ml_team_match_features.parquet') t
    ON m.match_key = t.match_key AND t.side = 'home'
    WHERE m.is_warm = true AND m.is_labeled = true
\"\"\").df()
df['target'] = df['y_result_code'].astype(int)
"""),
        nbf.v4.new_code_cell("""\
# KHẮC PHỤC DỮ LIỆU: Tách 2 tập (Premium vs Basic)
# Premium: 6 giải lớn có xác suất thị trường (Odds)
# Basic: Bao gồm OpenLiga (Không dùng Odds)

df_premium = df[df['f_mkt_p_home'].notnull()].copy()
df_basic = df.copy() # Dùng toàn bộ data

# Features
feat_premium = [c for c in df_premium.columns if c.startswith('f_')]
# Bỏ các cột Odds và Wiki, Age khỏi Basic vì OpenLiga ko có
feat_basic = [c for c in df_basic.columns if c.startswith('f_') and not c.startswith('f_mkt_') and not c.startswith('f_wiki_')]

print(f"Premium data: {len(df_premium)} trận | Basic data: {len(df_basic)} trận")
"""),
        nbf.v4.new_code_cell("""\
import mlflow
from sklearn.metrics import classification_report, log_loss

def train_eval(data, features, model_name):
    print(f"\\n--- Training {model_name} ---")
    train = data[data['split'] == 'train']
    valid = data[data['split'] == 'valid']
    test = data[data['split'] == 'test']
    
    # Định nghĩa tham số mô hình
    params = {
        'objective': 'multi:softprob',
        'num_class': 3,
        'learning_rate': 0.05,
        'max_depth': 4,
        'n_estimators': 300,
        'random_state': 42
    }
    
    with mlflow.start_run(run_name=model_name):
        # 1. Log tham số (Parameters)
        mlflow.log_params(params)
        mlflow.log_param("n_features", len(features))
        
        # 2. Khởi tạo & Huấn luyện
        model = xgb.XGBClassifier(**params)
        model.fit(train[features], train['target'], 
                  eval_set=[(valid[features], valid['target'])], 
                  verbose=False)
        
        # 3. Đánh giá (Metrics)
        # Trên Test
        preds = model.predict(test[features])
        probs = model.predict_proba(test[features])
        acc_test = accuracy_score(test['target'], preds)
        loss_test = log_loss(test['target'], probs)
        
        # Trên Valid
        acc_valid = accuracy_score(valid['target'], model.predict(valid[features]))
        
        # Log Metrics
        mlflow.log_metrics({
            "test_accuracy": acc_test,
            "test_log_loss": loss_test,
            "valid_accuracy": acc_valid
        })
        
        print(f"[{model_name}] Test Acc: {acc_test:.4f} | Valid Acc: {acc_valid:.4f}")
        print("\\nClassification Report (Test):\\n", classification_report(test['target'], preds))
        
        # 4. Lưu Model vào MLflow
        model_info = mlflow.xgboost.log_model(model, artifact_path="model")
        print(f"Đã lưu mô hình tại URI: {model_info.model_uri}")
        
        # Lưu các giá trị mẫu để tí nữa test tải model
        return model_info.model_uri, test

# Thực thi Train cho cả 2 nhánh (Premium vs Basic)
uri_premium, test_premium = train_eval(df_premium, feat_premium, "Premium_Model_With_Odds")
uri_basic, test_basic = train_eval(df_basic, feat_basic, "Basic_Model_Form_Only")
"""),
        nbf.v4.new_code_cell("""\
# ==========================================
# 5. TẢI MÔ HÌNH VỀ TỪ MLFLOW ĐỂ DÙNG THỬ (INFERENCE)
# ==========================================
import mlflow.xgboost

print("Đang tải lại mô hình Premium từ MLflow...")
loaded_model = mlflow.xgboost.load_model(uri_premium)

# Thử nghiệm dự đoán trên 5 trận đầu tiên của tập Test
sample_data = test_premium.head(5)
sample_features = sample_data[feat_premium]

print("Dự đoán 5 trận đầu (0=Sân khách, 1=Hòa, 2=Sân nhà):")
predictions = loaded_model.predict(sample_features)
probabilities = loaded_model.predict_proba(sample_features)

for i in range(5):
    print(f"Trận {i+1}:")
    print(f" - Nhãn thực tế: {sample_data['target'].iloc[i]}")
    print(f" - Mô hình dự đoán: {predictions[i]}")
    print(f" - Xác suất [Khách, Hòa, Nhà]: {probabilities[i]}\\n")
""")
    ]
    with open('01_match_prediction_shap.ipynb', 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

def create_nb2():
    nb = nbf.v4.new_notebook()
    nb.cells = [
        nbf.v4.new_markdown_cell("# Bài toán 2: Dự đoán Bàn thắng Kì vọng (xG Model)"),
        nbf.v4.new_code_cell("!pip install -q mlflow==3.16.1 pysocks kaggle xgboost shap duckdb"),
        nbf.v4.new_code_cell("""\
# Upload file colab_utils.py từ máy tính
from google.colab import files
uploaded = files.upload()
print("Uploaded:", list(uploaded.keys()))

import importlib
import colab_utils
importlib.invalidate_caches()
importlib.reload(colab_utils)
print("File đang dùng:", colab_utils.__file__)
"""),
        nbf.v4.new_code_cell("""\
import colab_utils
from google.colab import userdata

colab_utils.install_tailscale()
colab_utils.setup_tailscale(auth_key=userdata.get('TAILSCALE_AUTH_KEY'))
colab_utils.setup_mlflow("http://20.41.113.183:8080", experiment="xG-Prediction")
"""),
        nbf.v4.new_code_cell("""\
import duckdb
import pandas as pd

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f\"\"\"
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3, KEY_ID '{userdata.get('MINIO_ACCESS_KEY')}', SECRET '{userdata.get('MINIO_SECRET_KEY')}',
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
\"\"\")

# Dữ liệu ~100k cú sút, tỷ lệ bàn thắng ~10%
df_shots = con.execute("SELECT * FROM read_parquet('s3://football-lake/dwh/mart_ml_shots.parquet')").df()
print("Phân phối nhãn:", df_shots['y_is_goal'].value_counts())
display(df_shots.head())
"""),
        nbf.v4.new_code_cell("""\
# Binary Classification
features = [c for c in df_shots.columns if c.startswith('f_') and pd.api.types.is_numeric_dtype(df_shots[c])]
df_shots = df_shots.dropna(subset=features + ['y_is_goal'])

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(df_shots[features], df_shots['y_is_goal'], test_size=0.2, random_state=42)

import xgboost as xgb
from sklearn.metrics import roc_auc_score, classification_report
import mlflow
import mlflow.xgboost

# Thiết lập siêu tham số
params = {
    'objective': 'binary:logistic',
    'scale_pos_weight': 9, # vì tỷ lệ 90% trượt, 10% vào
    'learning_rate': 0.05,
    'max_depth': 5,
    'n_estimators': 300,
    'random_state': 42
}

with mlflow.start_run(run_name="xg_model_binary"):
    # 1. Log params
    mlflow.log_params(params)
    mlflow.log_param("n_features", len(features))
    
    # 2. Huấn luyện
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    
    # 3. Đánh giá (Metrics)
    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]
    
    auc = roc_auc_score(y_test, probs)
    print(f"ROC-AUC: {auc:.4f}")
    print("\\nClassification Report:\\n", classification_report(y_test, preds))
    
    mlflow.log_metrics({
        "test_roc_auc": auc
    })
    
    # 4. Lưu Model vào MLflow
    model_info = mlflow.xgboost.log_model(model, artifact_path="model")
    print(f"Đã lưu mô hình tại URI: {model_info.model_uri}")
    
# ==========================================
# 5. TẢI MÔ HÌNH VỀ TỪ MLFLOW ĐỂ DÙNG THỬ
# ==========================================
print("\\n--- Đang tải lại mô hình từ MLflow... ---")
loaded_xg_model = mlflow.xgboost.load_model(model_info.model_uri)

sample_data = X_test.head(5)
sample_labels = y_test.head(5)

print("Dự đoán xG (Expected Goals) cho 5 cú sút đầu tiên trong tập Test:")
pred_xg = loaded_xg_model.predict_proba(sample_data)[:, 1]

for i in range(5):
    print(f"Cú sút {i+1}: Thực tế = {'VÀO' if sample_labels.iloc[i]==1 else 'TRƯỢT'} | xG dự đoán = {pred_xg[i]:.2%}")
""")
    ]
    with open('02_xg_model_prediction.ipynb', 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

def create_nb3():
    nb = nbf.v4.new_notebook()
    nb.cells = [
        nbf.v4.new_markdown_cell("# Bài toán 3: Phân tích Cảm xúc Văn bản (NLP) - Knowledge Distillation"),
        nbf.v4.new_markdown_cell("Ở bài toán này, vì dữ liệu thô từ YouTube Comments không có nhãn (label) sẵn, ta sẽ áp dụng kỹ thuật **Teacher-Student (Knowledge Distillation)**:\\n1. **Teacher**: Dùng mô hình BERT đa ngữ của Hugging Face để gán nhãn tự động (Pseudo-labeling) cho một tập dữ liệu nhỏ.\\n2. **Student**: Train một mô hình TF-IDF + Logistic Regression siêu nhẹ dựa trên các nhãn đó để có thể dự đoán cực nhanh.\\n3. **MLflow**: Theo dõi toàn bộ quá trình train Student, log siêu tham số, metrics và lưu mô hình Scikit-Learn."),
        nbf.v4.new_code_cell("!pip install -q duckdb transformers sentencepiece wordcloud mlflow==3.16.1 pysocks kaggle scikit-learn"),
        nbf.v4.new_code_cell("""\
# Upload file colab_utils.py từ máy tính
from google.colab import files
uploaded = files.upload()
print("Uploaded:", list(uploaded.keys()))

import importlib
import colab_utils
importlib.invalidate_caches()
importlib.reload(colab_utils)
print("File đang dùng:", colab_utils.__file__)
"""),
        nbf.v4.new_code_cell("""\
import colab_utils
from google.colab import userdata

colab_utils.install_tailscale()
colab_utils.setup_tailscale(auth_key=userdata.get('TAILSCALE_AUTH_KEY'))
colab_utils.setup_mlflow("http://20.41.113.183:8080", experiment="NLP-Sentiment")
"""),
        nbf.v4.new_code_cell("""\
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f\"\"\"
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3, KEY_ID '{userdata.get('MINIO_ACCESS_KEY')}', SECRET '{userdata.get('MINIO_SECRET_KEY')}',
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
\"\"\")

# Lấy dữ liệu YouTube Comments (Lấy mẫu 2000 dòng để train nhanh)
df_yt = con.execute("SELECT text FROM read_parquet('s3://football-lake/dwh/mart_ml_text_youtube_comments.parquet') WHERE text IS NOT NULL LIMIT 2000").df()
print(f"Số lượng bình luận YouTube: {len(df_yt)}")
df_yt = df_yt.dropna()
texts = df_yt['text'].tolist()
"""),
        nbf.v4.new_code_cell("""\
# ==========================================
# 1. TEACHER: TẠO PSEUDO-LABELS BẰNG BERT
# ==========================================
from transformers import pipeline
from tqdm.auto import tqdm

print("Đang tải Teacher Model (BERT)...")
teacher = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment", device=-1) # dùng CPU hoặc GPU

print("Đang gán nhãn tự động cho tập dữ liệu (Pseudo-labeling)...")
# Label của model này là: '1 star', '2 stars' (Negative), '3 stars' (Neutral), '4 stars', '5 stars' (Positive)
results = teacher(texts, batch_size=16, truncation=True, max_length=512)

# Tiền xử lý nhãn về dạng 3 class: 0 (Negative), 1 (Neutral), 2 (Positive)
pseudo_labels = []
for res in results:
    stars = int(res['label'][0])
    if stars <= 2: pseudo_labels.append(0)
    elif stars == 3: pseudo_labels.append(1)
    else: pseudo_labels.append(2)

df_yt['label'] = pseudo_labels
print("Phân phối nhãn sinh ra:", df_yt['label'].value_counts())
"""),
        nbf.v4.new_code_cell("""\
# ==========================================
# 2. STUDENT: HUẤN LUYỆN TF-IDF + LOGISTIC REGRESSION CÓ MLFLOW
# ==========================================
import mlflow
import mlflow.sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

X_train, X_test, y_train, y_test = train_test_split(df_yt['text'], df_yt['label'], test_size=0.2, random_state=42)

# Cấu hình siêu tham số
params = {
    "tfidf__max_features": 5000,
    "tfidf__ngram_range": (1, 2),
    "lr__C": 1.0,
    "lr__max_iter": 500
}

with mlflow.start_run(run_name="student_tfidf_lr"):
    # Log tham số
    mlflow.log_params(params)
    
    # Định nghĩa Pipeline: TF-IDF -> Logistic Regression
    pipeline_model = Pipeline([
        ('tfidf', TfidfVectorizer(max_features=params["tfidf__max_features"], ngram_range=params["tfidf__ngram_range"])),
        ('lr', LogisticRegression(C=params["lr__C"], max_iter=params["lr__max_iter"], multi_class='multinomial'))
    ])
    
    # Huấn luyện
    print("Đang huấn luyện Student Model (TF-IDF + LR)...")
    pipeline_model.fit(X_train, y_train)
    
    # Đánh giá
    preds = pipeline_model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"Accuracy (Student): {acc:.4f}")
    print("\\nClassification Report:\\n", classification_report(y_test, preds, target_names=["Negative", "Neutral", "Positive"]))
    
    # Log metrics
    mlflow.log_metric("test_accuracy", acc)
    
    # Lưu Model Pipeline
    model_info = mlflow.sklearn.log_model(pipeline_model, "student_nlp_model")
    print(f"\\nĐã lưu mô hình NLP tại URI: {model_info.model_uri}")
    
# Gán biến để test bước 5
student_uri = model_info.model_uri
"""),
        nbf.v4.new_code_cell("""\
# ==========================================
# 3. TẢI MÔ HÌNH TỪ MLFLOW VÀ CHẠY DỰ ĐOÁN THỰC TẾ
# ==========================================
import mlflow.sklearn

print("Đang tải lại Student Model từ MLflow...")
loaded_nlp_model = mlflow.sklearn.load_model(student_uri)

sample_texts = [
    "Cầu thủ này đá quá tệ, thật thất vọng!",
    "Trận đấu diễn ra bình thường, không có gì nổi bật.",
    "Bàn thắng tuyệt vời! Cảm xúc vỡ òa, quá đỉnh!",
    "HLV chiến thuật quá sai lầm, đội bóng mất định hướng."
]

print("\\nKẾT QUẢ DỰ ĐOÁN:")
predictions = loaded_nlp_model.predict(sample_texts)
label_map = {0: "Tiêu cực 😡", 1: "Trung tính 😐", 2: "Tích cực 😍"}

for text, pred in zip(sample_texts, predictions):
    print(f"Text: '{text}'\\n => Phân loại: {label_map[pred]}\\n")
""")
    ]
    with open('03_nlp_sentiment_trends.ipynb', 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

if __name__ == '__main__':
    create_nb0_eda()
    create_nb1()
    create_nb2()
    create_nb3()
