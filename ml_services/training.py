import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

df = pd.read_csv('Fraud.csv')
X = df.drop(['isFraud', 'isFlaggedFraud', 'nameOrig', 'nameDest'], axis=1)
y = df['isFraud']
X['type'] = X['type'].replace({'PAYMENT': 0, 'TRANSFER': 1, 'CASH_OUT': 2, 'DEBIT': 3, 'CASH_IN': 4})
X['type'] = X['type'].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(X_train)
# # 3. Khởi tạo mô hình XGBoost
# # scale_pos_weight giúp xử lý dữ liệu mất cân bằng (vì Fraud rất ít)
# model = xgb.XGBClassifier(
#     n_estimators=100,
#     max_depth=6,
#     learning_rate=0.1,
#     scale_pos_weight=99, # Điều chỉnh dựa trên tỷ lệ Normal/Fraud
#     use_label_encoder=False,
#     eval_metric='logloss'
# )

# # 4. Huấn luyện
# model.fit(X_train, y_train)


# # 5. Lưu mô hình để Flink sử dụng sau này
# model.save_model('fraud_model.json')