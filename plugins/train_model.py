import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor
import os
from sqlalchemy import create_engine


pg_database = os.environ['PG_DATABASE']
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_table = 'trends'
engine = create_engine(f'postgresql://{pg_user}:{pg_password}@postgres:5432/{pg_database}')


df = pd.read_csv('/opt/airflow/logs/entity_daily_counts.csv', parse_dates=["published_date"])

df = df.rename(columns={"entity_text": "group_id", "count_mentions": "target"})

N_LAGS = 5
for lag in range(1, N_LAGS + 1):
    df[f"lag_{lag}"] = df.groupby("group_id")["target"].shift(lag)

df = df.dropna().reset_index(drop=True)

le = LabelEncoder()
df["group_id_enc"] = le.fit_transform(df["group_id"])
df["dayofweek"] = df["published_date"].dt.dayofweek
df["dayofmonth"] = df["published_date"].dt.day


lag_features = [f"lag_{i}" for i in range(1, N_LAGS + 1)]
features = lag_features + ["group_id_enc", "dayofweek", "dayofmonth"]
target = "target"

X = df[features]
y = df[target]

model = XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.1,
                     subsample=0.8, colsample_bytree=0.8, random_state=42)
model.fit(X, y)

last_date = df["published_date"].max()
next_date = last_date + pd.Timedelta(days=1)
predictions = []

for keyword in df["group_id"].unique():
    keyword_data = df[df["group_id"] == keyword].tail(N_LAGS)
    feature_row = {f"lag_{i}": keyword_data.iloc[-i]["target"] for i in range(1, N_LAGS + 1)}
    feature_row["group_id_enc"] = le.transform([keyword])[0]
    feature_row["dayofweek"] = next_date.dayofweek
    feature_row["dayofmonth"] = next_date.day
    feature_df = pd.DataFrame([feature_row])
    pred = max(0, round(model.predict(feature_df)[0]))
    predictions.append({"group_id": keyword, "date": next_date, "predicted_count": pred})

pred_df = pd.DataFrame(predictions)

try:
    pred_df.to_sql(pg_table, engine, if_exists='append', index=False)
except Exception as e:
    print(e)

print(pred_df.head(10))
