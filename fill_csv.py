import pandas as pd
from datetime import datetime, timedelta
import random

df = pd.read_csv("refined_bq_usage.csv")
rows = []

for i in range(100):
    row = df.iloc[i % len(df)].copy()
    start_dt = datetime.strptime(row['start_time'], "%Y-%m-%d %H:%M:%S.%f") + timedelta(minutes=i)
    end_dt = datetime.strptime(row['end_time'], "%Y-%m-%d %H:%M:%S.%f") + timedelta(minutes=i)

    row['start_time'] = start_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    row['end_time'] = end_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    row['date'] = start_dt.date()
    row['user_email'] = f"user{i}@example.com"
    row['project_id'] = f"project_{i % 5}"
    row['region'] = random.choice(['US', 'EU', 'ASIA-NORTHEAST1', 'AU', 'US-CENTRAL1'])
    row['estimated_cost'] = round(random.uniform(0.01, 2.00), 4)
    row['bytes_processed'] = random.randint(1e6, 1e9)
    row['bytes_billed'] = random.randint(1e6, 1e9)
    row['output_rows'] = random.randint(10, 5000)
    row['slot_ms'] = random.randint(100, 20000)
    row['duration_seconds'] = random.randint(1, 60)
    row['status'] = random.choice(['SUCCESS', 'FAILED'])
    row['gb_processed'] = round(row['bytes_processed'] / 1024**3, 2)
    row['gb_billed'] = round(row['bytes_billed'] / 1024**3, 2)
    row['cost_per_row'] = round((row['estimated_cost'] / max(row['output_rows'], 1)), 6)
    row['mb_per_row'] = round((row['bytes_processed'] / max(row['output_rows'], 1)) / 1024**2, 2)
    row['is_wasteful_query'] = row['bytes_processed'] > 5 * 1024**3 and row['output_rows'] < 1000
    row['day_of_week'] = start_dt.strftime("%A")
    row['hour'] = start_dt.hour

    rows.append(row)

df_mock = pd.DataFrame(rows)
df_mock.to_csv("refined_bq_usage_mock.csv", index=False)
