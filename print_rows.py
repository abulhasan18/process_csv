import pandas as pd

df = pd.read_csv("refined_bq_usage.csv")

# Print all column names neatly, one per line
for col in df.columns:
    print(col)


