import pandas as pd

df = pd.read_csv('./raw/order.csv')

# drop useless columns
df = df.drop(columns=[c for c in df.columns if "Unnamed" in c])

