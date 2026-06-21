import pandas as pd

# Parquetファイルを読み込む
df = pd.read_parquet("daily_stock_data.parquet")

# 1. 全体の行数と列数の確認
print("=== データ形状 ===")
print(f"行数: {df.shape[0]}, 列数: {df.shape[1]}")
print(f"列名: {list(df.columns)}")

# 2. データの先頭5行と末尾5行を表示
print("\n=== データの先頭（Head） ===")
print(df.head())

print("\n=== データの末尾（Tail） ===")
print(df.tail())

# 3. 含まれている銘柄コードの一覧と、それぞれの件数を確認
print("\n=== 含まれる銘柄ごとのデータ件数 ===")
print(df['Code'].value_counts())