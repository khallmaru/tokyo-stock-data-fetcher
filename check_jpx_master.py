# -*- coding: utf-8 -*-
"""jpx_master_raw.csv を人間可読（UTF-8）に表示する確認スクリプト。

CSV は cp932 エンコードのため、サーバ上で
    cat jpx_master_raw.csv
すると文字化けして見にくい。このスクリプトで特定シンボルを上から順に
読みやすく表示する。

使い方:
    python check_jpx_master.py              # サマリー + 先頭5銘柄
    python check_jpx_master.py 5401         # 4桁コードで検索
    python check_jpx_master.py 54010        # 5桁コードで検索
    python check_jpx_master.py 5401.T       # .T 付きでも可
    python check_jpx_master.py 5401 8801 2121   # 複数指定可

終了コード:
    0 = 全シンボルが見つかった
    1 = どれかのシンボルが見つからなかった
    2 = ファイル読み込みエラー
"""
import os
import re
import sys
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "jpx_master_raw.csv")

# ロケールが UTF-8 以外（C ロケール等）でも日本語表示でクラッシュしないようにする
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def normalize_code(arg):
    """入力から `.T` や記号を取り去って数字のみ返す（'5401.T' -> '5401'）。"""
    return re.sub(r"[^0-9]", "", str(arg))


def load_master():
    """jpx_master_raw.csv（cp932）を読み込み、コード用の補助列を追加して返す。"""
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: {CSV_PATH} not found.")
        sys.exit(2)
    try:
        df = pd.read_csv(CSV_PATH, encoding="cp932")
    except Exception as e:
        print(f"ERROR: jpx_master_raw.csv を読み込めませんでした: {e}")
        sys.exit(2)
    df["code_str"] = df["銘柄コード"].astype(str)
    df["code4"] = df["code_str"].str[:4]
    return df


def find_rows(df, code):
    """4桁入力=先頭4桁一致 / 5桁入力=完全一致 / その他=先頭一致。"""
    if len(code) == 4:
        return df[df["code4"] == code]
    if len(code) == 5:
        return df[df["code_str"] == code]
    return df[df["code_str"].str.startswith(code)]


def format_value(v):
    if v is None:
        return "(空)"
    try:
        if pd.isna(v):
            return "(空)"
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s if s else "(空)"


def show_row(row):
    for col in row.index:
        if col in ("code_str", "code4"):
            continue
        print(f"  {col:<16}: {format_value(row[col])}")


def run_lookup(df, codes):
    all_ok = True
    for code in codes:
        m = find_rows(df, code)
        if m.empty:
            all_ok = False
            print(f"===== {code} =====")
            print("  見つかりませんでした。")
            # 似たコードを候補として提示（先頭2桁一致）
            if len(code) >= 2:
                cand = (
                    df[df["code4"].str.startswith(code[:2])]
                    .drop_duplicates("code4")
                    .head(5)
                )
                if not cand.empty:
                    print("  似たコード:")
                    for _, r in cand.iterrows():
                        print(f"    {r['code4']}  {format_value(r['銘柄名称'])}  ({format_value(r['市場区分'])})")
            print()
            continue
        title = f"===== {code} ===== (該当 {len(m)} 行)" if len(m) > 1 else f"===== {code} ====="
        print(title)
        for _, row in m.head(20).iterrows():
            show_row(row)
        if len(m) > 20:
            print(f"  ... (残り {len(m) - 20} 行は省略)")
        print()
    sys.exit(0 if all_ok else 1)


def run_summary(df):
    size = os.path.getsize(CSV_PATH)
    mtime = datetime.fromtimestamp(os.path.getmtime(CSV_PATH)).strftime("%Y-%m-%d %H:%M")
    print(f"File: {CSV_PATH}")
    print(f"Size: {size:,} bytes / mtime: {mtime} (local)")
    print(f"Rows: {len(df):,} / Cols: {len(df.columns)}")

    upd = df["更新日"].astype(str)
    print(f"更新日: min={upd.min()} max={upd.max()} (unique {upd.nunique()} 種)")

    delisted = df["東証上場廃止日"].notna().sum()
    print(f"東証上場廃止日が入っている行: {delisted:,}")

    for col in ("市場区分", "商品分類"):
        counts = df[col].value_counts(dropna=False)
        print(f"{col}: " + " / ".join(f"{format_value(k)}={v:,}" for k, v in counts.items()))

    print("\n--- 先頭5銘柄 ---")
    cols = ["銘柄コード", "銘柄名称", "銘柄略称", "市場区分", "業種", "商品分類"]
    cols = [c for c in cols if c in df.columns]
    for _, row in df.head(5).iterrows():
        parts = "  ".join(f"{c}={format_value(row[c])}" for c in cols)
        print("  " + parts)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:]]
    df = load_master()
    codes = [normalize_code(a) for a in args]
    codes = [c for c in codes if c]
    if not codes:
        run_summary(df)
    else:
        run_lookup(df, codes)
