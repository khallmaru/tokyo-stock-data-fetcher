import pandas as pd
import yfinance as yf
import time
import os
from jpx_master_manager import get_jpx_codes_from_master
from datetime import datetime, timedelta

# =============================================================================
# 設定（環境変数で上書き可能）
# =============================================================================
FULL_REFRESH = os.environ.get("FULL_REFRESH", "0").strip().upper() in ("1", "TRUE", "YES")
DATA_WINDOW_DAYS = 730            # 保存する履歴の長さ（約2年）
SPLIT_GAP_THRESHOLD = float(os.environ.get("SPLIT_GAP_THRESHOLD", "0.35"))
# 分割候補を確定させる際に許容する ギャップ日↔分割記録日 の日数差
# （yf.Ticker(code).splits は全履歴を返すため、「過去に分割記録があるだけで」
#  実際のギャップの原因と無関係な分割記録に引っかかって過剰再取得するのを防ぐ）
SPLIT_VERIFY_WINDOW_DAYS = int(os.environ.get("SPLIT_VERIFY_WINDOW_DAYS", "15"))
OUTPUT_FILENAME = "daily_stock_data.parquet"
TARGET_COLUMNS = ['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
SPLIT_LOG_FILENAME = "splits_refresh.log"
FAILED_LOG_FILENAME = "failed_chunks.log"

# --- レート制限対策（チャンク単位バルク取得） ---
CHUNK_SIZE = 50      # yfinance の安定性と取得効率のバランス
INITIAL_SLEEP = 20   # チャンク成功時: 20秒待機
RETRY_SLEEP = 30     # 再試行時: 30秒待機

# 注: 全取得経路で auto_adjust=False（生価格・未調整）を明示的に固定する。
# 理由: 株式分割/併合時に「旧履歴（生価格）+ 新履歴（調整済み）」が混在すると
#       境界に整合しないギャップが残り、差分方式では修正不能になるため。
# 生価格なら分割ギャップがそのまま価格の乖離として現れ、
# detect_and_refresh_splits() が検知→該当銘柄のみ2年分再取得で自己修復する。


def load_existing_data():
    """リポジトリに保存済みの Parquet を読み込む（無ければ空）。"""
    if os.path.exists(OUTPUT_FILENAME):
        print(f"Loading existing data from {OUTPUT_FILENAME}...")
        try:
            existing_df = pd.read_parquet(OUTPUT_FILENAME)
            print(f"Loaded {len(existing_df):,} rows for {existing_df['Code'].nunique():,} codes.")
            return existing_df
        except Exception as e:
            print(f"⚠️  Failed to load existing parquet: {e}")
    else:
        print(f"No existing {OUTPUT_FILENAME} found.")
    print("   Falling back to full refresh for all codes.")
    return pd.DataFrame(columns=TARGET_COLUMNS)


def build_fetch_plan(all_codes, existing_df, jst_now):
    """銘柄ごとの差分取得 start 日付を割り当てる。

    - 新規銘柄（既存Parquetにない）  : start = 現在 - 730日（2年分一括）
    - 未更新銘柄（最終日 < 昨日JST） : start = その銘柄の最終日 + 1日
    - 最新化済み銘柄（最終日 >= 昨日JST）: スキップ
    FULL_REFRESH=1 なら全銘柄 2年分一括。

    戻り値: {start日文字列: [codes]}（yf.download は共通 start のため start 日でグループ化）
    """
    full_start = (jst_now - timedelta(days=DATA_WINDOW_DAYS)).strftime('%Y-%m-%d')

    if FULL_REFRESH:
        print("\n=== FULL_REFRESH mode: fetching 2 years for ALL codes ===")
        return {full_start: list(all_codes)}

    yst_jst = (jst_now - timedelta(days=1)).strftime('%Y-%m-%d')

    last_date_by_code = {}
    if not existing_df.empty and {'Code', 'Date'} <= set(existing_df.columns):
        last_date_by_code = (
            existing_df.assign(Date=existing_df['Date'].astype(str))
            .groupby('Code')['Date'].max().to_dict()
        )

    plan = {}
    new_count = delta_count = up_to_date_count = 0
    for code in all_codes:
        last_date = last_date_by_code.get(code)
        if last_date is None:
            start = full_start
            new_count += 1
        elif last_date >= yst_jst:
            up_to_date_count += 1
            continue
        else:
            start = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            delta_count += 1
        plan.setdefault(start, []).append(code)

    total = len(all_codes)
    print("\n=== Incremental fetch plan ===")
    print(f"  New codes (full 2y history) : {new_count} ({new_count / max(total, 1):.1%})")
    print(f"  Codes needing delta update  : {delta_count} ({delta_count / max(total, 1):.1%})")
    print(f"  Already up to date (skip)   : {up_to_date_count} ({up_to_date_count / max(total, 1):.1%})")
    for start, codes in sorted(plan.items()):
        print(f"  start={start}: {len(codes)} codes")
    return plan


def fetch_chunked(codes, start_date, end_date):
    """指定銘柄リスト・日付範囲のチャンク単位バルク取得と結合を行い、(final_df, failed_chunks) を返す。"""
    total_count = len(codes)

    # レート制限対策のローカル名前（下部のチャンク処理ループは既存実装を流用）
    chunk_size = CHUNK_SIZE
    initial_sleep = INITIAL_SLEEP
    retry_sleep = RETRY_SLEEP
    all_codes = codes

    all_chunks_data = []
    failed_chunks = []  # 失敗したチャンク情報を記録

    print(f"Starting bulk download in chunks of {chunk_size}...")

    # チャンクを分割してループ
    for i in range(0, total_count, chunk_size):
        chunk_codes = all_codes[i:i + chunk_size]
        current_block = (i // chunk_size) + 1
        total_blocks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)

        print(f"[{current_block}/{total_blocks}] Downloading {len(chunk_codes)} stocks...")
        
        # チャンク処理（最初の試行）
        chunk_success = False
        for retry_count in range(2):  # 最初の試行 + 1回の再試行 = 計2回
            try:
                # ダウンロード実行
                data = yf.download(
                    tickers=" ".join(chunk_codes),
                    start=start_date,
                    end=end_date,
                    group_by='ticker',
                    threads=False,
                    multi_level_index=False,
                    auto_adjust=False,  # 生価格（未調整）固定: 分割ギャップ検知のため
                )
                
                if data.empty:
                    print(f"  ⚠️  Chunk {current_block} returned empty data.")
                    chunk_success = False
                    break

                # 警告対策として future_stack=True を指定
                df_stacked = data.stack(level=0, future_stack=True).reset_index()
                
                # --- 列名の自動判別と安全なリネーム ---
                df_stacked.columns.values[0] = 'Date'
                df_stacked.columns.values[1] = 'Code'
                
                rename_dict = {
                    'Open': 'Open',
                    'High': 'High',
                    'Low': 'Low',
                    'Close': 'Close',
                    'Volume': 'Volume'
                }
                df_stacked = df_stacked.rename(columns=rename_dict)

                # 実際に存在する列だけを抽出
                target_columns = ['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
                available_columns = [col for col in target_columns if col in df_stacked.columns]
                
                df_cleaned = df_stacked[available_columns]
                all_chunks_data.append(df_cleaned)
                
                print(f"  ✅ Chunk {current_block} downloaded successfully.")
                chunk_success = True
                break  # 成功したらリトライループを抜ける

            except Exception as e:
                error_msg = str(e)
                if retry_count == 0:
                    # 1回目の失敗 → 再試行予告
                    print(f"  ⚠️  Chunk {current_block} failed: {error_msg}")
                    print(f"     Retrying after {retry_sleep} seconds...")
                    time.sleep(retry_sleep)
                else:
                    # 2回目の失敗 → ログ記録
                    print(f"  ❌ Chunk {current_block} failed after retry: {error_msg}")
                    failed_chunks.append({
                        'block': current_block,
                        'codes': chunk_codes,
                        'error': error_msg
                    })
        
        # チャンク成功時は初回sleepを実行
        if chunk_success:
            time.sleep(initial_sleep)
    
    # --- すべてのブロックのデータを1つに結合 ---
    if all_chunks_data:
        print("\nCombining all chunks into one file...")
        final_df = pd.concat(all_chunks_data, ignore_index=True)

        # 念のため重複データを排除
        final_df.drop_duplicates(subset=['Date', 'Code'], inplace=True)
    else:
        final_df = pd.DataFrame()

    return final_df, failed_chunks


def detect_and_refresh_splits(combined, end_date):
    """終値の1日ギャップから株式分割・併合を検知し、該当銘柄のみの履歴を再取得して自己修復する。

    1) 全データで 直前終値比 ±SPLIT_GAP_THRESHOLD 超過の銘柄を候補とする
    2) yfinance の分割履歴（yf.Ticker(code).splits）の分割日がギャップ日の
       SPLIT_VERIFY_WINDOW_DAYS 以内に存在する銘柄だけ確定させる
       （全履歴の分割記録に安易に引っかかって過剰再取得するのを防ぐ）
    3) 確定銘柄のみ 2年分を再取得（生価格 → 失敗時は調整済み価格にフォールバック）
    4) 再取得できた銘柄の既存行を差し替え、splits_refresh.log に記録
    """
    if combined.empty:
        return combined

    df = combined.assign(Date=combined['Date'].astype(str)).sort_values(['Code', 'Date'])
    prev_close = df.groupby('Code', sort=False)['Close'].shift(1)
    gap = (df['Close'] - prev_close) / prev_close
    gap_rows = df[gap.abs() > SPLIT_GAP_THRESHOLD]
    if gap_rows.empty:
        print("\n=== Split/consolidation check ===")
        print("   No price-gap candidates detected.")
        return combined

    gap_dates_by_code = {code: sorted(g['Date'].unique().tolist())
                         for code, g in gap_rows.groupby('Code', sort=False)}
    cand_codes = sorted(gap_dates_by_code.keys())

    print(f"\n=== Split/consolidation check ===")
    print(f"   Price-gap candidates (>±{SPLIT_GAP_THRESHOLD:.0%} 1-day move): {cand_codes}")

    confirmed_codes = []
    for code in cand_codes:
        try:
            splits = yf.Ticker(code).splits
        except Exception as e:
            print(f"   {code}: cannot verify split history ({e}); skipping.")
            continue
        if splits is None or len(splits) == 0:
            print(f"   ⏭️  {code}: no split on record — treating gap as a genuine price move.")
            continue

        # ギャップ日と分割記録日が SPLIT_VERIFY_WINDOW_DAYS 以内に近接するものだけを裏付けとする
        try:
            split_dates = [pd.Timestamp(idx) for idx in splits.index]
        except Exception:
            split_dates = []
        matched = any(
            abs((pd.Timestamp(gap_day) - split_day).days) <= SPLIT_VERIFY_WINDOW_DAYS
            for gap_day in gap_dates_by_code[code]
            for split_day in split_dates
        )
        if matched:
            confirmed_codes.append(code)
            print(f"   ✅ {code}: split/consolidation confirmed near gap date "
                  f"({len(splits)} event(s) in history)")
        else:
            print(f"   ⏭️  {code}: {len(splits)} split record(s) on file but none near the "
                  f"gap date(s) {gap_dates_by_code[code]} — treating gap as a genuine price move.")

    if not confirmed_codes:
        print("   No split/consolidation events requiring refresh.")
        return combined

    print(f"🔄 Refreshing {len(confirmed_codes)} code(s) after split/consolidation: {confirmed_codes}")
    jst_now = datetime.now() + timedelta(hours=9)
    refresh_start = (jst_now - timedelta(days=DATA_WINDOW_DAYS)).strftime('%Y-%m-%d')

    refreshed_parts = []
    refreshed_ok = []
    for code in confirmed_codes:
        part = None
        for auto_adjust in (False, True):  # 生価格が壊れていれば調整済み価格で取得し直す
            try:
                d = yf.Ticker(code).history(start=refresh_start, end=end_date, auto_adjust=auto_adjust)
                if d is not None and not d.empty:
                    part = d[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
                    break
            except Exception as e:
                print(f"   {code}: refresh failed (auto_adjust={auto_adjust}): {e}")
        if part is None or part.empty:
            print(f"   ⚠️  {code}: refresh failed; keeping existing rows.")
            continue
        part = part.reset_index()
        part = part.rename(columns={'index': 'Date'})
        part['Date'] = part['Date'].astype(str)
        part['Code'] = code
        part = part[['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']]
        refreshed_parts.append(part)
        refreshed_ok.append(code)

    if not refreshed_ok:
        print("   ⚠️  No code could be refreshed after split/consolidation.")
        return combined

    refreshed = pd.concat(refreshed_parts, ignore_index=True)
    combined = combined[~combined['Code'].isin(refreshed_ok)]
    combined = pd.concat([combined, refreshed], ignore_index=True)
    combined = combined.sort_values(['Code', 'Date']).reset_index(drop=True)

    with open(SPLIT_LOG_FILENAME, 'a', encoding='utf-8') as f:
        f.write(f"=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        for code in refreshed_ok:
            f.write(f"{code}: split/consolidation detected, 2-year history re-fetched\n")
    print(f"✅ Refreshed {len(refreshed_ok)} code(s); log written to {SPLIT_LOG_FILENAME}")
    return combined


def main():
    # --- 日本時間基準の日付計算 ---
    jst_now = datetime.now() + timedelta(hours=9)
    end_date = (jst_now + timedelta(days=1)).strftime('%Y-%m-%d')

    print("Fetching JPX stock list...")
    all_codes = get_jpx_codes_from_master()
    total_count = len(all_codes)
    print(f"Total JPX codes fetched: {total_count}")

    # 1) 差分取得計画（新規=2年分、既存=最終日+1日、最新化済み=スキップ）
    existing_df = load_existing_data()
    plan = build_fetch_plan(all_codes, existing_df, jst_now)

    if not plan and existing_df.empty:
        raise SystemExit("Abort: no codes to fetch and no existing data.")

    # 2) start日付グループごとにバルク取得
    fetched_parts = []
    failed_chunks = []
    for start in sorted(plan):
        df, failed = fetch_chunked(plan[start], start, end_date)
        failed_chunks.extend(failed)
        if not df.empty:
            fetched_parts.append(df)

    new_df = (pd.concat(fetched_parts, ignore_index=True) if fetched_parts
              else pd.DataFrame(columns=TARGET_COLUMNS))
    if not new_df.empty:
        new_df.drop_duplicates(subset=['Date', 'Code'], keep='last', inplace=True)

    # 3) 既存データと結合して重複排除
    if existing_df.empty:
        combined = new_df
    elif new_df.empty:
        combined = existing_df
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    if not combined.empty:
        combined.drop_duplicates(subset=['Date', 'Code'], keep='last', inplace=True)
        combined = combined.sort_values(['Code', 'Date']).reset_index(drop=True)

    # 4) 株式分割・併合の検知と該当銘柄のみの再取得（自己修復）
    combined = detect_and_refresh_splits(combined, end_date)

    # 5) 直近730日ウィンドウに整形
    cutoff = (jst_now - timedelta(days=DATA_WINDOW_DAYS)).strftime('%Y-%m-%d')
    combined = combined[combined['Date'].astype(str) >= cutoff].reset_index(drop=True)
    available_columns = [col for col in TARGET_COLUMNS if col in combined.columns]
    combined = combined[available_columns]

    # 6) 品質チェック（失敗時は例外で終了 → GitHub Actions 側の再実行に委譲）
    if combined.empty:
        raise SystemExit("Abort: no data was collected.")
    latest_date = combined['Date'].astype(str).max()
    latest_df = combined[combined['Date'].astype(str) == latest_date]
    ohlc_cols = ['Open', 'High', 'Low', 'Close']
    nan_ratio = latest_df[ohlc_cols].isna().mean().mean()
    print(f"Latest date in dataset: {latest_date}")
    print(f"Latest OHLC NaN ratio: {nan_ratio:.2%}")
    if nan_ratio > 0.30:
        print(f"   Saved rows: {len(combined)}; latest_date rows: {len(latest_df)}")
        raise SystemExit(f"Abort: latest OHLC data quality check failed (NaN ratio {nan_ratio:.2%})")

    # 7) Parquet形式で保存
    combined.to_parquet(OUTPUT_FILENAME, index=False)
    print(f"✅ Successfully saved all data to {OUTPUT_FILENAME}")
    print(f"   Total records: {len(combined)}")

    # --- 失敗したチャンクをログファイルに記録 ---
    if failed_chunks:
        with open(FAILED_LOG_FILENAME, 'a', encoding='utf-8') as log_file:
            log_file.write(f"\n=== Execution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            for failed_chunk in failed_chunks:
                log_file.write(f"Block {failed_chunk['block']}: {', '.join(failed_chunk['codes'])}\n")
                log_file.write(f"  Error: {failed_chunk['error']}\n")
        print(f"\n⚠️  {len(failed_chunks)} chunks failed. Details saved to {FAILED_LOG_FILENAME}")
    else:
        print("\n✅ All chunks downloaded successfully!")
    
if __name__ == "__main__":
    main()