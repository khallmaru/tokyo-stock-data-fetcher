import pandas as pd
import yfinance as yf
import time
import os
from jpx_master_manager import get_jpx_codes_from_master
from datetime import datetime, timedelta

def fetch_all_data(start_date, end_date):
    """1回分の全株価データ取得と結合を行い、(final_df, failed_chunks) を返す。"""
    print("Fetching JPX stock list...")
    all_codes = get_jpx_codes_from_master()
    #all_codes = ["7203.T", "6758.T", "6861.T"]  # テスト用のコードリスト
    total_count = len(all_codes)
    print(f"Total JPX codes fetched: {total_count}")
    
    # --- 設定：レート制限対策 ---
    chunk_size = 50  # yfinance の安定性と取得効率のバランス
    initial_sleep = 20  # 初回: 20秒待機
    retry_sleep = 30  # 再試行時: 30秒待機

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
                    multi_level_index=False
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


def main():
    # --- 品質チェック失敗時の再実行設定（環境変数で上書き可能） ---
    max_attempts = int(os.environ.get("MAX_ATTEMPTS", "3"))
    retry_wait_minutes = int(os.environ.get("RETRY_WAIT_MINUTES", "60"))

    final_df = pd.DataFrame()
    failed_chunks = []

    for attempt in range(1, max_attempts + 1):
        print(f"\n{'=' * 40}")
        print(f"=== Attempt {attempt}/{max_attempts} ===")

        # 日本時間での現在時刻を基準にし、終値が確定している直近営業日まで取得する
        # 再試行の待機時間を考慮し、試行ごとに日付範囲を再計算する
        jst_now = datetime.now() + timedelta(hours=9)
        start_date = (jst_now - timedelta(days=730)).strftime('%Y-%m-%d')
        end_date = (jst_now + timedelta(days=1)).strftime('%Y-%m-%d')

        final_df, failed_chunks = fetch_all_data(start_date, end_date)

        # --- 直近日付のデータ品質を検証 ---
        failure_reason = None
        if final_df.empty:
            print("❌ Error: No data was collected.")
            failure_reason = "no data was collected"
        else:
            latest_date = final_df['Date'].max()
            latest_df = final_df[final_df['Date'] == latest_date]
            ohlc_cols = ['Open', 'High', 'Low', 'Close']
            nan_ratio = latest_df[ohlc_cols].isna().mean().mean()
            print(f"Latest date in dataset: {latest_date}")
            print(f"Latest OHLC NaN ratio: {nan_ratio:.2%}")

            if nan_ratio > 0.30:
                print("⚠️  Latest date OHLC data is too sparse; skipping Parquet save to avoid writing bad data.")
                print(f"   Saved rows: {len(final_df)}; latest_date rows: {len(latest_df)}")
                failure_reason = "latest OHLC data quality check failed"

        # 品質チェック合格 → Parquet形式で保存してループを抜ける
        if failure_reason is None:
            output_filename = "daily_stock_data.parquet"
            final_df.to_parquet(output_filename, index=False)
            print(f"✅ Successfully saved all data to {output_filename}")
            print(f"   Total records: {len(final_df)}")
            break

        # 品質チェック失敗 → 試行回数が残っていれば待機してから再実行
        if attempt < max_attempts:
            print(f"\n⏳ {failure_reason}.")
            print(f"   再実行まで {retry_wait_minutes} 分待機します（{attempt}/{max_attempts} 回目）。")
            time.sleep(retry_wait_minutes * 60)
        else:
            print(f"\n❌ {max_attempts} 回の試行を終了しました: {failure_reason}")
            raise SystemExit(f"Abort: {failure_reason} after {max_attempts} attempts.")

    # --- 失敗したチャンクをログファイルに記録 ---
    if failed_chunks:
        log_filename = "failed_chunks.log"
        with open(log_filename, 'a', encoding='utf-8') as log_file:
            log_file.write(f"\n=== Execution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            for failed_chunk in failed_chunks:
                log_file.write(f"Block {failed_chunk['block']}: {', '.join(failed_chunk['codes'])}\n")
                log_file.write(f"  Error: {failed_chunk['error']}\n")
        print(f"\n⚠️  {len(failed_chunks)} chunks failed. Details saved to {log_filename}")
    else:
        print("\n✅ All chunks downloaded successfully!")
    
if __name__ == "__main__":
    main()