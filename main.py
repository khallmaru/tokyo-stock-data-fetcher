import pandas as pd
import yfinance as yf
import time


def get_jpx_codes():
    # 1. JPXから銘柄コードを取得
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    jpx_df = pd.read_excel(url)
    # 一度文字列として扱い、全角や余計な空白を除去してから「XXXX.T」にする
    codes = []
    for code in jpx_df['コード'].dropna():
        code_str = str(code).strip()
    
        # もしExcelの読み込み仕様で「1301.0」のように小数点が付いてしまった場合の対策
        if code_str.endswith('.0'):
            code_str = code_str[:-2]
            
        codes.append(f"{code_str}.T")
    
    return codes

def main():
    print("Fetching JPX stock list...")
    all_codes = get_jpx_codes()
    #all_codes = ["7203.T", "6758.T", "6861.T"]  # テスト用のコードリスト
    total_count = len(all_codes)
    print(f"Total JPX codes fetched: {total_count}")
    
    # --- 設定：何個ずつに分割してダウンロードするか ---
    chunk_size = 100  # 例: 100銘柄ずつダウンロード
    all_chunks_data = []

    print(f"Starting bulk download in chunks of {chunk_size}...")

    # 4400件のリストを100件ずつの塊（チャンク）に分けてループ
    for i in range(0, total_count, chunk_size):
        chunk_codes = all_codes[i:i + chunk_size]
        current_block = (i // chunk_size) + 1
        total_blocks = (total_count // chunk_size) + 1

        print(f"[{current_block}/{total_blocks}] Downloading {len(chunk_codes)} stocks...")
        
        try:
            # 100件一括ダウンロード
            # (threads=Trueにすることで、100件のダウンロードを内部で高速に並列処理してくれます)
            data = yf.download(" ".join(chunk_codes), period="2y", group_by='ticker', threads=True)
            
            if data.empty:
                print(f"Warning: Chunk {current_block} returned empty data.")
                continue

            # 警告対策として future_stack=True を指定
            # level=0 をスタックすると、銘柄コードが縦軸（インデックス）に移動します
            df_stacked = data.stack(level=0, future_stack=True).reset_index()
            
            # --- 【根本修正】列名の自動判別と安全なリネーム ---
            # 1. 最初の2列は必ず [Date, 銘柄コード] になるため、強制的に名前を固定
            df_stacked.columns.values[0] = 'Date'
            df_stacked.columns.values[1] = 'Code'
            
            # 2. yfinanceが返してくる可能性のある主要な列名のマッピング辞書
            # (環境によって 'Adj Close' だったり 'Adj_Close' だったりする表記ブレを吸収します)
            rename_dict = {
                'Open': 'Open',
                'High': 'High',
                'Low': 'Low',
                'Close': 'Close',
                'Volume': 'Volume'
            }
            df_stacked = df_stacked.rename(columns=rename_dict)

            # 3. 実際にデータフレーム内に存在する列だけを抽出する（存在しない列を要求してエラーになるのを防ぐ）
            target_columns = ['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
            available_columns = [col for col in target_columns if col in df_stacked.columns]
            
            df_cleaned = df_stacked[available_columns]
            all_chunks_data.append(df_cleaned)

        except Exception as e:
            print(f"Error downloading chunk {current_block}: {e}")
            
        time.sleep(5)  # サーバー負荷軽減のため、各チャンクの間に5秒待機

    # --- すべてのブロックのデータを1つに結合 ---
    if all_chunks_data:
        print("Combining all chunks into one file...")
        final_df = pd.concat(all_chunks_data, ignore_index=True)
        
        # 念のため重複データを排除
        final_df.drop_duplicates(subset=['Date', 'Code'], inplace=True)

        # --- Parquet形式で保存 ---
        # ※GitHub Actions環境で動かすには、ワークフロー(YAML)のpipに 'pyarrow' の追加が必要です
        output_filename = "daily_stock_data.parquet"
        final_df.to_parquet(output_filename, index=False)
        print(f"Successfully saved all data to {output_filename}")
        print(f"Total records: {len(final_df)}")
    else:
        print("Error: No data was collected.")
    
if __name__ == "__main__":
    main()