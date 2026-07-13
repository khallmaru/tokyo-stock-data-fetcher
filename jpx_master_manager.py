import pandas as pd
import os

# 💡 自分（jpx_master_manager.py）が置いてあるフォルダの絶対パスを取得
# これにより、どこから実行されても常に「tokyo-stock-data-fetcher/」を指すようになる
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, "jpx_master_raw.csv")

# グローバルキャッシュ（初回読み込み時のみCSVを読み込む）
_master_cache = None

def _load_master_cache():
    """
    jpx_master_raw.csvを読み込みキャッシュに保存します（初回のみ実行）。
    4桁コードをキーにした辞書を返します。
    
    処理フロー：
    1. JPXマスタダウンロードを自動実行（失敗時もエラーのみ出力）
    2. jpx_master_raw.csv を読み込む（既存ファイルがあればそれを使用）
    3. ファイルがない場合は空のマスタをキャッシュ
    """
    global _master_cache
    if _master_cache is not None:
        return _master_cache
    
    # 【新規】冒頭でJPXマスタダウンロードを自動試行
    try:
        from jpx_master_get import download_jpx_master_csv
        print("📥 JPXマスタの最新版ダウンロードを試みています...")
        success = download_jpx_master_csv()
        if success:
            print("✅ JPXマスタのダウンロードに成功しました。")
        else:
            print("⚠️  JPXマスタのダウンロードに失敗しました。既存ファイルがあればそれを使用します。")
    except Exception as e:
        print(f"⚠️  JPXマスタダウンロード中にエラーが発生しました（後続処理で既存CSVを使用）: {e}")
    
    try:
        # CSV読み込み（ダウンロード成功後または既存ファイルがある場合）
        if not os.path.exists(csv_path):
            raise FileNotFoundError("jpx_master_raw.csv が見つかりません。")
        
        df = pd.read_csv(csv_path, encoding="cp932")
        # 「13010」(5桁) の先頭4桁を切り出して「1301」にする
        df['code_4deg'] = df['銘柄コード'].astype(str).str[:4]
        
        # 4桁コードをキー、行全体を辞書にした値を持つマスタ辞書を作成
        _master_cache = {}
        for _, row in df.iterrows():
            code = str(row['code_4deg'])
            _master_cache[code] = row.to_dict()
        
        print(f"✅ JPXマスタを読み込みました（{len(_master_cache)}件の銘柄）。")
        return _master_cache
        
    except FileNotFoundError:
        print("❌ jpx_master_raw.csv が見つかりません。空のマスタで続行します。")
        _master_cache = {}
        return _master_cache
        
    except Exception as e:
        print(f"❌ jpx_master_raw.csv の読み込みに失敗しました: {e}。空のマスタで続行します。")
        _master_cache = {}
        return _master_cache

def load_jpx_master_dict():
    """
    jpx_master_raw.csv（CP932エンコード）から
    4桁の銘柄コードをキー、その銘柄の全カラム情報を辞書にした値を返します。
    
    Returns:
        dict: {'1301': {'銘柄名称': '〇〇', '銘柄略称': '〇〇', '業種': '水産・農林業', ...}, ...}
    """
    return _load_master_cache()

def get_short_name(code):
    """
    4桁の銘柄コード（例: '1301'）から銘柄略称を取得します。
    
    優先順位：
    1. マスタ内の「銘柄略称」（空でない場合）
    2. 「銘柄名称」
    3. コード自身
    
    Args:
        code (str): 4桁の銘柄コード
    
    Returns:
        str: 銘柄略称、またはフォールバック値
    """
    try:
        master = _load_master_cache()
        code_str = str(code)
        
        if code_str not in master:
            return code_str
        
        brand_info = master[code_str]
        
        # 銘柄略称がある場合はそれを返す
        short_name = brand_info.get('銘柄略称', '')
        if pd.notna(short_name) and str(short_name).strip():  # 空でない場合
            return short_name
        
        # 銘柄略称がない場合は銘柄名称を返す
        full_name = brand_info.get('銘柄名称', '')
        if pd.notna(full_name) and str(full_name).strip():
            return full_name
        
        # どちらもない場合はコード自身を返す
        return code_str
    
    except Exception:
        # エラー時はコード自身を返す
        return str(code)

def get_brand_info(code):
    """
    4桁の銘柄コード（例: '1301'）から全情報を取得します。
    
    Args:
        code (str): 4桁の銘柄コード
    
    Returns:
        dict: その銘柄の全カラム情報、見つからない場合はNone
    """
    try:
        master = _load_master_cache()
        code_str = str(code)
        return master.get(code_str, None)
    except Exception:
        return None

def get_jpx_codes_from_master():
    """
    jpx_master_raw.csv から銘柄コード（「XXXX.T」形式）のリストを返します。
    """
    try:
        master = _load_master_cache()
        if not master:
            print("⚠️  マスタが空です。銘柄コードリストが取得できませんでした。")
            return []
        codes = [f"{code}.T" for code in master.keys()]
        return codes
    except Exception as e:
        print(f"❌ 銘柄コード一覧の取得に失敗しました: {e}")
        return []