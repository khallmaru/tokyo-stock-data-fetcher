import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import glob
import shutil


def download_jpx_master_csv():
    """
    JPXポータルから CSV を自動ダウンロードしてカレントディレクトリに jpx_master_raw.csv として保存します。
    ヘッドレスモード対応（Linux環境のGitHub Actions対応）。
    
    Returns:
        bool: 成功時 True、失敗時 False
    """
    download_dir = os.getcwd()
    driver = None
    
    try:
        # 1. Chromeオプションの設定（ヘッドレスモード対応）
        chrome_options = webdriver.ChromeOptions()
        
        # ヘッドレスモード設定（Linux環境対応）
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        # ダウンロード設定
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # 2. Chromeブラウザの起動
        print("ブラウザを起動しています（ヘッドレスモード）...")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # 3. ページへのアクセス
        target_url = "https://clientportal.jpx.co.jp/ClientPortal/s/Issue?language=ja"
        print(f"ページにアクセス中: {target_url}")
        driver.get(url=target_url)

        # 4. ページの完全な描画を待つ
        print("ページの完全な描画を待っています（10秒）...")
        time.sleep(10)

        # 5. JavaScriptによる「Shadow DOM」の強制突き破り
        print("Shadow DOMの内部を解析して、ボタンを探索中...")
        
        shadow_click_script = """
        function findButtonInShadow(root) {
            // 1. 通常のDOMから「全件CSV取得」ボタンを探す
            const buttons = root.querySelectorAll('button');
            for (const button of buttons) {
                if (button.textContent.includes('全件CSV取得')) {
                    return button;
                }
            }
            
            // 2. もし見つからなければ、配下にあるすべてのShadow Root（影の壁）の内部を再帰的に探す
            const allElements = root.querySelectorAll('*');
            for (const el of allElements) {
                if (el.shadowRoot) {
                    const found = findButtonInShadow(el.shadowRoot);
                    if (found) return found;
                }
            }
            return null;
        }

        // ページ全体（document）から探索を開始
        const targetButton = findButtonInShadow(document);
        
        if (targetButton) {
            targetButton.scrollIntoView({block: 'center'}); // 画面中央へスクロール
            targetButton.click(); // 物理クリックを実行
            return "SUCCESS";
        } else {
            return "NOT_FOUND";
        }
        """

        # スクリプトを実行
        result = driver.execute_script(shadow_click_script)

        if result != "SUCCESS":
            raise Exception("ボタンが見つかりませんでした。Shadow DOMの構造が変わった可能性があります。")
        
        print("👉 Shadow DOMの壁を突破し、「全件CSV取得」ボタンのクリックに成功しました！")
        
        # 6. ダウンロード完了待ち
        print("CSVファイルのダウンロード処理を待機しています（15秒）...")
        time.sleep(15)

        # ファイル管理の処理
        prefix = "Issues_*"
        destination_dir = "data"
        renamed_file_name = "jpx_master_raw.csv"

        # 1. カレントディレクトリ内の Issues_* ファイルを取得
        files = glob.glob(prefix)

        if not files:
            raise Exception("カレントディレクトリに Issues_* ファイルが見つかりませんでした。")

        # 2. 移動先フォルダ（data/）が存在しない場合は作成
        os.makedirs(destination_dir, exist_ok=True)

        # 3. すべての Issues_* ファイルを data/ フォルダへ移動
        for file in files:
            shutil.move(file, os.path.join(destination_dir, file))
        print(f"{len(files)} 個のファイルを '{destination_dir}/' に移動しました。")

        # 4. 移動完了後、data/ フォルダ内から Issues_* ファイルを再取得
        moved_files = glob.glob(os.path.join(destination_dir, prefix))

        # 5. 移動後のファイル群の中から最新ファイルを特定
        latest_file = max(moved_files, key=os.path.getmtime)

        # 6. 最新ファイルをカレントディレクトリへ jpx_master_raw.csv として上書きコピー
        shutil.copy(latest_file, renamed_file_name)
        print(f"✅ 最新ファイル '{os.path.basename(latest_file)}' を '{renamed_file_name}' として上書き配置しました。")
        
        return True

    except Exception as e:
        print(f"❌ JPXマスタダウンロードに失敗しました: {e}")
        return False

    finally:
        # 7. ブラウザを閉じる
        if driver:
            print("ブラウザを閉じています...")
            driver.quit()


if __name__ == "__main__":
    # スタンドアロン実行対応
    success = download_jpx_master_csv()
    exit(0 if success else 1)   


