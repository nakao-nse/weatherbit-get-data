"""
設定管理モジュール
環境変数から設定を読み込む（ローカル実行時は設定ファイルやkey.txtからも読み込む）
"""
import os
import json
from pathlib import Path
from typing import Optional, Dict


class Config:
    """設定クラス"""
    
    # 環境変数名
    WEATHERBIT_API_KEY = "WEATHERBIT_API_KEY"
    S3_BUCKET = "S3_BUCKET"
    S3_PREFIX = "S3_PREFIX"
    EXECUTION_MODE = "EXECUTION_MODE"
    LOCAL_OUTPUT_DIR = "LOCAL_OUTPUT_DIR"
    VERIFY_SSL = "VERIFY_SSL"
    
    # 設定ファイル名
    CONFIG_FILE = "config.json"
    KEY_FILE = "key.txt"
    
    # デフォルト値
    DEFAULT_EXECUTION_MODE = "aws"
    DEFAULT_LOCAL_OUTPUT_DIR = "./output"
    
    def __init__(self):
        """設定を初期化"""
        # 設定ファイルから読み込む（ローカル実行時のフォールバック用）
        config_data = self._load_config_file()
        
        # 実行モードを先に判定（ローカル実行時のデフォルト値判定に使用）
        self.execution_mode = os.getenv(
            self.EXECUTION_MODE,
            config_data.get("EXECUTION_MODE", self.DEFAULT_EXECUTION_MODE)
        ).lower()
        
        # 実行モードの検証
        if self.execution_mode not in ["local", "aws"]:
            raise ValueError(
                f"EXECUTION_MODE must be 'local' or 'aws', got '{self.execution_mode}'"
            )
        
        is_local = self.execution_mode == "local"
        
        # API Key: 環境変数 → key.txt → config.json → エラー
        self.api_key = os.getenv(self.WEATHERBIT_API_KEY)
        if not self.api_key:
            self.api_key = self._load_api_key_from_file()
        if not self.api_key:
            self.api_key = config_data.get("WEATHERBIT_API_KEY", "")
        if not self.api_key:
            if is_local:
                raise ValueError(
                    f"API Key not found. Set {self.WEATHERBIT_API_KEY} environment variable, "
                    f"provide {self.KEY_FILE}, or set in {self.CONFIG_FILE}"
                )
            else:
                raise ValueError(f"Environment variable {self.WEATHERBIT_API_KEY} is required")
        
        # S3設定: 環境変数 → config.json → デフォルト値（ローカルのみ）
        if is_local:
            self.s3_bucket = os.getenv(
                self.S3_BUCKET,
                config_data.get("S3_BUCKET", "dummy-bucket")
            )
            self.s3_prefix = os.getenv(
                self.S3_PREFIX,
                config_data.get("S3_PREFIX", "weather-data")
            )
        else:
            # AWS実行時は必須
            self.s3_bucket = self._get_required_env(self.S3_BUCKET)
            self.s3_prefix = self._get_required_env(self.S3_PREFIX)
        
        # ローカル出力ディレクトリ
        self.local_output_dir = os.getenv(
            self.LOCAL_OUTPUT_DIR,
            config_data.get("LOCAL_OUTPUT_DIR", self.DEFAULT_LOCAL_OUTPUT_DIR)
        )
        
        # SSL検証設定
        # ローカル実行時はデフォルトでFalse（企業プロキシ等の環境に対応）
        # AWS実行時は常にTrue（セキュリティのため）
        if is_local:
            verify_ssl_str = os.getenv(
                self.VERIFY_SSL,
                config_data.get("VERIFY_SSL", "false")
            ).lower()
            self.verify_ssl = verify_ssl_str in ["true", "1", "yes"]
        else:
            # AWS実行時は常にTrue
            self.verify_ssl = True
    
    def _get_required_env(self, env_name: str) -> str:
        """必須環境変数を取得"""
        value = os.getenv(env_name)
        if not value:
            raise ValueError(f"Environment variable {env_name} is required")
        return value
    
    def _load_config_file(self) -> Dict[str, str]:
        """
        設定ファイル（config.json）を読み込む
        
        Returns:
            設定辞書（ファイルが存在しない場合は空辞書）
        """
        config_path = Path(self.CONFIG_FILE)
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load config file '{self.CONFIG_FILE}': {e}")
        return {}
    
    def _load_api_key_from_file(self) -> str:
        """
        key.txtからAPI Keyを読み込む
        
        Returns:
            API Key文字列（見つからない場合は空文字列）
        """
        key_file_path = Path(self.KEY_FILE)
        if not key_file_path.exists():
            return ""
        
        try:
            with open(key_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # コメント行や空行をスキップ
                    if not line or line.startswith('#'):
                        continue
                    
                    # 32文字の英数字の文字列を探す（API Keyの形式）
                    parts = line.split()
                    for part in parts:
                        # 32文字の英数字をAPI Keyとみなす
                        if len(part) == 32 and part.isalnum():
                            return part
                    
                    # "API Key" という文字列の後の行を探す
                    if "api key" in line.lower() or "apikey" in line.lower():
                        # 次の行を読み込む
                        next_line = f.readline().strip()
                        if next_line and len(next_line) >= 32:
                            # 32文字以上の部分から32文字を抽出
                            for i in range(len(next_line) - 31):
                                candidate = next_line[i:i+32]
                                if candidate.isalnum():
                                    return candidate
        except (IOError, UnicodeDecodeError) as e:
            print(f"Warning: Failed to read key file '{self.KEY_FILE}': {e}")
        
        return ""
    
    def is_local_mode(self) -> bool:
        """ローカル実行モードかどうかを判定"""
        return self.execution_mode == "local"
    
    def is_aws_mode(self) -> bool:
        """AWS実行モードかどうかを判定"""
        return self.execution_mode == "aws"

