"""
Forecast用ローカルファイル操作モジュール
日単位のファイルを扱う
"""
import logging
import os
from typing import Optional, Set
from pathlib import Path
import csv
from io import StringIO

logger = logging.getLogger(__name__)


class LocalHandlerForecast:
    """Forecast用ローカルファイル操作ハンドラ"""
    
    def __init__(self, output_dir: str, prefix: str = ""):
        """
        初期化
        
        Args:
            output_dir: 出力ディレクトリ
            prefix: プレフィックス（S3と同じ構造を維持）
        """
        self.output_dir = Path(output_dir)
        self.prefix = prefix.rstrip("/") if prefix else ""
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_file_path(self, point: str, date_str: str) -> Path:
        """
        ローカルファイルパスを生成（日単位）
        
        Args:
            point: 地点名
            date_str: 日付文字列（YYYYMMDD形式）
        
        Returns:
            ファイルパス
        """
        # date_strから年月を抽出
        year = int(date_str[:4])
        month = int(date_str[4:6])
        
        filename = f"wbfc_{date_str}.csv"
        if self.prefix:
            dir_path = self.output_dir / self.prefix / point / f"{year:04d}" / f"{month:02d}"
        else:
            dir_path = self.output_dir / point / f"{year:04d}" / f"{month:02d}"
        
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / filename
    
    def file_exists(self, file_path: Path) -> bool:
        """
        ファイルが存在するかチェック
        
        Args:
            file_path: ファイルパス
        
        Returns:
            存在する場合True
        """
        return file_path.exists()
    
    def read_existing_records(self, file_path: Path) -> Set[str]:
        """
        既存ファイルからレコードキーを読み込む（重複チェック用）
        
        Args:
            file_path: ファイルパス
        
        Returns:
            既存レコードキーのセット
        """
        if not self.file_exists(file_path):
            return set()
        
        try:
            with open(file_path, 'r', encoding='shift-jis') as f:
                content = f.read()
            
            # CSVをパースしてレコードキーを抽出
            records = set()
            csv_reader = csv.DictReader(StringIO(content))
            for row in csv_reader:
                acquisition_date = row.get('acquisition_date', '')
                timestamp_local = row.get('timestamp_local', '')
                # acquisition_dateとtimestamp_localの組み合わせで重複チェック
                # 実際の重複チェックはcsv_converter_forecastで行う（lat, lonも含む）
                # ここでは簡易的なキーを使用（CSVにlat, lonが含まれていないため）
                if acquisition_date and timestamp_local:
                    record_key = f"{acquisition_date}_{timestamp_local}"
                    records.add(record_key)
            
            logger.info(f"Loaded {len(records)} existing records from {file_path}")
            return records
            
        except Exception as e:
            logger.warning(f"Failed to read existing records from {file_path}: {e}")
            return set()
    
    def read_existing_records_by_date(self, point: str, date_str: str) -> Set[str]:
        """
        指定日付のファイルから既存レコードキーを読み込む
        
        Args:
            point: 地点名
            date_str: 日付文字列（YYYYMMDD形式）
        
        Returns:
            既存レコードキーのセット
        """
        file_path = self.get_file_path(point, date_str)
        return self.read_existing_records(file_path)
    
    def append_csv_data(
        self,
        file_path: Path,
        csv_data: bytes,
        is_new_file: bool
    ) -> None:
        """
        CSVデータをローカルファイルに保存（追記または新規作成）
        
        Args:
            file_path: ファイルパス
            csv_data: CSVデータ（Shift-JISエンコード済み）
            is_new_file: 新規ファイルの場合True
        """
        try:
            if is_new_file:
                # 新規ファイルの場合はそのまま保存
                with open(file_path, 'wb') as f:
                    f.write(csv_data)
                logger.info(f"Created new file: {file_path}")
            else:
                # 既存ファイルの場合は追記（ヘッダーなしで追記）
                with open(file_path, 'ab') as f:
                    f.write(csv_data)
                logger.info(f"Appended data to existing file: {file_path}")
                
        except Exception as e:
            logger.error(f"Failed to save CSV data to local file: {e}")
            raise

