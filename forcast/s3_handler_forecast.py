"""
Forecast用S3操作モジュール
日単位のファイルを扱う
"""
import logging
from typing import Optional, Set, Dict
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import csv
from io import StringIO

logger = logging.getLogger(__name__)


class S3HandlerForecast:
    """Forecast用S3操作ハンドラ"""
    
    def __init__(self, bucket: str, prefix: str):
        """
        初期化
        
        Args:
            bucket: S3バケット名
            prefix: S3プレフィックス
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.s3_client = boto3.client('s3')
    
    def get_file_path(self, point: str, date_str: str) -> str:
        """
        S3ファイルパスを生成（日単位）
        
        Args:
            point: 地点名
            date_str: 日付文字列（YYYYMMDD形式）
        
        Returns:
            S3キー（パス）
        """
        # date_strから年月を抽出
        year = int(date_str[:4])
        month = int(date_str[4:6])
        
        filename = f"wbfc_{date_str}.csv"
        return f"{self.prefix}/{point}/{year:04d}/{month:02d}/{filename}"
    
    def file_exists(self, s3_key: str) -> bool:
        """
        ファイルが存在するかチェック
        
        Args:
            s3_key: S3キー
        
        Returns:
            存在する場合True
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def read_existing_records(self, s3_key: str) -> Set[str]:
        """
        既存ファイルからレコードキーを読み込む（重複チェック用）
        
        Args:
            s3_key: S3キー
        
        Returns:
            既存レコードキーのセット
        """
        if not self.file_exists(s3_key):
            return set()
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=s3_key
            )
            content = response['Body'].read().decode('shift-jis')
            
            # CSVをパースしてレコードキーを抽出
            records = set()
            lines = content.strip().split('\n')
            if len(lines) < 2:  # ヘッダーのみ
                return set()
            
            # ヘッダーをスキップしてデータ行を処理
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
            
            logger.info(f"Loaded {len(records)} existing records from {s3_key}")
            return records
            
        except Exception as e:
            logger.warning(f"Failed to read existing records from {s3_key}: {e}")
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
        s3_key = self.get_file_path(point, date_str)
        return self.read_existing_records(s3_key)
    
    def append_csv_data(
        self,
        s3_key: str,
        csv_data: bytes,
        is_new_file: bool
    ) -> None:
        """
        CSVデータをS3に保存（追記または新規作成）
        
        Args:
            s3_key: S3キー
            csv_data: CSVデータ（Shift-JISエンコード済み）
            is_new_file: 新規ファイルの場合True
        """
        try:
            if is_new_file:
                # 新規ファイルの場合はヘッダーを含めて保存
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=csv_data,
                    ContentType='text/csv; charset=shift-jis'
                )
                logger.info(f"Created new file: {s3_key}")
            else:
                # 既存ファイルの場合は追記
                # S3は直接追記できないため、既存データを取得して結合
                existing_data = self._get_existing_file_content(s3_key)
                combined_data = existing_data + csv_data
                
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=combined_data,
                    ContentType='text/csv; charset=shift-jis'
                )
                logger.info(f"Appended data to existing file: {s3_key}")
                
        except ClientError as e:
            logger.error(f"Failed to save CSV data to S3: {e}")
            raise
    
    def _get_existing_file_content(self, s3_key: str) -> bytes:
        """
        既存ファイルの内容を取得
        
        Args:
            s3_key: S3キー
        
        Returns:
            ファイル内容（バイト列）
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=s3_key
            )
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return b''
            raise

