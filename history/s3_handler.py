"""
S3操作モジュール
"""
import logging
from typing import Optional, Set
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Handler:
    """S3操作ハンドラ"""
    
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
    
    def get_file_path(self, point: str, year: int, month: int) -> str:
        """
        S3ファイルパスを生成
        
        Args:
            point: 地点名
            year: 年
            month: 月
        
        Returns:
            S3キー（パス）
        """
        filename = f"wb_{year:04d}_{month:02d}.csv"
        return f"{self.prefix}/{point}/{year:04d}/{filename}"
    
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
            import csv
            from io import StringIO
            
            csv_reader = csv.DictReader(StringIO(content))
            for row in csv_reader:
                timestamp_utc = row.get('timestamp_utc', '')
                lat = row.get('lat', '')
                lon = row.get('lon', '')
                if timestamp_utc and lat and lon:
                    record_key = f"{timestamp_utc}_{lat}_{lon}"
                    records.add(record_key)
            
            logger.info(f"Loaded {len(records)} existing records from {s3_key}")
            return records
            
        except Exception as e:
            logger.warning(f"Failed to read existing records from {s3_key}: {e}")
            return set()
    
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

