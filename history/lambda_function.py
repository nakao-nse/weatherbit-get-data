"""
Weatherbit実績データ取得Lambda関数
"""
import logging
import json
import time
from typing import Dict, List, Any
from datetime import datetime
import pytz

from config import Config
from weatherbit_client import WeatherbitClient
from csv_converter import CSVConverter
from s3_handler import S3Handler
from local_handler import LocalHandler

# ロギング設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# 標準出力にも出力
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# JSTタイムゾーン
JST = pytz.timezone('Asia/Tokyo')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda関数のエントリーポイント
    
    Args:
        event: イベントデータ
        context: Lambdaコンテキスト
    
    Returns:
        レスポンス辞書
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # 設定を読み込み
        config = Config()
        logger.info(f"Execution mode: {config.execution_mode}")
        
        # イベントから位置情報と日付を取得
        locations, date_str = parse_event(event)
        logger.info(f"Processing {len(locations)} location(s) for date: {date_str}")
        
        # 日付範囲を計算（JST基準で前日）
        start_date, end_date = WeatherbitClient.calculate_date_range(date_str)
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # クライアントとコンバーターを初期化
        client = WeatherbitClient(
            config.api_key,
            verify_ssl=config.verify_ssl,
            proxy_url=config.proxy_url if config.proxy_url else None
        )
        converter = CSVConverter()
        
        # 保存ハンドラを初期化
        if config.is_aws_mode():
            handler = S3Handler(config.s3_bucket, config.s3_prefix)
        else:
            handler = LocalHandler(config.local_output_dir, config.s3_prefix)
        
        # 各地点のデータを処理
        total_records = 0
        for i, location in enumerate(locations):
            lat = location["lat"]
            lon = location["lon"]
            point = location["point"]
            
            logger.info(f"Processing location: point={point}, lat={lat}, lon={lon}")
            
            try:
                # APIからデータを取得
                json_data = client.get_hourly_data(
                    lat=lat,
                    lon=lon,
                    start_date=start_date,
                    end_date=end_date
                )
                
                # デバッグ: APIから取得したデータの範囲を確認
                data_list_raw = json_data.get("data", [])
                logger.info(
                    f"API response: date_range={start_date} to {end_date}, "
                    f"total records={len(data_list_raw)}"
                )
                if data_list_raw:
                    first_raw = data_list_raw[0]
                    last_raw = data_list_raw[-1]
                    logger.info(
                        f"API data range - first: timestamp_local={first_raw.get('timestamp_local', '')}, "
                        f"timestamp_utc={first_raw.get('timestamp_utc', '')}, "
                        f"datetime={first_raw.get('datetime', '')}"
                    )
                    logger.info(
                        f"API data range - last: timestamp_local={last_raw.get('timestamp_local', '')}, "
                        f"timestamp_utc={last_raw.get('timestamp_utc', '')}, "
                        f"datetime={last_raw.get('datetime', '')}"
                    )
                
                # 年月を取得（最初のレコードから）
                data_list = json_data.get("data", [])
                if not data_list:
                    logger.warning(f"No data for location: point={point}, lat={lat}, lon={lon}")
                    continue
                
                # 最初のレコードから年月を取得
                first_record = data_list[0]
                timestamp_utc = first_record.get("timestamp_utc", "")
                year, month = extract_year_month(timestamp_utc)
                
                # 既存ファイルの存在確認とレコード読み込み
                if config.is_aws_mode():
                    s3_key = handler.get_file_path(point, year, month)
                    file_exists = handler.file_exists(s3_key)
                    existing_records = handler.read_existing_records(s3_key) if file_exists else set()
                else:
                    file_path = handler.get_file_path(point, year, month)
                    file_exists = handler.file_exists(file_path)
                    existing_records = handler.read_existing_records(file_path) if file_exists else set()
                
                # CSVに変換
                csv_data = converter.convert_to_csv(
                    json_data,
                    lat=lat,
                    lon=lon,
                    target_date_str=date_str,
                    existing_records=existing_records
                )
                
                if not csv_data:
                    logger.info(f"No new records to add for location: point={point}, lat={lat}, lon={lon}")
                    continue
                
                # 新規ファイルの場合はヘッダーを追加
                if not file_exists:
                    headers = converter.get_csv_headers_bytes()
                    csv_data = headers + csv_data
                
                # ファイルに保存
                if config.is_aws_mode():
                    handler.append_csv_data(s3_key, csv_data, is_new_file=not file_exists)
                else:
                    handler.append_csv_data(file_path, csv_data, is_new_file=not file_exists)
                
                # レコード数をカウント
                record_count = len(data_list)
                total_records += record_count
                logger.info(
                    f"Successfully saved {record_count} records for location: "
                    f"point={point}, lat={lat}, lon={lon}"
                )
                
                # 複数地点の場合、次のリクエスト前に待機（最後の地点以外）
                if i < len(locations) - 1:
                    delay_seconds = 1.0  # 1秒待機（レート制限対策）
                    logger.debug(f"Waiting {delay_seconds} seconds before next request...")
                    time.sleep(delay_seconds)
                
            except Exception as e:
                logger.error(
                    f"Error processing location point={point}, lat={lat}, lon={lon}: {e}",
                    exc_info=True
                )
                # エラーが発生しても他の地点の処理は続行
                continue
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Success",
                "locations_processed": len(locations),
                "total_records": total_records
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda function error: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error",
                "error": str(e)
            })
        }


def parse_event(event: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    """
    イベントから位置情報と日付を取得
    
    Args:
        event: イベントデータ
    
    Returns:
        (locations, date_str) のタプル
        locations: 位置情報のリスト [{"lat": float, "lon": float, "point": str}, ...]
        date_str: 日付文字列（YYYY-MM-DD形式）
    
    Raises:
        ValueError: イベント形式が不正な場合
    """
    # 日付を取得
    date_str = event.get("date")
    if not date_str:
        # 日付が指定されていない場合は現在日（JST）を使用
        now_jst = datetime.now(JST)
        date_str = now_jst.strftime("%Y-%m-%d")
        logger.info(f"Date not specified in event, using current date (JST): {date_str}")
    
    # 位置情報を取得（locations配列形式で統一）
    if "locations" not in event:
        raise ValueError("Event must contain 'locations' array")
    
    locations_list = event["locations"]
    if not isinstance(locations_list, list):
        raise ValueError("'locations' must be a list")
    
    if len(locations_list) == 0:
        raise ValueError("'locations' array must contain at least one location")
    
    locations = []
    for loc in locations_list:
        if "lat" not in loc or "lon" not in loc:
            raise ValueError("Each location must have 'lat' and 'lon'")
        if "point" not in loc:
            raise ValueError("Each location must have 'point' (location name)")
        locations.append({
            "lat": float(loc["lat"]),
            "lon": float(loc["lon"]),
            "point": str(loc["point"])
        })
    
    return locations, date_str


def extract_year_month(timestamp_str: str) -> tuple[int, int]:
    """
    タイムスタンプから年月を抽出
    
    Args:
        timestamp_str: タイムスタンプ文字列（ISO形式）
    
    Returns:
        (year, month) のタプル
    """
    try:
        # ISO形式のタイムスタンプをパース
        if timestamp_str.endswith("Z"):
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        elif "+" in timestamp_str or timestamp_str.count("-") >= 3:
            dt = datetime.fromisoformat(timestamp_str)
        else:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        
        return dt.year, dt.month
        
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to extract year/month from '{timestamp_str}': {e}")
        # フォールバック: 現在日（JST）を使用
        now_jst = datetime.now(JST)
        return now_jst.year, now_jst.month


if __name__ == "__main__":
    # ローカル実行用のテスト
    from pathlib import Path
    
    # test_event.jsonから読み込む（存在する場合）
    test_event_file = Path("test_event.json")
    if test_event_file.exists():
        try:
            with open(test_event_file, 'r', encoding='utf-8') as f:
                test_event = json.load(f)
            logger.info(f"Loaded event from {test_event_file}")
        except Exception as e:
            logger.warning(f"Failed to load {test_event_file}: {e}")
            # フォールバック: デフォルトのテストイベント
            test_event = {
                "locations": [
                    {"lat": 36.1833, "lon": 139.7167, "point": "Koga"}
                ],
                "date": "2026-01-10"
            }
            logger.info("Using default test event")
    else:
        # test_event.jsonが存在しない場合はデフォルトのテストイベント
        test_event = {
            "locations": [
                {"lat": 36.1833, "lon": 139.7167, "point": "Koga"}
            ],
            "date": "2026-01-10"
        }
        logger.info("test_event.json not found, using default test event")
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))

