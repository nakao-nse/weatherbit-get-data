"""
Weatherbit予測データ取得Lambda関数
"""
import logging
import json
import time
from typing import Dict, List, Any, Set
from datetime import datetime
import pytz

from config_forecast import ConfigForecast
from weatherbit_client_forecast import WeatherbitClientForecast
from csv_converter_forecast import CSVConverterForecast
from s3_handler_forecast import S3HandlerForecast
from local_handler_forecast import LocalHandlerForecast

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
        config = ConfigForecast()
        logger.info(f"Execution mode: {config.execution_mode}")
        logger.info(f"Forecast hours: {config.forecast_hours}")
        
        # イベントから位置情報を取得
        locations = parse_event(event)
        logger.info(f"Processing {len(locations)} location(s)")
        
        # 現在時刻（JST）を取得（acquisition_dateとして使用、秒まで）
        now_jst = datetime.now(JST)
        # 秒までに制限（マイクロ秒を削除）
        now_jst_seconds = now_jst.replace(microsecond=0)
        acquisition_date = now_jst_seconds.isoformat()
        logger.info(f"Acquisition date (JST): {acquisition_date}")
        
        # クライアントとコンバーターを初期化
        client = WeatherbitClientForecast(
            config.api_key,
            verify_ssl=config.verify_ssl,
            proxy_url=config.proxy_url if config.proxy_url else None
        )
        converter = CSVConverterForecast()
        
        # 保存ハンドラを初期化
        if config.is_aws_mode():
            handler = S3HandlerForecast(config.s3_bucket, config.s3_prefix)
        else:
            handler = LocalHandlerForecast(config.local_output_dir, config.s3_prefix)
        
        # 各地点のデータを処理
        total_records = 0
        for i, location in enumerate(locations):
            lat = location["lat"]
            lon = location["lon"]
            point = location["point"]
            
            logger.info(f"Processing location: point={point}, lat={lat}, lon={lon}")
            
            try:
                # APIから予測データを取得
                json_data = client.get_hourly_forecast(
                    lat=lat,
                    lon=lon,
                    hours=config.forecast_hours
                )
                
                # デバッグ: APIから取得したデータの範囲を確認
                data_list_raw = json_data.get("data", [])
                logger.info(
                    f"API response: hours={config.forecast_hours}, "
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
                
                if not data_list_raw:
                    logger.warning(f"No data for location: point={point}, lat={lat}, lon={lon}")
                    continue
                
                # 日付ごとに既存レコードを読み込み
                # まず、データから日付のリストを取得
                dates_in_data = set()
                for record in data_list_raw:
                    timestamp_local = record.get("timestamp_local", "")
                    if timestamp_local:
                        date_str = converter._extract_date_from_timestamp(timestamp_local)
                        if date_str:
                            dates_in_data.add(date_str)
                
                # 日付ごとに既存レコードを読み込み
                existing_records_by_date: Dict[str, Set[str]] = {}
                for date_str in dates_in_data:
                    if config.is_aws_mode():
                        s3_key = handler.get_file_path(point, date_str)
                        file_exists = handler.file_exists(s3_key)
                        if file_exists:
                            # 既存レコードを読み込む（簡易版、実際の重複チェックはconverterで行う）
                            existing_records_by_date[date_str] = handler.read_existing_records(s3_key)
                        else:
                            existing_records_by_date[date_str] = set()
                    else:
                        file_path = handler.get_file_path(point, date_str)
                        file_exists = handler.file_exists(file_path)
                        if file_exists:
                            existing_records_by_date[date_str] = handler.read_existing_records(file_path)
                        else:
                            existing_records_by_date[date_str] = set()
                
                # CSVに変換（日付ごとに）
                csv_by_date = converter.convert_to_csv_by_date(
                    json_data,
                    lat=lat,
                    lon=lon,
                    acquisition_date=acquisition_date,
                    existing_records_by_date=existing_records_by_date
                )
                
                if not csv_by_date:
                    logger.info(f"No new records to add for location: point={point}, lat={lat}, lon={lon}")
                    continue
                
                # 日付ごとにファイルに保存
                for date_str, csv_data in csv_by_date.items():
                    if config.is_aws_mode():
                        s3_key = handler.get_file_path(point, date_str)
                        file_exists = handler.file_exists(s3_key)
                        
                        # 新規ファイルの場合はヘッダーを追加
                        if not file_exists:
                            headers = converter.get_csv_headers_bytes()
                            csv_data = headers + csv_data
                        
                        handler.append_csv_data(s3_key, csv_data, is_new_file=not file_exists)
                    else:
                        file_path = handler.get_file_path(point, date_str)
                        file_exists = handler.file_exists(file_path)
                        
                        # 新規ファイルの場合はヘッダーを追加
                        if not file_exists:
                            headers = converter.get_csv_headers_bytes()
                            csv_data = headers + csv_data
                        
                        handler.append_csv_data(file_path, csv_data, is_new_file=not file_exists)
                    
                    logger.info(
                        f"Saved data for date {date_str}: "
                        f"point={point}, file={'new' if not file_exists else 'appended'}"
                    )
                
                # レコード数をカウント
                record_count = len(data_list_raw)
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
                "message": "Internal Server Error",
                "error": str(e)
            })
        }


def parse_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    イベントから位置情報を取得
    
    Args:
        event: イベントデータ
    
    Returns:
        位置情報のリスト
    """
    locations = event.get("locations", [])
    
    if not locations:
        raise ValueError("'locations' field is required in event")
    
    if not isinstance(locations, list):
        raise ValueError("'locations' must be a list")
    
    # 各位置情報を検証
    for i, location in enumerate(locations):
        if not isinstance(location, dict):
            raise ValueError(f"Location at index {i} must be a dictionary")
        
        if "lat" not in location or "lon" not in location:
            raise ValueError(f"Location at index {i} must have 'lat' and 'lon' fields")
        
        if "point" not in location:
            raise ValueError(f"Location at index {i} must have 'point' field")
        
        try:
            float(location["lat"])
            float(location["lon"])
        except (ValueError, TypeError):
            raise ValueError(f"Location at index {i} has invalid 'lat' or 'lon' values")
    
    return locations


if __name__ == "__main__":
    # ローカル実行用
    import sys
    from pathlib import Path
    
    # test_event_forecast.jsonから読み込む
    test_event_file = Path("test_event_forecast.json")
    if test_event_file.exists():
        with open(test_event_file, 'r', encoding='utf-8') as f:
            event = json.load(f)
        logger.info(f"Loaded event from {test_event_file}")
    else:
        # デフォルトのテストイベント
        event = {
            "locations": [
                {
                    "lat": 33.705172,
                    "lon": 130.470369,
                    "point": "Koga"
                }
            ]
        }
        logger.info("Using default test event")
    
    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))

