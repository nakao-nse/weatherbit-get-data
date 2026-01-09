"""
JSON→CSV変換モジュール
"""
import logging
import csv
import io
from typing import Dict, List, Set, Optional
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)


class CSVConverter:
    """JSONデータをCSV形式に変換"""
    
    # JSTタイムゾーン
    JST = pytz.timezone('Asia/Tokyo')
    
    def __init__(self):
        """初期化"""
        pass
    
    def convert_to_csv(
        self,
        json_data: Dict,
        lat: float,
        lon: float,
        target_date_str: str,
        existing_records: Optional[Set[str]] = None
    ) -> bytes:
        """
        JSONデータをCSV形式に変換
        
        Args:
            json_data: Weatherbit APIレスポンス（JSON形式）
            lat: 緯度（重複チェック用、CSVには含めない）
            lon: 経度（重複チェック用、CSVには含めない）
            target_date_str: 対象日（YYYY-MM-DD形式、JST基準）
            existing_records: 既存レコードのセット（重複チェック用）
                           形式: {f"{timestamp_utc}_{lat}_{lon}"}
        
        Returns:
            CSVデータ（Shift-JISエンコード済みバイト列、ヘッダーなし）
        """
        if existing_records is None:
            existing_records = set()
        
        data_list = json_data.get("data", [])
        if not data_list:
            logger.warning("No data found in JSON response")
            return b""
        
        # JST基準で前日の0時から24時間分のデータのみをフィルタリング
        data_list = self._filter_by_jst_date_range(data_list, target_date_str)
        
        if not data_list:
            logger.warning("No data found in the target date range (JST)")
            return b""
        
        # JST基準でソート（前日の0時から昇順）
        data_list = self._sort_by_jst_timestamp(data_list)
        
        # トップレベルのcity_nameとcountry_codeを取得
        city_name = json_data.get("city_name", "")
        country_code = json_data.get("country_code", "")
        
        # CSV出力用のバッファ
        output = io.StringIO()
        
        # ヘッダーを定義
        headers = self._get_headers()
        writer = csv.DictWriter(output, fieldnames=headers)
        
        # 新しいレコードを収集
        new_records = []
        duplicate_count = 0
        
        for record in data_list:
            # 重複チェック
            record_key = self._get_record_key(record, lat, lon)
            if record_key in existing_records:
                duplicate_count += 1
                continue
            
            # レコードを展開してフラット化
            flat_record = self._flatten_record(
                record, lat, lon, city_name, country_code
            )
            new_records.append(flat_record)
            existing_records.add(record_key)
        
        if duplicate_count > 0:
            logger.info(f"Skipped {duplicate_count} duplicate records")
        
        if not new_records:
            logger.info("No new records to add")
            return b""
        
        # ヘッダーを書き込み（新規ファイルの場合のみ）
        # 既存レコードがある場合はヘッダーは既に存在するため、ここでは書き込まない
        # 実際のヘッダー書き込みは呼び出し側で制御
        
        # データを書き込み
        for record in new_records:
            writer.writerow(record)
        
        csv_content = output.getvalue()
        output.close()
        
        # Shift-JISにエンコード
        try:
            csv_bytes = csv_content.encode('shift-jis')
            return csv_bytes
        except UnicodeEncodeError as e:
            logger.error(f"Failed to encode to Shift-JIS: {e}")
            # フォールバック: 問題のある文字を置換
            csv_bytes = csv_content.encode('shift-jis', errors='replace')
            return csv_bytes
    
    def _get_headers(self) -> List[str]:
        """CSVヘッダーを取得"""
        return [
            "city_name",
            "country_code",
            "datetime",
            "timestamp_utc",
            "timestamp_local",
            "ts",
            "temp",
            "app_temp",
            "rh",
            "dewpt",
            "pres",
            "slp",
            "clouds",
            "vis",
            "wind_spd",
            "wind_dir",
            "wind_gust_spd",
            "precip",
            "snow",
            "uv",
            "solar_rad",
            "ghi",
            "dni",
            "dhi",
            "pod",
            "weather_code",
            "weather_description",
            "weather_icon"
        ]
    
    def _flatten_record(
        self,
        record: Dict,
        lat: float,
        lon: float,
        city_name: str = "",
        country_code: str = ""
    ) -> Dict:
        """
        レコードをフラット化
        
        Args:
            record: 元のレコード
            lat: 緯度（重複チェック用、CSVには含めない）
            lon: 経度（重複チェック用、CSVには含めない）
            city_name: 都市名（JSONトップレベルから）
            country_code: 国コード（JSONトップレベルから）
        
        Returns:
            フラット化されたレコード
        """
        flat = {
            "city_name": city_name,
            "country_code": country_code,
            "datetime": record.get("datetime", ""),
            "timestamp_utc": self._format_timestamp(record.get("timestamp_utc"), is_local=False),
            "timestamp_local": self._format_timestamp(record.get("timestamp_local"), is_local=True),
            "ts": str(record.get("ts", "")),
            "temp": self._format_value(record.get("temp")),
            "app_temp": self._format_value(record.get("app_temp")),
            "rh": self._format_value(record.get("rh")),
            "dewpt": self._format_value(record.get("dewpt")),
            "pres": self._format_value(record.get("pres")),
            "slp": self._format_value(record.get("slp")),
            "clouds": self._format_value(record.get("clouds")),
            "vis": self._format_value(record.get("vis")),
            "wind_spd": self._format_value(record.get("wind_spd")),
            "wind_dir": self._format_value(record.get("wind_dir")),
            "wind_gust_spd": self._format_value(record.get("wind_gust_spd")),
            "precip": self._format_value(record.get("precip")),
            "snow": self._format_value(record.get("snow")),
            "uv": self._format_value(record.get("uv")),
            "solar_rad": self._format_value(record.get("solar_rad")),
            "ghi": self._format_value(record.get("ghi")),
            "dni": self._format_value(record.get("dni")),
            "dhi": self._format_value(record.get("dhi")),
            "pod": record.get("pod", ""),
            "weather_code": "",
            "weather_description": "",
            "weather_icon": ""
        }
        
        # weatherオブジェクトを展開
        weather = record.get("weather")
        if weather:
            flat["weather_code"] = str(weather.get("code", ""))
            flat["weather_description"] = weather.get("description", "")
            flat["weather_icon"] = weather.get("icon", "")
        
        return flat
    
    def _format_timestamp(self, timestamp_str: Optional[str], is_local: bool = False) -> str:
        """
        タイムスタンプをISO形式（JST）にフォーマット
        
        Args:
            timestamp_str: タイムスタンプ文字列
            is_local: Trueの場合、timestamp_localとして扱い（JST）、Falseの場合、timestamp_utcとして扱い（UTC）
        
        Returns:
            フォーマット済みタイムスタンプ（ISO形式、JST）
        """
        if not timestamp_str:
            return ""
        
        try:
            # UTCまたはローカルタイムスタンプをパース
            if timestamp_str.endswith("Z") or "+" in timestamp_str or timestamp_str.count("-") >= 3:
                # ISO形式のタイムスタンプ
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                # その他の形式
                dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                # タイムゾーン情報がない場合
                if is_local:
                    # timestamp_localの場合はJSTとして扱う
                    dt = self.JST.localize(dt)
                else:
                    # timestamp_utcの場合はUTCとして扱う
                    dt = pytz.UTC.localize(dt)
            
            # タイムゾーン情報がない場合の処理（念のため）
            if dt.tzinfo is None:
                if is_local:
                    dt = self.JST.localize(dt)
                else:
                    dt = pytz.UTC.localize(dt)
            
            # JSTに変換
            dt_jst = dt.astimezone(self.JST)
            return dt_jst.isoformat()
            
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to format timestamp '{timestamp_str}': {e}")
            return timestamp_str
    
    def _format_value(self, value) -> str:
        """
        値を文字列にフォーマット
        
        Args:
            value: 任意の値
        
        Returns:
            文字列
        """
        if value is None:
            return ""
        return str(value)
    
    def _filter_by_jst_date_range(self, data_list: List[Dict], target_date_str: str) -> List[Dict]:
        """
        JST基準で前日の0時から24時間分のデータのみをフィルタリング
        
        Args:
            data_list: データレコードのリスト
            target_date_str: 対象日（YYYY-MM-DD形式、JST基準）
        
        Returns:
            フィルタリング済みのデータレコードのリスト
        """
        jst = pytz.timezone('Asia/Tokyo')
        
        # 対象日をパース
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        target_date = jst.localize(target_date)
        
        # 前日の0時から24時間分の範囲を計算
        previous_day = target_date - timedelta(days=1)
        start_datetime = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_datetime = previous_day.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # デバッグ: フィルタリング前のデータ範囲を確認
        logger.info(
            f"Filtering data: target_date={target_date_str}, "
            f"filter range={start_datetime.strftime('%Y-%m-%d %H:%M:%S JST')} to "
            f"{end_datetime.strftime('%Y-%m-%d %H:%M:%S JST')}, "
            f"input records={len(data_list)}"
        )
        
        # フィルタリング前のデータ範囲をログ出力
        if data_list:
            first_record = data_list[0]
            last_record = data_list[-1]
            first_ts_local = first_record.get("timestamp_local", "")
            first_ts_utc = first_record.get("timestamp_utc", "")
            last_ts_local = last_record.get("timestamp_local", "")
            last_ts_utc = last_record.get("timestamp_utc", "")
            logger.info(
                f"Input data range - first: timestamp_local={first_ts_local}, "
                f"timestamp_utc={first_ts_utc}"
            )
            logger.info(
                f"Input data range - last: timestamp_local={last_ts_local}, "
                f"timestamp_utc={last_ts_utc}"
            )
        
        filtered_list = []
        for record in data_list:
            # timestamp_localを優先（JST時刻）
            timestamp_str = record.get("timestamp_local", "")
            if not timestamp_str:
                timestamp_str = record.get("timestamp_utc", "")
            
            if not timestamp_str:
                # フォールバック: ts（Unix timestamp）を使用
                ts = record.get("ts")
                if ts:
                    dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
                    dt_jst = dt.astimezone(self.JST)
                else:
                    continue
            else:
                try:
                    # ISO形式のタイムスタンプをパース
                    if timestamp_str.endswith("Z"):
                        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    elif "+" in timestamp_str or timestamp_str.count("-") >= 3:
                        dt = datetime.fromisoformat(timestamp_str)
                    else:
                        # タイムゾーン情報がない場合
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                        # timestamp_localの場合はJSTとして扱う
                        if timestamp_str == record.get("timestamp_local", ""):
                            dt = self.JST.localize(dt)
                        else:
                            # timestamp_utcの場合はUTCとして扱う
                            dt = pytz.UTC.localize(dt)
                    
                    # JSTに変換（既にJSTの場合はそのまま）
                    if dt.tzinfo is None:
                        dt = pytz.UTC.localize(dt)
                    dt_jst = dt.astimezone(self.JST)
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
                    continue
            
            # 前日の0時から24時間分の範囲内かチェック
            if start_datetime <= dt_jst <= end_datetime:
                filtered_list.append(record)
        
        logger.info(
            f"Filtered {len(filtered_list)} records from {len(data_list)} records "
            f"for date range: {start_datetime.strftime('%Y-%m-%d %H:%M:%S JST')} to "
            f"{end_datetime.strftime('%Y-%m-%d %H:%M:%S JST')}"
        )
        
        return filtered_list
    
    def _sort_by_jst_timestamp(self, data_list: List[Dict]) -> List[Dict]:
        """
        JST基準でタイムスタンプをソート（前日の0時から昇順）
        
        Args:
            data_list: データレコードのリスト
        
        Returns:
            ソート済みのデータレコードのリスト
        """
        def get_jst_timestamp(record: Dict) -> datetime:
            """レコードからJSTタイムスタンプを取得"""
            # timestamp_localを優先（JST時刻）
            timestamp_str = record.get("timestamp_local", "")
            if not timestamp_str:
                # timestamp_localがない場合はtimestamp_utcを使用
                timestamp_str = record.get("timestamp_utc", "")
            
            if not timestamp_str:
                # フォールバック: ts（Unix timestamp）を使用
                ts = record.get("ts")
                if ts:
                    dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
                    return dt.astimezone(self.JST)
                return datetime.min.replace(tzinfo=self.JST)
            
            try:
                # ISO形式のタイムスタンプをパース
                if timestamp_str.endswith("Z"):
                    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                elif "+" in timestamp_str or timestamp_str.count("-") >= 3:
                    dt = datetime.fromisoformat(timestamp_str)
                else:
                    # タイムゾーン情報がない場合
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                    # timestamp_localの場合はJSTとして扱う
                    if timestamp_str == record.get("timestamp_local", ""):
                        dt = self.JST.localize(dt)
                    else:
                        # timestamp_utcの場合はUTCとして扱う
                        dt = pytz.UTC.localize(dt)
                
                # JSTに変換（既にJSTの場合はそのまま）
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                
                return dt.astimezone(self.JST)
                
            except (ValueError, AttributeError) as e:
                logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
                return datetime.min.replace(tzinfo=self.JST)
        
        # JSTタイムスタンプでソート
        sorted_list = sorted(data_list, key=get_jst_timestamp)
        
        return sorted_list
    
    def _get_record_key(self, record: Dict, lat: float, lon: float) -> str:
        """
        レコードの一意キーを生成（重複チェック用）
        
        Args:
            record: レコード
            lat: 緯度
            lon: 経度
        
        Returns:
            一意キー文字列
        """
        timestamp_utc = record.get("timestamp_utc", "")
        return f"{timestamp_utc}_{lat}_{lon}"
    
    def get_csv_headers_bytes(self) -> bytes:
        """
        CSVヘッダー行をShift-JISエンコードされたバイト列で取得
        
        Returns:
            ヘッダー行のバイト列
        """
        headers = self._get_headers()
        header_line = ",".join(headers) + "\n"
        return header_line.encode('shift-jis')

