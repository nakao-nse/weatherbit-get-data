"""
Forecast用JSON→CSV変換モジュール
acquisition_date列を追加し、日付ごとにファイルを分けて保存
"""
import logging
import csv
import io
from typing import Dict, List, Set, Optional
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)


class CSVConverterForecast:
    """Forecast用JSONデータをCSV形式に変換"""
    
    # JSTタイムゾーン
    JST = pytz.timezone('Asia/Tokyo')
    
    def __init__(self):
        """初期化"""
        pass
    
    def convert_to_csv_by_date(
        self,
        json_data: Dict,
        lat: float,
        lon: float,
        acquisition_date: str,
        existing_records_by_date: Dict[str, Set[str]]
    ) -> Dict[str, bytes]:
        """
        JSONデータを日付ごとにCSV形式に変換
        
        Args:
            json_data: Weatherbit APIレスポンス（JSON形式）
            lat: 緯度（重複チェック用、CSVには含めない）
            lon: 経度（重複チェック用、CSVには含めない）
            acquisition_date: 取得時刻（ISO形式、JST）
            existing_records_by_date: 日付ごとの既存レコードセット
                                   形式: {date_str: {f"{acquisition_date}_{timestamp_local}_{lat}_{lon}"}}
        
        Returns:
            日付ごとのCSVデータ辞書（Shift-JISエンコード済みバイト列、ヘッダーなし）
            形式: {date_str: csv_bytes}
        """
        data_list = json_data.get("data", [])
        if not data_list:
            logger.warning("No data found in JSON response")
            return {}
        
        # トップレベルのcity_nameとcountry_codeを取得
        city_name = json_data.get("city_name", "")
        country_code = json_data.get("country_code", "")
        
        # 日付ごとにデータをグループ化
        data_by_date: Dict[str, List[Dict]] = {}
        
        for record in data_list:
            # timestamp_localから日付を抽出
            timestamp_local = record.get("timestamp_local", "")
            if not timestamp_local:
                logger.warning(f"Missing timestamp_local in record: {record}")
                continue
            
            # 日付を抽出（YYYYMMDD形式）
            date_str = self._extract_date_from_timestamp(timestamp_local)
            if not date_str:
                logger.warning(f"Failed to extract date from timestamp_local: {timestamp_local}")
                continue
            
            if date_str not in data_by_date:
                data_by_date[date_str] = []
            data_by_date[date_str].append(record)
        
        # 日付ごとにCSVを生成
        csv_by_date: Dict[str, bytes] = {}
        
        for date_str, records in data_by_date.items():
            # 日付ごとにソート（timestamp_localで昇順）
            records = self._sort_by_timestamp_local(records)
            
            # 既存レコードセットを取得
            existing_records = existing_records_by_date.get(date_str, set())
            
            # CSV出力用のバッファ
            output = io.StringIO()
            
            # ヘッダーを定義
            headers = self._get_headers()
            writer = csv.DictWriter(output, fieldnames=headers)
            
            # 新しいレコードを収集
            new_records = []
            duplicate_count = 0
            
            for record in records:
                # 重複チェック
                # 完全なキー（acquisition_date、timestamp_local、lat、lonを含む）
                record_key = self._get_record_key(record, lat, lon, acquisition_date)
                
                # 既存レコードセットとの比較
                # handler側では簡易キー（acquisition_dateとtimestamp_localのみ）を使用しているため、
                # 簡易キーでもチェック
                timestamp_local = record.get("timestamp_local", "")
                # timestamp_localをフォーマット
                if timestamp_local:
                    try:
                        if timestamp_local.endswith("Z") or "+" in timestamp_local or timestamp_local.count("-") >= 3:
                            dt = datetime.fromisoformat(timestamp_local.replace("Z", "+00:00"))
                        else:
                            dt = datetime.strptime(timestamp_local, "%Y-%m-%dT%H:%M:%S")
                            dt = self.JST.localize(dt)
                        
                        if dt.tzinfo is None:
                            dt = self.JST.localize(dt)
                        
                        dt_jst = dt.astimezone(self.JST)
                        timestamp_local_formatted = dt_jst.isoformat()
                    except (ValueError, AttributeError):
                        timestamp_local_formatted = timestamp_local
                else:
                    timestamp_local_formatted = ""
                
                simple_key = f"{acquisition_date}_{timestamp_local_formatted}"
                
                # 完全キーまたは簡易キーのいずれかが既存レコードに含まれている場合は重複とみなす
                if record_key in existing_records or simple_key in existing_records:
                    duplicate_count += 1
                    continue
                
                # レコードを展開してフラット化
                flat_record = self._flatten_record(
                    record, lat, lon, city_name, country_code, acquisition_date
                )
                new_records.append(flat_record)
                # 完全キーと簡易キーの両方を追加
                existing_records.add(record_key)
                existing_records.add(simple_key)
            
            if duplicate_count > 0:
                logger.info(f"Skipped {duplicate_count} duplicate records for date {date_str}")
            
            if not new_records:
                logger.info(f"No new records to add for date {date_str}")
                continue
            
            # CSVデータを書き込み
            for record in new_records:
                writer.writerow(record)
            
            # Shift-JISエンコード
            csv_content = output.getvalue()
            csv_bytes = csv_content.encode('shift-jis', errors='replace')
            csv_by_date[date_str] = csv_bytes
        
        return csv_by_date
    
    def _extract_date_from_timestamp(self, timestamp_str: str) -> Optional[str]:
        """
        timestamp_localから日付（YYYYMMDD）を抽出
        
        Args:
            timestamp_str: タイムスタンプ文字列
        
        Returns:
            日付文字列（YYYYMMDD形式）、抽出できない場合はNone
        """
        if not timestamp_str:
            return None
        
        try:
            # ISO形式のタイムスタンプをパース
            if timestamp_str.endswith("Z") or "+" in timestamp_str or timestamp_str.count("-") >= 3:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                # タイムゾーン情報がない場合はJSTとして扱う
                dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                dt = self.JST.localize(dt)
            
            # タイムゾーン情報がない場合の処理（念のため）
            if dt.tzinfo is None:
                dt = self.JST.localize(dt)
            
            # JSTに変換
            dt_jst = dt.astimezone(self.JST)
            return dt_jst.strftime("%Y%m%d")
            
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to extract date from timestamp '{timestamp_str}': {e}")
            return None
    
    def _sort_by_timestamp_local(self, data_list: List[Dict]) -> List[Dict]:
        """
        timestamp_localでソート（昇順）
        
        Args:
            data_list: データリスト
        
        Returns:
            ソート済みデータリスト
        """
        def get_jst_timestamp(record: Dict) -> datetime:
            """JSTタイムスタンプを取得"""
            timestamp_str = record.get("timestamp_local", "")
            if not timestamp_str:
                return datetime.min.replace(tzinfo=self.JST)
            
            try:
                if timestamp_str.endswith("Z") or "+" in timestamp_str or timestamp_str.count("-") >= 3:
                    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                    dt = self.JST.localize(dt)
                
                if dt.tzinfo is None:
                    dt = self.JST.localize(dt)
                
                return dt.astimezone(self.JST)
            except (ValueError, AttributeError):
                return datetime.min.replace(tzinfo=self.JST)
        
        return sorted(data_list, key=get_jst_timestamp)
    
    def _get_headers(self) -> List[str]:
        """CSVヘッダーを取得"""
        return [
            "acquisition_date",
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
        country_code: str = "",
        acquisition_date: str = ""
    ) -> Dict:
        """
        レコードをフラット化
        
        Args:
            record: 元のレコード
            lat: 緯度（重複チェック用、CSVには含めない）
            lon: 経度（重複チェック用、CSVには含めない）
            city_name: 都市名（JSONトップレベルから）
            country_code: 国コード（JSONトップレベルから）
            acquisition_date: 取得時刻（ISO形式、JST）
        
        Returns:
            フラット化されたレコード
        """
        flat = {
            "acquisition_date": acquisition_date,
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
    
    def _get_record_key(
        self,
        record: Dict,
        lat: float,
        lon: float,
        acquisition_date: str
    ) -> str:
        """
        レコードの一意キーを生成（重複チェック用）
        
        Args:
            record: レコード
            lat: 緯度
            lon: 経度
            acquisition_date: 取得時刻（ISO形式、JST）
        
        Returns:
            一意キー文字列
        """
        timestamp_local = record.get("timestamp_local", "")
        # timestamp_localをフォーマット（タイムゾーン情報を統一）
        if timestamp_local:
            try:
                if timestamp_local.endswith("Z") or "+" in timestamp_local or timestamp_local.count("-") >= 3:
                    dt = datetime.fromisoformat(timestamp_local.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(timestamp_local, "%Y-%m-%dT%H:%M:%S")
                    dt = self.JST.localize(dt)
                
                if dt.tzinfo is None:
                    dt = self.JST.localize(dt)
                
                dt_jst = dt.astimezone(self.JST)
                timestamp_local_formatted = dt_jst.isoformat()
            except (ValueError, AttributeError):
                timestamp_local_formatted = timestamp_local
        else:
            timestamp_local_formatted = ""
        
        return f"{acquisition_date}_{timestamp_local_formatted}_{lat}_{lon}"
    
    def get_csv_headers_bytes(self) -> bytes:
        """
        CSVヘッダーをShift-JISエンコード済みバイト列で取得
        
        Returns:
            ヘッダー行（Shift-JISエンコード済みバイト列）
        """
        headers = self._get_headers()
        header_line = ",".join(headers) + "\n"
        return header_line.encode('shift-jis', errors='replace')

