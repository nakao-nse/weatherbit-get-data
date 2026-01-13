"""
Weatherbit APIクライアント
"""
import logging
import time
import requests
import urllib3
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pytz

# SSL警告を抑制（ローカル実行時のみ使用）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class WeatherbitClient:
    """Weatherbit APIクライアント"""
    
    BASE_URL = "https://api.weatherbit.io/v2.0/history/hourly"
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 1  # 秒
    
    def __init__(self, api_key: str, verify_ssl: bool = True, proxy_url: Optional[str] = None):
        """
        初期化
        
        Args:
            api_key: Weatherbit API Key
            verify_ssl: SSL証明書の検証を行うか（デフォルト: True）
            proxy_url: Proxy URL（オプショナル、例: "http://proxy.example.com:8080"）
        """
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
        # Proxy設定
        if proxy_url:
            # requestsライブラリのproxies形式に変換
            self.proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
        else:
            self.proxies = None
    
    def get_hourly_data(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Hourly Historical Weatherデータを取得
        
        Args:
            lat: 緯度
            lon: 経度
            start_date: 開始日（YYYY-MM-DD形式）
            end_date: 終了日（YYYY-MM-DD形式）
        
        Returns:
            APIレスポンス（JSON形式）
        
        Raises:
            requests.RequestException: API呼び出しエラー
        """
        params = {
            "lat": lat,
            "lon": lon,
            "start_date": start_date,
            "end_date": end_date,
            "key": self.api_key
        }
        
        for attempt in range(self.MAX_RETRIES):
            try:
                logger.info(
                    f"Fetching weather data: lat={lat}, lon={lon}, "
                    f"start_date={start_date}, end_date={end_date} "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=30,
                    verify=self.verify_ssl,
                    proxies=self.proxies
                )
                response.raise_for_status()
                
                data = response.json()
                logger.info(
                    f"Successfully fetched {len(data.get('data', []))} records"
                )
                return data
                
            except requests.exceptions.Timeout:
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY_BASE * (2 ** attempt)
                    logger.warning(
                        f"Request timeout, retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error("Request timeout after all retries")
                    raise
                    
            except requests.exceptions.HTTPError as e:
                logger.error(
                    f"HTTP error: {e.response.status_code} - {e.response.text}"
                )
                raise
                
            except requests.exceptions.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY_BASE * (2 ** attempt)
                    logger.warning(
                        f"Request error: {e}, retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"Request error after all retries: {e}")
                    raise
        
        raise Exception("Failed to fetch data after all retries")
    
    @staticmethod
    def calculate_date_range(date_str: str) -> tuple[str, str]:
        """
        JST基準で前日の日付範囲を計算
        
        Args:
            date_str: 日付文字列（YYYY-MM-DD形式、JST基準）
        
        Returns:
            (start_date, end_date) のタプル（YYYY-MM-DD形式）
            start_date: 前日の前日の日付（JST基準）
            end_date: 当日の日付（JST基準）
            
            注意: Weatherbit APIはUTC基準で日付範囲を解釈するため、
            JST基準で前日の0時から24時間分を取得するには、
            前日の前日から当日までの範囲を指定する必要がある。
            取得したデータは後でフィルタリングして前日の0時から24時間分のみを抽出する。
        """
        jst = pytz.timezone('Asia/Tokyo')
        
        # JST基準で日付をパース
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_obj = jst.localize(date_obj)
        
        # 前日を計算
        previous_day = date_obj - timedelta(days=1)
        # 前日の前日を計算（APIに渡す範囲を広げるため）
        day_before_previous = previous_day - timedelta(days=1)
        
        # 前日の前日から当日までの範囲を返す
        # これにより、UTC基準で解釈されても前日の0時から24時間分のデータが含まれる
        start_date_str = day_before_previous.strftime("%Y-%m-%d")
        end_date_str = date_obj.strftime("%Y-%m-%d")
        
        return (start_date_str, end_date_str)

