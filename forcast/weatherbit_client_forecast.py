"""
Weatherbit Forecast APIクライアント
"""
import logging
import time
import requests
import urllib3
from typing import Dict

# SSL警告を抑制（ローカル実行時のみ使用）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class WeatherbitClientForecast:
    """Weatherbit Forecast APIクライアント"""
    
    BASE_URL = "https://api.weatherbit.io/v2.0/forecast/hourly"
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 1  # 秒
    
    def __init__(self, api_key: str, verify_ssl: bool = True):
        """
        初期化
        
        Args:
            api_key: Weatherbit API Key
            verify_ssl: SSL証明書の検証を行うか（デフォルト: True）
        """
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
    
    def get_hourly_forecast(
        self,
        lat: float,
        lon: float,
        hours: int = 72
    ) -> Dict:
        """
        Hourly Weather Forecastデータを取得
        
        Args:
            lat: 緯度
            lon: 経度
            hours: 予測時間数（デフォルト: 72時間、最大: 240時間）
        
        Returns:
            APIレスポンス（JSON形式）
        
        Raises:
            requests.RequestException: API呼び出しエラー
        """
        params = {
            "lat": lat,
            "lon": lon,
            "hours": hours,
            "key": self.api_key,
            "units": "M"  # メートル法
        }
        
        for attempt in range(self.MAX_RETRIES):
            try:
                logger.info(
                    f"Fetching forecast data: lat={lat}, lon={lon}, "
                    f"hours={hours} (attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=30,
                    verify=self.verify_ssl
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

