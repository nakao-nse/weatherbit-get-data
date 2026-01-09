# Weatherbit予測データ取得Lambda関数 仕様書

## 1. 概要

Weatherbit APIからHourly Weather Forecastデータを取得し、S3にCSV形式で保存するAWS Lambda関数。
実績データ取得Lambda関数（history）と同様の構造で、予測データを取得・保存する。

## 2. 機能要件

### 2.1 主要機能
- EventBridgeから1日4回実行される（例：00:00, 06:00, 12:00, 18:00 JST）
- 指定時間先（hours）の予測データを取得
- 取得したJSONデータをCSV形式に変換
- S3バケットに保存（月フォルダ配下、日単位のファイル）
- 各実行時にCSVファイルに追記（重複チェックあり）
- 取得時刻（acquisition_date）をISO形式で記録

### 2.2 実行環境
- ローカル実行とAWS上での実行を環境変数で切り替え可能
- 既存のhistory用モジュールを可能な限り再利用

## 3. 技術要件

### 3.1 使用技術
- Python 3.x
- AWS Lambda
- AWS S3
- AWS EventBridge
- Weatherbit API (Hourly Weather Forecast)

### 3.2 API仕様
- エンドポイント: `https://api.weatherbit.io/v2.0/forecast/hourly`
- 認証: API Key（環境変数から取得）
- リクエストパラメータ:
  - `key` (必須): API Key
  - `lat` (必須): 緯度
  - `lon` (必須): 経度
  - `hours` (オプション): 予測時間数（環境変数`FORECAST_HOURS`で設定、デフォルト: 72時間、最大: 240時間）
  - `units` (オプション): 単位系（`M`=メートル法、`S`=科学的、`I`=ヤード・ポンド法、デフォルト: `M`）
  - `lang` (オプション): 言語コード（例: `ja`=日本語、デフォルト: `en`）

### 3.3 データ形式

#### 3.3.1 イベント入力形式
単一地点・複数地点ともに`locations`配列形式で統一します。
ターゲット日付は指定せず、実行タイミングと`hours`パラメータから計算します。

単一地点の場合:
```json
{
  "locations": [
    {
      "lat": 36.1833,
      "lon": 139.7167,
      "point": "Koga"
    }
  ]
}
```

複数地点の場合:
```json
{
  "locations": [
    {
      "lat": 36.1833,
      "lon": 139.7167,
      "point": "Koga"
    },
    {
      "lat": 35.6762,
      "lon": 139.6503,
      "point": "Tokyo"
    }
  ]
}
```

#### 3.3.2 APIレスポンス形式（JSON）
Weatherbit Hourly Forecast APIのレスポンス形式:
```json
{
  "data": [
    {
      "timestamp_utc": "2026-01-10T00:00:00",
      "timestamp_local": "2026-01-10T09:00:00",
      "ts": 1768003200,
      "datetime": "2026-01-10:00",
      "temp": 6.7,
      "app_temp": 4.8,
      "rh": 52,
      "dewpt": -2.5,
      "pres": 1022,
      "slp": 1024,
      "clouds": 0,
      "vis": 24,
      "wind_spd": 2.65,
      "wind_dir": 158,
      "wind_gust_spd": 5.9,
      "precip": 0,
      "snow": 0,
      "uv": 0,
      "solar_rad": 0,
      "ghi": 0,
      "dni": 0,
      "dhi": 0,
      "pod": "n",
      "weather": {
        "code": 800,
        "description": "Clear sky",
        "icon": "c01n"
      }
    }
  ],
  "city_name": "Koga",
  "lon": 139.7167,
  "lat": 36.1833,
  "country_code": "JP",
  "state_code": "08",
  "timezone": "Asia/Tokyo"
}
```

**レスポンス構造の説明**:
- `data`: 予測データの配列（各要素は1時間ごとの予測データ）
- `city_name`: 都市名
- `lon`: 経度
- `lat`: 緯度
- `country_code`: 国コード
- `state_code`: 州/都道府県コード（オプション）
- `timezone`: タイムゾーン

**データ要素の主要フィールド**:
- `timestamp_utc`: UTCタイムスタンプ（ISO形式）
- `timestamp_local`: ローカルタイムスタンプ（ISO形式、タイムゾーン情報を含む場合あり）
- `ts`: Unixタイムスタンプ
- `datetime`: 日時文字列（`YYYY-MM-DD:HH`形式）
- `temp`: 気温（℃）
- `app_temp`: 体感温度（℃）
- `rh`: 相対湿度（%）
- `dewpt`: 露点温度（℃）
- `pres`: 気圧（hPa）
- `slp`: 海面気圧（hPa）
- `clouds`: 雲量（%）
- `vis`: 視程（km）
- `wind_spd`: 風速（m/s）
- `wind_dir`: 風向（度）
- `wind_gust_spd`: 最大瞬間風速（m/s）
- `precip`: 降水量（mm）
- `snow`: 積雪量（mm）
- `uv`: UV指数
- `solar_rad`: 太陽放射（W/m²）
- `ghi`: 全球水平面放射（W/m²）
- `dni`: 直達日射（W/m²）
- `dhi`: 散乱日射（W/m²）
- `pod`: 昼/夜（`d`=昼、`n`=夜）
- `weather`: 天気情報オブジェクト
  - `code`: 天気コード
  - `description`: 天気説明
  - `icon`: 天気アイコンコード

#### 3.3.3 CSV出力形式
- エンコーディング: Shift-JIS
- 日付時刻フォーマット: ISO形式（JST）
- ヘッダー行: 1行目
- データ行: 2行目以降

CSV列（予定）:
- `acquisition_date`: 取得時刻（ISO形式、JST）
- `city_name`: 都市名
- `country_code`: 国コード
- `datetime`: 予測日時（APIレスポンスから）
- `timestamp_utc`: UTCタイムスタンプ
- `timestamp_local`: ローカルタイムスタンプ（JST）
- `ts`: Unixタイムスタンプ
- `temp`: 気温
- `app_temp`: 体感温度
- `rh`: 湿度
- `dewpt`: 露点
- `pres`: 気圧
- `slp`: 海面気圧
- `clouds`: 雲量
- `vis`: 視程
- `wind_spd`: 風速
- `wind_dir`: 風向
- `wind_gust_spd`: 最大瞬間風速
- `precip`: 降水量
- `snow`: 積雪量
- `uv`: UV指数
- `solar_rad`: 太陽放射
- `ghi`: 全球水平面放射
- `dni`: 直達日射
- `dhi`: 散乱日射
- `pod`: 昼/夜（d/n）
- `weather_code`: 天気コード
- `weather_description`: 天気説明
- `weather_icon`: 天気アイコン

### 3.4 S3保存構造

#### 3.4.1 ファイルパス構造
```
{s3_prefix}/{point}/{year}/{month}/wbfc_{yyyymmdd}.csv
```

例:
```
weather-data-forecast/Koga/2026/01/wbfc_20260110.csv
```

#### 3.4.2 ファイル名規則
- 形式: `wbfc_{yyyymmdd}.csv`
- `yyyymmdd`: 予測対象日（実行タイミングとhoursパラメータから計算）
- 例: `wbfc_20260110.csv`（2026年1月10日の予測データ）
- 各予測データの`timestamp_local`から日付を抽出してファイル名を決定

#### 3.4.3 データ追記
- 1日4回実行されるため、同じファイルに追記する
- 重複チェック: `acquisition_date`と`timestamp_local`の組み合わせで判定
- 既存のレコードと重複する場合はスキップ

## 4. 環境変数

| 環境変数名 | 説明 | 必須 | デフォルト値 |
|-----------|------|------|------------|
| `WEATHERBIT_API_KEY` | Weatherbit APIキー | 必須 | - |
| `S3_BUCKET` | S3バケット名 | 必須 | - |
| `S3_PREFIX` | S3プレフィックス | 必須 | - |
| `EXECUTION_MODE` | 実行モード（local/aws） | 必須 | aws |
| `LOCAL_OUTPUT_DIR` | ローカル実行時の出力ディレクトリ | オプション | ./output |
| `VERIFY_SSL` | SSL検証（true/false） | オプション | true（AWS）/false（local） |
| `FORECAST_HOURS` | 予測時間数（hoursパラメータ） | 必須 | 72 |

## 5. 処理フロー

### 5.1 メイン処理
1. イベントから位置情報を取得
2. 環境変数から`FORECAST_HOURS`を取得（デフォルト: 72時間）
3. 現在時刻（JST）を取得（acquisition_dateとして使用）
4. 各地点について以下を実行:
   a. Weatherbit APIから予測データを取得（hoursパラメータを指定）
   b. 取得時刻（acquisition_date）を記録（ISO形式、JST）
   c. 各予測データの`timestamp_local`から日付を抽出してファイル名を決定
   d. 既存CSVファイルを読み込み（存在する場合）
   e. 重複チェック（acquisition_dateとtimestamp_localの組み合わせ）
   f. CSV形式に変換
   g. 日付ごとにファイルに追記（新規ファイルの場合はヘッダーも追加）
5. 複数地点の場合は、リクエスト間に1秒の待機時間を設ける

### 5.2 ファイル名の決定
- 各予測データの`timestamp_local`から日付（YYYYMMDD）を抽出
- 同じ日付のデータは同じファイルに保存
- 例: `timestamp_local=2026-01-10T00:00:00` → `wbfc_20260110.csv`
- 1回の実行で複数の日付のデータが含まれる場合、日付ごとにファイルを分けて保存

### 5.3 重複チェック
- キー: `{acquisition_date}_{timestamp_local}_{lat}_{lon}`
- 既存レコードと重複する場合はスキップ
- 同じ予測時点（timestamp_local）のデータを複数回取得した場合、各取得時刻（acquisition_date）ごとに記録される

## 6. ファイル構成

### 6.1 新規作成ファイル（すべて新規作成）
- `lambda_function_forecast.py`: Forecast用Lambda関数のエントリーポイント
- `config_forecast.py`: Forecast用設定管理クラス
- `weatherbit_client_forecast.py`: Forecast用APIクライアント
- `csv_converter_forecast.py`: Forecast用CSVコンバーター（acquisition_date列を追加）
- `s3_handler_forecast.py`: Forecast用S3操作ハンドラ
- `local_handler_forecast.py`: Forecast用ローカルファイル操作ハンドラ

### 6.2 モジュール設計方針
- history用モジュールとは完全に分離して新規作成
- コードの重複は許容し、forecast専用の実装とする
- 保守性と独立性を優先

## 7. エラーハンドリング

### 7.1 リトライ
- API呼び出しエラー時: 最大3回リトライ（指数バックオフ）
- タイムアウト: 30秒

### 7.2 エラー処理
- 1つの地点でエラーが発生しても、他の地点の処理は続行
- エラーはログに記録

## 8. 確認事項

### 8.1 API仕様に関する確認
1. **hoursパラメータのデフォルト値**: ✅ 72時間（確定、APIデフォルトは48時間だが、環境変数で72時間に設定）
2. **APIレスポンス形式**: ✅ 確認済み（history APIと同様の構造、`timestamp_utc`、`timestamp_local`、`datetime`、`ts`を含む）
3. **レート制限**: ⚠️ 実装時に確認（historyと同様に1秒間隔を設ける）
4. **hoursパラメータの最大値**: ✅ 240時間（API仕様より）
5. **unitsパラメータ**: メートル法（`M`）を使用（デフォルト）
6. **langパラメータ**: 日本語（`ja`）を使用するか検討（オプション）

### 8.2 ビジネスロジックに関する確認
1. **ターゲット日付の決定方法**: ✅ 確定
   - ターゲット日付ではなく、APIパラメータの`hours`を環境変数で設定
   - 時間指定なので、重複することがある（同じ予測時点のデータを複数回取得可能）
2. **hoursパラメータの設定値**: ✅ 確定
   - デフォルト値: 72時間
   - 実行タイミングによって変える必要はない
3. **重複チェックの粒度**: ✅ 確定
   - `acquisition_date`と`timestamp_local`の組み合わせで重複チェック
   - 同じ予測時点のデータを複数回取得した場合、各取得時刻（acquisition_date）ごとに記録
4. **1日4回の実行タイミング**: ✅ 確定
   - 実行時刻: 04:00, 10:00, 16:00, 22:00 JST（変更の可能性あり）
   - 各実行で取得するターゲット日付の範囲: 実行タイミングとhoursの指定値から計算
   - 例: 2026-01-10 04:00 JST実行、hours=72の場合、2026-01-10 04:00 から 72時間後（2026-01-13 04:00）までの予測データを取得

### 8.3 実装に関する確認
1. **モジュールの再利用方針**: ✅ 確定
   - すべて新規作成（history用モジュールとは完全に分離）
   
2. **S3パス構造**: ✅ 確定
   - 月フォルダ配下に日単位のファイル: 問題なし
   - ファイル名の形式（`wbfc_yyyymmdd.csv`）: 問題なし

## 9. 実装方針

### 9.1 モジュール設計
- **すべて新規作成**: history用モジュールとは完全に分離
- **新規作成ファイル**:
  - `lambda_function_forecast.py`: Forecast用Lambda関数のエントリーポイント
  - `config_forecast.py`: Forecast用設定管理クラス（環境変数`FORECAST_HOURS`を読み込み、デフォルト: 72）
  - `weatherbit_client_forecast.py`: Forecast用APIクライアント（Hourly Forecast API専用）
  - `csv_converter_forecast.py`: Forecast用CSVコンバーター（acquisition_date列を先頭に追加）
  - `s3_handler_forecast.py`: Forecast用S3操作ハンドラ（日単位ファイル用のパス生成）
  - `local_handler_forecast.py`: Forecast用ローカルファイル操作ハンドラ
- **理由**: historyとforecastは用途が異なるため、完全に分離して保守性と独立性を確保

### 9.2 設定管理
- `ConfigForecast`クラスを新規作成
- 環境変数`FORECAST_HOURS`から読み込み（デフォルト: 72時間）
- その他の環境変数はhistory用と同様（`WEATHERBIT_API_KEY`, `S3_BUCKET`, `S3_PREFIX`, `EXECUTION_MODE`, `LOCAL_OUTPUT_DIR`, `VERIFY_SSL`）

### 9.3 CSVコンバーター
- `acquisition_date`列を先頭に追加
- 取得時刻をISO形式（JST）で記録
- 重複チェックキー: `{acquisition_date}_{timestamp_local}_{lat}_{lon}`
- 日付ごとにファイルを分けて保存するため、`timestamp_local`から日付を抽出してファイル名を決定

### 9.4 ファイル保存ロジック
- 1回のAPI呼び出しで複数日分のデータが返される可能性がある
- 各データの`timestamp_local`から日付（YYYYMMDD）を抽出
- 同じ日付のデータは同じファイルに保存
- 異なる日付のデータは別ファイルに保存

## 10. テスト

### 10.1 ローカルテスト
- `test_event_forecast.json`を作成
- ローカル実行で動作確認

### 10.2 AWSテスト
- Lambda関数をデプロイ
- EventBridgeルールを作成（1日4回実行）
- S3にデータが正しく保存されるか確認

