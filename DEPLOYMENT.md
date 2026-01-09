# AWS Lambda デプロイ手順

## デプロイパッケージの作成

### PowerShellスクリプトを使用する方法（推奨）

1. PowerShellでプロジェクトディレクトリに移動
```powershell
cd C:\Users\N9636728\Documents\work\weatherbit
```

2. デプロイパッケージを作成
```powershell
.\create-deployment-package.ps1
```

3. オプション指定例
```powershell
# カスタムファイル名を指定
.\create-deployment-package.ps1 -OutputZip "my-lambda-package.zip"

# Pythonコマンドを指定
.\create-deployment-package.ps1 -PythonVersion "python3"

# 一時ディレクトリを残す（デバッグ用）
.\create-deployment-package.ps1 -Cleanup:$false
```

### 手動で作成する方法

1. 仮想環境を作成
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. 依存パッケージをインストール
```powershell
pip install -r requirements.txt
```

3. デプロイ用ディレクトリを作成
```powershell
mkdir lambda-package
```

4. Lambda関数のコードをコピー
```powershell
Copy-Item lambda_function.py,config.py,weatherbit_client.py,csv_converter.py,s3_handler.py,local_handler.py lambda-package\
```

5. 依存パッケージをコピー
```powershell
Copy-Item -Recurse venv\Lib\site-packages\* lambda-package\
```

6. zipファイルを作成
```powershell
Compress-Archive -Path lambda-package\* -DestinationPath lambda-deployment.zip
```

## AWS Lambdaへのデプロイ

### 1. Lambda関数の作成

1. AWSマネジメントコンソール → Lambda
2. 「関数の作成」をクリック
3. 設定:
   - 関数名: `weatherbit-historical-data`
   - ランタイム: Python 3.11 または 3.12
   - アーキテクチャ: x86_64
   - 実行ロール: 新規作成

### 2. 実行ロールの権限設定

IAMロールに以下の権限を追加:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name/*",
        "arn:aws:s3:::your-bucket-name"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 3. 環境変数の設定

Lambda関数の「設定」→「環境変数」で以下を設定:

| 環境変数名 | 値 | 説明 |
|-----------|-----|------|
| `WEATHERBIT_API_KEY` | `your-api-key` | Weatherbit APIキー |
| `S3_BUCKET` | `your-bucket-name` | S3バケット名 |
| `S3_PREFIX` | `weather-data` | S3プレフィックス |
| `EXECUTION_MODE` | `aws` | 実行モード（必須） |
| `VERIFY_SSL` | `true` | SSL検証 |

### 4. デプロイパッケージのアップロード

1. Lambda関数の「コード」タブ
2. 「アップロード元」→「.zipファイル」を選択
3. `lambda-deployment.zip`をアップロード
4. 「保存」をクリック

### 5. ハンドラーの設定

- ハンドラー: `lambda_function.lambda_handler`
- タイムアウト: 5分（推奨）
- メモリ: 256 MB（推奨）

## テスト実行

### Lambdaコンソールでのテスト

1. Lambda関数の「テスト」タブ
2. 「新しいイベントを作成」
3. イベント名: `test-event`
4. イベントJSON:
```json
{
  "locations": [
    {
      "lat": 33.705172,
      "lon": 130.470369,
      "point": "Koga"
    }
  ],
  "date": "2026-01-09"
}
```
5. 「保存」→「テスト」をクリック

### 実行結果の確認

- 「モニタリング」タブでログを確認
- CloudWatch Logsで詳細ログを確認
- S3バケットにCSVファイルが作成されているか確認

## EventBridgeの設定（定期実行）

### EventBridgeルールの作成

1. EventBridge → 「ルール」→ 「ルールを作成」
2. 設定:
   - ルール名: `weatherbit-daily-trigger`
   - ルールタイプ: 「スケジュール」
   - スケジュールパターン: 「定期的なスケジュール」
   - スケジュール式: `cron(0 1 * * ? *)` （毎日JST 10時 = UTC 1時）
3. ターゲット:
   - ターゲットタイプ: 「AWS のサービス」
   - サービス: 「Lambda関数」
   - 関数: `weatherbit-historical-data`
   - 入力: 定数（JSONテキスト）:
```json
{
  "locations": [
    {
      "lat": 33.705172,
      "lon": 130.470369,
      "point": "Koga"
    }
  ]
}
```

### 複数地点の場合

```json
{
  "locations": [
    {
      "lat": 33.705172,
      "lon": 130.470369,
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

## S3バケットの確認

### ファイル構造

```
your-bucket-name/
  weather-data/
    Koga/
      2026/
        wb_2026_01.csv
```

### AWS CLIで確認

```bash
aws s3 ls s3://your-bucket-name/weather-data/Koga/2026/
aws s3 cp s3://your-bucket-name/weather-data/Koga/2026/wb_2026_01.csv ./
```

## トラブルシューティング

### CloudWatch Logsの確認

- Lambda関数の「モニタリング」→「CloudWatch Logsを表示」
- エラーメッセージやログを確認

### よくある問題

- **タイムアウト**: タイムアウトを延長（最大15分）
- **メモリ不足**: メモリを増やす（最大10GB）
- **権限エラー**: IAMロールの権限を確認
- **APIキーエラー**: 環境変数の`WEATHERBIT_API_KEY`を確認

### デプロイパッケージのサイズ制限

- Lambda関数のデプロイパッケージは最大50MB（zip圧縮後）
- 50MBを超える場合は、Lambdaレイヤーを使用することを検討してください

