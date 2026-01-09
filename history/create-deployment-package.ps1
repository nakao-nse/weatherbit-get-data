<#
.SYNOPSIS
    Lambdaデプロイパッケージ作成スクリプト
    
.DESCRIPTION
    このスクリプトはLambda関数のデプロイパッケージ（zipファイル）を作成します。
    Lambda関数のコードファイルと依存パッケージを含むzipファイルを生成します。
    
.PARAMETER OutputZip
    出力するzipファイル名（デフォルト: lambda-deployment.zip）
    
.PARAMETER PythonVersion
    Pythonコマンド名（デフォルト: python）
    Python 3.11または3.12を推奨
    
.PARAMETER Cleanup
    一時ディレクトリを削除するかどうか（デフォルト: true）
    
.PARAMETER UseUv
    uvを使用して依存パッケージをインストールするかどうか（デフォルト: true）
    uvが利用できない場合は自動的にpipにフォールバックします
    
.EXAMPLE
    .\create-deployment-package.ps1
    
.EXAMPLE
    .\create-deployment-package.ps1 -OutputZip "my-lambda-package.zip"
    
.EXAMPLE
    .\create-deployment-package.ps1 -PythonVersion "python3" -Cleanup:$false
    
.EXAMPLE
    .\create-deployment-package.ps1 -UseUv:$false
    
.NOTES
    - デフォルトでuvを使用して依存パッケージをインストールします（高速）
    - uvが利用できない場合は自動的にpipにフォールバックします
    - pyproject.tomlが存在する場合、それを使用します
    - requirements.txtが存在する場合、それを使用します
    - どちらも存在しない場合、boto3, pytz, requestsを直接インストールします
    - 作成されたzipファイルはAWS Lambdaに直接アップロードできます
#>

# Lambdaデプロイパッケージ作成スクリプト
# このスクリプトはLambda関数のデプロイパッケージ（zipファイル）を作成します

param(
    [string]$OutputZip = "lambda-deployment.zip",
    [string]$PythonVersion = "python",
    [switch]$Cleanup = $true,
    [switch]$UseUv = $true
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Lambdaデプロイパッケージ作成スクリプト" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# スクリプトのディレクトリを取得
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# 一時ディレクトリを作成
$TempDir = Join-Path $env:TEMP "lambda-deployment-$(Get-Date -Format 'yyyyMMddHHmmss')"
Write-Host "一時ディレクトリを作成: $TempDir" -ForegroundColor Yellow
New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

try {
    # Lambda関数のコードファイルをコピー
    Write-Host "`nLambda関数のコードファイルをコピー中..." -ForegroundColor Yellow
    $CodeFiles = @(
        "lambda_function.py",
        "config.py",
        "weatherbit_client.py",
        "csv_converter.py",
        "s3_handler.py",
        "local_handler.py"
    )
    
    foreach ($file in $CodeFiles) {
        $sourcePath = Join-Path $ScriptDir $file
        if (Test-Path $sourcePath) {
            Copy-Item -Path $sourcePath -Destination $TempDir -Force
            Write-Host "  ✓ $file" -ForegroundColor Green
        } else {
            Write-Host "  ✗ $file (見つかりません)" -ForegroundColor Red
        }
    }
    
    # 依存パッケージをインストール
    Write-Host "`n依存パッケージをインストール中..." -ForegroundColor Yellow
    
    # uvが利用可能か確認
    $useUvForInstall = $false
    if ($UseUv) {
        $uvCmd = Get-Command uv -ErrorAction SilentlyContinue
        if ($uvCmd) {
            $useUvForInstall = $true
            Write-Host "  uvを使用してインストールします" -ForegroundColor Gray
        } else {
            Write-Host "  uvが見つかりません。pipを使用します。" -ForegroundColor Yellow
        }
    }
    
    if ($useUvForInstall) {
        # uvを使用して依存パッケージをインストール
        Write-Host "  依存パッケージをインストール中（uv）..." -ForegroundColor Gray
        
        # pyproject.tomlが存在する場合はそれを使用、なければrequirements.txtを使用
        $pyprojectFile = Join-Path $ScriptDir "pyproject.toml"
        $requirementsFile = Join-Path $ScriptDir "requirements.txt"
        
        if (Test-Path $pyprojectFile) {
            # pyproject.tomlを使用（依存関係のみをインストール）
            Write-Host "  pyproject.tomlから依存関係を読み込み中..." -ForegroundColor Gray
            # pyproject.tomlから依存関係を抽出してインストール
            # boto3, pytz, requestsを直接指定（pyproject.tomlに記載されているもの）
            & uv pip install --system --target "$TempDir" boto3 pytz requests
            if ($LASTEXITCODE -ne 0) {
                Write-Host "  pyproject.tomlからのインストールに失敗。requirements.txtを試します..." -ForegroundColor Yellow
                if (Test-Path $requirementsFile) {
                    & uv pip install --system --target "$TempDir" -r $requirementsFile
                }
            } else {
                Write-Host "  ✓ pyproject.tomlから依存関係をインストール完了" -ForegroundColor Green
            }
        } elseif (Test-Path $requirementsFile) {
            # requirements.txtを使用
            Write-Host "  requirements.txtから依存関係を読み込み中..." -ForegroundColor Gray
            & uv pip install --system --target "$TempDir" -r $requirementsFile
            if ($LASTEXITCODE -ne 0) {
                Write-Host "エラー: 依存パッケージのインストールに失敗しました。" -ForegroundColor Red
                exit 1
            }
            Write-Host "  ✓ requirements.txtからインストール完了（uv）" -ForegroundColor Green
        } else {
            # 直接パッケージ名を指定
            Write-Host "  依存パッケージを直接指定してインストール中..." -ForegroundColor Gray
            & uv pip install --system --target "$TempDir" boto3 pytz requests
            if ($LASTEXITCODE -ne 0) {
                Write-Host "エラー: 依存パッケージのインストールに失敗しました。" -ForegroundColor Red
                exit 1
            }
            Write-Host "  ✓ 依存パッケージのインストール完了（uv）" -ForegroundColor Green
        }
    } else {
        # pipを使用（従来の方法）
        $VenvDir = Join-Path $TempDir "venv"
        
        # Pythonのバージョンを確認
        $pythonCmd = Get-Command $PythonVersion -ErrorAction SilentlyContinue
        if (-not $pythonCmd) {
            Write-Host "エラー: Pythonが見つかりません。PythonVersionパラメータを指定してください。" -ForegroundColor Red
            Write-Host "例: .\create-deployment-package.ps1 -PythonVersion python3" -ForegroundColor Yellow
            exit 1
        }
        
        $pythonExe = $pythonCmd.Path
        Write-Host "  Python: $pythonExe" -ForegroundColor Gray
        
        # 仮想環境を作成
        Write-Host "  仮想環境を作成中..." -ForegroundColor Gray
        & $pythonExe -m venv $VenvDir
        if ($LASTEXITCODE -ne 0) {
            Write-Host "エラー: 仮想環境の作成に失敗しました。" -ForegroundColor Red
            exit 1
        }
        
        # pipをアップグレード
        Write-Host "  pipをアップグレード中..." -ForegroundColor Gray
        & "$VenvDir\Scripts\python.exe" -m pip install --upgrade pip --quiet
        if ($LASTEXITCODE -ne 0) {
            Write-Host "警告: pipのアップグレードに失敗しましたが、続行します。" -ForegroundColor Yellow
        }
        
        # 依存パッケージをインストール
        Write-Host "  依存パッケージをインストール中（pip）..." -ForegroundColor Gray
        $requirementsFile = Join-Path $ScriptDir "requirements.txt"
        if (Test-Path $requirementsFile) {
            & "$VenvDir\Scripts\python.exe" -m pip install -r $requirementsFile --target "$TempDir" --quiet
            if ($LASTEXITCODE -ne 0) {
                Write-Host "エラー: 依存パッケージのインストールに失敗しました。" -ForegroundColor Red
                exit 1
            }
            Write-Host "  ✓ requirements.txtからインストール完了（pip）" -ForegroundColor Green
        } else {
            # requirements.txtがない場合、pyproject.tomlから依存関係を取得
            Write-Host "  requirements.txtが見つかりません。pyproject.tomlから依存関係を取得します。" -ForegroundColor Yellow
            & "$VenvDir\Scripts\python.exe" -m pip install boto3 pytz requests --target "$TempDir" --quiet
            if ($LASTEXITCODE -ne 0) {
                Write-Host "エラー: 依存パッケージのインストールに失敗しました。" -ForegroundColor Red
                exit 1
            }
            Write-Host "  ✓ 依存パッケージのインストール完了（pip）" -ForegroundColor Green
        }
        
        # 仮想環境を削除（不要なファイルを除外）
        Write-Host "  仮想環境を削除中..." -ForegroundColor Gray
        Remove-Item -Path $VenvDir -Recurse -Force -ErrorAction SilentlyContinue
    }
    
    # 不要なファイルを削除
    Write-Host "`n不要なファイルを削除中..." -ForegroundColor Yellow
    
    # __pycache__と*.pycファイルを削除
    Get-ChildItem -Path $TempDir -Recurse -Include "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TempDir -Recurse -Include "*.pyc" | Remove-Item -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TempDir -Recurse -Include "*.pyo" | Remove-Item -Force -ErrorAction SilentlyContinue
    
    # .dist-infoと.egg-infoディレクトリを削除（Lambdaでは不要）
    Get-ChildItem -Path $TempDir -Recurse -Include "*.dist-info" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TempDir -Recurse -Include "*.egg-info" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    
    # テストファイルやドキュメントを削除
    Get-ChildItem -Path $TempDir -Recurse -Include "test*" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TempDir -Recurse -Include "*.md" | Remove-Item -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TempDir -Recurse -Include "*.txt" -Exclude "requirements.txt" | Remove-Item -Force -ErrorAction SilentlyContinue
    
    # zipファイルを作成
    Write-Host "`nデプロイパッケージ（zipファイル）を作成中..." -ForegroundColor Yellow
    $OutputPath = Join-Path $ScriptDir $OutputZip
    
    # 既存のzipファイルを削除
    if (Test-Path $OutputPath) {
        Remove-Item -Path $OutputPath -Force
        Write-Host "  既存のzipファイルを削除しました。" -ForegroundColor Gray
    }
    
    # zipファイルを作成（最適化: 直接ディレクトリを圧縮）
    Write-Host "  ファイルを圧縮中（大量のファイルがある場合、数分かかる場合があります）..." -ForegroundColor Gray
    try {
        # より効率的な方法: ディレクトリ全体を直接圧縮
        Compress-Archive -Path "$TempDir\*" -DestinationPath $OutputPath -CompressionLevel Optimal -Force
        
        # ファイルサイズを取得
        if (Test-Path $OutputPath) {
            $zipSize = (Get-Item $OutputPath).Length / 1MB
            Write-Host "  ✓ デプロイパッケージを作成しました: $OutputPath" -ForegroundColor Green
            Write-Host "  ファイルサイズ: $([math]::Round($zipSize, 2)) MB" -ForegroundColor Gray
        } else {
            Write-Host "エラー: zipファイルの作成に失敗しました。" -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host "エラー: zipファイルの作成中にエラーが発生しました: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
    
    # パッケージ内容を確認
    Write-Host "`nパッケージ内容を確認中..." -ForegroundColor Yellow
    $rootFiles = Get-ChildItem -Path $TempDir -File | Select-Object -ExpandProperty Name
    Write-Host "  ルートファイル:" -ForegroundColor Gray
    foreach ($file in $rootFiles) {
        Write-Host "    - $file" -ForegroundColor Gray
    }
    
    $packageDirs = Get-ChildItem -Path $TempDir -Directory | Select-Object -ExpandProperty Name
    if ($packageDirs) {
        Write-Host "  パッケージディレクトリ:" -ForegroundColor Gray
        foreach ($dir in $packageDirs) {
            Write-Host "    - $dir/" -ForegroundColor Gray
        }
    }
    
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "デプロイパッケージの作成が完了しました！" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "次のステップ:" -ForegroundColor Yellow
    Write-Host "1. AWS Lambdaコンソールに移動" -ForegroundColor White
    Write-Host "2. 関数のコードタブで「アップロード元」→「.zipファイル」を選択" -ForegroundColor White
    Write-Host "3. $OutputZip をアップロード" -ForegroundColor White
    Write-Host ""
    
} catch {
    Write-Host "`nエラーが発生しました:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
} finally {
    # 一時ディレクトリを削除
    if ($Cleanup -and (Test-Path $TempDir)) {
        Write-Host "`n一時ディレクトリを削除中..." -ForegroundColor Yellow
        Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "  ✓ クリーンアップ完了" -ForegroundColor Green
    } else {
        Write-Host "`n一時ディレクトリ: $TempDir" -ForegroundColor Yellow
        Write-Host "（デバッグ用に残しています）" -ForegroundColor Gray
    }
}

