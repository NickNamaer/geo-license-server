# =========================
# GeoTivity License DB Backup Script
# =========================

# 作業ディレクトリをこのスクリプトの場所に固定
Set-Location -Path $PSScriptRoot

# 環境変数設定（ローカル用）
$env:GEOTIVITY_DB_PATH = "data\licenses.db"
$env:GEOTIVITY_BACKUP_DIR = "data\backups"
$env:GEOTIVITY_BACKUP_KEEP = "14"

# ログ出力用
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Output "[$timestamp] Backup start"

# Python 実行
try {
    python data\backup_db.py

    if ($LASTEXITCODE -eq 0) {
        Write-Output "[$timestamp] Backup success"
    } else {
        Write-Output "[$timestamp] Backup failed (exit code: $LASTEXITCODE)"
    }
}
catch {
    Write-Output "[$timestamp] Backup exception: $_"
}

Write-Output "[$timestamp] Backup end"