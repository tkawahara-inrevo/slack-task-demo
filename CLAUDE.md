# TaskHub — CLAUDE.md

## プロジェクト概要
**TaskHub** — INREVO社のオールインワン業務管理システム（社内業務 + ヒトトレ採用代行支援）。

ヒトトレ事業フロー: `MK → BC → CR → ST/AN → DR → CS/OP`、横断: 経理/総務

## 技術スタック
- フロントエンド: React + Vite (`web/src/`)
- バックエンド: Node.js + Express (`src/features/`)
- DB: PostgreSQL (DB名: `slacktask`)
- Slack: Bolt.js
- デプロイ: AWS Lightsail `3.222.101.208` / pm2 / git pull + サーバービルド
- 外部連携: kintone (App102: 案件, App170: 入金)

## デプロイ
```bash
git push origin main
ssh -i ~/.ssh/lightsail-us-east-1.pem ubuntu@3.222.101.208 "cd /home/ubuntu/slack-task-demo && git pull && cd web && npm run build && cd .. && pm2 restart slack-task"
```
- `web/dist` は .gitignore のためサーバーでビルド必須
- DB操作: `ssh ubuntu@3.222.101.208 "sudo -u postgres psql -d slacktask -c \"SQL\""`
- **ALTER TABLE注意**: `db/index.js` では必ず `.catch(() => {})` を付ける（権限エラー42501対策）

## 主要ファイル
- `src/features/dashboard-api.js` — 認証・ミドルウェア
- `src/features/crm-api.js` — CRM全エンドポイント
- `src/features/kintone-api.js` — kintone同期（KINTONE_DEAL_FIELD_MAP）
- `src/features/rpo-api.js` — RPO案件API
- `src/db/index.js` — DB接続・テーブル定義・マイグレーション
- `web/src/api/client.js` — 全APIクライアント
- `docs/roadmap.md` — TODO・ロードマップ
- `docs/specs.md` — 詳細仕様（CRMダッシュボード・採用管理・kintone連携）

## ロール権限
`authWithRole`で自動判定: `admin`（明示設定）/ `corp`（Corporateチーム）/ `it`（ITチーム）/ `personnel`（Personnelチーム）

## 主要DBテーブル
`customers`, `deals`（yomi/status/first_meeting_date/order_date）, `deal_activities`, `kintone_cache`（App102）, `kintone_payments`（App170 incentive_amount）, `crm_activity_settings`, `crm_period_settings`, `crm_role_targets`, `recruitment_candidates`, `recruitment_settings`, `rpo_clients`

## kintone連携
- App102→`kintone_cache`（30分同期）、`KINTONE_DEAL_FIELD_MAP`でdealsにマッピング
- App170→`kintone_payments`（incentive_amountが入金確定額）
- 手動同期: `POST /api/kintone/sync`

## 採用管理（自社）
- GASスクリプト: `docs/recruitment_test.gs`
- テンプレートスプシ: `1Jq-6I_276W-e6J91X544wY6d9kIi6JnyPh6I8UKR95k`
- Webhook Secret: GAS Script Properties `WEBHOOK_SECRET`

## HRMOS勤怠 API連携（接続確認済み）
- ベースURL: `https://ieyasu.co/api/inrevo/v1/`
- 認証: `GET /authentication/token` に `Authorization: Basic {IEYASU_API_TOKEN}` → トークン取得
- 以降: `Authorization: Token {token}`
- 打刻: `POST /stamp_logs` `{ user_id: int, stamp_type: 1(出勤)|2(退勤) }`
- ユーザー取得: `GET /users?page=N`（ページング、メールで社員特定）
- 実装: `src/features/ieyasu.js`（トークン・ユーザーキャッシュ実装済み）
- Slackリスナー: `index.js` で出退勤日報チャンネルをフック済み
- env: `IEYASU_API_TOKEN`（サーバー .env 設定済み）

## HRMOS採用 CSV連携（未実装・方針確定）
- HRMOS採用にAPIなし、**CSVエクスポートは可能**（サポート確認済み）
- 方針: 週1CSVアップロード → TaskHubで自動集計・ダッシュボード表示
- **未決**: CSVヘッダー確認待ち・配置場所未決定（次回作業時に確認）
- 目的: HR担当者の週次MTG報告資料作成を自動化

## Git運用
- デプロイはgit push経由（SCPなし）
- SSH鍵: `~/.ssh/lightsail-us-east-1.pem`（リポジトリ外）
- **このシステムがkintoneに代わるデータマスター**（移行期間はkintone参照）
