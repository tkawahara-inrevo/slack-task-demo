# TaskHub — CLAUDE.md
> このファイルは Claude Code が会話開始時に自動で読み込む引き継ぎ情報です。
> 作業後は必ず最新状態に更新してから git push してください。

---

## プロジェクト概要

**TaskHub** — INREVO社のオールインワン業務管理システム。

### 2つの目的
1. **INREVO社内ツール** — 申請・管理業務をすべてここに集約。外部ツール情報もここで確認。
2. **ヒトトレ事業サポート** — 採用代行事業（MK→BC→HR→DR/CS）の全フローをシステム上でサポート。

### ヒトトレ事業フロー
```
MK（リード獲得: 広告/架電/アライアンス/代理店）
 └→ BC（商談・追客 → 受注/失注）
      └→ CR（初回MTG・カルテ取得）→ ST/AN（媒体分析・提案）
           └→ DR（顧客折衝・CSタスク管理）
                └→ CS（媒体運用・応募者対応）/ OP（求人作成）
横断: 経理（入出金）/ 総務（請求書・契約書・反社チェック）
```

---

## 技術スタック

| 区分 | 内容 |
|------|------|
| フロントエンド | React + Vite (web/src/) |
| バックエンド | Node.js + Express (src/features/) |
| DB | PostgreSQL (Lightsailローカル、DB名: slacktask) |
| Slack | Bolt.js |
| デプロイ | AWS Lightsail (3.222.101.208) / pm2 / scp |
| 外部連携 | kintone API (App102: 案件, App170: 入金) |

---

## サーバー情報

- **IP**: `3.222.101.208`
- **ユーザー**: `ubuntu`
- **アプリディレクトリ**: `/home/ubuntu/slack-task-demo/`
- **pm2プロセス名**: `slack-task`
- **ポート**: 3000
- **SSH鍵**: `LightsailDefaultKey-us-east-1.txt`（都度ユーザーから提供）

### デプロイ手順
```bash
# 鍵を準備
cat > /tmp/deploy/key.pem << 'EOF'
（SSH鍵を貼り付け）
EOF
chmod 600 /tmp/deploy/key.pem

# フロントエンドビルド
cd web && npm run build

# バックエンド転送（変更ファイルのみ）
scp -i /tmp/deploy/key.pem -o StrictHostKeyChecking=no \
  src/features/crm-api.js ubuntu@3.222.101.208:/home/ubuntu/slack-task-demo/src/features/

# フロントエンド転送（dist を完全入れ替え）
ssh ... "rm -rf /home/ubuntu/slack-task-demo/web/dist && mkdir -p /home/ubuntu/slack-task-demo/web/dist"
scp -i /tmp/deploy/key.pem -o StrictHostKeyChecking=no -r web/dist/. ubuntu@3.222.101.208:/home/ubuntu/slack-task-demo/web/dist/

# 再起動
ssh ... "pm2 restart slack-task && sleep 5 && ss -tlnp | grep ':3000'"
```

### DB操作（直接実行が必要な場合）
```bash
ssh ubuntu@3.222.101.208 "sudo -u postgres psql -d slacktask -c \"SQL文\""
```

### 注意: ALTER TABLE の権限
- `slacktask` DBユーザーは一部テーブルの ALTER TABLE が不可（エラーコード 42501）
- `db/index.js` の ALTER TABLE には必ず `.catch(() => {})` を付ける
- 新カラムは sudo で直接実行するか、マイグレーション後に確認

---

## 主要ファイル構成

```
index.js                    # エントリーポイント
src/
  features/
    dashboard-api.js        # メインAPI登録・認証・ミドルウェア
    crm-api.js              # CRM全エンドポイント（ダッシュボード含む）
    kintone-api.js          # kintone同期スケジューラー・マッピング
    kintone-sync.js         # kintone APIクライアント（全フィールド取得）
    rpo-api.js              # RPO案件管理API
    channel-mapping.js      # チャンネルマッピング（IT/adminのみ）
  db/
    index.js                # DB接続・全テーブル定義・マイグレーション
    kintone.js              # kintone_cache CRUD
web/src/
  pages/
    crm/
      CRM.jsx               # CRMタブコンテナ（ダッシュボード/パイプライン/顧客/成績）
      CrmDashboard.jsx      # ダッシュボード（recharts使用）
      CustomerDetail.jsx    # 顧客詳細・商談カード・アクティビティ記録
      SalesPerformance.jsx  # 成績（個人成績・ヨミ管理）
    admin/
      AdminLayout.jsx       # Corp管理サイドナビ（IT/Personnelロール分け）
      Recruitment.jsx       # 採用管理（実技テスト送付・スプシ取り込み）
  api/client.js             # 全APIクライアントメソッド
docs/
  roadmap.md                # 機能ロードマップ
  recruitment_test.gs       # 採用テスト用GASスクリプト（最新版）
```

---

## ロール・権限システム

`authWithRole` ミドルウェアがチームメンバーシップからロールを自動判定：

| ロール | 条件 | アクセス可能 |
|--------|------|------------|
| `admin` | dashboard_roles に明示設定 | 全機能 |
| `corp` | Corporateチーム所属 | 日報管理・ランキング |
| `it` | ITチーム所属（ILIKE '%IT%'） | 管理者権限・チャンネルマッピング・Slackメンション管理 |
| `personnel` | Personnelチーム所属 | 採用管理（自社） |
| その他 | Slackタイトルから推定 | 一般機能 |

---

## 主要DBテーブル

| テーブル | 用途 |
|---------|------|
| `customers` | 顧客情報 |
| `deals` | 商談（yomi, status, first_meeting_date, order_date 等） |
| `deal_activities` | アクティビティ記録（架電/商談/メール等 + yomi_at_time） |
| `kintone_cache` | kintone App102の全フィールドキャッシュ（同期毎に更新） |
| `kintone_payments` | kintone App170の入金データ（incentive_amount, staff, payment_date） |
| `crm_activity_settings` | カスタムアクション種別・結果選択肢 |
| `crm_period_settings` | 前期・今期の期間定義 |
| `crm_role_targets` | ロール別月間目標 |
| `recruitment_candidates` | 採用テスト候補者 |
| `recruitment_settings` | GAS URL・テンプレートID・メールテンプレート等 |
| `rpo_clients` | RPO案件（受注時に deals から自動生成） |

---

## kintone連携

### App102（案件情報）
- **フィールド**: 全フィールド取得（fields: [] で制限なし）
- **同期先**: `kintone_cache` テーブル（data JSONB に全フィールド）
- **deals へのマッピング**: `kintone-api.js` の `KINTONE_DEAL_FIELD_MAP`
  - `初回商談日_コンサルチーム` → `first_meeting_date`
  - `受注日` → `order_date`
  - `ヨミ` → `yomi` 等

### App170（入金管理）
- **フィールド**: company, 数値, 数値_0, date, plan, Staff
- **同期先**: `kintone_payments` テーブル
- **注意**: `incentive_amount` が入金確定額（ダッシュボード・月次収支見込みに使用）

### 同期スケジュール
- 起動時 + 30分ごとに自動同期
- `POST /api/kintone/sync` で手動同期可能

---

## CRMダッシュボード仕様

**場所**: CRM > ダッシュボードタブ（デフォルト表示）

### 期間切り替え
- **指定月**（デフォルト: 今月）/ **今期**
- 今期の場合、前期との対比（%）を表示

### データソース
| 指標 | ソース | 備考 |
|------|--------|------|
| 入金額 | `kintone_payments.incentive_amount` | 入金日基準 |
| 受注件数 | `deals` WHERE status='won' | 受注日基準 |
| 初回商談数 | `deals.first_meeting_date` | BCの初回商談日 |
| 収支見込み（確定） | `kintone_payments` 当月 | |
| 収支見込み（確実） | A/Sヨミ案件の初期費用×1.1 | |
| 収支見込み（見込み） | B/Cヨミ案件の初期費用×1.1 | |

### 担当者絞り込み（固定6名）
`['山本 夏乃', '板金 慎太郎', '添田 剛', '萩原 隼人', '藤原 一矢', '野村 尭弘']`
その他の担当者は「その他」として合算表示。

---

## 採用管理（自社）

**GAS**: `docs/recruitment_test.gs` をスプシのスクリプトエディタに貼り付けて使用。

| 設定 | 値 |
|------|-----|
| テンプレートスプシID | `1Jq-6I_276W-e6J91X544wY6d9kIi6JnyPh6I8UKR95k` |
| GAS URL | DB の `recruitment_settings.gas_endpoint_url` を参照 |
| 採点方式 | 10点満点（Q1〜10各1点）、Q13タイピング参考、Q14人事確認 |
| 候補者スプシ | `recruitment_settings.import_sheet_url` で固定URL管理 |

### GASのWebhook Secret
コードに書かず、Script Properties（プロパティ名: `WEBHOOK_SECRET`）で管理。

---

## 今後のTODO（優先順）

### Phase 1（緊急度高）
- [ ] IEYASU勤怠連携（API確認済み・実装待ち）
- [ ] BCの長期化案件ステータス（担当者と相談）
- [ ] アクティビティ設定UI（Admin画面から種別・結果選択肢を変更できる画面）

### Phase 2（MK強化）
- [ ] MKリード管理画面（kintone + スプシからの集約）
- [ ] 広告指標ダッシュボード（CV数・ビュー数）
- [ ] MK→BC引き継ぎフロー

### Phase 3（HR準備フェーズ）
- [ ] カルテ管理
- [ ] 媒体提案ワークフロー（ST/AN→DR引き継ぎ）

### Phase 4（経理・総務）
- [ ] 反社チェック申請・管理フロー
- [ ] 入出金管理

### テクニカルTODO
- [ ] `kintone_payments` → `payments` にリネーム（今後このシステムがマスター）
- [ ] `kintone_cache` もリネーム検討

---

## 未確認・要ヒアリング

- BCの長期化案件ステータスの扱い（BC担当者と要相談）
- カルテの具体的な項目・フォーマット
- 請求書・契約書の発行フロー詳細（総務ヒアリング必要）
- 反社チェックツールの切り替え状況（ジーサーチ→他社、現在ステイ）
- 広告指標データのソース（どのスプシ・ツール）
- DRへの引き継ぎ時に渡す具体的な情報

---

## Git運用ルール

- コードはこのリポジトリで管理
- デプロイは `scp` で Lightsail に直接転送（CI/CDなし）
- SSH鍵はリポジトリに含めない（都度ユーザーから提供）
- `docs/` フォルダにGASスクリプト・ロードマップ等を格納
