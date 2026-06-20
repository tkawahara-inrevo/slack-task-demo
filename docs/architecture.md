# TaskHub - 開発者向け仕様書

最終更新: 2026-06-19

---

## 1. プロジェクト概要

**TaskHub**（社内通称: **Pochi / ポチ**）は INREVO 社のオールインワン業務管理システム。
Slack ボット + Web ダッシュボード + 外部システム連携（kintone / HRMOS）の統合プラットフォーム。

### 事業フロー
ヒトトレ採用代行支援: `MK → BC → CR → ST/AN → DR → CS/OP`、横断: 経理/総務

### コード規模
- `index.js`: 約3,700行（メイン Bolt.js 起動・ルーティング・cron）
- `src/features/*.js`: 合計 ~15,000行
- `web/src/`: React + Vite フロント
- 合計 ~20,000行規模

---

## 2. 技術スタック

| レイヤ | 採用技術 |
|---|---|
| Slack Bot | `@slack/bolt` (Bolt.js) — Socket Mode |
| Backend API | Node.js + Express |
| Frontend | React + Vite (basename `/dashboard`) |
| DB | PostgreSQL 14+（DB名: `slacktask`） |
| AI | Anthropic Claude API |
| 外部 | kintone REST API, HRMOS（イエヤス）勤怠 API, Google Drive API, Wix Webhook |
| デプロイ | AWS Lightsail (3.222.101.208) / PM2 |
| プロセス管理 | pm2 (slack-task / daily-report-watcher / slack-relay-bot) |

---

## 3. デプロイ・運用

### デプロイ手順
```bash
git push origin main
ssh -i ~/.ssh/lightsail-us-east-1.pem ubuntu@3.222.101.208 \
  "cd /home/ubuntu/slack-task-demo && git pull && cd web && npm run build && cd .. && pm2 restart slack-task"
```

### PM2 プロセス
| プロセス名 | 役割 |
|---|---|
| `slack-task` | メイン Bolt.js + Express API（全機能） |
| `daily-report-watcher` | 日報未提出者リスト集計（独立プロセス） |
| `slack-relay-bot` | 補助リレーボット |

### DB 操作
```bash
ssh ubuntu@3.222.101.208 "sudo -u postgres psql -d slacktask -c \"SQL\""
```

### 重要な運用ルール
- `web/dist` は `.gitignore` のためサーバービルド必須
- `db/index.js` の `ALTER TABLE` には必ず `.catch(() => {})` を付ける（権限エラー 42501 対策）
- SSH 鍵: `~/.ssh/lightsail-us-east-1.pem`（リポジトリ外）

---

## 4. ディレクトリ構成

```
slack-task-demo/
├─ index.js                     # メイン Bolt.js + Express 起動
├─ src/
│  ├─ db/
│  │  ├─ index.js              # 主要スキーマ・マイグレーション
│  │  └─ rpo.js                # RPO 系スキーマ
│  └─ features/
│     ├─ admin.js              # 管理者コンソール
│     ├─ approval.js           # 電子決裁フロー
│     ├─ ai-assistant.js       # Claude API ラッパー
│     ├─ an-api.js             # AN 案件管理 API
│     ├─ crm-api.js            # CRM REST API
│     ├─ crm-inquiry.js        # Wix 問い合わせ自動登録
│     ├─ daily-report-classifier.js  # 日報未提出者仕分け
│     ├─ dashboard-api.js      # 認証・基幹 REST API
│     ├─ home.js               # Slack App Home タブ
│     ├─ ieyasu.js             # HRMOS 勤怠 API
│     ├─ kintone-api.js        # kintone API ラッパー
│     ├─ kintone-sync.js       # kintone 同期ジョブ
│     ├─ legal-api.js          # 法務案件
│     ├─ notifications.js      # cron リマインド
│     ├─ pochi-ai-slack.js     # Claude AI Slack 連携
│     ├─ ranking.js            # ランキング集計
│     ├─ reaction-task.js      # リアクション起点タスク化
│     ├─ recruit-notify.js     # 採用日程通知
│     ├─ recruitment-api.js    # 自社採用 API
│     ├─ rpo-api.js            # RPO 案件 API
│     ├─ settings.js           # ユーザー/チーム設定
│     ├─ task-source-reaction.js  # `:済:` 同期
│     └─ task-ui.js            # タスクモーダル/ボタン
└─ web/
   └─ src/
      ├─ App.jsx
      ├─ main.jsx              # basename="/dashboard"
      ├─ api/client.js         # 全 API 呼び出し集約 (~700行)
      ├─ components/
      │  ├─ Layout.jsx
      │  ├─ TaskTable.jsx
      │  └─ ...
      └─ pages/
         ├─ Dashboard.jsx
         ├─ Analytics.jsx
         ├─ Approvals.jsx
         ├─ TaskDetail.jsx
         ├─ WorkloadGantt.jsx
         ├─ MySettings.jsx
         ├─ crm/               # CRM ハブ・LeadsDashboard・Pipeline 等
         ├─ rpo/               # ClientList・ClientDetail 等
         ├─ admin/             # 管理画面群
         ├─ legal/             # 法務 LegalHub
         └─ an/                # AN 依頼管理
```

---

## 5. Slack インターフェース

### 5.1 Slash Commands

| コマンド | ファイル | 概要 |
|---|---|---|
| `/dashboard` | index.js:2970 | ダッシュボード URL+認証トークン生成 |
| `/pochi-approval` | approval.js:213 | 電子決裁起票 |
| `/pochi-ask` | pochi-ai-slack.js:160 | Claude AI Q&A |
| `/task-admin` | admin.js | 管理者コンソール |
| `/task-user-settings` | settings.js | ユーザー個別設定 |
| `/task-team-settings` | settings.js | チーム全体設定 |

### 5.2 Message / Global Shortcuts

| ID | ファイル | 種別 | 概要 |
|---|---|---|---|
| `create_task_from_message` | index.js:2894 | Message | メッセージからタスク作成 |
| `open_my_tasks` | task-ui.js:330 | Global | マイタスク即起動 |
| `create_approval` | approval.js:226 | Message | メッセージから決裁起票 |
| `create_approval_global` | approval.js:243 | Global | 決裁グローバル起票 |

### 5.3 リアクションリスナー

| Emoji | ファイル | 動作 |
|---|---|---|
| `:scroll:` `:memo:` | pochi-ai-slack.js | Claude AI スレッド要約 |
| `:済:` | task-source-reaction.js | タスク完了同期（双方向） |
| 設定可 | reaction-task.js | リアクション起点タスク作成 |

### 5.4 メッセージリスナー

| 条件 | ファイル | 動作 |
|---|---|---|
| `RECRUIT_NOTIFY_CHANNEL` (#is-採用_日程調整通知) | recruit-notify.js | 担当者抽出→マップ照合→メンション返信 |
| `DAILY_REPORT_CLASSIFIER_CHANNELS` | daily-report-classifier.js | 未提出者を HRMOS と突き合わせ仕分け |
| `INQUIRY_CHANNEL_ID` (C086WBPGYAX) | crm-inquiry.js | Wix フォーム問い合わせ→自動登録 |
| `HRMOS_STAMP_IN_CHANNELS` / `HRMOS_STAMP_OUT_CHANNELS` | index.js | 日報投稿→HRMOS 打刻（60秒/10分遅延） |
| `AN_CHANNEL_ID` (C09EFPSSAF2) | an-api.js | AN 依頼管理 |

### 5.5 主要 callback_id / action_id（モーダル）

| callback_id | ファイル | 概要 |
|---|---|---|
| `task_modal` | task-ui.js | タスク作成/編集 |
| `detail_modal` | task-ui.js | 詳細表示 |
| `edit_task_modal` | task-ui.js | 編集（担当・期限） |
| `comment_modal` | task-ui.js | コメント投稿 |
| `edit_due_modal` | task-ui.js | 期限変更 |
| `progress_modal` | task-ui.js | 一斉タスク進捗 |
| `approval_create_modal` | approval.js | 決裁起票 |
| `approval_reorder_modal` | approval.js | 順次承認の順序変更 |
| `approval_reject_modal` | approval.js | 否認コメント |
| `admin_modal` | admin.js | 管理者コンソール |
| `home_filters_modal` | home.js | Home タブフィルター |

主要 action_id: `complete_task`, `reopen_task`, `open_detail_modal`, `open_progress_modal`, `confirm_broadcast_done`, `status_select`, `remove_target_user`, `remind_incomplete_users`, `approval_approve`, `approval_reject` 等。

---

## 6. cron / スケジュールジョブ

| スケジュール | ファイル | 動作 |
|---|---|---|
| `*/10 7-10 * * *` | notifications.js:216 | 朝のリマインド DM（期限切れ+当日期限を集約） |
| `0 11 * * *` | notifications.js:267 | 昼のリマインド DM |
| `0 16 * * *` | notifications.js:588 | 午後のリマインド DM |
| 30分毎 | kintone-sync.js | kintone App102/170/103/225/221 同期 |
| 30秒毎 | index.js（IEYASU） | 遅延打刻処理セーフティワーカー |
| 起動時 | index.js | DB 遅延処理リハイドレート |

### 営業日判定
`isJpBusinessDayYmd(ymd)`: 土日・祝日を除外。各 cron が業務日のみ起動するよう内部判定。

---

## 7. REST API

### 7.1 認証ミドルウェア（`authWithRole`）
**ファイル**: `src/features/dashboard-api.js`

1. **セッション検証**: Cookie `dashboard_session` or `Authorization: Bearer` → DB照合
2. **ロール決定**（優先順）:
   - DB 明示設定（`dashboard_roles.role`）: `admin` / `corp`
   - チーム名推定: `it`（IT/情シス含む）, `personnel`（人事/HR/Personnel含む）, `corporate`
   - Slack タイトル推定: `manager` / `chief` / `lead` / `member`
3. **キャッシュ**: ロール 5分 TTL
4. **View-As**: admin はなりきり可（`POST /api/dashboard/view-as`）

### 7.2 主要エンドポイント一覧

#### Dashboard 基幹 (`/api/dashboard/*`)
| METHOD | PATH | 概要 | ロール |
|---|---|---|---|
| GET | `/dashboard/auth` | マジックリンク認証 | 不要 |
| POST | `/api/auth/transfer-token` | セッション転送トークン発行 | 認証済み |
| GET | `/api/auth/transfer` | セッション移行 | 不要 |
| GET | `/api/dashboard/me` | 現在ユーザー | auth |
| GET | `/api/dashboard/me/task-triggers` | 自動タスク化キーワード一覧 | auth |
| POST/PATCH/DELETE | `/api/dashboard/me/task-triggers[/:id]` | キーワード CRUD | auth |
| GET | `/api/dashboard/summary` | KPI 概要 | auth |
| GET | `/api/dashboard/tasks` | タスク検索（多フィルター） | auth |
| GET | `/api/dashboard/admin/overdue-report[.csv\|.xlsx]` | 期限切れレポート | admin |
| GET | `/api/dashboard/admin/hrmos-monthly-check` | HRMOS 月次チェック | personnel/admin |
| GET/POST/DELETE | `/api/dashboard/admin/hrmos-excluded[/:id]` | 除外ユーザー管理 | personnel/admin |
| GET | `/api/dashboard/view-as/users` | なりきり対象 | admin |
| POST | `/api/dashboard/view-as[/clear]` | なりきり設定/解除 | admin |

#### CRM (`/api/crm/*`)
| METHOD | PATH | 概要 |
|---|---|---|
| GET/POST/PATCH | `/customers[/:id]` | 顧客 CRUD |
| GET/POST/PATCH/DELETE | `/customers/:id/contacts`, `/contacts/:contactId` | 顧客担当者 |
| GET | `/deals-list` | 案件一覧（ヘルス・ステージ） |
| GET/POST/DELETE | `/deals/:id/:path` | サブテーブル（rpo-costs, hiring-costs等） |
| GET/POST/DELETE | `/deals/:id/activities[/:actId]` | アクティビティ |
| PATCH | `/deals/:id/dormant` | 見送り化 |
| GET/PUT | `/activity-settings` | アクティビティ設定 |
| GET | `/my-crm-access`, `/permissions` | 権限確認 |
| PUT | `/permissions` | 権限設定（admin） |
| GET | `/dashboard` | CRM ダッシュボード（KPI・売上） |
| POST | `/period-settings/rollover` | 期繰越（今期→前期） |

#### RPO (`/api/rpo/*`)
| METHOD | PATH | 概要 |
|---|---|---|
| GET | `/access`, `/teams` | アクセス権・チーム |
| PATCH | `/teams/:id/hr` | HR フラグ切り替え |
| GET/POST/PUT/DELETE | `/clients[/:id]` | RPO案件 CRUD |
| GET | `/clients/:id/crm-info`, `/related` | CRM 連携情報 |
| GET/POST/DELETE | `/media-masters[/:id]` | 媒体マスター |
| GET/POST/PATCH/DELETE | `/my-tasks[/:id]` | RPOタスク |
| GET/POST/PUT/DELETE | `/task-templates[/:id]` | テンプレート |
| POST | `/clients/:id/apply-templates` | テンプレート適用 |
| GET/POST/PUT/DELETE | `/clients/:id/applicants`, `/applicants/:apId` | 応募者 |
| POST | `/webhook/:key` | 求人媒体 Webhook 受信（key検証, レート制限） |
| POST | `/clients/:id/import-csv` | 応募者CSV一括 |
| POST | `/monthly-tasks/generate` | 月次タスク自動生成 |
| GET | `/summary` | 全案件サマリー |
| GET/PATCH/POST | `/drive/candidates`, `/clients/:id/sheets-url`, `/auto-link-sheets`, `/sheets-sync` | Google Drive 連携 |

#### kintone (`/api/kintone/*`)
| METHOD | PATH | 概要 |
|---|---|---|
| GET | `/search?q=...` | 企業名部分一致 |
| GET | `/record/:recordId` | 記録詳細 |
| POST | `/sync` | 手動同期（admin） |
| GET | `/status` | 同期状態 |

#### 日報管理
| METHOD | PATH | 概要 |
|---|---|---|
| GET | `/api/dashboard/admin/daily-report/members` | 対象メンバー一覧 |
| PATCH | `/api/dashboard/admin/daily-report/members/:userId` | フラグ切替 |
| POST | `/api/dashboard/admin/daily-report/sync` | ユーザーグループから同期 |
| GET | `/api/admin/daily-report/excludes` | 公開API（外部用） |

---

## 8. DB スキーマ（テーブル一覧）

### タスク・承認
- `tasks`（task_type: personal/broadcast, status, due_date, time_of_day_*等）
- `task_targets`（broadcast 対象者）
- `task_completions`（ユーザー完了状態）
- `task_comments`, `task_activity`
- `approvals`（origin_thread_ts でスレッド連携）, `approval_voters`
- `thread_cards`
- `attendance_report_reminders`（PK: team_id+slack_user_id+reminded_date）
- `user_task_triggers`（自動タスク化キーワード）

### CRM
- `customers`, `customer_contacts`
- `deals`（yomi/status/first_meeting_date/order_date, data JSONB）
- `deal_activities`, `deal_positions`, `deal_media_plans`
- `deal_rpo_costs`, `deal_hiring_costs`, `deal_labor_costs`, `deal_application_forecasts`
- `deal_calc_defs`（計算式定義）
- `crm_permissions`, `crm_rep_roles`（is_retired/exclude_from_kpi/monthly_target_override）
- `crm_role_targets`（monthly_target, **prev_monthly_target**）
- `crm_period_settings`（curr_start/end, prev_start/end, term_target, **prev_term_target**）
- `crm_activity_settings`

### kintone 連携
- `kintone_cache`（App102/94 キャッシュ）
- `kintone_payments`（App170, **incentive_amount** = 入金確定額）
- `kintone_activities`（App103）

### RPO・採用
- `rpo_clients`, `rpo_workload_items`
- `rpo_applicants`, `rpo_applicant_actions`
- `rpo_media_sources`, `rpo_task_templates`
- `recruitment_candidates`, `recruitment_settings`
- `hrmos_applicants`（HRMOS採用 CSV インポート用）
- `hrmos_stamps`, `hrmos_excluded_users`
- `daily_report_members`

### Dashboard・権限・組織
- `dashboard_roles`（admin/corp の明示設定）
- `dashboard_sessions`
- `dashboard_user_directory`（display_name, real_name, profile_json）
- `dash_teams`, `dash_team_members`
- `legal_cases`

---

## 9. 外部連携

### 9.1 HRMOS（イエヤス）勤怠 API
**ファイル**: `src/features/ieyasu.js`
**ベース**: `https://ieyasu.co/api/inrevo/v1/`

| 関数 | 用途 |
|---|---|
| `getToken()` | 認証トークン取得（有効期限-1分で再取得） |
| `getAllUsers(token)` | ユーザー一覧（30分メモリキャッシュ） |
| `getDailyWorkOutput(token, userId, ymd)` | 日次打刻状況 |
| `stampAttendance(client, slackUserId, type)` | 打刻実行（1=出勤, 2=退勤、二重打刻チェック付） |
| `getMonthlyWorkOutputs(month)` | 月次全従業員勤怠（DB永続キャッシュ） |
| `getUserDailyContext(slackUserId, today)` | 今日+昨日の stamping 情報 |
| `getStampedUsersForDay(ymd)` | 指定日の打刻済みユーザー集合 |

**Slack 連携**:
- 出勤チャンネル（C086Z08063W）に投稿 → 60秒遅延で打刻
- 退勤チャンネル（C087K939R9N）に投稿 → 10分遅延で打刻
- 前日退勤の遅出し検知: 昨日出勤打刻あり/退勤なし + 今日投稿（出勤打刻から2時間未満）→ 自動打刻スキップ + ⏪リアクション + 案内文

### 9.2 kintone
**ファイル**: `src/features/kintone-api.js`, `kintone-sync.js`
**フィールドマップ**: `KINTONE_DEAL_FIELD_MAP`（40+フィールド）

| App | 用途 | 同期先 |
|---|---|---|
| App102 | 案件マスター | `kintone_cache` → `deals`, `customers` |
| App170 | 入金確定 | `kintone_payments` (incentive_amount) |
| App103 | 活動履歴 | `kintone_activities` |
| App225/221/94 | その他 | — |

**同期**: サーバー起動時 + 30分毎ポーリング。手動: `POST /api/kintone/sync`

### 9.3 自社採用（GAS 連携）
- スプシテンプレ: `1Jq-6I_276W-e6J91X544wY6d9kIi6JnyPh6I8UKR95k`
- GAS スクリプト: `docs/recruitment_test.gs`
- Webhook Secret: GAS Script Properties `WEBHOOK_SECRET`
- スプシから自動インポート + 性格診断テスト送信

### 9.4 HRMOS 採用（CSV方式・未実装）
- API なし、CSV エクスポートのみ
- 週1 CSV → 自動集計予定（仕様確定済み、ヘッダー仕様確認待ち）

### 9.5 Wix フォーム
- Webhook → `INQUIRY_CHANNEL_ID` (C086WBPGYAX) に投稿
- `crm-inquiry.js` が検知 → `customers` + `deals` 自動登録

### 9.6 Google Drive
- RPO 案件スプシ自動検出・URL 保存・データ同期

---

## 10. 環境変数（主要）

### Slack
| 変数 | 用途 |
|---|---|
| `SLACK_BOT_TOKEN` | Bot Token |
| `SLACK_USER_TOKEN` | User Token（アーカイブメッセージ取得用、optional） |
| `SLACK_TEAM_ID` | T086C06L5V0 |
| `PORT` | 3000 |

### コマンド名カスタマイズ
`ADMIN_COMMAND`, `USER_SETTINGS_COMMAND`, `TEAM_SETTINGS_COMMAND`

### チャンネル設定
| 変数 | 用途 |
|---|---|
| `HRMOS_STAMP_IN_CHANNELS` | 出勤日報 (C086Z08063W) |
| `HRMOS_STAMP_OUT_CHANNELS` | 退勤日報 (C087K939R9N) |
| `DAILY_REPORT_CLASSIFIER_CHANNELS` | 日報仕分け対象 |
| `INQUIRY_CHANNEL_ID` | Wix問い合わせ (C086WBPGYAX) |
| `RECRUIT_NOTIFY_CHANNEL` | 採用日程通知 (C086G2B60SK) |
| `RECRUIT_NOTIFY_HANDLER_MAP` | 担当者マップ「板金:U08KXJ599QW」形式 |
| `MK_OVERDUE_NOTIFY_CHANNEL_ID` | MK期限切れ通知 (C087A0B6597) |
| `AN_CHANNEL_ID` | AN管理 (C09EFPSSAF2) |
| `BACKOFFICE_TASK_THREAD_CHANNEL` | C0AP0PEL5ME |

### 部署・ユーザーグループ
`DEPT_ALL_HANDLES`, `DEPT_PRIORITY`, `DEPT_CACHE_TTL_SEC`, `CORP_SOUMU_USERGROUP_ID`, `CORP_SYSTEM_USERGROUP_ID`, `BC_CONTRACT_ASSIGNEE_USER_ID_*`

### 外部 API
| 変数 | 用途 |
|---|---|
| `ANTHROPIC_API_KEY` | Claude AI |
| `IEYASU_API_TOKEN` | HRMOS API（base64） |
| `HRMOS_WEBHOOK_SECRET` | HRMOS Webhook 検証 |
| `KINTONE_DOMAIN` / `KINTONE_APP_*_TOKEN` | kintone |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | Google OAuth |
| `DATABASE_URL` | PostgreSQL |
| `DB_POOL_SIZE` | 20 |
| `PGSSL` | false |

### URL
| 変数 | 用途 |
|---|---|
| `DASHBOARD_BASE_URL` | https://inrevo-task.com |
| `PUBLIC_BASE_URL` / `APP_BASE_URL` | 同上 |

### フラグ
`HOME_V2`, `AN_AUTO_REPLY`, `RUN_NOTIFY_NOW`, `NODE_ENV`

---

## 11. Web フロントエンド構成

### ルーティング
`web/src/main.jsx` で `BrowserRouter basename="/dashboard"`。

| パス | 画面 | 概要 |
|---|---|---|
| `/` | Dashboard | ホーム（KPI・統計） |
| `/tasks/new` | TaskNew | タスク作成 |
| `/tasks/:id` | TaskDetail | タスク詳細・AI分析 |
| `/projects/:id` | ProjectDetail | プロジェクト配下タスク |
| `/analytics` | Analytics | メンバー別実績・タスク統計 |
| `/workload` | WorkloadGantt | 工数 Gantt |
| `/org-chart` | OrgChart | 組織図 |
| `/ranking` | Ranking | Slack ランキングジョブ |
| `/workflows` | Slack Workflows | WF 一覧 |
| `/my-settings` | MySettings | 自動タスク化キーワード等 |
| `/approvals` | Approvals | 電子決裁一覧 |
| `/floating` | FloatingTasks | 浮動ウィンドウ |
| `/unauthorized` | Unauthorized | 401/403 |
| `/crm` | CRM ハブ | Leads / SalesPerformance / Customers タブ |
| `/crm/leads` | LeadsDashboard | リード漏斗・ドリルダウン |
| `/crm/customers[/:id]` | CustomerList/Detail | 顧客管理（新） |
| `/crm/pipeline` | Pipeline | Kanban（旧） |
| `/rpo` | ClientList | RPO 案件ランキング |
| `/rpo/:id` | ClientDetail | スプシ連携・タスク・工数 |
| `/rpo/summary` | RpoSummary | 全案件サマリー |
| `/rpo/workload` | Workload | 工数管理 |
| `/rpo/mytasks` | MyTasks | RPO 個別タスク |
| `/legal` | LegalHub | 法務案件管理 |
| `/an` | AnList | AN 依頼管理 |
| `/admin/*` | Admin 各画面 | 管理者・IT・人事・Corp |

### ロール別アクセス
| ロール | 主なアクセス |
|---|---|
| `admin` | 全機能 + Admin 画面（全タブ） |
| `corp` | CRM / RPO / 工数 / 採用（基本） |
| `it` | チャンネルマッピング / Slackグループ / 日報管理 |
| `personnel` | 採用管理 / HRMOS連携 |
| 一般 | ホーム / タスク / Analytics / 個人設定 |

403 → `/unauthorized` リダイレクト。

### API クライアント
`web/src/api/client.js`（~700行）に全エンドポイントを集約。`credentials: 'include'` でセッションクッキー自動送信。

### モバイル対応
- ヘッダーに Safari 転送ボタン（モバイル → ブラウザセッション引き継ぎ）
- iPhone/iPad/Android 判定で UI 切替

---

## 12. 主要機能の実装ポイント

### 12.1 タスク
- **personal**: 1対1割当、`tasks.task_type='personal'`
- **broadcast**: 一斉送信、`task_targets` で対象者管理、`task_completions` で個別完了状態
- **デフォルト期限**: 投稿時刻 JST 15:00以降は翌日、それ以前は当日（`time_of_day` で時間指定可）
- **完了同期**: `:済:` リアクション ⇔ ボタン完了

### 12.2 電子決裁
**ファイル**: `src/features/approval.js`
- 順次承認/並列承認モード（モーダル選択）
- 順次の場合は順序変更モーダル `approval_reorder_modal`
- スレッド連携: `approvals.origin_thread_ts` でショートカット起点のスレッド保持
- メッセージショートカット `create_approval` でクリックメッセージを初期説明に流用

### 12.3 通知集約（DM リマインド）
**ファイル**: `src/features/notifications.js`
- 朝 7:00-11:00 の 10分毎: 期限切れ + 当日期限を **1通の集約 DM** で送付
- DM 重複防止: `dbGetNotificationThread` でユーザー別 thread 管理
- 営業日のみ実施

### 12.4 出退勤 HRMOS 連携
- 出勤チャンネル投稿 → DB 事前 INSERT → 60秒 setTimeout → 打刻
- プロセス再起動対策: 起動時に DB から遅延処理リハイドレート + 30秒毎セーフティワーカー
- 遅出し退勤検知: `getUserDailyContext` で前日 stamping 確認
- 重複防止: `attendance_report_reminders` フラグ + HRMOS daily 二重チェック

### 12.5 日報未提出者仕分け
**ファイル**: `src/features/daily-report-classifier.js`
- daily-report-watcher の bot 投稿を検知（チャンネル: 出勤/退勤両方）
- 名前抽出: `^([一-鿿ぁ-んァ-ヶー々〆〤\s]{2,20})\/[A-Za-z]/` の厳格パターン
- 3カテゴリ仕分け:
  - 📝 出すべき: segment 出勤系 + 出勤打刻あり
  - ❓ 要確認: segment 出勤系 + 出勤打刻なし
  - 🏖️ 休暇確認済み: segment 公休/有給/祝日
- 出勤日報 = 当日、退勤日報 = 前日のデータをチェック
- `link_names: false` でメンション化抑止

### 12.6 CRM ダッシュボード
**ファイル**: `src/features/crm-api.js`, `web/src/pages/crm/CrmDashboard.jsx`
- 期間モード: term（今期）/ prevterm（前期）/ range（範囲）/ custom（任意）
- 前期目標保管: `crm_period_settings.prev_term_target` + `crm_role_targets.prev_monthly_target`
- 期繰越 API: `POST /api/crm/period-settings/rollover` で今期→前期コピー
- 前前期以前 or 前期目標未設定 → KPI 達成率カード非表示

### 12.7 自動タスク化
- 個人設定（`/my-settings`）でキーワード登録（`user_task_triggers`）
- 該当キーワード含む Slack 投稿 → タスク自動生成（自分宛 personal）

### 12.8 Wix 問い合わせ → CRM 自動登録
**ファイル**: `src/features/crm-inquiry.js`
- `INQUIRY_CHANNEL_ID` 監視
- フォーム本文をパース → `customers` + `deals` レコード生成

### 12.9 採用日程通知（recruit-notify）
- `RECRUIT_NOTIFY_CHANNEL` 投稿から「担当者」キーワード抽出
- `RECRUIT_NOTIFY_HANDLER_MAP`（`板金:U08KXJ599QW` 形式）で照合
- スレッドに該当者をメンション返信

---

## 13. データマスター方針
- **TaskHub がデータマスター**（kintone は移行期間中のみ参照）
- kintone 同期は片方向（kintone → TaskHub）
- 今後の入力は TaskHub 側で完結

---

## 14. 開発時のリスク・注意点

| 項目 | 対策 |
|---|---|
| ALTER TABLE 権限エラー (42501) | `.catch(() => {})` 必須 |
| `due_at` 等の新カラム追加で本番崩壊 | マイグレーション後に依存コード追加 |
| timezone バグ | `stamped_at::date` ではなく `(stamped_at AT TIME ZONE 'Asia/Tokyo')::date` |
| HRMOS daily API 遅延 | reminder フラグ + 時間ヒューリスティックで補強 |
| プロセス再起動で setTimeout 消失 | DB 事前 INSERT + 起動時リハイドレート |
| SQL 予約語（do） | エイリアス回避（dn など） |
| Slash command がスレッド内で動かない | message shortcut で対応 |
| `link_names: true` で意図せぬメンション化 | bot 投稿は明示的に `false` |

---

## 15. リファレンス
- `docs/roadmap.md` — TODO・将来計画
- `docs/specs.md` — CRM/採用/kintone 詳細仕様
- `docs/crm-dashboard-spec.md` — CRMダッシュボード仕様
- `docs/taskhub-features.md` — 機能サマリ
- `docs/system-design.md` — 設計メモ
- `docs/features.md` — 機能一覧
