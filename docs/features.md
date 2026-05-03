# TaskHub / RPO システム 機能仕様書

> 最終更新: 2026-04-18

---

## 目次

1. [システム全体構成](#1-システム全体構成)
2. [認証・アクセス制御](#2-認証アクセス制御)
3. [Slack ホーム画面（Slack App Home）](#3-slack-ホーム画面slack-app-home)
4. [タスク管理（Slack Bot）](#4-タスク管理slack-bot)
5. [ダッシュボード Web アプリ](#5-ダッシュボード-web-アプリ)
6. [業務ガント（Workload Gantt）](#6-業務ガントworkload-gantt)
7. [チーム設定・組織図（OrgChart）](#7-チーム設定組織図orgchart)
8. [RPO 案件管理](#8-rpo-案件管理)
9. [kintone 連携](#9-kintone-連携)
10. [Google Drive 連携](#10-google-drive-連携)
11. [管理者機能（Admin）](#11-管理者機能admin)
12. [データベーススキーマ](#12-データベーススキーマ)
13. [デプロイ・インフラ](#13-デプロイインフラ)

---

## 1. システム全体構成

### 技術スタック

| 層 | 技術 |
|----|------|
| Slack Bot ランタイム | Node.js + [@slack/bolt](https://github.com/slackapi/bolt-js) |
| API サーバー | Express（Bolt と同一プロセス） |
| フロントエンド | React 18 + Vite + React Router v6 |
| データベース | PostgreSQL（AWS Lightsail / `DATABASE_URL` 環境変数） |
| ホスティング | AWS Lightsail (2 GB RAM / 2 vCPU / 60 GB SSD) |
| プロセスマネージャ | pm2 (`slack-task` という名前で管理) |
| デプロイ | scp で `~/slack-task-demo/` に転送 → `pm2 restart slack-task` |

### ポート・エンドポイント

- Slack Bolt: ポート `3000`（Slack からのイベント受信）
- Express API: Bolt と同一プロセス（`/api/*` を担当）
- React SPA: `web/` ディレクトリ、`/` 以下でサーブ（Vite ビルド後の静的ファイルを Express が配信）

### ディレクトリ構造

```
slack-home-demo/
├── index.js                  # エントリポイント。Bolt App + Express 初期化、全機能を登録
├── src/
│   ├── features/
│   │   ├── home.js           # Slack Home タブ（フィルタUI、タスク一覧表示）
│   │   ├── dashboard-api.js  # ダッシュボード Web 用 REST API + 認証
│   │   ├── rpo-api.js        # RPO 案件管理 REST API
│   │   ├── kintone-api.js    # kintone 検索・同期 API
│   │   ├── kintone-sync.js   # kintone → kintone_cache 同期ロジック
│   │   ├── drive-api.js      # Google Drive ファイル一覧・フォルダ検索
│   │   ├── task-ui.js        # タスク作成・詳細モーダル UI
│   │   ├── notifications.js  # 通知スレッド送信
│   │   ├── reaction-task.js  # リアクションタスク化
│   │   ├── admin.js          # 管理コマンド（/admin）
│   │   └── settings.js       # ユーザー設定
│   ├── db/
│   │   ├── index.js          # メイン DB ヘルパー（全テーブル定義・CRUD）
│   │   ├── rpo.js            # RPO 専用 DB ヘルパー
│   │   └── kintone.js        # kintone キャッシュ DB ヘルパー
│   └── utils/
│       └── common.js
└── web/
    └── src/
        ├── App.jsx            # ルーティング
        ├── api/client.js      # フロントエンド API クライアント
        ├── components/
        │   ├── Layout.jsx
        │   └── TaskTable.jsx
        └── pages/
            ├── Dashboard.jsx
            ├── WorkloadGantt.jsx
            ├── OrgChart.jsx
            ├── Analytics.jsx
            ├── admin/         # 管理者画面群
            └── rpo/           # RPO 案件管理画面群
```

---

## 2. 認証・アクセス制御

### Slack Bot 側

- Slack App が Workspace にインストールされていることが前提
- `SLACK_BOT_TOKEN` と `SLACK_SIGNING_SECRET` で認証
- ユーザーは Slack のワークスペースメンバーとして識別（`userId = req.body.user.id`）

### ダッシュボード Web 側

#### マジックリンク認証

1. Slack ユーザーが `/api/auth/request-link` を叩く（または Slack の App Home からリンクを送信）
2. サーバーで 32 バイトランダムトークンを生成、インメモリ `authTokens` Map に保存（TTL: 1 時間、使い捨て）
3. トークン付き URL を Slack DM で送信
4. ユーザーがリンクをクリック → `GET /auth/verify?token=xxx` でトークンを消費
5. サーバーは `user_sessions` テーブルに `sessionId`（32 バイト）を保存
6. `Set-Cookie: session=<sessionId>; HttpOnly; SameSite=Strict; Max-Age=10年`
7. 以降すべての `/api/*` リクエストでクッキーを検証 → `req.dashboardUser = { teamId, userId, role }` を付与

#### ロール体系

| ロール | 設定場所 | 意味 |
|--------|---------|------|
| `admin` | `dashboard_roles` テーブル（グローバル） | 全機能管理権限。「すべて」フィルタ表示、RPO フルアクセスなど |
| `manager` | `dashboard_roles` テーブル | RPO フルアクセス相当（admin と同扱いの箇所あり） |
| `member` | デフォルト | 一般ユーザー |
| チームロール | `dash_team_members.role` | `admin` / `dept_leader` / `team_leader` / `sub_leader` / `member` |

#### `authWithRole` ミドルウェア

- クッキー `session` を `user_sessions` テーブルに照合
- `dashboard_roles` からグローバルロールを取得して `req.dashboardUser.role` にセット
- `dash_team_members` から最上位チームロールを取得して `req.dashboardUser.teamRole` にセット
- 認証失敗時は `403` または `401`

#### `adminOnly` ミドルウェア

- `req.dashboardUser.role === 'admin'` を検証
- 管理者専用エンドポイントに適用（チーム削除、kintone 手動同期など）

---

## 3. Slack ホーム画面（Slack App Home）

`src/features/home.js` で実装。

### 概要

Slack の「ホーム」タブに Block Kit で構築したタスクダッシュボードを表示する。ユーザーごとに状態（フィルター・表示モード）を保持しており、タスクの確認・完了操作が Slack 上で完結する。

### 状態管理（homeState）

インメモリ Map（`homeState`）でユーザーごとの状態を管理する。

```js
{
  broadcastScopeKey: "to_me",  // 表示範囲キー（下記参照）
  scopeKey: "active",          // "active" | "done"
  displayMode: "standard",     // "standard" | "compact"
  homeMore: { overdue: false, today: false },   // セクション展開
  homeFold: { overdue: false, today: false, later: false }, // セクション折りたたみ
}
```

起動時に `resolveHomeDefaults` でDBから永続化された値を読み込み（`user_settings` に保存）。

### 表示範囲キー（rangeKey / broadcastScopeKey）

| キー | 内容 |
|------|------|
| `to_me` | 自分あて（自分が assignee または target のタスク） |
| `requested_by_me` | 自分が発行したタスク |
| `pf:<id>` | 個人フィルタ（登録したメンバーリスト） |
| `dash_dept:<id>` | 指定部署のメンバー全員が assignee のタスク |
| `dash_team:<id>` | 指定チームのメンバー全員が assignee のタスク |
| `all` | すべて（管理者のみ表示） |

### 個人フィルタ（Personal Filter）

- `personal_filters` + `personal_filter_members` テーブルで管理
- ユーザーが任意のメンバーセットに名前をつけて保存
- フィルタモーダルで CRUD 操作可能
- `pf:<id>` キーで参照

### タスクの種別

| 種別 | 説明 |
|------|------|
| `personal` | 個人タスク。`tasks` テーブル、`assignee_id` で誰あてかを識別 |
| `broadcast` | 全員または指定グループへの一斉タスク。`task_targets` テーブルで対象ユーザーを管理 |

#### `dash_dept` / `dash_team` フィルタ時のタスク絞り込みロジック

1. `dbGetDashTeamSubtree(teamId, rootId)` でサブツリー全チームID一覧を取得
2. `dash_team_members` からそのチームに属する全ユーザーIDを `memberSet` として収集
3. personal タスク: `assignee_id` が `memberSet` に含まれるもの（依頼者は対象外）
4. broadcast タスク: `task_targets` に自分が含まれているもの

### フィルタモーダル

フィルタボタン押下で `buildHomeFiltersModalView` が生成するモーダルが開く。

- **範囲セレクタ**: 自分あて → 自分が発行 → 個人フィルタ(★) → 部署(🏢) → すべて（admin のみ）
  - `dispatch_action: true` で部署選択時に即座にチームサブセレクタを更新
- **チームサブセレクタ**: 部署選択時に子チームが存在する場合に追加表示
- **状態セレクタ**: 未完了 / 完了
- **個人フィルタ管理**: 追加・削除・メンバー編集

### アクションバー（インライン）

ホーム画面上部に表示。フィルタモーダルを開かずに範囲・状態を変更できる。

- `dash_dept:X` / `dash_team:X` 選択中はインラインでチームサブセレクタも表示
- `app.action("home_team_sub_select")` ハンドラーでチームを変更

### タスク表示構造

タスクは以下の3セクションに分けて表示：

| セクション | 条件 |
|-----------|------|
| 期限切れ | `due_date < today` |
| 今日まで | `due_date == today` |
| 今後 | `due_date > today` または期限なし |

各セクション：
- 折りたたみ可能（`homeFold`）
- 「もっと見る」で最大10件 → 追加10件ずつ表示（`homeMore`）
- タスク行に「完了」ボタン、クリックで詳細モーダル

### 一覧モーダル（buildTaskListModalView）

「一覧で見る」ボタンで開くモーダル。ホームのフィルター条件を完全に引き継ぐ。

- `rangeKey`（`pf:`, `dash_dept:`, `dash_team:`, `to_me` など）を維持
- 同じ絞り込みロジックを適用
- 範囲・状態を変更するセレクタ付き（モーダル内で完結）
- `dash_team:X` 選択時は親部署をデフォルト表示、サブセレクタで子チームへ絞り込み可能

### 表示モード

- `standard`: 通常表示（タスクタイトル + 期限 + 担当者）
- `compact`: コンパクト表示（1行）

### 通知

`notifications.js` と連携し、タスク更新時に関連ユーザーのホームを再描画（`publishHomeForUsers`）。

---

## 4. タスク管理（Slack Bot）

`src/features/task-ui.js`、`src/features/reaction-task.js` で実装。

### タスク作成

- **コマンド**: `/task` でタスク作成モーダルを開く
- **リアクションタスク化**: メッセージに特定リアクション（絵文字）を付けるとタスク化
- **モーダル項目**:
  - タイトル（必須）
  - 説明
  - 担当者（`assignee_id`）
  - 期限日（`due_date`）
  - ユーザーグループ（Slack ユーザーグループ）→ broadcast タスクとして全員に配信
  - プロジェクト紐付け

### タスクの状態遷移

```
in_progress → done
```

- 「完了」ボタン押下で `status = 'done'`
- ホーム画面上で直接完了操作可能

### broadcast タスク

- `task_type = 'broadcast'` のタスクは `task_targets` テーブルで対象ユーザーを管理
- `dbIsUserTarget(teamId, taskId, userId)` で自分が対象か確認
- ユーザーグループ指定時は `getUsergroupMembers()` でメンバー展開 → `task_targets` に一括挿入

### タスク詳細モーダル

`openDetailModal` で開く。

- タイトル・説明・担当者・期限の表示と編集
- コメント入力
- 完了・削除操作

### プロジェクト紐付け

- `projects` テーブルでプロジェクトを管理
- `project_tasks` でタスクとプロジェクトの多対多関係を管理
- `/projects/:id` でプロジェクトビュー表示

---

## 5. ダッシュボード Web アプリ

React SPA。`web/src/` 以下。

### ルーティング

| パス | コンポーネント | 説明 |
|------|--------------|------|
| `/` | `Dashboard` | タスク一覧ダッシュボード |
| `/tasks/new` | `TaskCreate` | タスク作成 |
| `/tasks/:id` | `TaskDetail` | タスク詳細 |
| `/projects/:id` | `ProjectView` | プロジェクトビュー |
| `/analytics` | `Analytics` | 分析画面 |
| `/workload` | `WorkloadGantt` | 業務ガント |
| `/org-chart` | `OrgChart` | 組織図・チーム設定 |
| `/admin/*` | `AdminLayout` | 管理者画面群 |
| `/rpo` | `ClientList` | RPO 案件一覧 |
| `/rpo/summary` | `RpoSummary` | RPO サマリー |
| `/rpo/workload` | `RpoWorkload` | RPO 工数管理 |
| `/rpo/tree` | `TaskTreeView` | RPO タスクツリー |
| `/rpo/:id` | `ClientDetail` | RPO 案件詳細 |

### API クライアント（`web/src/api/client.js`）

フロントエンド共通の fetch ラッパー。クッキーセッションで認証。全 API 呼び出しはここに集約。

---

## 6. 業務ガント（Workload Gantt）

`web/src/pages/WorkloadGantt.jsx` で実装。

### 概要

チームメンバーごとの業務（定常業務 + Slack タスク）を横並びのガントチャートで表示・管理する。

### データモデル

- `workload_items`: 業務アイテム（誰のどんな業務）
  - `team_id`, `dash_team_id`, `owner_user_id`, `title`, `category`, `sort_order`, `rpo_client_id`
- `workload_cells`: 各日の塗りデータ
  - `item_id`, `month_key`（YYYY-MM）, `day_num`（1〜31）, `intensity`（0〜3）

### 繰り返しパターン

| パターン | 説明 |
|---------|------|
| `range` | 開始日〜終了日の連続範囲 |
| `daily` | 毎日（平日のみ、土日祝除外） |
| `weekly` | 指定曜日（0〜6）に毎週 |
| `monthly` | 指定日（1〜31）に毎月。`businessDay=true` で営業日換算 |

**営業日換算ルール**:
- 1〜26日: 月初から N 番目の営業日
- 27〜31日: 月末から逆算して (31-N+1) 番目の営業日
- 土日・祝日は除外（祝日データは外部ライブラリで取得）

### 表示期間

| モード | 内容 |
|--------|------|
| `week` | 今週（7日） |
| `14d` | 14日間 |
| `31d` | 31日間 |
| `month` | 月（1日〜末日）。前月・次月ボタンで移動 |

### ドラッグ&ドロップ操作

- **行の並び替え**: メンバー内で `sort_order` を変更
- **担当者変更**: 別メンバーのヘッダーにドロップ
- **バーの伸縮**: 右端ドラッグで終了日変更
- **バーの移動**: バー全体ドラッグで開始日変更
- **Slack タスクの期限変更**: バーをドラッグして `due_date` 更新

### Slack タスク表示

- Slack タスク（`in_progress` のもの）をガントに重ねて表示
- 期限切れは赤表示
- ユーザーごとに色設定可能（`user_settings` の `ganttTaskColor`）
- 表示 ON/OFF 切替可能

### カテゴリ管理

- `workload_categories` テーブルで色付きカテゴリを管理
- 業務作成時にカテゴリを選択するとバー色が統一される

### 前月コピー機能（月表示時）

月表示のとき、前月の業務登録内容を現月にコピーするボタンが表示される。

### API エンドポイント

| メソッド | パス | 説明 |
|---------|-----|------|
| GET | `/api/workload/items` | 業務一覧 |
| POST | `/api/workload/items` | 業務作成 |
| PUT | `/api/workload/items/:id` | 業務更新 |
| DELETE | `/api/workload/items/:id` | 業務削除 |
| GET | `/api/workload/cells` | セルデータ取得 |
| PUT | `/api/workload/cells` | セルデータ更新（一括） |
| GET | `/api/workload/users` | チームメンバー一覧 |
| GET | `/api/workload/categories` | カテゴリ一覧 |
| POST | `/api/workload/categories` | カテゴリ作成 |

---

## 7. チーム設定・組織図（OrgChart）

`web/src/pages/OrgChart.jsx` で実装。

### 組織構造

```
dash_teams テーブル
  ├── 部署A (parent_id = null)
  │   ├── チームA1 (parent_id = 部署A.id)
  │   └── チームA2 (parent_id = 部署A.id)
  └── 独立チームX (parent_id = null, 子なし)
```

- `parent_id = null` かつ子チームを持つ → 部署
- `parent_id = null` かつ子なし → 独立チーム
- `parent_id != null` → チーム（部署の子）

### dash_team_members のロール

| ロール | 説明 |
|--------|------|
| `admin` | チーム管理者 |
| `dept_leader` | 部署リーダー |
| `team_leader` | チームリーダー |
| `sub_leader` | サブリーダー |
| `member` | メンバー |

### 表示モード

- **表示モード**: 部署・チーム一覧と所属メンバーを閲覧。名前・役職で検索
- **編集モード**:「チームを組む」ボタンで切替
  - メンバーのドラッグ&ドロップでチーム間移動
  - チーム作成・削除・改名
  - チームの部署変更（親チーム変更）
  - ロール変更
  - 未所属メンバーの非表示化（退職者など）

### API エンドポイント

| メソッド | パス | 説明 |
|---------|-----|------|
| GET | `/api/dash-teams` | チーム一覧（階層付き） |
| POST | `/api/dash-teams` | チーム作成 |
| PUT | `/api/dash-teams/:id` | チーム更新 |
| DELETE | `/api/dash-teams/:id` | チーム削除 |
| POST | `/api/dash-teams/:id/members` | メンバー追加 |
| DELETE | `/api/dash-teams/:id/members/:userId` | メンバー削除 |
| PATCH | `/api/dash-teams/:id/members/:userId/role` | ロール変更 |
| GET | `/api/user-directory` | ユーザー一覧 |

### dbGetDashTeamSubtree

指定チームID配下の全チームIDを再帰的に取得。CTE（WITH RECURSIVE）で実装。`dash_dept:X` / `dash_team:X` フィルタ時のメンバー収集に使用。

---

## 8. RPO 案件管理

`src/features/rpo-api.js`、`src/db/rpo.js`、`web/src/pages/rpo/` で実装。

### 概要

HR 採用支援（RPO）の案件管理システム。案件（クライアント）ごとに応募者・媒体・予算・タスクを一元管理する。

### アクセス権限

`dbGetUserRpoAccess(teamId, userId, role)` で判定：

| 条件 | アクセス権 |
|------|-----------|
| `admin` または `manager` | フルアクセス（全案件閲覧） |
| `is_hr_dept=true` チームのメンバー | 自分のチームの案件のみ閲覧 |
| 上記以外 | アクセス不可 |

`is_hr_dept` は `dash_teams` テーブルのフラグ。Admin画面の RPO 設定でチームごとに設定可能。

### データモデル

#### rpo_clients（案件）

| カラム | 型 | 説明 |
|--------|---|------|
| `id` | TEXT | UUID |
| `team_id` | TEXT | Slack ワークスペース ID |
| `name` | TEXT | 企業名 |
| `color` | TEXT | アクセントカラー（Ocean/Emerald/Amber/Rose/Violet/Pink/Teal/Slate） |
| `plan` | TEXT | 契約プラン（`monthly` 月額 / `guarantee` 保証型） |
| `status` | TEXT | `active` / `inactive` |
| `dash_team_id` | TEXT | 担当チーム（外部キーなし） |
| `data` | JSONB | 全付帯情報（下記参照） |

**`data` JSONB の構造**:

```json
{
  "projectInfo": {
    "hiringTarget": 5,
    "contractAmount": 2000000,
    "totalBudget": 1500000,
    "inrevoContact": "担当営業名",
    "clientContact": "クライアント担当者",
    "memo": "メモ"
  },
  "kpiData": {
    "applied": 50,
    "document_passed": 30,
    "first_interview": 20,
    "second_interview": 10,
    "final_interview": 5,
    "offered": 3,
    "accepted": 2
  },
  "mediaStatus": [
    {
      "id": "uuid",
      "name": "Indeed",
      "mediaCost": 300000,
      "hiredCount": 2,
      "periodStart": "2026-01",
      "periodEnd": "2026-03"
    }
  ],
  "driveFolder": "https://drive.google.com/drive/folders/xxx",
  "sheetsUrl": "https://docs.google.com/spreadsheets/d/xxx",
  "hrAssigneeId": "U12345",
  "hrAssigneeName": "担当者名",
  "workloadByMonth": {
    "2026-04": { "hours": 40, "otherHours": 10 }
  },
  "csAssignee": { "userId": "U12345", "displayName": "名前" },
  "kintone": {
    "recordId": "123",
    "appId": "102",
    "data": { "顧客": "...", "受注日": "..." }
  }
}
```

### 案件一覧画面（ClientList）

- 担当チームフィルタ（フルアクセス時はチーム選択必須）
- HR 担当者フィルタ
- 案件作成モーダル:
  - 企業名入力 → kintone サジェスト（debounce）
  - kintone レコード選択で支払方式・受注金額を自動セット
  - Drive フォルダ候補を検索してサジェスト
  - 案件作成時に Drive フォルダ + 管理シートを自動検索して `data.driveFolder` / `data.sheetsUrl` をセット
- 一括登録（CSV インポート）ボタン

### 案件詳細画面（ClientDetail）

7つのタブで構成。一度開いたタブはアンマウントせず CSS で非表示にする（再描画防止）。

#### ダッシュボードタブ

- **プロジェクト概要**: HR 担当者・営業担当者・採用目標人数・受注金額・メモ
- **kintone 再同期ボタン**: 企業名で kintone を検索し、「反映」で受注金額・採用目標を上書き
- **予算管理**: 総予算入力、使用率バー、媒体別コスト内訳
  - 使用率 80%以上で警告色（amber）、100%以上で危険色（red）
- **売上・収益カード**:
  - 受注金額
  - 媒体費合計（▲表示）
  - 粗利（受注金額 − 媒体費合計）
  - 内定承諾者数（応募者 DB から集計）
  - 1名あたり売り上げ（最新）

**売り上げ計算式**:
```
revenuePerHead = contractAmount / hiringTarget

各掲載期間について（掲載期間開始日順）:
  remaining = hiringTarget - 前期間までの採用累計
  costPerHead = mediaCost / remaining
  accCostPerHead += costPerHead
  revenuePerHire = revenuePerHead - accCostPerHead
```

- **媒体別採用数チャート** (SVG バーチャート)
- **KPI ファネルチャート** (SVG ファネル)
- **期間別売り上げ内訳テーブル**

#### KPI タブ

採用ファネルの各フェーズ（応募 → 書類通過 → 一次面接 → 二次面接 → 最終面接 → 内定 → 承諾）の件数を入力。フェーズ間の通過率を自動計算して表示。

#### 求人・媒体タブ（ContentTab）

- 媒体ごとの掲載期間・コスト・採用数を管理
- 媒体マスタ（`rpo_media_masters`）からプルダウンで選択
- Google Drive フォルダのファイル一覧表示（`/api/drive/files`）
- Apps Script URL が設定されていれば Google Sheets データを取得して表示

#### 応募者タブ（SheetsApplicantTab）

- `rpo_applicants` テーブルの CRUD
- ステータス別タブ（応募・書類通過・一次通過・二次通過・最終通過・内定・内定承諾・不合格・辞退など）
- 担当 CS（スカウター）割り当て
- 応募日・媒体・備考
- 各応募者のアクション履歴（`rpo_applicant_actions`）
- CSV インポート機能

#### タスクタブ（TasksTab）

- `workload_items` から `rpo_client_id` で紐づくタスク一覧
- 月次タスク自動生成（`dbGenerateMonthlyTasksForClient`）
- タスクテンプレート（`rpo_task_templates`）を適用して一括作成

#### 書類タブ（DocumentsTab）

- Google Drive フォルダ内のファイル一覧表示
- フォルダ URL の設定・変更
- 管理シート URL の設定・変更

#### 歩留まりタブ（FunnelTab）

- 応募者ステータスの遷移を可視化
- ステータス別件数をファネルチャートで表示

### 月次タスク自動生成

`dbGenerateMonthlyTasksAll(teamId, yearMonth, userId)`:

1. `status = 'active'` の全案件を取得
2. `rpo_task_templates` の全テンプレートを取得
3. 案件 × テンプレートで `workload_items` を作成（重複チェックあり）

`POST /api/rpo/monthly-tasks/generate` で実行（全案件一括）  
`POST /api/rpo/clients/:id/monthly-tasks/generate` で個別案件実行

### RPO サマリー画面（RpoSummary）

`/api/rpo/summary` から全案件の集計データを取得して一覧表示。

- 案件名・担当チーム・プラン・受注金額・媒体費・粗利・採用状況

### RPO 工数管理画面（RpoWorkload）

- 案件ごとの月次工数（HR 時間・その他時間）を管理
- `PATCH /api/rpo/clients/:id/workload-hours` で更新

### 外部 Webhook 受信

`POST /api/rpo/webhook/:key`（認証不要）:

- `rpo_media_sources` の `webhook_key` で媒体ソースを識別
- ペイロードから応募者名を抽出して `rpo_applicants` に追加
- `rpo_applicant_actions` に `webhook_received` アクションを記録

### CSV 一括インポート

`POST /api/rpo/clients/:id/import-csv`（multer でファイル受信）:

- ヘッダー行: `氏名/名前/name/applicant_name`, `ステータス/status`, `媒体/source/求人媒体`, `応募日/applied_at`, `備考/notes`
- `名前` カラムが必須

---

## 9. kintone 連携

`src/features/kintone-api.js`、`src/features/kintone-sync.js`、`src/db/kintone.js` で実装。

### 概要

kintone（サイボウズ）の CRM データを定期同期してローカルキャッシュに保存し、RPO 案件作成時の企業名サジェストや受注情報の自動反映に使用する。

### 同期対象

`src/features/kintone-sync.js` の `APPS` 定数で定義:

| アプリ ID | 企業名フィールド | 取得フィールド |
|-----------|--------------|-------------|
| `102` | `顧客` | `$id`, `顧客`, `支払方式`, `受注日`, `担当営業_0`, `見込売り上げ_税抜き`, `数値_0`（採用目標） |

- ドメイン: `KINTONE_DOMAIN` 環境変数（デフォルト: `ca7n5wh2hfvv.cybozu.com`）
- 認証: `X-Cybozu-API-Token` ヘッダー（アプリごとのトークン）

### 同期ロジック

1. サーバー起動時に `dbEnsureKintoneSchema()` → 即座に `runSync()` を実行
2. 以後 30 分ごとに自動同期（`setInterval`）
3. `syncKintoneApp(appId, cfg)`: kintone REST API で 500 件ずつページングして全件取得
4. `dbUpsertKintoneRecords(appId, records)`: `kintone_cache` テーブルに UPSERT

### kintone_cache テーブル

```sql
CREATE TABLE kintone_cache (
  id           TEXT PRIMARY KEY,  -- "{appId}:{recordId}"
  app_id       TEXT NOT NULL,
  record_id    TEXT NOT NULL,
  company_name TEXT,              -- 企業名フィールドの値
  data         JSONB,             -- 全フィールドのフラット化データ
  synced_at    TIMESTAMPTZ
);
```

### API エンドポイント

| メソッド | パス | 説明 |
|---------|-----|------|
| GET | `/api/kintone/search?q=xxx` | 企業名部分一致検索（最大10件） |
| GET | `/api/kintone/record/:recordId` | レコード詳細取得 |
| POST | `/api/kintone/sync` | 手動同期（admin のみ） |
| GET | `/api/kintone/status` | 同期状態確認 |

### RPO との連携

- 案件作成モーダルで企業名を入力すると debounce 経由で `GET /api/kintone/search` を呼び出しサジェスト表示
- サジェストから選択すると:
  - `支払方式` → `plan` にマッピング（月額 → `monthly`、保証/成功 → `guarantee`）
  - 受注金額・採用目標を `projectInfo` にセット
  - kintone レコード参照情報を `data.kintone` に保存
- 案件詳細の「↻ kintoneと再同期」ボタンで随時更新可能

---

## 10. Google Drive 連携

`src/features/drive-api.js` で実装。

### 認証

- サービスアカウント認証（`drive-service-account.json`）
- スコープ: `https://www.googleapis.com/auth/drive.readonly`

### フォルダ構造の前提

```
共有ドライブ（rootFolderId）
├── _あ行/
│   ├── 株式会社AAAA/
│   └── 株式会社BBBB/
├── _か行/
│   └── 株式会社CCCC/
└── ...
```

- ルート直下に「行別」サブフォルダ（行フォルダ）がある
- 行フォルダの中に企業フォルダがある（2階層構造）

### フォルダ検索

**`findClientFolder(parentFolderId, clientName)`**:
1. ルートフォルダのメタ情報（`driveId`）と行フォルダ一覧を取得
2. 全行フォルダを並列スキャンして企業名を検索
3. 完全一致 > 前方一致の優先順位で返す

**`searchClientFolders(parentFolderId, query, limit=8)`**:
- `findClientFolder` の複数結果版（最大 `limit` 件）
- 完全一致 → 前方一致 → 部分一致の順でソートして返す
- 案件作成時のフォルダ候補サジェストに使用

### 管理シート検索

**`findManagementSheet(folderId)`**:
- 企業フォルダ内で `mimeType='application/vnd.google-apps.spreadsheet'` かつ `name contains '管理シート'` を検索
- `__管理シート` を含むファイルを優先

### API エンドポイント

| メソッド | パス | 説明 |
|---------|-----|------|
| GET | `/api/drive/files?folderId=xxx` | フォルダ内ファイル一覧（アイコン付き） |
| GET | `/api/rpo/drive/candidates?name=xxx` | フォルダ候補検索（RPO 用） |

### RPO 設定（driveFolderUrl）

- `rpo_settings` テーブルの `driveFolderUrl` キーで親フォルダ URL/ID を保存
- Admin 画面の RPO 設定から変更可能
- 案件作成・詳細で Drive 連携が有効になる

### Google Sheets 連携

- `data.sheetsUrl` に管理シートの URL を保存
- Apps Script URL（`rpo_settings` の `appsScriptUrl`）が設定されている場合、Apps Script 経由でシートデータを取得
- 直接の Sheets API は使用せず、Apps Script を仲介

---

## 11. 管理者機能（Admin）

`web/src/pages/admin/` 配下の各ページ。`dashboard_roles.role = 'admin'` のユーザーのみアクセス可能。

### 管理画面一覧

| パス | ページ | 説明 |
|-----|--------|------|
| `/admin/roles` | RolesAdmin | グローバルロール管理（admin/manager/member 付与） |
| `/admin/teams` | TeamsAdmin | dash_teams の管理 |
| `/admin/projects` | ProjectsAdmin | プロジェクト管理 |
| `/admin/permissions` | PermissionsAdmin | 権限設定 |
| `/admin/user-mapping` | UserMappingAdmin | Slack ユーザーとダッシュボードユーザーのマッピング |
| `/admin/integrations` | IntegrationsAdmin | 外部連携設定（integrations テーブル） |
| `/admin/formulas` | FormulasAdmin | 数式設定（RPO 売上計算など） |
| `/admin/rpo` | RpoAdmin | RPO 設定（Drive URL, Apps Script URL, HR 部署フラグ, 媒体マスタ） |

### RPO Admin（RpoAdmin）

- **Drive フォルダ URL 設定**: 企業フォルダの親フォルダ URL を入力
- **Apps Script URL 設定**: Google Sheets データ取得用の GAS URL
- **HR 部署フラグ**: 各チームを「HR 部署」としてマークする（RPO アクセス権の付与）
- **媒体マスタ管理**: `rpo_media_masters` テーブルの CRUD（媒体名の追加・削除）

### Roles Admin（RolesAdmin）

- ユーザー一覧を表示
- 各ユーザーに `admin` / `manager` / `member` ロールを付与
- `dashboard_roles` テーブルを更新

---

## 12. データベーススキーマ

### コアテーブル

| テーブル | 説明 |
|---------|------|
| `tasks` | タスク本体（id, title, description, assignee_id, due_date, status, task_type） |
| `task_targets` | broadcast タスクの対象ユーザー（task_id::uuid, user_id） |
| `projects` | プロジェクト |
| `project_tasks` | タスクとプロジェクトの多対多 |

### ユーザー・チーム

| テーブル | 説明 |
|---------|------|
| `user_departments` | ユーザーの部署情報（旧フィールド型、現在は dash_teams に移行） |
| `dashboard_roles` | グローバルロール（team_id, user_id, role） |
| `dash_teams` | チーム・部署定義（id, team_id, name, parent_id, is_hr_dept） |
| `dash_team_members` | チームメンバー（dash_team_id, team_id, user_id, role） |
| `dashboard_user_directory` | Slack ユーザーのプロフィールキャッシュ |
| `dashboard_user_visibility` | ユーザーごとの閲覧可能ユーザー設定 |
| `dashboard_team_visibility` | ユーザーごとの閲覧可能チーム設定 |

### セッション・設定

| テーブル | 説明 |
|---------|------|
| `user_sessions` | Web ダッシュボードのセッション（session_id, team_id, user_id） |
| `team_settings` | チーム設定（JSONB） |
| `user_settings` | ユーザー設定（JSONB）。ガントカラー・デフォルトフィルタなど |
| `notification_threads` | 通知スレッドの親 ts（重複投稿防止） |

### 業務ガント

| テーブル | 説明 |
|---------|------|
| `workload_items` | 業務アイテム（繰り返しパターン含む、rpo_client_id で案件紐付け） |
| `workload_cells` | 日別塗りデータ |
| `workload_categories` | カテゴリ（名前・色） |

### 個人フィルタ

| テーブル | 説明 |
|---------|------|
| `personal_filters` | フィルタ定義（owner_user_id が所有） |
| `personal_filter_members` | フィルタに含まれるユーザーID |

### RPO

| テーブル | 説明 |
|---------|------|
| `rpo_clients` | 案件（data JSONB で全付帯情報） |
| `rpo_task_templates` | タスクテンプレート |
| `rpo_applicants` | 応募者 |
| `rpo_applicant_actions` | 応募者アクション履歴 |
| `rpo_media_sources` | 媒体連携設定（webhook_key で識別） |
| `rpo_media_masters` | 媒体マスタ（名前の正規化用） |
| `rpo_settings` | RPO 設定（key-value: driveFolderUrl, appsScriptUrl） |

### kintone

| テーブル | 説明 |
|---------|------|
| `kintone_cache` | kintone レコードのキャッシュ（company_name で検索） |
| `kintone_sync_log` | 同期ログ |

### 外部連携

| テーブル | 説明 |
|---------|------|
| `integrations` | 外部サービス連携設定 |
| `integration_field_mappings` | フィールドマッピング定義 |
| `integration_sync_log` | 連携同期ログ |

---

## 13. デプロイ・インフラ

### サーバー

- AWS Lightsail: 2 GB RAM / 2 vCPU / 60 GB SSD
- SSH エイリアス: `slack-task`
- 実行ディレクトリ: `~/slack-task-demo/`
- pm2 アプリ名: `slack-task`

### デプロイ手順

```bash
# ローカルからサーバーへ転送
scp src/features/home.js slack-task:~/slack-task-demo/src/features/
scp index.js slack-task:~/slack-task-demo/

# サーバーで再起動
ssh slack-task "cd ~/slack-task-demo && pm2 restart slack-task"

# ログ確認
ssh slack-task "pm2 logs slack-task --lines 50"
```

### Swap 設定（OOM 対策）

2 GB RAM のため、スワップ 2 GB を設定済み：

```bash
sudo fallocate -l 2G /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# /etc/fstab に追記済み（永続化）
```

### 環境変数

| 変数 | 説明 |
|------|------|
| `DATABASE_URL` | PostgreSQL 接続文字列 |
| `SLACK_BOT_TOKEN` | Slack Bot トークン |
| `SLACK_SIGNING_SECRET` | Slack 署名シークレット |
| `SLACK_APP_TOKEN` | Socket Mode 用アプリトークン |
| `KINTONE_DOMAIN` | kintone ドメイン |
| `KINTONE_APP_102_TOKEN` | kintone アプリ 102 の API トークン |
| `PGSSL` | `true` で SSL 接続 |
| `DB_POOL_SIZE` | DB 接続プールサイズ（デフォルト 20） |

### フロントエンドビルド

```bash
cd web && npm run build
# dist/ が Express の静的ファイルとして配信される
```

### DB 接続管理

- `pg.Pool` で最大 20 接続（`DB_POOL_SIZE` で変更可）
- 接続タイムアウト: 3 秒、アイドルタイムアウト: 30 秒
- 一時的な接続エラー（ECONNRESET, ETIMEDOUT など）は自動リトライ（最大 2 回、150ms 間隔）

---

*このドキュメントはソースコードから自動抽出した内容をもとに作成されています。*
