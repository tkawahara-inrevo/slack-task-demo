# システム設計図

## 1. 部署別業務フロー

```mermaid
flowchart LR
  subgraph MKT["マーケティング"]
    A1[リード獲得\n流入経路管理]
  end

  subgraph SALES["営業"]
    B1[商談作成\n顧客情報登録]
    B2[ヨミ管理\nE→D→C→B→A→S]
    B3{受注?}
    B4[失注記録]
  end

  subgraph HR["HR / RPO管理"]
    C1[RPO案件\n自動生成]
    C2[タスク管理]
    C3[応募者管理]
    C4[KPI・歩留まり]
    C5[媒体・予算管理]
  end

  subgraph FIN["経理"]
    D1[請求書管理]
    D2[入金管理]
  end

  A1 -->|リードを商談化| B1
  B1 --> B2
  B2 --> B3
  B3 -->|Yes 受注| C1
  B3 -->|No| B4
  C1 --> C2
  C1 --> C3
  C1 --> C4
  C1 --> C5
  C1 -->|契約情報連携| D1
  D1 --> D2
```

---

## 2. データ構造（ER図）

```mermaid
erDiagram
  customers {
    text id PK
    text team_id
    text name
    text industry
    text prefecture
    jsonb data
  }

  deals {
    text id PK
    text team_id
    text customer_id FK
    text yomi
    text contract_type
    text payment_type
    text sales_user_id FK
    text na_user_id FK
    int initial_fee
    int monthly_fee
    date contract_start
    date contract_end
    text status
    text lost_reason
    jsonb data
  }

  rpo_projects {
    text id PK
    text team_id
    text deal_id FK
    text customer_id FK
    text name
    text color
    text status
    text dash_team_id FK
    jsonb data
  }

  payments {
    text id PK
    text team_id
    text deal_id FK
    text customer_id FK
    text type
    int amount
    date scheduled_date
    date paid_date
    bool invoice_issued
  }

  dash_users {
    text id PK
    text team_id
    text name
    text email
  }

  customers ||--o{ deals : "1顧客 N商談"
  deals ||--o{ rpo_projects : "受注でRPO案件生成"
  customers ||--o{ rpo_projects : "顧客から直参照"
  deals ||--o{ payments : "1商談 N請求"
  dash_users ||--o{ deals : "担当営業"
```

---

## 3. 画面構成

```
新システム
├── ダッシュボード（既存）
│
├── 顧客管理（新規）
│   ├── 顧客一覧
│   │   ├── 会社名・業界・担当営業・最新ヨミ・案件数
│   │   └── 新規顧客追加
│   └── 顧客詳細
│       ├── 会社情報タブ（基本情報・担当者）
│       ├── 商談タブ（ヨミ一覧・商談詳細）
│       └── RPO案件タブ（案件①②③ をタブ切替）
│           └── ← 現在の ClientDetail がここに入る
│
├── 営業パイプライン（新規）
│   ├── カンバン形式（ヨミ別カラム）
│   └── 一覧表示
│
├── RPO管理（既存を統合）
│   ├── マイタスク
│   ├── サマリー
│   ├── 業務ガント
│   └── ワークロード
│
└── 入金管理（新規）
    ├── 請求一覧
    └── 入金状況
```

---

## 4. 画面遷移図

```mermaid
flowchart TD
  TOP[ダッシュボード]

  TOP --> CL[顧客一覧]
  TOP --> PL[営業パイプライン\nカンバン]
  TOP --> RPO[RPO管理\nマイタスク/サマリー等]
  TOP --> PAY[入金管理]

  CL --> CD[顧客詳細]
  CD --> CD_INFO[会社情報タブ]
  CD --> CD_DEALS[商談タブ]
  CD --> CD_RPO[RPO案件タブ\n案件①②③]

  CD_DEALS --> DEAL[商談詳細\nヨミ・費用・BANT等]
  DEAL -->|受注| RPO_AUTO[RPO案件\n自動生成]
  RPO_AUTO --> CD_RPO

  CD_RPO --> PROJ[案件詳細\n現ClientDetail]
  PROJ --> DASH_TAB[ダッシュボードタブ]
  PROJ --> KPI_TAB[KPIタブ]
  PROJ --> MEDIA_TAB[媒体・予算管理タブ]
  PROJ --> APP_TAB[応募者タブ]
  PROJ --> TASK_TAB[タスクタブ]
  PROJ --> FUNNEL_TAB[歩留まりタブ]

  PL --> CD
```

---

## 5. 受注→RPO案件生成フロー

```mermaid
sequenceDiagram
  participant S as 営業担当
  participant SYS as システム
  participant HR as HR担当

  S->>SYS: 商談のヨミを「受注」に変更
  SYS->>SYS: RPO案件を自動生成\n（顧客名・契約形態・担当者を引き継ぐ）
  SYS->>HR: 通知（Slack or 画面バッジ）
  HR->>SYS: 案件詳細を開いてHR管理開始
  SYS->>SYS: 入金スケジュールを\n自動作成（オプション）
```

---

## 6. 移行方針（既存データ）

| 現在 | 移行後 |
|---|---|
| `rpo_clients` 1件 | `customers` 1件 + `rpo_projects` 1件 |
| `rpo_clients.data.projectInfo` | `deals` テーブルに分離 |
| 担当者（文字列） | `dash_users` のIDに紐付け |
| kintone同期 | 移行後は廃止 / 手動移行 |

---

## 7. 実装フェーズ案

| フェーズ | 内容 | 優先度 |
|---|---|---|
| Phase 1 | 顧客マスタ + 商談管理 + ヨミ管理 | 🔴 高 |
| Phase 2 | 受注→RPO案件自動生成 + 既存RPO統合 | 🔴 高 |
| Phase 3 | 入金管理 | 🟡 中 |
| Phase 4 | リード管理（App94相当） | 🟢 低 |
| Phase 5 | kintoneからのデータ移行スクリプト | 🟡 中 |
