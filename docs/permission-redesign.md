# 権限・組織モデル 再設計仕様書

最終更新: 2026-06-20
ステータス: **設計完了・実装未着手**

---

## 1. 背景・目的

現状の権限システムは以下が混在し、メンテナンス性が低い：

- ロール判定が4段階（DB明示 / チーム名 ILIKE / 親チーム名 ILIKE / Slackタイトル推定）
- 暗黙ルール: `dash_teams.name ILIKE '%IT%' OR '%情シス%'` → `it` ロール自動付与など
- 未定義テーブル（`crm_bc_managers` / `daily_report_members` / `feature_permissions`）が参照される
- 環境変数にハードコードされたユーザーID・チャンネルID・usergroup ID
- 粗粒度（adminOnly 等）と細粒度（feature_access）の二重管理

**ゴール**: admin が UI 上から組織・役職・権限のすべてを管理できる状態。

---

## 2. 組織構造の前提

### 階層
```
CEO (固定の最上位 org_unit)
├─ Marketing 事業部
│   ├─ MK 部署
│   ├─ BC 部署
│   ├─ CR 部署
│   └─ ... (フェーズごとの部署)
├─ HR 事業部
│   ├─ Bizops (部署)
│   ├─ CR (部署)
│   ├─ DR (部署)
│   ├─ CS (部署)
│   └─ OP (部署)
└─ HR チーム群（別ツリー・マトリクス組織）
    ├─ 土井チーム (DR×2 + CS×数名)
    ├─ ○○チーム
    └─ ... (×5チーム程度)
```

### HR のマトリクス組織
HR 部署では「部署」と「チーム」が **直交する2軸**：
- **部署（縦軸・機能）**: Bizops / CR / DR / CS / OP
- **チーム（横軸・オペレーション単位）**: 土井チーム / ○○チーム

1人が両方に所属する（兼務モデルで表現）：
```
山田さん (CS担当・土井チーム所属):
  assignment 1: org_unit=CS部署,    position=メンバー,        is_primary=true
  assignment 2: org_unit=土井チーム, position=メンバー,        is_primary=false

土井さん (DR担当・土井チームのリーダー):
  assignment 1: org_unit=DR部署,    position=メンバー,        is_primary=true
  assignment 2: org_unit=土井チーム, position=マネージャー,    is_primary=false
                                    ↑ チームのマネジメント層
```

---

## 3. データモデル

### 3.1 `org_units` - 組織ツリー

```sql
CREATE TABLE org_units (
  id            SERIAL PRIMARY KEY,
  team_id       TEXT NOT NULL,
  name          TEXT NOT NULL,
  type          TEXT NOT NULL CHECK (type IN ('ceo','division','dept','team')),
  parent_id     INTEGER REFERENCES org_units(id),
  sort_order    INTEGER DEFAULT 0,
  is_active     BOOLEAN DEFAULT true,
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_org_units_parent ON org_units(parent_id);
```

- `type='ceo'` の unit はシステム固定（削除不可）
- HRチーム群は別ツリーとして配置（`parent_id` を別 root か HR事業部直下）
- 階層深さは parent_id で再帰、上限なし

### 3.2 `positions` - 役職マスター（全社共通）

```sql
CREATE TABLE positions (
  id          SERIAL PRIMARY KEY,
  team_id     TEXT NOT NULL,
  name        TEXT NOT NULL,
  level       INTEGER NOT NULL,
  sort_order  INTEGER DEFAULT 0,
  is_active   BOOLEAN DEFAULT true
);
```

#### 初期シード
| id | name | level |
|---|---|---|
| 1 | メンバー | 1 |
| 2 | リード | 2 |
| 3 | チーフ | 3 |
| 4 | マネージャー | 4 |
| 5 | エキスパート | 4 |
| 6 | ディレクター | 5 |
| 7 | 取締役 | 6 |
| 8 | CTO | 7 |
| 9 | CEO | 8 |

- 各 level に 1〜4 のサブ階層が運用上存在するが、権限差なしのため master では集約
- `level` 比較で「マネージャー以上(level>=4)」のような表現が可能

### 3.3 `user_assignments` - 所属（履歴付き・兼務対応）

```sql
CREATE TABLE user_assignments (
  id              SERIAL PRIMARY KEY,
  team_id         TEXT NOT NULL,
  user_id         TEXT NOT NULL,
  org_unit_id     INTEGER NOT NULL REFERENCES org_units(id),
  position_id     INTEGER NOT NULL REFERENCES positions(id),
  is_primary      BOOLEAN DEFAULT false,
  effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
  effective_to    DATE,  -- NULL = 現在も有効
  created_at      TIMESTAMPTZ DEFAULT now(),
  created_by      TEXT
);
CREATE INDEX idx_user_assignments_user ON user_assignments(user_id);
CREATE INDEX idx_user_assignments_org ON user_assignments(org_unit_id);
CREATE INDEX idx_user_assignments_active ON user_assignments(user_id) WHERE effective_to IS NULL;
```

#### 異動・昇降格の運用
- **異動**: 新所属を `INSERT` → 兼務状態 → 旧所属に `effective_to=CURRENT_DATE` セット
- **昇格**: 旧 assignment に `effective_to` セット → 同 org_unit_id で新 position_id で `INSERT`
- 履歴がすべて残るので人事監査対応可

### 3.4 `permission_grants` - 権限グラント（hybrid）

```sql
CREATE TABLE permission_grants (
  id             SERIAL PRIMARY KEY,
  team_id        TEXT NOT NULL,
  feature_key    TEXT NOT NULL,
  subject_type   TEXT NOT NULL CHECK (subject_type IN ('user','org_unit','position','composite')),
  subject_id     INTEGER,         -- subject_type が user/org_unit/position の場合
  composite_json JSONB,           -- subject_type='composite' の場合
  effect         TEXT NOT NULL CHECK (effect IN ('allow','deny')),
  granted_by     TEXT,
  granted_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_permission_grants_feature ON permission_grants(team_id, feature_key);
```

#### subject_type の使い分け
- `user`: 個別ユーザー（subject_id = ユーザーID）
- `org_unit`: 組織単位（subject_id = org_units.id）
- `position`: 役職（subject_id = positions.id、グローバル）
- `composite`: 組み合わせ（composite_json で柔軟に表現）
  - 例: `{"org_unit_id": 5, "position_min_level": 4}` = 「営業部のマネージャー以上」

#### 解決アルゴリズム（hybrid: 継承 + deny override）
```
function hasPermission(user, feature_key):
  grants = collect_all_grants(user, feature_key)
    - user 直接
    - 所属 org_unit + 全祖先（継承）
    - 役職 + 上位役職（level >= で継承）
    - composite で該当するもの

  if any grant.effect == 'deny':
    return false  // deny がひとつでもあれば即拒否
  if any grant.effect == 'allow':
    return true
  return false  // デフォルト拒否
```

### 3.5 `visibility_overrides` - 可視範囲の例外

```sql
CREATE TABLE visibility_overrides (
  id              SERIAL PRIMARY KEY,
  team_id         TEXT NOT NULL,
  viewer_user_id  TEXT NOT NULL,
  target_type     TEXT NOT NULL CHECK (target_type IN ('user','org_unit')),
  target_id       INTEGER NOT NULL,
  effect          TEXT NOT NULL CHECK (effect IN ('include','exclude')),
  granted_by      TEXT,
  granted_at      TIMESTAMPTZ DEFAULT now()
);
```

可視範囲のデフォルトは下記ルールで決定し、`visibility_overrides` で個別に追加/除外する。

---

## 4. 可視範囲のデフォルトルール（level ベース）

| Position level | 自動的に見える範囲 |
|---|---|
| 1〜3（メンバー〜チーフ） | 自分のデータ + 自所属 org_unit のメンバー |
| 4（マネージャー/エキスパート） | 自所属 org_unit の subtree 全員（HRチームリーダーも同様にチーム全員） |
| 5（ディレクター） | 自事業部 subtree 全員 |
| 6〜7（取締役/CTO） | 全社 |
| 8（CEO） | 全社（admin 同等） |

`visibility_overrides` で：
- `include`: デフォルトでは見えない人を見えるようにする
- `exclude`: デフォルトでは見える人を隠す

---

## 5. 編集権限（UI 制御）

新しい feature_keys：

| feature_key | 制御対象 |
|---|---|
| `org.unit.edit` | 部署/チーム/事業部の CRUD |
| `org.member.assign` | メンバー所属変更・異動・兼任追加・昇降格 |
| `org.position.edit` | 役職マスター CRUD |
| `org.permission.edit` | 権限グラント・可視範囲の設定（最強・誤設定リスクあり） |

### 初期 seed
- admin ロール（または CEO position level=8）に全付与
- 人事 / 総務などへの委譲は UI から `permission_grants` で個別付与

---

## 6. UI 画面構成

| パス | 画面名 | 機能 |
|---|---|---|
| `/admin/org-chart` | 組織図 | ツリー表示。DnD で並び替え・組織移動 |
| `/admin/org-units` | 部署/チーム管理 | CRUD（論理削除）・改名 |
| `/admin/positions` | 役職マスター | CRUD・level 設定・並べ替え |
| `/admin/members` | メンバー管理 | 一覧 → 行クリック → 所属/異動/兼任/昇降格モーダル |
| `/admin/permissions` | 権限マトリクス | 機能 × 部署 × 役職 のチェックボックス UI |
| `/admin/permissions/check` | 権限シミュレート | 「この人は何ができる？」「このデータは誰が見える？」テスト画面 |

### View-As（なりきり）は継続
- 既存の admin 限定 View-As 機能は **存続**（権限変更後のテスト用途）
- ただし Cookie ベースのセキュリティ穴を塞ぐ（DB 管理 + admin 限定チェック強化）

---

## 7. 移行ステップ（本番影響最小）

| Phase | 内容 | 本番影響 |
|---|---|---|
| **1** | 新テーブル追加（`org_units` / `positions` / `user_assignments` / `permission_grants` / `visibility_overrides`） | ゼロ |
| **2** | 既存データ移行スクリプト: `dash_teams` / `dash_team_members` → `org_units` / `user_assignments` | ゼロ（コピーのみ） |
| **3** | UI 画面追加（読み取り専用モード） | ゼロ |
| **4** | UI 編集可能化。新ロジックは新テーブル参照、旧テーブル fallback | 低 |
| **5** | 1〜2週間の並行運用・ログ比較 | 低 |
| **6** | チーム名 ILIKE 暗黙判定（`%IT%`/`%人事%`/`%corporate%`）廃止 | 中（事前検証必須） |
| **7** | 役職タイトル推定（Slack title から `manager`/`chief`/`lead`/`member`）廃止 | 中 |
| **8** | 旧テーブル削除 / 環境変数のハードコード ID を DB へ移行 | ゼロ |

### 安全策
- 各 Phase は独立してデプロイ可能
- 新ロジックは fallback で旧ロジックに戻れる構造に
- 本番のステージング環境がないため、深夜・週末作業 + 翌朝ログ確認

---

## 8. 未定義テーブルの整理

設計時に判明した「参照されているが定義がない」テーブルは Phase 1 で正式定義：

| テーブル | 現状 | 対応 |
|---|---|---|
| `crm_bc_managers` | 参照あり、定義なし | 削除 or 新モデルの `position='BCマネージャー'` で表現 |
| `daily_report_members` | 参照あり、定義なし | 正式マイグレーション追加 |
| `feature_permissions` | 参照あり、定義なし | `permission_grants` に統合（または削除） |

---

## 9. 環境変数 → DB 移行候補

Phase 8 で以下を DB 管理に移行：

| 現状の .env | 移行先テーブル（案） |
|---|---|
| `DASHBOARD_ADMIN_USER_ID`（初期 admin） | `permission_grants` で `org.permission.edit` 付与 |
| `CORP_SOUMU_USERGROUP_ID` / `CORP_SYSTEM_USERGROUP_ID` | `org_units` の Slack usergroup 連携カラム |
| `BC_CONTRACT_ASSIGNEE_USER_ID_*` | role-to-user マッピングテーブル |
| `RECRUIT_NOTIFY_HANDLER_MAP`（板金:U08K...） | キーワード×担当者マッピングテーブル |
| `DEPT_ALL_HANDLES` / `DEPT_PRIORITY` | `org_units` の Slack 連携カラム |
| ハードコードされた除外ユーザーID | 既存の `daily_report_members.is_target=false` |

`.env` に残すのは **インフラ・秘匿のみ**：
- Slack/HRMOS/kintone/Anthropic API トークン
- DB 接続情報
- URL / PORT / NODE_ENV

---

## 10. 未決事項・将来検討

- 役職マスターは全社共通でスタート。部署別カスタム役職が必要になったらカラム追加で対応
- 権限グラントの「composite」をどこまで複雑に許容するか（運用見ながら拡張）
- 監査ログ（誰がいつ権限変更したか）の保持期間とエクスポート機能
- 退職者の `effective_to` セット運用フロー
- 組織変更前のドラフト機能（4月の組織改編時に予約適用）
