# CRM ダッシュボード 仕様書

## 概要
案件管理 > ダッシュボードタブ。CRM業務のKPI・収支・担当者実績をリアルタイムで把握するための管理画面。

---

## フィルター

| 要素 | 内容 |
|------|------|
| 期間 | **指定月**（デフォルト）/ **今期** のトグル切り替え |
| 対象月 | 月ピッカー（今期選択時はグレーアウト・無効） |
| 担当者 | 固定6名＋全員 のドロップダウン（選択すると担当者別実績テーブルも1行に絞り込み） |
| kintoneデータ取得 | 手動同期ボタン（進捗バー付き）。**kintone移行完了後に削除予定** |

---

## KPIカード（5枚）

| カード | 計算元 | 比較 |
|--------|--------|------|
| 入金額 | `SUM(kintone_payments.amount)` | 前月比 / 前期比 |
| 受注件数 | `deals WHERE status='won' AND order_date IN 期間` | 同上 |
| 初回商談数 | `deals.first_meeting_date IN 期間` | 同上 |
| 受注率 | 受注件数 ÷ 初回商談数 | 同上 |
| KPI達成率 | インセン合計 ÷ KPI目標（詳細後述） | — |

### KPI達成率の計算

- **分子**：`SUM(kintone_payments.incentive_amount)` （インセン合計）
- **分母**：担当者役職別目標の合計（`crm_role_targets × termMonths`）
  - 今期モード：月次目標 × 今期月数
  - 指定月モード：月次目標のみ
  - 目標未設定時フォールバック：パイプラインの calcKpi() 合計
- カード下部に「インセン XXX万 / 目標 XXX万（入金 XXX万）」を細字表示

---

## メインレイアウト（2カラム 5:5）

### 左カラム

#### 担当者別実績テーブル

固定5名 `['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘']` ＋ その他合算。

| 列 | 内容 | 備考 |
|----|------|------|
| 担当者 | 姓・名・カラーアバター | — |
| 入金額 | `SUM(kintone_payments.amount)` | — |
| インセン | `SUM(kintone_payments.incentive_amount)` | クリックで入金内訳ドリルダウン |
| 受注 | `deals` 受注件数 | クリックで受注案件ドリルダウン |
| 初回商談 | `deals.first_meeting_date` 件数 | — |
| 受注率 | 受注 ÷ 初回商談 | — |
| 達成率 | インセン ÷ 担当者月次目標 | 役職から自動取得（後述） |

**達成率の目標取得フロー：**
1. `crm_rep_roles` に手動設定あり → その値を優先
2. なし → Slack `dashboard_user_directory` の `profile_json.title` から `inferRoleFromTitle()` で役職を推定
3. 役職名で `crm_role_targets.monthly_target` を参照

達成率セルに推定役職名を細字で表示。

#### アラート

常時表示（件数0の場合はグリーン「なし」バッジ）。展開/折りたたみ可。

| 種別 | 条件 |
|------|------|
| アクション期限切れ | `deals.next_action_date < today AND status='active'` |
| 停滞中案件 | `updated_at < 14日前 AND status='active' AND yomi NOT IN ('アポ化前','受注','失注')` |

担当者フィルターの影響を受けない（常に全担当者対象）。

---

### 右カラム

#### 指定月モード

1. **収支見込み**
   - 対象月の収支見込みを3段階で表示
   - 見込み合計（実入金ベース）と KPI達成見込み%（インセンベース）を上部に表示

   | 行 | メイン（大） | サブ（細字） | 件数 |
   |----|------------|------------|------|
   | 入金確定 | `SUM(kintone_payments.amount)` | インセン `SUM(incentive_amount)` | kintone_payments 件数 |
   | 締結ほぼ確実 [A/S] | 売上見込 `(monthly_fee\|initial_fee) × 1.1` | インセン見込 `calcKpi()` | A/S案件数 |
   | 締結多分いける [B/C] | 売上見込 `(monthly_fee\|initial_fee) × 1.1` | インセン見込 `calcKpi()` | B/C案件数 |

   **KPI達成見込み% = (confirmedIncentive + highKpi + mediumKpi) ÷ kpiDenom**

   calcKpi() の計算式：
   - 採用保証: `(monthly_fee \| initial_fee) × 1.1 × 0.6`
   - 月額: `unit_price × 1.1 + initial_fee × 1.1`

2. **入金推移（実入金額）** — 過去6ヶ月の `SUM(amount)` 月別棒グラフ
3. **受注プラン割合** — `kintone_payments` の plan 別ドーナツグラフ（`COUNT(DISTINCT company)` で1顧客=1件）。金額/件数トグル切り替え可
4. ~~パイプラインファネル~~（削除済み）

#### 今期モード

指定月との差分：

| セクション | 今期での扱い |
|-----------|------------|
| 収支見込み | **非表示** |
| 今期KPI達成状況 | 新規表示（代替）|
| 入金推移 | 今期全月分（最大12ヶ月）に拡張 |

**今期KPI達成状況カード：**
- インセン合計 ÷ (月次目標 × 今期月数) = 達成率%
- 太い進捗バー（色は達成度で変化）
- 前期入金額・前期比・前期達成率を表示

---

## ドリルダウン

担当者行のインセン列・受注列をクリックすると詳細モーダルを表示。

| タイプ | 表示内容 |
|--------|---------|
| 入金内訳 | 入金日・会社名・プランバッジ・インセン額。ヘッダーに合計表示 |
| 受注案件 | 受注日・顧客名・受注バッジ |

---

## 設定タブ（案件管理 > 設定）

### 集計期間
`crm_period_settings` テーブルで前期・今期の開始/終了日を管理。

### 役職別月次目標
`crm_role_targets` テーブル。役職名と月次目標額（円）。
現在の登録値：役職無し 400万 / Lead 500万 / Sub Chief 600万 / Chief 700万 / Sub Expert 900万 / Expert 1,100万

成績ページの昇降格ライン計算にも共用。

### 担当者別 役職・KPI目標
`crm_rep_roles` テーブル。

- **役職**: 通常は Slack プロフィールから自動取得（手動設定で上書き可）
- **手動上書き**: 例外的に個人目標を設定したい場合のみ入力（万円単位）
- 画面下部にチーム月次目標合計をリアルタイム表示

---

## データソース一覧

| 指標 | テーブル | 集計キー |
|------|---------|---------|
| 入金額 | `kintone_payments.amount` | `payment_date` |
| インセン | `kintone_payments.incentive_amount` | `payment_date` |
| 受注件数 | `deals` WHERE `status='won'` | `order_date` |
| 初回商談数 | `deals.first_meeting_date` | — |
| A/S見込み | `deals` WHERE `yomi IN ('A 70%','S 90%')` | 全進行中案件 |
| B/C見込み | `deals` WHERE `yomi IN ('B 50%','C 30%')` | 全進行中案件 |
| 役職情報 | `dashboard_user_directory.profile_json.title` | Slack プロフィール自動推定 |

---

## kintone同期

- App102（案件）→ `kintone_cache` → `deals` へマッピング（30分自動同期）
- App170（入金管理）→ `kintone_payments`（フィールド: `数値`=amount, `数値_0`=incentive_amount）
- 手動同期: ダッシュボード右上「kintoneデータ取得」ボタン（完了後にダッシュボード自動リロード）

**このシステムがkintoneに代わるデータマスターとなる予定（移行中）**
