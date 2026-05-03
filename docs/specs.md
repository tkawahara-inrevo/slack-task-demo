# TaskHub 詳細仕様

## CRMダッシュボード

**場所**: CRM > ダッシュボードタブ（デフォルト表示）

### 期間切り替え
- 指定月（デフォルト: 今月）/ 今期
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

## kintone連携 詳細

### App102（案件情報）
- フィールド: 全フィールド取得（fields: [] で制限なし）
- 同期先: `kintone_cache` テーブル（data JSONB に全フィールド）
- deals へのマッピング: `kintone-api.js` の `KINTONE_DEAL_FIELD_MAP`
  - `初回商談日_コンサルチーム` → `first_meeting_date`
  - `受注日` → `order_date`
  - `ヨミ` → `yomi` 等

### App170（入金管理）
- フィールド: company, 数値, 数値_0, date, plan, Staff
- 同期先: `kintone_payments` テーブル
- `incentive_amount` が入金確定額（ダッシュボード・月次収支見込みに使用）

---

## 採用管理 詳細

| 設定 | 値 |
|------|-----|
| テンプレートスプシID | `1Jq-6I_276W-e6J91X544wY6d9kIi6JnyPh6I8UKR95k` |
| GAS URL | DB の `recruitment_settings.gas_endpoint_url` を参照 |
| 採点方式 | 10点満点（Q1〜10各1点）、Q13タイピング参考、Q14人事確認 |
| 候補者スプシ | `recruitment_settings.import_sheet_url` で固定URL管理 |

---

## 未確認・要ヒアリング

- BCの長期化案件ステータスの扱い（BC担当者と要相談）
- カルテの具体的な項目・フォーマット
- 請求書・契約書の発行フロー詳細（総務ヒアリング必要）
- 反社チェックツールの切り替え状況（ジーサーチ→他社、現在ステイ）
- 広告指標データのソース（どのスプシ・ツール）
- DRへの引き継ぎ時に渡す具体的な情報
