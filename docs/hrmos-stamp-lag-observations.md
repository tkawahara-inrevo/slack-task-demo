# HRMOS daily API 反映ラグ実測ログ

## 概要

奥谷さんの「手動打刻が bot に上書きされた」案件を受けて、
HRMOS の `work_outputs/daily/{date}` API が打刻実態をどれだけ早く反映するかを実測。

**結論**: HRMOS daily API は **数十分ラグのバッチ更新型**。
60秒2段階チェックでもカバー不能。

---

## 実装

- 30秒毎に HRMOS `work_outputs/daily/2026-06-26` をフェッチ
- 初回観測した時刻と打刻時刻の差を記録
- テーブル: `hrmos_stamp_observations`
- ファイル: [src/features/hrmos-stamp-poller.js](../src/features/hrmos-stamp-poller.js)

---

## 観測データ（2026-06-26 出勤打刻分）

| 打刻時刻 | HRMOS user_id | 名前 | 初回観測時刻 | ラグ(秒) | ラグ(分) |
|---|---|---|---|---|---|
| 07:01 | 113 | 外村 彩乃/Ayano Tonomura | 07:02:28 | 88 | **1.5** |
| 07:04 | 28 | 山本 夏乃 / Kano Yamamoto | 07:05:28 | 88 | **1.5** |
| 07:07 | 95 | 山本 真人/Shinto Yamamoto | 07:07:58 | 58 | **1.0** |
| 07:44 | 53 | 齋藤 優/Yu saito | 07:45:29 | 89 | **1.5** |
| 07:55 | 72 | 川渕 陽子/Yoko Kawabuchi | 08:01:58 | 418 | **7.0** |
| 07:56 | 103 | 志摩 啓一朗/Keiichiro Shima | 08:05:29 | 569 | **9.5** |
| 07:59 | 71 | 渡辺 郁美/Ikumi Watanabe | 08:09:58 | 658 | **11.0** |
| 07:59 | 39 | 束田 拓夢/Takumu Tsukada | 08:10:58 | 718 | **12.0** |
| 08:00 | 97 | 八木橋亜美/Ami Yagihasi | 08:11:28 | 688 | **11.5** |
| 08:01 | 104 | 平野 将/Sho Hirano | 08:14:58 | 838 | **14.0** |
| 08:22 | 11 | 藤原 一矢/Kazuya Fujiwara | 08:51:58 | 1798 | **30.0** |
| 08:25 | 92 | 金原 菜々子/Nanako kimpara | 08:58:29 | 2009 | **33.5** |

※「打刻時刻」は HRMOS API が返す `stamping_start_at`（HH:MM 形式、秒は不明なので 00 秒として計算）

---

## 検証結果

### ✅ 取得方法は正しい
- ページネーション正常（4ページ全部取得、計99件）
- 全社員の存在を確認できている
- 打刻されてない人は `stamping_start_at: null` で返ってくる（取得失敗ではない）

### ❌ 代替エンドポイントなし
試したが全て 404:
- `/api/inrevo/v1/users/{id}/work_outputs/{date}`
- `/api/inrevo/v1/users/{id}/work_outputs`
- `/api/inrevo/v1/work_outputs/daily/{date}/users/{id}`
- `?refresh=1` パラメータ → 効果なし

---

## 時系列パターン

```
07:00 ┃ 1〜2分ラグ（朝早めは速い）
      ┃
07:55 ┃ ラグが急増、7〜14分台へ
08:00 ┃
      ┃
08:14 ┃ ← 観測停止（しばらく新規反映なし）
      ┃
08:51 ┃ 一気に2件反映（30〜34分ラグ）
      ┃
```

**仮説**: HRMOS は内部で集計バッチを 30分間隔（00分/30分）で走らせている可能性。
打刻直後はリアルタイムに見えず、次のバッチタイミングまで `daily` API には反映されない。

---

## 河原さん（id=109）の 8:46 打刻

- HRMOS UI 上では 8:46 出勤打刻が確認できている（本人目視）
- HRMOS daily API では **9:00 過ぎても未反映**（`stamping_start_at: null`）
- bot 処理は 8:56（投稿+10分）に発火 → daily API 上は「打刻なし」 → bot 打刻実行 → **8:47 の手動打刻を上書きしていた可能性が高い**

実際には bot 自身の打刻は早すぎる時刻だと skip するなど別ロジックがあるため、上書きが発生したかは要確認。

---

## 結論

- HRMOS daily API は **リアルタイム参照には使えない**
- 60秒2段階チェックでも カバー不能（30分以上のラグがあるため）
- bot 側の対策：
  - リアクション opt-out（手動打刻するから打刻不要、と本人が宣言）
  - HRMOS サポートへ問い合わせ（生 stamp_logs を GET できる API はないか）

---

## 関連ファイル

- 実装: [src/features/hrmos-stamp-poller.js](../src/features/hrmos-stamp-poller.js)
- 既存の打刻処理: [src/features/ieyasu.js](../src/features/ieyasu.js)
- 日報→打刻フロー: [index.js](../index.js) `processDelayedRecord`
