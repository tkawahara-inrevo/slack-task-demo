// kintone連携 API（検索・同期）
const { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord } = require('../db/kintone');
const { syncKintoneApp, syncKintonePayments, APPS } = require('./kintone-sync');
const { dbQuery } = require('../db/index');

// kintone App102 フィールド → deals カラム のマッピング定義
const KINTONE_DEAL_FIELD_MAP = {
  '受注日':                             'order_date',
  '結論日':                             'conclusion_date',
  'ヨミ_2':                             'contract_type',      // 採用保証/月額の契約形態
  '担当営業_0':                         'sales_user_id',
  'IS用最終対応日付':                    'inflow_date',        // 流入日（商談獲得日）
  '見込売り上げ_税抜き':                 'initial_fee',
  '初回商談日_コンサルチーム':           'first_meeting_date',
  '商談獲得日_マーケチーム':             'inflow_date',
  '流入日':                             'inflow_date',
  '流入経路':                           'inflow_source',
  'ヨミ':                               'yomi',
  'ヨミ_経過フロー':                     'yomi_flow',          // ヨミ推移履歴
  '失注理由':                           'lost_reason',
  'NextAction日':                       'next_action_date',
  '案件名':                             'name',
  '_1ヶ月or1名_当たりの単価_税抜き':    'guarantee_salary',   // 採用単価
  '契約月数or採用人数_税抜き':          'contract_months',    // 採用目標人数or契約月数
  '担当営業':                           'na_user_id',         // NA担当者（担当営業_0と別）
};

// kintone_cache から deals テーブルへマッピングを反映
// マッチング: kintone_cache.company_name → customers.name → deals.customer_id
async function syncDealsFromKintoneCache() {
  try {
    // company_name経由でdealsと結合してまとめて更新
    await dbQuery(`
      UPDATE deals d
      SET
        yomi        = CASE WHEN kc.data->>'ヨミ' IS NOT NULL AND kc.data->>'ヨミ' != ''
                           THEN TRIM(SPLIT_PART(SPLIT_PART(kc.data->>'ヨミ', '（', 1), '(', 1))
                           ELSE d.yomi END,
        status      = CASE TRIM(SPLIT_PART(SPLIT_PART(kc.data->>'ヨミ', '（', 1), '(', 1))
                           WHEN '受注'   THEN 'won'
                           WHEN '失注'   THEN 'lost'
                           WHEN '見送り' THEN 'dormant'
                           ELSE d.status END,
        order_date  = CASE WHEN kc.data->>'受注日' IS NOT NULL AND kc.data->>'受注日' != ''
                           THEN (kc.data->>'受注日')::date ELSE d.order_date END,
        conclusion_date = CASE WHEN kc.data->>'結論日' IS NOT NULL AND kc.data->>'結論日' != ''
                               THEN (kc.data->>'結論日')::date ELSE d.conclusion_date END,
        first_meeting_date = CASE WHEN kc.data->>'初回商談日_コンサルチーム' IS NOT NULL AND kc.data->>'初回商談日_コンサルチーム' != ''
                                  THEN (kc.data->>'初回商談日_コンサルチーム')::date ELSE d.first_meeting_date END,
        initial_fee = CASE WHEN kc.data->>'見込売り上げ_税抜き' IS NOT NULL AND kc.data->>'見込売り上げ_税抜き' != ''
                           THEN (kc.data->>'見込売り上げ_税抜き')::numeric ELSE d.initial_fee END,
        contract_type = CASE WHEN kc.data->>'ヨミ_2' IS NOT NULL AND kc.data->>'ヨミ_2' != ''
                             THEN kc.data->>'ヨミ_2' ELSE d.contract_type END,
        lost_reason = CASE WHEN kc.data->>'失注理由' IS NOT NULL AND kc.data->>'失注理由' != ''
                           THEN kc.data->>'失注理由' ELSE d.lost_reason END,
        data        = d.data || jsonb_build_object('kintone_record_id', kc.record_id),
        updated_at  = now()
      FROM kintone_cache kc
      JOIN customers c ON c.name = kc.company_name
      WHERE kc.app_id = '102'
        AND d.customer_id = c.id
        AND d.team_id = c.team_id
    `);
    console.log('[kintone] deals mapping updated via company name');
  } catch (e) {
    console.error('[kintone] deals mapping error:', e.message);
  }
}

const SYNC_INTERVAL_MS = 30 * 60 * 1000; // 30分
let syncInProgress = false;

async function runSync() {
  if (syncInProgress) return;
  syncInProgress = true;
  try {
    for (const [appId, cfg] of Object.entries(APPS)) {
      console.log(`[kintone] syncing app ${appId}...`);
      const records = await syncKintoneApp(Number(appId), cfg);
      await dbUpsertKintoneRecords(String(appId), records);
      // kintone側で削除されたレコードをDBからも削除（記事0件のときは安全側で削除しない）
      if (records.length > 0) {
        const ids = records.map(r => r.recordId).filter(Boolean);
        const del = await dbQuery(
          'DELETE FROM kintone_cache WHERE app_id=$1 AND record_id <> ALL($2::text[])',
          [String(appId), ids]
        );
        if (del.rowCount > 0) console.log(`[kintone] app ${appId}: removed ${del.rowCount} stale records`);
      }
      console.log(`[kintone] synced ${records.length} records from app ${appId}`);
    }
    // deals テーブルへのフィールドマッピング
    await syncDealsFromKintoneCache();
    // kintone_payments（App170）同期
    await syncKintonePayments();
    // リード管理ダッシュボード用マテリアライズドビューをリフレッシュ
    await dbQuery('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lead_customers').catch(() => {});
    await dbQuery('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lead_flags').catch(() => {});
    console.log('[kintone] lead views refreshed');
  } catch (e) {
    console.error('[kintone] sync error:', e.message);
  } finally {
    syncInProgress = false;
  }
}

function registerKintoneApi({ expressApp, authWithRole, adminOnly }) {
  // 起動時にスキーマ確保 → 初回同期
  dbEnsureKintoneSchema()
    .then(() => runSync())
    .catch(e => console.error('[kintone] init error:', e));

  // 30分ごとに自動同期
  setInterval(runSync, SYNC_INTERVAL_MS);

  // ─────────────────────────────────────────
  // GET /api/kintone/search?q=xxx  企業名部分一致検索
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/search', authWithRole, async (req, res) => {
    const { q = '' } = req.query;
    if (q.trim().length < 1) return res.json({ results: [] });
    try {
      const results = await dbSearchKintoneCompanies(q.trim(), 10);
      res.json({ results });
    } catch (e) {
      console.error('[kintone] search error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // GET /api/kintone/record/:recordId
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/record/:recordId', authWithRole, async (req, res) => {
    try {
      const rec = await dbGetKintoneRecord(req.params.recordId);
      if (!rec) return res.status(404).json({ error: 'not_found' });
      res.json({ record: rec });
    } catch (e) {
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─────────────────────────────────────────
  // POST /api/kintone/sync  手動同期（adminのみ）
  // ─────────────────────────────────────────
  expressApp.post('/api/kintone/sync', authWithRole, adminOnly, (req, res) => {
    runSync().catch(e => console.error('[kintone] manual sync error:', e));
    res.json({ message: '同期を開始しました' });
  });

  // ─────────────────────────────────────────
  // GET /api/kintone/status  同期状態確認
  // ─────────────────────────────────────────
  expressApp.get('/api/kintone/status', authWithRole, async (req, res) => {
    try {
      const lastSync = await dbGetKintoneLastSync();
      res.json({ lastSync, inProgress: syncInProgress });
    } catch (e) {
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerKintoneApi };
