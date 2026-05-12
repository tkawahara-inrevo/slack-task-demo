// kintone連携 API（検索・同期）
const { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord } = require('../db/kintone');
const { syncKintoneApp, APPS } = require('./kintone-sync');
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
async function syncDealsFromKintoneCache() {
  try {
    const { rows } = await dbQuery(
      `SELECT record_id, company_name, data FROM kintone_cache WHERE app_id='102'`
    );
    for (const rec of rows) {
      const d = rec.data || {};
      const sets = [];
      const vals = [rec.record_id];

      for (const [kField, dbCol] of Object.entries(KINTONE_DEAL_FIELD_MAP)) {
        const val = d[kField];
        if (val == null || val === '') continue;
        sets.push(`${dbCol}=COALESCE(${dbCol}, $${vals.length + 1})`);
        vals.push(val);
      }

      if (sets.length === 0) continue;

      // kintone_record_id でマッチして deals を更新（NULLのカラムのみ上書き）
      await dbQuery(
        `UPDATE deals SET ${sets.join(', ')}
         WHERE data->>'kintone_record_id'=$1`,
        vals
      ).catch(() => {});
    }
    console.log('[kintone] deals mapping updated');
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
      console.log(`[kintone] synced ${records.length} records from app ${appId}`);
    }
    // deals テーブルへのフィールドマッピング
    await syncDealsFromKintoneCache();
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
