// kintone連携 API（検索・同期）
const { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord } = require('../db/kintone');
const { syncKintoneApp, syncKintonePayments, syncKintoneActivities, APPS } = require('./kintone-sync');
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

// kintone_cache から deals テーブルへマッピングを反映（record_id ベースの UPSERT）
// 動作:
//   1. customers: company_name でなければ新規作成
//   2. deals:     data->>'kintone_record_id' でマッチ。なければ新規作成。あれば全フィールド更新
//   3. ヨミから status を導出（受注/失注/見送り）
async function syncDealsFromKintoneCache() {
  const teamId = 'T086C06L5V0';
  try {
    // ヨミの（）以降を剥がす式 / status導出式（SQL内で共通利用）
    const YOMI = `TRIM(regexp_replace(COALESCE(kc.data->>'ヨミ',''), '[（(].*$', ''))`;
    const STATUS = `CASE ${YOMI}
        WHEN '受注' THEN 'won' WHEN '失注' THEN 'lost' WHEN '見送り' THEN 'dormant' ELSE 'active' END`;

    // 1) customers 補完（company_name が customers に無ければ作成）
    const custIns = await dbQuery(`
      INSERT INTO customers (team_id, name)
      SELECT DISTINCT $1, kc.company_name
      FROM kintone_cache kc
      WHERE kc.app_id='102' AND kc.company_name IS NOT NULL AND kc.company_name <> ''
        AND NOT EXISTS (SELECT 1 FROM customers c WHERE c.team_id=$1 AND c.name=kc.company_name)
    `, [teamId]);

    // 2) 新規 deal 作成（kintone_record_id がまだ deals に無いもの）
    const dealIns = await dbQuery(`
      INSERT INTO deals (team_id, customer_id, name, yomi, status,
                         order_date, conclusion_date, first_meeting_date, inflow_date, inflow_source,
                         initial_fee, contract_type, lost_reason, sales_person, data)
      SELECT $1, c.id,
        COALESCE(NULLIF(kc.data->>'案件名',''), kc.company_name || '_' || kc.record_id),
        NULLIF(${YOMI},''),
        ${STATUS},
        NULLIF(kc.data->>'受注日','')::date,
        NULLIF(kc.data->>'結論日','')::date,
        NULLIF(kc.data->>'初回商談日_コンサルチーム','')::date,
        NULLIF(COALESCE(NULLIF(kc.data->>'流入日',''), kc.data->>'商談獲得日_マーケチーム'),'')::date,
        NULLIF(kc.data->>'流入経路',''),
        NULLIF(kc.data->>'見込売り上げ_税抜き','')::numeric,
        NULLIF(kc.data->>'ヨミ_2',''),
        NULLIF(kc.data->>'失注理由',''),
        NULLIF(kc.data->>'担当営業_0',''),
        jsonb_build_object('kintone_record_id', kc.record_id)
      FROM kintone_cache kc
      JOIN customers c ON c.team_id=$1 AND c.name=kc.company_name
      WHERE kc.app_id='102' AND kc.company_name IS NOT NULL AND kc.company_name <> ''
        AND NOT EXISTS (SELECT 1 FROM deals d WHERE d.team_id=$1 AND d.data->>'kintone_record_id'=kc.record_id)
    `, [teamId]);

    // 3) 既存 deal を一括更新（kintone_record_id で結合。空値は既存値を保持）
    const dealUpd = await dbQuery(`
      UPDATE deals d SET
        yomi   = COALESCE(NULLIF(${YOMI},''), d.yomi),
        status = ${STATUS},
        order_date         = COALESCE(NULLIF(kc.data->>'受注日','')::date, d.order_date),
        conclusion_date    = COALESCE(NULLIF(kc.data->>'結論日','')::date, d.conclusion_date),
        first_meeting_date = COALESCE(NULLIF(kc.data->>'初回商談日_コンサルチーム','')::date, d.first_meeting_date),
        inflow_date        = COALESCE(NULLIF(COALESCE(NULLIF(kc.data->>'流入日',''), kc.data->>'商談獲得日_マーケチーム'),'')::date, d.inflow_date),
        inflow_source      = COALESCE(NULLIF(kc.data->>'流入経路',''), d.inflow_source),
        initial_fee        = COALESCE(NULLIF(kc.data->>'見込売り上げ_税抜き','')::numeric, d.initial_fee),
        contract_type      = COALESCE(NULLIF(kc.data->>'ヨミ_2',''), d.contract_type),
        lost_reason        = COALESCE(NULLIF(kc.data->>'失注理由',''), d.lost_reason),
        sales_person       = COALESCE(NULLIF(kc.data->>'担当営業_0',''), d.sales_person),
        updated_at = now()
      FROM kintone_cache kc
      WHERE kc.app_id='102' AND d.team_id=$1 AND d.data->>'kintone_record_id'=kc.record_id
    `, [teamId]);

    console.log(`[kintone] deals upsert(bulk): customers+${custIns.rowCount} deals_new+${dealIns.rowCount} deals_upd=${dealUpd.rowCount}`);
  } catch (e) {
    console.error('[kintone] deals upsert error:', e.message);
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
    // ※ App103（活動履歴）連携は方針変更により停止。活動履歴はTaskHub側UIで完結。
    // await syncKintoneActivities();
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
