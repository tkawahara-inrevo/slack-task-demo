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
  try {
    const { rows: cacheRows } = await dbQuery(
      `SELECT record_id, company_name, data FROM kintone_cache WHERE app_id='102' AND company_name IS NOT NULL`
    );

    const teamId = 'T086C06L5V0';
    let created = 0, updated = 0, customersCreated = 0;

    for (const kc of cacheRows) {
      const d = kc.data || {};
      // ヨミから（）以降の説明を剥がす
      const rawYomi = d['ヨミ'] || '';
      const yomi = rawYomi.replace(/[（(].*$/, '').trim();
      if (!yomi && !kc.record_id) continue;

      // status 導出
      const status = yomi === '受注'   ? 'won'
                   : yomi === '失注'   ? 'lost'
                   : yomi === '見送り' ? 'dormant'
                   : 'active';

      // customer 確保
      let custRes = await dbQuery(
        `SELECT id FROM customers WHERE team_id=$1 AND name=$2 LIMIT 1`,
        [teamId, kc.company_name]
      );
      let customerId;
      if (custRes.rows[0]) {
        customerId = custRes.rows[0].id;
      } else {
        const ins = await dbQuery(
          `INSERT INTO customers (team_id, name) VALUES ($1,$2) RETURNING id`,
          [teamId, kc.company_name]
        );
        customerId = ins.rows[0].id;
        customersCreated++;
      }

      // deal: kintone_record_id でマッチ
      const dealRes = await dbQuery(
        `SELECT id FROM deals WHERE team_id=$1 AND data->>'kintone_record_id'=$2 LIMIT 1`,
        [teamId, String(kc.record_id)]
      );

      const fields = {
        yomi:               yomi || null,
        status,
        order_date:          d['受注日']                 || null,
        conclusion_date:     d['結論日']                 || null,
        first_meeting_date:  d['初回商談日_コンサルチーム'] || null,
        inflow_date:         d['流入日'] || d['商談獲得日_マーケチーム'] || null,
        inflow_source:       d['流入経路']               || null,
        initial_fee:         d['見込売り上げ_税抜き']     || null,
        contract_type:       d['ヨミ_2']                 || null,
        lost_reason:         d['失注理由']               || null,
        sales_person:        d['担当営業_0']             || null,
        name:                d['案件名'] || `${kc.company_name}_kintone_${kc.record_id}`,
      };

      if (dealRes.rows[0]) {
        // 既存deal: 値があるフィールドだけ上書き（空文字はnullに正規化）
        const sets = [], vals = [];
        let i = 1;
        const push = (col, val, cast='') => {
          if (val === '' || val == null) return;
          sets.push(`${col}=$${i++}${cast}`);
          vals.push(val);
        };
        push('yomi', fields.yomi);
        sets.push(`status='${status}'`);
        push('order_date',         fields.order_date,         '::date');
        push('conclusion_date',    fields.conclusion_date,    '::date');
        push('first_meeting_date', fields.first_meeting_date, '::date');
        push('inflow_date',        fields.inflow_date,        '::date');
        push('inflow_source',      fields.inflow_source);
        push('initial_fee',        fields.initial_fee,        '::numeric');
        push('contract_type',      fields.contract_type);
        push('lost_reason',        fields.lost_reason);
        push('sales_person',       fields.sales_person);
        sets.push(`updated_at=now()`);
        vals.push(dealRes.rows[0].id);
        await dbQuery(`UPDATE deals SET ${sets.join(', ')} WHERE id=$${i}`, vals);
        updated++;
      } else {
        // 新規deal
        await dbQuery(`
          INSERT INTO deals (team_id, customer_id, name, yomi, status,
                             order_date, conclusion_date, first_meeting_date, inflow_date, inflow_source,
                             initial_fee, contract_type, lost_reason, sales_person, data)
          VALUES ($1,$2,$3,$4,$5,
                  NULLIF($6,'')::date, NULLIF($7,'')::date, NULLIF($8,'')::date, NULLIF($9,'')::date, $10,
                  NULLIF($11,'')::numeric, $12, $13, $14,
                  jsonb_build_object('kintone_record_id', $15::text))
        `, [
          teamId, customerId, fields.name, fields.yomi, status,
          fields.order_date || '', fields.conclusion_date || '', fields.first_meeting_date || '', fields.inflow_date || '',
          fields.inflow_source,
          fields.initial_fee || '', fields.contract_type, fields.lost_reason, fields.sales_person,
          String(kc.record_id),
        ]);
        created++;
      }
    }

    console.log(`[kintone] deals upsert: created=${created} updated=${updated} customers=${customersCreated}`);
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
