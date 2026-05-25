// kintone APIからレコードを取得してキャッシュに同期する
const https = require('https');

const DOMAIN = process.env.KINTONE_DOMAIN || 'ca7n5wh2hfvv.cybozu.com';

// アプリごとの設定
// fields: [] → 全フィールド取得（kintoneのfields指定なし）
const APPS = {
  102: {
    token: process.env.KINTONE_APP_102_TOKEN || '01jtfMuLD7b8d28JyBODFrMp7T2QHJK76pF0XkZq',
    companyField: '顧客',
    fields: [], // 全フィールドを取得
  },
  170: {
    token: process.env.KINTONE_APP_170_TOKEN,
    companyField: 'company',
    fields: ['$id', 'company', '数値', '数値_0', 'date', 'plan', 'Staff'],
  },
};

// kintone Records APIをGET（fields が空配列の場合は全フィールド取得）
function kintoneGet(appId, token, fields, offset) {
  return new Promise((resolve, reject) => {
    const fieldParams = fields.length > 0
      ? '&' + fields.map((f, i) => `fields[${i}]=${encodeURIComponent(f)}`).join('&')
      : '';
    const query = encodeURIComponent(`order by $id asc limit 500 offset ${offset}`);
    const path  = `/k/v1/records.json?app=${appId}${fieldParams}&query=${query}`;

    const req = https.request({
      hostname: DOMAIN,
      path,
      method:  'GET',
      headers: { 'X-Cybozu-API-Token': token },
    }, res => {
      let buf = '';
      res.on('data', c  => { buf += c; });
      res.on('end',  () => {
        try { resolve(JSON.parse(buf)); }
        catch (e) { reject(new Error(`kintone parse error: ${buf.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// フィールド値を文字列に正規化（USER_SELECT等の配列型に対応）
function extractValue(fieldValue) {
  if (!fieldValue) return null;
  const v = fieldValue.value;
  if (Array.isArray(v)) {
    return v.map(u => u.name || u.code || String(u)).filter(Boolean).join(', ') || null;
  }
  return v != null && v !== '' ? String(v) : null;
}

// 1アプリ分を全件取得して返す
async function syncKintoneApp(appId, cfg) {
  const { token, companyField, fields } = cfg;
  const results = [];
  let offset = 0;

  while (true) {
    const data = await kintoneGet(appId, token, fields, offset);
    if (data.code) throw new Error(`kintone error [app${appId}]: ${data.code} - ${data.message}`);
    if (!data.records || data.records.length === 0) break;

    for (const rec of data.records) {
      const companyName = extractValue(rec[companyField]);
      if (!companyName) continue;

      const recordId = extractValue(rec['$id']);
      const parsed = {};
      // fields が空 = 全フィールド取得モード → レコードの全キーをパース
      const keys = fields.length > 0 ? fields : Object.keys(rec);
      for (const f of keys) {
        if (f === '$id') continue;
        parsed[f] = extractValue(rec[f]);
      }
      results.push({ recordId: recordId || String(offset + results.length), companyName, data: parsed });
    }

    if (data.records.length < 500) break;
    offset += 500;
  }

  return results;
}

// App170（入金管理）→ kintone_payments テーブルへ同期
async function syncKintonePayments() {
  const { dbQuery } = require('../db/index');
  const { rows } = await dbQuery(
    `SELECT record_id, company_name, data FROM kintone_cache WHERE app_id='170'`
  );
  let upserted = 0;
  for (const rec of rows) {
    const d = rec.data || {};
    const company      = d.company || rec.company_name || null;
    const amount       = d['数値']   != null && d['数値']   !== '' ? Number(d['数値'])   : null;
    const incentive    = d['数値_0'] != null && d['数値_0'] !== '' ? Number(d['数値_0']) : null;
    const paymentDate  = d.date      || null;
    const plan         = d.plan      || null;
    const staff        = d.Staff || d.staff || null;

    if (!paymentDate) continue;

    await dbQuery(`
      INSERT INTO kintone_payments (id, team_id, record_id, company, amount, incentive_amount, payment_date, plan, staff, synced_at)
      VALUES (gen_random_uuid()::text, 'T086C06L5V0', $1, $2, $3, $4, $5::date, $6, $7, now())
      ON CONFLICT (record_id) DO UPDATE SET
        company = EXCLUDED.company,
        amount = EXCLUDED.amount,
        incentive_amount = EXCLUDED.incentive_amount,
        payment_date = EXCLUDED.payment_date,
        plan = EXCLUDED.plan,
        staff = EXCLUDED.staff,
        synced_at = now()
    `, [rec.record_id, company, amount, incentive, paymentDate, plan, staff]).catch(() => {});
    upserted++;
  }

  // kintone_cache (app_id=170) に存在しない record_id を kintone_payments から削除
  // = kintone 上で削除されたレコードを DB からも除去する
  if (rows.length > 0) {
    const del = await dbQuery(`
      DELETE FROM kintone_payments
      WHERE record_id NOT IN (SELECT record_id FROM kintone_cache WHERE app_id='170')
    `);
    if (del.rowCount > 0) console.log(`[kintone] kintone_payments: removed ${del.rowCount} stale records`);
  }

  console.log(`[kintone] kintone_payments upserted: ${upserted}`);
  return upserted;
}

module.exports = { syncKintoneApp, syncKintonePayments, APPS };
