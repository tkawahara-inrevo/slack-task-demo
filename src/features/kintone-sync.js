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

// App103専用トークン（活動履歴）
const APP_103_TOKEN = process.env.KINTONE_APP_103_TOKEN || 'cekSgk3whWbQU6KFOGhr7wnnFOiSp2hzRHQP0Zhz';

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
      // chunk境界でのマルチバイト文字割れ防止: Bufferで集約してから utf8 デコード
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end',  () => {
        const buf = Buffer.concat(chunks).toString('utf8');
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

  // App102 (案件) から会社名 → 流入経路 マップを構築
  // 同じ会社名で複数案件がある場合は直近の流入経路を採用（最後の上書きが残る）
  const dealRowsRes = await dbQuery(
    `SELECT company_name, data->>'流入経路' AS inflow FROM kintone_cache WHERE app_id='102'`
  );
  const inflowMap = {};
  for (const r of dealRowsRes.rows) {
    if (r.company_name && r.inflow) inflowMap[r.company_name] = r.inflow;
  }

  let upserted = 0;
  for (const rec of rows) {
    const d = rec.data || {};
    const company      = d.company || rec.company_name || null;
    const amount       = d['数値']   != null && d['数値']   !== '' ? Number(d['数値'])   : null;
    const incentive    = d['数値_0'] != null && d['数値_0'] !== '' ? Number(d['数値_0']) : null;
    const paymentDate  = d.date      || null;
    const plan         = d.plan      || null;
    const staff        = d.Staff || d.staff || null;
    const inflowSource = company ? (inflowMap[company] || null) : null;

    if (!paymentDate) continue;

    await dbQuery(`
      INSERT INTO kintone_payments (id, team_id, record_id, company, amount, incentive_amount, payment_date, plan, staff, inflow_source, synced_at)
      VALUES (gen_random_uuid()::text, 'T086C06L5V0', $1, $2, $3, $4, $5::date, $6, $7, $8, now())
      ON CONFLICT (record_id) DO UPDATE SET
        company = EXCLUDED.company,
        amount = EXCLUDED.amount,
        incentive_amount = EXCLUDED.incentive_amount,
        payment_date = EXCLUDED.payment_date,
        plan = EXCLUDED.plan,
        staff = EXCLUDED.staff,
        inflow_source = EXCLUDED.inflow_source,
        synced_at = now()
    `, [rec.record_id, company, amount, incentive, paymentDate, plan, staff, inflowSource]).catch(() => {});
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

// App103（活動履歴）→ kintone_activities テーブルへ同期
// 案件との紐付けは 関連レコード紐付用（App102 record_id）で行う
async function syncKintoneActivities() {
  const { dbQuery } = require('../db/index');
  if (!APP_103_TOKEN) {
    console.warn('[kintone] APP_103 token not set, skipping activities sync');
    return 0;
  }

  // 全件取得
  let offset = 0;
  let upserted = 0;
  const seenIds = new Set();
  while (true) {
    const data = await kintoneGet(103, APP_103_TOKEN, [], offset);
    if (data.code) throw new Error(`kintone error [app103]: ${data.code} - ${data.message}`);
    if (!data.records || data.records.length === 0) break;

    for (const rec of data.records) {
      const get = (k) => {
        const v = rec[k]?.value;
        if (Array.isArray(v)) return v.map(x => x.name || x.code || x).join(', ');
        return v ?? null;
      };
      const recordId = String(rec['$id']?.value || '');
      const dealRecId = get('関連レコード紐付用');
      if (!recordId) continue;
      seenIds.add(recordId);

      const isDone = (() => {
        const v1 = rec['対応済']?.value;
        const v2 = rec['対応済_0']?.value;
        const arr = [...(Array.isArray(v1)?v1:[]), ...(Array.isArray(v2)?v2:[])];
        return arr.length > 0;
      })();

      await dbQuery(`
        INSERT INTO kintone_activities
          (record_id, team_id, deal_record_id, activity_date, activity_type, assignee, content,
           next_action_date, next_action_content, next_action_detail, next_assignee, yomi_at_time,
           is_done, created_at, updated_at, synced_at)
        VALUES ($1,'T086C06L5V0',$2,NULLIF($3,'')::date,$4,$5,$6,NULLIF($7,'')::date,$8,$9,$10,$11,$12,NULLIF($13,'')::timestamptz,NULLIF($14,'')::timestamptz,now())
        ON CONFLICT (record_id) DO UPDATE SET
          deal_record_id      = EXCLUDED.deal_record_id,
          activity_date       = EXCLUDED.activity_date,
          activity_type       = EXCLUDED.activity_type,
          assignee            = EXCLUDED.assignee,
          content             = EXCLUDED.content,
          next_action_date    = EXCLUDED.next_action_date,
          next_action_content = EXCLUDED.next_action_content,
          next_action_detail  = EXCLUDED.next_action_detail,
          next_assignee       = EXCLUDED.next_assignee,
          yomi_at_time        = EXCLUDED.yomi_at_time,
          is_done             = EXCLUDED.is_done,
          updated_at          = EXCLUDED.updated_at,
          synced_at           = now()
      `, [
        recordId,
        dealRecId ? String(dealRecId) : null,
        get('対応日付') || '',
        get('対応内容'),
        get('対応者'),
        get('MEMO'),
        get('Next_action日') || '',
        get('Next_action内容'),
        get('Next_action詳細'),
        get('次回の対応予定者'),
        get('ヨミ'),
        isDone,
        get('作成日時') || '',
        get('更新日時') || '',
      ]).catch(e => console.error('[kintone activities] upsert err:', e.message));
      upserted++;
    }

    if (data.records.length < 500) break;
    offset += 500;
  }

  // kintone側で削除された活動を localからも削除
  if (seenIds.size > 0) {
    const del = await dbQuery(
      `DELETE FROM kintone_activities WHERE record_id NOT IN (${Array.from(seenIds).map((_,i)=>`$${i+1}`).join(',')})`,
      Array.from(seenIds)
    );
    if (del.rowCount > 0) console.log(`[kintone] kintone_activities: removed ${del.rowCount} stale records`);
  }

  console.log(`[kintone] kintone_activities upserted: ${upserted}`);
  return upserted;
}

module.exports = { syncKintoneApp, syncKintonePayments, syncKintoneActivities, APPS };
