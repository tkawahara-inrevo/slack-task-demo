// kintoneの全フィールドをdataに保存するフル移行スクリプト
require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const DOMAIN = 'ca7n5wh2hfvv.cybozu.com';

const TOKENS = {
  94:  'Yeeyud4zlzonWqp1D8LEyIFeY9mzWCqQJVihQbCP',
  102: 'AZsoAU6lPUhmuHr0YC0H194VWQPCGkeb0Fvq6LaU',
  105: 'vw5XgwHxUSGCpK3KwAIRamLft1vKp2MMOjGPAWTU',
  170: 'F3j0G13EGAT2Sa5wZZ1x720pkpqHCcQiZpXPzkiJ',
};

function kintoneGetAll(appId) {
  return new Promise((resolve, reject) => {
    const token = TOKENS[appId];
    const all = [];
    const fetch = (offset) => {
      const q = encodeURIComponent(`order by $id asc limit 500 offset ${offset}`);
      const path = `/k/v1/records.json?app=${appId}&query=${q}`;
      const req = https.request({ hostname: DOMAIN, path, method: 'GET', headers: { 'X-Cybozu-API-Token': token } }, res => {
        const chunks = [];
        res.on('data', d => chunks.push(d));
        res.on('end', () => {
          try {
            const data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
            if (!data.records) return reject(new Error(JSON.stringify(data).slice(0, 200)));
            all.push(...data.records);
            if (data.records.length === 500) fetch(offset + 500);
            else resolve(all);
          } catch(e) { reject(e); }
        });
      });
      req.on('error', reject);
      req.end();
    };
    fetch(0);
  });
}

// kintoneレコードをフラットなJSONに変換（全フィールドを保持）
function flattenRecord(r) {
  const out = {};
  for (const [key, field] of Object.entries(r)) {
    if (key.startsWith('$')) continue;
    const { type, value } = field;
    if (['CREATOR','MODIFIER','RECORD_NUMBER','__REVISION__','STATUS','STATUS_ASSIGNEE','CATEGORY'].includes(type)) continue;
    out[key] = value;
  }
  return out;
}

const v = (r, field) => r[field]?.value ?? null;
const vStr = (r, field) => v(r, field) || null;
const vNum = (r, field) => { const n = Number(v(r, field)); return isNaN(n) || n === 0 ? null : n; };

async function getTeamId() {
  const { rows } = await pool.query('SELECT DISTINCT team_id FROM dash_teams LIMIT 1');
  return rows[0]?.team_id;
}

async function updateDealsData(teamId) {
  console.log('\n📦 App102 → deals.data を全フィールドで更新');
  const records = await kintoneGetAll(102);
  console.log(`  取得: ${records.length}件`);

  let updated = 0;
  for (const r of records) {
    const kintoneId = v(r, '$id');
    const dealName = vStr(r, '案件名');
    const customerName = vStr(r, '顧客');

    // 正しいフィールドで費用を取得
    const initialFee = vNum(r, '初期請求費用_税抜き');
    const monthlyFee = vNum(r, '_1ヶ月or1名_当たりの単価_税抜き') || vNum(r, '見込売り上げ_税抜き');

    // 全フィールドをflattenしてdataに格納
    const allData = flattenRecord(r);

    const { rowCount } = await pool.query(`
      UPDATE deals SET
        initial_fee = $1,
        monthly_fee = $2,
        data = $3::jsonb,
        updated_at = now()
      WHERE team_id = $4
        AND (data->>'kintoneId' = $5 OR (
          name = $6 AND customer_id IN (
            SELECT id FROM customers WHERE team_id=$4 AND name=$7
          )
        ))
    `, [initialFee, monthlyFee, JSON.stringify({ kintoneId: String(kintoneId), ...allData }), teamId, String(kintoneId), dealName, customerName]);

    if (rowCount > 0) updated++;
  }
  console.log(`  更新: ${updated}件`);
}

async function updateCustomersData(teamId) {
  console.log('\n📦 App94 → customers.data を全フィールドで更新');
  const records = await kintoneGetAll(94);
  console.log(`  取得: ${records.length}件`);

  let updated = 0;
  for (const r of records) {
    const name = vStr(r, '会社名');
    if (!name) continue;
    const allData = flattenRecord(r);
    const { rowCount } = await pool.query(`
      UPDATE customers SET data=$1::jsonb, updated_at=now()
      WHERE team_id=$2 AND name=$3 AND (data IS NULL OR data='{}'::jsonb)
    `, [JSON.stringify(allData), teamId, name]);
    if (rowCount > 0) updated++;
  }
  console.log(`  更新: ${updated}件`);
}

async function main() {
  const teamId = await getTeamId();
  console.log(`team_id: ${teamId}`);

  await updateDealsData(teamId);
  await updateCustomersData(teamId);

  // 確認
  const { rows } = await pool.query(`
    SELECT name, initial_fee, monthly_fee, jsonb_object_keys(data) as k
    FROM deals WHERE team_id=$1 AND monthly_fee > 1000000 LIMIT 1
  `, [teamId]);
  if (rows.length) console.log('\n✅ サンプル確認:', rows[0].name, '月額:', rows[0].monthly_fee);

  await pool.end();
  console.log('\n✅ 完了');
}

main().catch(e => { console.error('❌', e.message); pool.end(); process.exit(1); });
