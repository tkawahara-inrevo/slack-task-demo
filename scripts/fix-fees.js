// 費用フィールドの修正スクリプト
require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const DOMAIN = 'ca7n5wh2hfvv.cybozu.com';
const TOKEN = 'AZsoAU6lPUhmuHr0YC0H194VWQPCGkeb0Fvq6LaU';

const FIELDS = [
  '$id', '案件名', '顧客',
  '初期請求費用_税抜き',           // 初期費用
  '_1ヶ月or1名_当たりの単価_税抜き', // 月額/受注単価
  '見込売り上げ_税抜き',            // 見込売上（CALC）
  '契約月数or採用人数_税抜き',       // 契約月数 or 採用人数
];

function kintoneGetAll() {
  return new Promise((resolve, reject) => {
    const all = [];
    const fetch = (offset) => {
      const fp = FIELDS.map((f, i) => `fields[${i}]=${encodeURIComponent(f)}`).join('&');
      const q = encodeURIComponent(`order by $id asc limit 500 offset ${offset}`);
      const path = `/k/v1/records.json?app=102&${fp}&query=${q}`;
      const req = https.request({ hostname: DOMAIN, path, method: 'GET', headers: { 'X-Cybozu-API-Token': TOKEN } }, res => {
        let body = '';
        res.on('data', d => body += d);
        res.on('end', () => {
          const data = JSON.parse(body);
          if (!data.records) return reject(new Error(JSON.stringify(data)));
          all.push(...data.records);
          if (data.records.length === 500) fetch(offset + 500);
          else resolve(all);
        });
      });
      req.on('error', reject);
      req.end();
    };
    fetch(0);
  });
}

async function main() {
  console.log('kintoneから費用フィールドを取得中...');
  const records = await kintoneGetAll();
  console.log(`取得: ${records.length}件`);

  const { rows: teamRows } = await pool.query('SELECT DISTINCT team_id FROM dash_teams LIMIT 1');
  const teamId = teamRows[0]?.team_id;

  let updated = 0, skipped = 0;
  for (const r of records) {
    const kintoneId = r['$id']?.value;
    const customerName = r['顧客']?.value;
    const dealName = r['案件名']?.value;

    const initialFee = parseFloat(r['初期請求費用_税抜き']?.value) || null;
    // 月額単価 or 見込売上のうち大きい方を使用
    const unitPrice = parseFloat(r['_1ヶ月or1名_当たりの単価_税抜き']?.value) || null;
    const expectedRevenue = parseFloat(r['見込売り上げ_税抜き']?.value) || null;
    const monthlyFee = unitPrice || expectedRevenue || null;

    if (!initialFee && !monthlyFee) { skipped++; continue; }

    // data->kintoneId でdealsを特定
    const { rowCount } = await pool.query(`
      UPDATE deals SET
        initial_fee = $1,
        monthly_fee = $2,
        updated_at = now()
      WHERE team_id = $3
        AND (data->>'kintoneId' = $4 OR (name = $5 AND customer_id IN (
          SELECT id FROM customers WHERE team_id = $3 AND name = $6
        )))
        AND (initial_fee IS DISTINCT FROM $1 OR monthly_fee IS DISTINCT FROM $2)
    `, [initialFee, monthlyFee, teamId, String(kintoneId), dealName, customerName]);

    if (rowCount > 0) updated++;
  }

  console.log(`更新: ${updated}件 / スキップ(費用なし): ${skipped}件`);

  // 確認
  const { rows } = await pool.query(`
    SELECT name, initial_fee, monthly_fee FROM deals
    WHERE team_id=$1 AND monthly_fee > 1000
    ORDER BY monthly_fee DESC LIMIT 5
  `, [teamId]);
  console.log('\n上位5件（月額）:');
  rows.forEach(r => console.log(` ${r.name?.slice(0,30)} 初期:${r.initial_fee} 月額:${r.monthly_fee}`));

  await pool.end();
}

main().catch(e => { console.error('エラー:', e.message); pool.end(); process.exit(1); });
