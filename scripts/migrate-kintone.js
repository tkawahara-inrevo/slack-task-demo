// kintone → DB 移行スクリプト
// 実行: node scripts/migrate-kintone.js
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

// kintone 全件取得（ページネーション）
function kintoneGetAll(appId, fields = []) {
  return new Promise((resolve, reject) => {
    const token = TOKENS[appId];
    const allRecords = [];
    const fetch = (offset) => {
      const fieldParams = fields.map((f, i) => `fields[${i}]=${encodeURIComponent(f)}`).join('&');
      const query = encodeURIComponent(`order by $id asc limit 500 offset ${offset}`);
      const path = `/k/v1/records.json?app=${appId}${fields.length ? '&' + fieldParams : ''}&query=${query}`;
      const req = https.request({ hostname: DOMAIN, path, method: 'GET', headers: { 'X-Cybozu-API-Token': token } }, res => {
        let body = '';
        res.on('data', d => body += d);
        res.on('end', () => {
          const data = JSON.parse(body);
          if (data.records) {
            allRecords.push(...data.records);
            if (data.records.length === 500) fetch(offset + 500);
            else resolve(allRecords);
          } else reject(new Error(JSON.stringify(data)));
        });
      });
      req.on('error', reject);
      req.end();
    };
    fetch(0);
  });
}

const v = (r, field) => r[field]?.value ?? null;
const vStr = (r, field) => v(r, field) || null;
const vNum = (r, field) => { const n = Number(v(r, field)); return isNaN(n) || n === 0 ? null : n; };
const vArr = (r, field) => Array.isArray(v(r, field)) ? v(r, field) : [];

function yomiToStatus(yomi) {
  if (!yomi) return 'active';
  if (yomi === '受注' || yomi === '受注済み') return 'won';
  if (yomi === '失注') return 'lost';
  return 'active';
}

async function getTeamId() {
  const { rows } = await pool.query('SELECT DISTINCT team_id FROM dash_teams LIMIT 1');
  return rows[0]?.team_id;
}

async function migrateCustomers(teamId) {
  console.log('\n📦 App94 → customers テーブル');
  const records = await kintoneGetAll(94);
  console.log(`  取得: ${records.length}件`);

  let inserted = 0, skipped = 0;
  for (const r of records) {
    const name = vStr(r, '会社名');
    if (!name) { skipped++; continue; }

    const industry = vArr(r, '複数選択')[0] || null;
    const prefecture = vStr(r, 'ドロップダウン_0');
    const employeeCount = vStr(r, 'ドロップダウン') || vStr(r, '従業員数');
    const website = vStr(r, '会社HPリンク_0') || vStr(r, '会社HPリンク');
    const memo = vStr(r, '文字列__複数行__1');
    const kintoneId = v(r, '$id');

    await pool.query(`
      INSERT INTO customers (team_id, name, industry, prefecture, employee_count, website, memo, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'kintone_migration')
      ON CONFLICT DO NOTHING
    `, [teamId, name, industry, prefecture, employeeCount, website, memo]);
    inserted++;
  }
  console.log(`  挿入: ${inserted}件 / スキップ: ${skipped}件`);
}

async function migrateDeals(teamId) {
  console.log('\n📦 App102 → deals テーブル');
  const records = await kintoneGetAll(102);
  console.log(`  取得: ${records.length}件`);

  // 顧客名→ID マップ作成
  const { rows: customers } = await pool.query('SELECT id, name FROM customers WHERE team_id=$1', [teamId]);
  const custMap = {};
  for (const c of customers) custMap[c.name] = c.id;

  let inserted = 0, noCustomer = 0;
  for (const r of records) {
    const name = vStr(r, '案件名') || vStr(r, '顧客') || '（案件名未設定）';
    const customerName = vStr(r, '顧客');
    const customerId = customerName ? custMap[customerName] : null;

    if (!customerId) {
      // 顧客が存在しない場合は新規作成
      if (customerName) {
        const { rows } = await pool.query(
          `INSERT INTO customers (team_id, name, created_by) VALUES ($1,$2,'kintone_migration') RETURNING id`,
          [teamId, customerName]
        );
        custMap[customerName] = rows[0].id;
      }
      noCustomer++;
    }

    const cid = custMap[customerName] || null;
    if (!cid) continue;

    const yomi = vStr(r, 'ヨミ') || 'アポ化前';
    const contractType = vStr(r, 'ヨミ_0');   // 契約形態
    const paymentType  = vStr(r, '支払方式');
    const salesUser    = vStr(r, '担当営業_0'); // 担当営業
    const naUser       = vStr(r, '担当営業');   // NA担当者
    const initialFee   = vNum(r, '数値')  || vNum(r, '初期請求費用_税抜き');
    const monthlyFee   = vNum(r, '数値_0') || vNum(r, 'ヨミ金額_月額費用_税抜き');
    const lostReason   = vStr(r, 'ドロップダウン_0');
    const status       = yomiToStatus(yomi);
    const memo         = [vStr(r, '文字列__複数行_'), vStr(r, '案件最新状況')].filter(Boolean).join('\n') || null;
    const orderDate    = vStr(r, '受注日');

    await pool.query(`
      INSERT INTO deals (team_id, customer_id, name, yomi, contract_type, payment_type,
        sales_user_id, na_user_id, initial_fee, monthly_fee, lost_reason, status, memo,
        data, created_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,'kintone_migration')
      ON CONFLICT DO NOTHING
    `, [teamId, cid, name, yomi, contractType, paymentType,
        salesUser, naUser, initialFee, monthlyFee, lostReason, status, memo,
        JSON.stringify({ kintoneId: v(r, '$id'), orderDate })]);
    inserted++;
  }
  console.log(`  挿入: ${inserted}件 / 顧客新規作成: ${noCustomer}件`);
}

async function main() {
  console.log('🚀 kintone → DB 移行開始');
  const teamId = await getTeamId();
  if (!teamId) { console.error('team_id が見つかりません'); process.exit(1); }
  console.log(`  team_id: ${teamId}`);

  await migrateCustomers(teamId);
  await migrateDeals(teamId);

  // DB容量確認
  const { rows } = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM customers WHERE team_id=$1) AS customers,
      (SELECT COUNT(*) FROM deals WHERE team_id=$1) AS deals,
      pg_size_pretty(pg_database_size(current_database())) AS db_size
  `, [teamId]);
  console.log('\n✅ 完了');
  console.log(`  customers: ${rows[0].customers}件`);
  console.log(`  deals:     ${rows[0].deals}件`);
  console.log(`  DB容量:    ${rows[0].db_size}`);

  await pool.end();
}

main().catch(e => { console.error('❌ エラー:', e.message); pool.end(); process.exit(1); });
