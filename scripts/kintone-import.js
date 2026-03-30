#!/usr/bin/env node
/**
 * kintone-import.js
 * KintoneからCRMへデータを移行するスクリプト
 *
 * 使い方:
 *   node scripts/kintone-import.js [--limit 100] [--dry-run]
 *
 * --dry-run: DBに書かず、取込予定データを表示するだけ
 * --limit N: 各アプリから取得する最大件数（デフォルト100）
 */

require('dotenv').config();
const { Pool } = require('pg');

const SUBDOMAIN = 'ca7n5wh2hfvv';
const TOKENS = {
  94:  'Yeeyud4zlzonWqp1D8LEyIFeY9mzWCqQJVihQbCP',
  102: '01jtfMuLD7b8d28JyBODFrMp7T2QHJK76pF0XkZq',
  103: 'cekSgk3whWbQU6KFOGhr7wnnFOiSp2hzRHQP0Zhz',
  170: 'F3j0G13EGAT2Sa5wZZ1x720pkpqHCcQiZpXPzkiJ',
};

const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = (() => { const i = args.indexOf('--limit'); return i >= 0 ? parseInt(args[i+1]) || 100 : 100; })();

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// ─────────────────────────────
// Kintone fetch helpers
// ─────────────────────────────
async function kintoneRecords(appId, query = '') {
  const base = `https://${SUBDOMAIN}.cybozu.com`;
  const q = encodeURIComponent(`order by $id desc limit ${LIMIT}${query ? ' where '+query : ''}`);
  const url = `${base}/k/v1/records.json?app=${appId}&query=${q}`;
  const res = await fetch(url, { headers: { 'X-Cybozu-API-Token': TOKENS[appId] } });
  if (!res.ok) { const t = await res.text(); throw new Error(`kintone ${appId}: ${res.status} ${t}`); }
  const data = await res.json();
  return data.records || [];
}

function val(rec, field) {
  return rec[field]?.value ?? null;
}

function numVal(rec, field) {
  const v = val(rec, field);
  return (v !== null && v !== '') ? Number(v) : null;
}

function dateVal(rec, field) {
  const v = val(rec, field);
  return (v && v !== '1970-01-01' && v !== '') ? v : null;
}

function boolVal(rec, field) {
  const v = rec[field]?.value;
  return Array.isArray(v) ? v.length > 0 : Boolean(v);
}

function checkVal(rec, field, matchValue) {
  const v = rec[field]?.value;
  if (!Array.isArray(v)) return false;
  if (matchValue) return v.includes(matchValue);
  return v.length > 0;
}

// ヨミ → ステージマッピング
function yomiToStage(yomi) {
  if (!yomi) return 'mk';
  if (yomi.includes('アポ化前')) return 'mk';
  if (yomi.includes('アポ化済商談前') || yomi.includes('商談中') || yomi.includes('BC') || yomi.includes('アポ化済')) return 'bc';
  if (yomi === '受注' || yomi === '受注済' || yomi.includes('S（90%）')) return 'contracted';
  if (yomi.includes('失注')) return 'lost';
  if (yomi.includes('HR') || yomi.includes('hr')) return 'hr';
  if (yomi.includes('ディレクション')) return 'direction';
  if (yomi.includes('CS') || yomi.includes('スカウト')) return 'cs';
  if (yomi.includes('完了')) return 'completed';
  // E/D/C/B/A/Sはほとんどが商談中
  return 'bc';
}

// ─────────────────────────────
// Main
// ─────────────────────────────
async function main() {
  console.log(`\n🚀 Kintone import — limit: ${LIMIT}, dry-run: ${DRY_RUN}\n`);

  // チームID取得
  const teamRes = await pool.query('SELECT id FROM teams LIMIT 1');
  if (!teamRes.rows.length) throw new Error('No teams found in DB');
  const teamId = teamRes.rows[0].id;
  console.log(`📌 Team ID: ${teamId}`);

  // ─── 1. App94 → clients ───────────────────────────────────
  console.log('\n📦 Fetching App94 (顧客情報)...');
  const app94 = await kintoneRecords(94);
  console.log(`   ${app94.length}件取得`);

  let clientImported = 0, clientSkipped = 0;
  const clientNameToId = {};  // 名前 → CRM ID のマップ（deals連携用）

  // 既存クライアント名をロード
  const existingClients = await pool.query('SELECT id, name FROM clients WHERE team_id=$1', [teamId]);
  const existingClientNames = new Set(existingClients.rows.map(r => r.name.trim()));
  existingClients.rows.forEach(r => { clientNameToId[r.name.trim()] = r.id; });

  for (const rec of app94) {
    const name = (val(rec, '会社名') || '').trim();
    if (!name) { clientSkipped++; continue; }

    if (existingClientNames.has(name)) {
      clientSkipped++;
      // IDはすでにマップに入っている
      continue;
    }

    const competition = (() => {
      const v = rec['チェックボックス_1']?.value || [];
      return v.length > 0 ? v : null;
    })();

    const industry = (() => {
      const v = rec['複数選択']?.value || [];
      return v.length > 0 ? v[0] : null;
    })();

    const clientData = {
      name,
      team_id: teamId,
      inrevo_person: val(rec, 'INREVO担当者'),
      industry,
      prefecture: val(rec, 'ドロップダウン_0'),
      employee_range: val(rec, 'ドロップダウン'),
      competition: competition ? JSON.stringify(competition) : null,
      corporate_url: val(rec, '会社HPリンク_0') || val(rec, '会社HPリンク') || null,
      service_url1: val(rec, '会社HPリンク_1') || null,
      notes: val(rec, '文字列__複数行__1') || null,
    };

    if (DRY_RUN) {
      console.log(`  [DRY] client: ${name}`);
      clientImported++;
      existingClientNames.add(name);
      clientNameToId[name] = `dry-${name}`;
      continue;
    }

    const ins = await pool.query(
      `INSERT INTO clients (team_id, name, inrevo_person, industry, prefecture, employee_range, competition, corporate_url, service_url1, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT DO NOTHING
       RETURNING id`,
      [teamId, clientData.name, clientData.inrevo_person, clientData.industry, clientData.prefecture,
       clientData.employee_range, clientData.competition, clientData.corporate_url, clientData.service_url1, clientData.notes]
    );

    if (ins.rows.length) {
      const clientId = ins.rows[0].id;
      clientNameToId[name] = clientId;
      existingClientNames.add(name);
      clientImported++;

      // 担当者サブテーブル
      const contacts = rec['担当者情報テーブル']?.value || [];
      for (let i = 0; i < contacts.length; i++) {
        const cv = contacts[i].value;
        const lastName = cv['担当者名']?.value || '';
        const firstName = cv['文字列__1行__0']?.value || '';
        if (!lastName && !firstName) continue;
        await pool.query(
          `INSERT INTO client_contacts (client_id, team_id, last_name, first_name, furigana, title, department, email, phone, notes, do_not_contact, sort_order)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
           ON CONFLICT DO NOTHING`,
          [clientId, teamId, lastName, firstName,
           cv['担当者名_ふりがな']?.value || null,
           cv['役職']?.value || null,
           cv['部署名']?.value || null,
           cv['メールアドレス']?.value || null,
           cv['電話番号']?.value || null,
           cv['備考']?.value || null,
           (cv['チェックボックス_0']?.value || []).length > 0,
           i]
        );
      }
    } else {
      clientSkipped++;
    }
  }

  console.log(`   ✅ clients: ${clientImported}件追加, ${clientSkipped}件スキップ`);

  // ─── 2. App102 → deals ───────────────────────────────────
  console.log('\n📦 Fetching App102 (案件情報)...');
  const app102 = await kintoneRecords(102);
  console.log(`   ${app102.length}件取得`);

  // 既存deal名をロード（重複スキップ用）
  const existingDeals = await pool.query('SELECT name FROM deals WHERE team_id=$1', [teamId]);
  const existingDealNames = new Set(existingDeals.rows.map(r => r.name.trim()));

  let dealImported = 0, dealSkipped = 0;

  for (const rec of app102) {
    const name = (val(rec, '案件名') || '').trim();
    if (!name) { dealSkipped++; continue; }

    if (existingDealNames.has(name)) { dealSkipped++; continue; }

    const clientName = (val(rec, '顧客') || '').trim();
    const clientId = clientNameToId[clientName] || null;

    if (!clientId && !DRY_RUN) {
      // クライアントが見つからなければスキップ（または自動作成）
      console.log(`  ⚠️  client not found for deal: ${name} (client: ${clientName})`);
      dealSkipped++;
      continue;
    }

    const yomi = val(rec, 'ヨミ');
    const stage = yomiToStage(yomi);

    const dealData = {
      team_id: teamId,
      client_id: clientId,
      name,
      stage,
      yomi,
      sales_person: val(rec, '担当営業_0') || val(rec, '担当営業') || null,
      acquisition_person: val(rec, '商談獲得者'),
      payment_method: val(rec, '支払方式'),
      first_meeting_date: dateVal(rec, '初回商談日_コンサルチーム'),
      acquisition_date: dateVal(rec, '商談獲得日_マーケチーム'),
      contract_approval_date: dateVal(rec, '契約稟議完了日'),
      contract_send_date: dateVal(rec, '契約書送付日'),
      order_date: dateVal(rec, '受注日'),
      conclusion_date: dateVal(rec, '結論日'),
      next_action_date: dateVal(rec, 'NextAction日'),
      next_action_detail: val(rec, 'NA内容'),
      initial_cost: numVal(rec, '初期請求費用_税抜き'),
      unit_price: numVal(rec, '_1ヶ月or1名_当たりの単価_税抜き'),
      contract_months: numVal(rec, '契約月数or採用人数_税抜き'),
      budget_confirmed: boolVal(rec, 'Budget_予算'),
      budget_detail: val(rec, 'Budget_予算_概要'),
      authority_confirmed: boolVal(rec, 'Authority_決済権'),
      authority_detail: val(rec, 'Authority_決済権_概要'),
      needs_confirmed: boolVal(rec, 'Needs_ニーズ'),
      needs_detail: val(rec, 'Needs_ニーズ_概要'),
      timeframe_confirmed: boolVal(rec, 'Timeframe_導入時期'),
      timeframe_detail: val(rec, 'Timeframe_導入時期_概要'),
      anti_social_check: boolVal(rec, '反社チェック完了'),
      legal_check: boolVal(rec, 'リーガルチェック完了'),
      contract_approval_done: boolVal(rec, '契約稟議完了'),
      loss_reason_detail: val(rec, '失注理由') || null,
      invoice_name: val(rec, '請求書送付先宛名') || val(rec, '契約書送付先宛名') || null,
      notes: val(rec, '文字列__複数行_'),
      sales_memo: val(rec, '案件最新状況'),
    };

    if (DRY_RUN) {
      console.log(`  [DRY] deal: ${name} | client: ${clientName} | stage: ${stage} | yomi: ${yomi}`);
      dealImported++;
      existingDealNames.add(name);
      continue;
    }

    const ins = await pool.query(
      `INSERT INTO deals (
        team_id, client_id, name, stage, yomi, sales_person, acquisition_person,
        payment_method, first_meeting_date, acquisition_date, contract_approval_date,
        contract_send_date, order_date, conclusion_date, next_action_date, next_action_detail,
        initial_cost, unit_price, contract_months,
        budget_confirmed, budget_detail, authority_confirmed, authority_detail,
        needs_confirmed, needs_detail, timeframe_confirmed, timeframe_detail,
        anti_social_check, legal_check, contract_approval_done,
        loss_reason_detail, invoice_name, notes, sales_memo
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,
        $8,$9,$10,$11,
        $12,$13,$14,$15,$16,
        $17,$18,$19,
        $20,$21,$22,$23,
        $24,$25,$26,$27,
        $28,$29,$30,
        $31,$32,$33,$34
      ) ON CONFLICT DO NOTHING RETURNING id`,
      [
        dealData.team_id, dealData.client_id, dealData.name, dealData.stage, dealData.yomi,
        dealData.sales_person, dealData.acquisition_person,
        dealData.payment_method, dealData.first_meeting_date, dealData.acquisition_date,
        dealData.contract_approval_date, dealData.contract_send_date, dealData.order_date,
        dealData.conclusion_date, dealData.next_action_date, dealData.next_action_detail,
        dealData.initial_cost, dealData.unit_price, dealData.contract_months,
        dealData.budget_confirmed, dealData.budget_detail, dealData.authority_confirmed, dealData.authority_detail,
        dealData.needs_confirmed, dealData.needs_detail, dealData.timeframe_confirmed, dealData.timeframe_detail,
        dealData.anti_social_check, dealData.legal_check, dealData.contract_approval_done,
        dealData.loss_reason_detail, dealData.invoice_name, dealData.notes, dealData.sales_memo,
      ]
    );

    if (ins.rows.length) {
      dealImported++;
      existingDealNames.add(name);
    } else {
      dealSkipped++;
    }
  }

  console.log(`   ✅ deals: ${dealImported}件追加, ${dealSkipped}件スキップ`);

  // ─── 3. App170 → payments ─────────────────────────────────
  console.log('\n📦 Fetching App170 (入金管理)...');
  const app170 = await kintoneRecords(170);
  console.log(`   ${app170.length}件取得`);

  // 案件名 → deal_id のマップを再構築
  const dealNameRes = await pool.query('SELECT id, name FROM deals WHERE team_id=$1', [teamId]);
  const dealNameToId = {};
  dealNameRes.rows.forEach(r => { dealNameToId[r.name.trim()] = r.id; });

  let payImported = 0, paySkipped = 0;

  for (const rec of app170) {
    const dealName = (val(rec, '案件名') || '').trim();
    const dealId = dealNameToId[dealName];
    if (!dealId) { paySkipped++; continue; }

    const amount = numVal(rec, '金額');
    if (!amount) { paySkipped++; continue; }

    const directionRaw = rec['入出金']?.value || [];
    const direction = directionRaw.includes('出金') ? '出金' : '入金';
    const label = val(rec, '契約プラン') || '入金';
    const dueDate = dateVal(rec, '入金予定日');
    const paidDate = dateVal(rec, '入金日');
    const status = paidDate ? 'paid' : 'pending';
    const invoiceSent = boolVal(rec, '請求書発行状況');
    const incentive = numVal(rec, 'インセン金額');

    if (DRY_RUN) {
      console.log(`  [DRY] payment: ${dealName} | ${direction} ¥${amount} | ${label}`);
      payImported++;
      continue;
    }

    await pool.query(
      `INSERT INTO deal_payments (deal_id, team_id, label, amount, direction, due_date, paid_date, status, invoice_sent, incentive_amount)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [dealId, teamId, label, amount, direction, dueDate, paidDate, status, invoiceSent, incentive]
    );
    payImported++;
  }

  console.log(`   ✅ payments: ${payImported}件追加, ${paySkipped}件スキップ`);

  // ─── Summary ─────────────────────────────────────────────
  console.log('\n' + '─'.repeat(50));
  console.log(`✅ 完了${DRY_RUN ? ' (dry-run)' : ''}`);
  console.log(`   顧客:   ${clientImported}件`);
  console.log(`   案件:   ${dealImported}件`);
  console.log(`   入金:   ${payImported}件`);

  await pool.end();
}

main().catch(e => { console.error('❌ Error:', e.message); process.exit(1); });
