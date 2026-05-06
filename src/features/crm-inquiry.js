// CRM問い合わせ自動登録
// all-問い合わせ報告チャンネル (C086WBPGYAX) のメッセージを監視し
// Wixフォームの問い合わせを顧客+商談として自動登録する
const { dbQuery } = require('../db/index');
const { randomUUID } = require('crypto');

const INQUIRY_CHANNEL_ID = process.env.INQUIRY_CHANNEL_ID || 'C086WBPGYAX';

// Wix フォームメッセージをパースして顧客情報を抽出する
// ※ 実際のSlackメッセージ形式を確認後に精度向上
function parseWixMessage(text, blocks) {
  const result = {
    companyName: null,
    contactName: null,
    email: null,
    phone: null,
    inflowSource: null,
    planType: null,   // 問い合わせフォーム種別（成果保証・上など）
    raw: text || '',
  };

  // Slackのブロック形式を優先してパース
  if (blocks && blocks.length > 0) {
    const fullText = blocks
      .flatMap(b => b.fields || [b.text])
      .filter(Boolean)
      .map(f => f?.text || '')
      .join('\n');
    return parseTextFields(fullText, result);
  }

  return parseTextFields(text || '', result);
}

function parseTextFields(text, result) {
  // Wix / Studio.site フォームの実際のフィールド名に対応
  const patterns = [
    // 会社名（Wix: 貴社名、Studio: 会社名/企業名）
    [/貴社名[：:\s]*(.+)/,         'companyName'],
    [/会社名[：:\s]*(.+)/,         'companyName'],
    [/企業名[：:\s]*(.+)/,         'companyName'],
    [/社名[：:\s]*(.+)/,           'companyName'],
    [/Company[：:\s]*(.+)/i,       'companyName'],
    // 担当者名
    [/お名前[：:\s]*(.+)/,         'contactName'],
    [/担当者名?[：:\s]*(.+)/,      'contactName'],
    [/氏名[：:\s]*(.+)/,           'contactName'],
    [/Name[：:\s]*(.+)/i,          'contactName'],
    // メール
    [/メールアドレス[：:\s]*(.+)/, 'email'],
    [/メール[：:\s]*(.+)/,         'email'],
    [/Email[：:\s]*(.+)/i,         'email'],
    [/e-mail[：:\s]*(.+)/i,        'email'],
    // 電話
    [/電話番号[：:\s]*(.+)/,       'phone'],
    [/電話[：:\s]*(.+)/,           'phone'],
    [/TEL[：:\s]*(.+)/i,           'phone'],
    [/Phone[：:\s]*(.+)/i,         'phone'],
    // 流入経路
    [/知ったきっかけ[：:\s]*(.+)/, 'inflowSource'],
    [/流入経路[：:\s]*(.+)/,        'inflowSource'],
    [/きっかけ[：:\s]*(.+)/,       'inflowSource'],
  ];

  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    for (const [pattern, field] of patterns) {
      if (!result[field]) {
        const m = trimmed.match(pattern);
        if (m) result[field] = m[1].trim().replace(/\*+/g, '').trim();
      }
    }
  }

  // メールアドレスを正規表現で拾う（フォームがない場合）
  if (!result.email) {
    const emailMatch = text.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
    if (emailMatch) result.email = emailMatch[0];
  }

  return result;
}

// メッセージから MK チャンネルか問い合わせかを判定
function isWixInquiry(message) {
  const text = message.text || '';
  // Wix または Studio.site フォームの通知
  return text.includes('Wix') ||
         text.includes('Studio') ||
         text.includes('フォーム') ||
         text.includes('問い合わせ') ||
         text.includes('new response') ||
         text.includes('貴社名') ||
         (message.bot_profile?.name?.toLowerCase().includes('email')) ||
         (message.username?.toLowerCase().includes('email'));
}

// CRMに顧客+商談を自動登録
async function registerInquiryAsCRM(teamId, parsed, sourceText) {
  const companyName = parsed.companyName || '不明（問い合わせ）';

  try {
    // 既存顧客チェック（会社名またはメール）
    let customerId;
    const { rows: existing } = await dbQuery(
      `SELECT id FROM customers WHERE team_id=$1 AND (name=$2 OR (name ILIKE $3))`,
      [teamId, companyName, `%${companyName}%`]
    );

    if (existing.length > 0) {
      customerId = existing[0].id;
      console.log(`[CRM Inquiry] existing customer: ${customerId}`);
    } else {
      // 新規顧客作成
      const { rows: [customer] } = await dbQuery(`
        INSERT INTO customers (id, team_id, name, memo, created_by)
        VALUES ($1, $2, $3, $4, 'system') RETURNING id
      `, [randomUUID(), teamId, companyName,
          `問い合わせフォームから自動登録\n${parsed.contactName ? '担当者: ' + parsed.contactName : ''}\n${parsed.email || ''}\n${parsed.phone || ''}`]);
      customerId = customer.id;
      console.log(`[CRM Inquiry] created customer: ${customerId}`);
    }

    // 商談作成（アポ化前 で追加）
    const dealName = `${companyName}（問い合わせ）`;
    const memo = [
      'Slack問い合わせ報告から自動登録',
      parsed.contactName ? `担当者: ${parsed.contactName}` : null,
      parsed.email ? `メール: ${parsed.email}` : null,
      parsed.phone ? `電話: ${parsed.phone}` : null,
      parsed.inflowSource ? `きっかけ: ${parsed.inflowSource}` : null,
      '',
      sourceText?.slice(0, 400) || '',
    ].filter(v => v !== null).join('\n');

    const { rows: [deal] } = await dbQuery(`
      INSERT INTO deals (id, team_id, customer_id, name, yomi, status, inflow_source, inflow_date, memo, created_by)
      VALUES ($1, $2, $3, $4, 'アポ化前', 'active', $5, CURRENT_DATE, $6, 'system')
      RETURNING id
    `, [randomUUID(), teamId, customerId, dealName,
        parsed.inflowSource || '問い合わせフォーム', memo]);

    if (deal) {
      console.log(`[CRM Inquiry] created deal: ${deal.id}`);
      return { ok: true, customerId, dealId: deal.id, companyName };
    }
    return { ok: true, customerId, dealId: null, companyName };
  } catch (e) {
    console.error('[CRM Inquiry] registration error:', e.message);
    return { ok: false, error: e.message };
  }
}

// メインの問い合わせハンドラー（Slack app.message から呼び出す）
async function handleInquiryMessage(message, client) {
  if (!isWixInquiry(message)) return;

  // team_id を取得（最初のチームを使用）
  const { rows: teamRows } = await dbQuery(
    `SELECT DISTINCT team_id FROM customers LIMIT 1`
  );
  const teamId = teamRows[0]?.team_id;
  if (!teamId) { console.warn('[CRM Inquiry] no team_id found'); return; }

  const parsed = parseWixMessage(message.text, message.blocks);
  console.log('[CRM Inquiry] parsed:', JSON.stringify(parsed));

  if (!parsed.companyName && !parsed.email) {
    console.log('[CRM Inquiry] could not extract company/email, skipping');
    return;
  }

  const result = await registerInquiryAsCRM(teamId, parsed, message.text);

  if (result.ok) {
    // Slackでリアクションを付けて処理済みを示す
    try {
      await client.reactions.add({ channel: message.channel, timestamp: message.ts, name: 'white_check_mark' });
    } catch (_) {}
    console.log(`[CRM Inquiry] registered: ${result.companyName}`);
  }
}

module.exports = { INQUIRY_CHANNEL_ID, handleInquiryMessage, parseWixMessage };
