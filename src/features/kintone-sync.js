// kintone APIからレコードを取得してキャッシュに同期する
const https = require('https');

const DOMAIN = process.env.KINTONE_DOMAIN || 'ca7n5wh2hfvv.cybozu.com';

// アプリごとの設定（$id は常に先頭に含める）
const APPS = {
  102: {
    token: process.env.KINTONE_APP_102_TOKEN || '01jtfMuLD7b8d28JyBODFrMp7T2QHJK76pF0XkZq',
    companyField: '顧客',
    fields: ['$id', '顧客', '支払方式', '受注日', '担当営業_0', '見込売り上げ_税抜き', '数値_0'],
  },
};

// kintone Records APIをGET
function kintoneGet(appId, token, fields, offset) {
  return new Promise((resolve, reject) => {
    const fieldParams = fields.map((f, i) => `fields[${i}]=${encodeURIComponent(f)}`).join('&');
    const query      = encodeURIComponent(`order by $id asc limit 500 offset ${offset}`);
    const path       = `/k/v1/records.json?app=${appId}&${fieldParams}&query=${query}`;

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
      for (const f of fields) {
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

module.exports = { syncKintoneApp, APPS };
