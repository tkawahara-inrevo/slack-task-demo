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
// App225専用トークン（媒体マスタ）
const APP_225_TOKEN = process.env.KINTONE_APP_225_TOKEN || 'N3ztvAhgRFJu0yzCYz5FhFLn8Hi4IzTqTrLamCzC';
// App221専用トークン（AN調査管理表）
const APP_221_TOKEN = process.env.KINTONE_APP_221_TOKEN || 'LXmG25yThDbXYsWTDu2HEEu8sEGlXuNooQ23lkbt';

// App225用: 業種/職種カテゴリ判定（フィールド名がそのままカテゴリ名）
const APP225_INDUSTRIES = [
  '農業・林業','漁業','鉱業・採石業・砂利採取業','建設業','製造業',
  '電気・ガス・熱供給・水道業','情報通信業','運輸業・郵便業','卸売業・小売業',
  '金融業・保険業','不動産業・物品賃貸業','学術研究・専門・技術サービス業',
  '宿泊業・飲食サービス業','生活関連サービス業・娯楽業','教育・学習支援業',
  '医療・福祉','複合サービス事業','サービス業','公務','その他'
];
const APP225_JOBS = [
  '経営・企画','事務・管理','専門職','営業',
  'IT・Webエンジニア','マーケティング・クリエイティブ',
  '技術_電気・機械・化学','建築・土木','販売・サービス'
];

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
    const plan         = d.plan ? String(d.plan).replace(/採用保証/g, '一括払い') : null;
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

// App225（媒体マスタ）→ media_master テーブルへ同期
async function syncMediaMaster() {
  const { dbQuery } = require('../db/index');
  if (!APP_225_TOKEN) {
    console.warn('[kintone] APP_225 token not set, skipping media_master sync');
    return 0;
  }

  let offset = 0, upserted = 0;
  const seenIds = new Set();
  while (true) {
    const data = await kintoneGet(225, APP_225_TOKEN, [], offset);
    if (data.code) throw new Error(`kintone error [app225]: ${data.code} - ${data.message}`);
    if (!data.records || data.records.length === 0) break;

    for (const rec of data.records) {
      const get = (k) => rec[k]?.value ?? null;
      const getStr = (k) => {
        const v = get(k);
        if (Array.isArray(v)) return v.map(x => x.name || x.code || x).join(', ');
        return v == null ? null : String(v);
      };
      const getArr = (k) => {
        const v = get(k);
        return Array.isArray(v) ? v.filter(Boolean) : [];
      };
      const isChecked = (k) => {
        const v = get(k);
        return Array.isArray(v) && v.length > 0;
      };

      const recordId = String(rec['$id']?.value || '');
      if (!recordId) continue;
      seenIds.add(recordId);

      // 業種・職種チェックボックスから true のものだけ抽出
      const industries = APP225_INDUSTRIES.filter(isChecked);
      const job_types  = APP225_JOBS.filter(isChecked);

      // 全フィールドを data に保存（値は plain）
      const rawData = {};
      for (const [k, fv] of Object.entries(rec)) {
        if (k.startsWith('$')) continue;
        rawData[k] = fv?.value ?? null;
      }

      const score = Number(getStr('オススメ度'));
      await dbQuery(`
        INSERT INTO media_master
          (record_id, team_id, name, vendor_url, recommend_score, service_type,
           hire_methods, areas, age_targets, industries, job_types, employment_types,
           basic_billing, norma, notes, caution, data, synced_at)
        VALUES ($1,'T086C06L5V0',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb, now())
        ON CONFLICT (record_id) DO UPDATE SET
          name=$2, vendor_url=$3, recommend_score=$4, service_type=$5,
          hire_methods=$6, areas=$7, age_targets=$8, industries=$9, job_types=$10, employment_types=$11,
          basic_billing=$12, norma=$13, notes=$14, caution=$15, data=$16::jsonb, synced_at=now()
      `, [
        recordId,
        getStr('媒体名'),
        getStr('公式HP'),
        Number.isFinite(score) ? score : null,
        getStr('種別'),
        getArr('採用手法'),
        getArr('エリア'),
        getArr('利用者年齢層'),
        industries,
        job_types,
        getArr('対象区分'),
        getStr('基本請求先'),
        getStr('ノルマ'),
        getStr('媒体備考'),
        getStr('注意事項'),
        JSON.stringify(rawData),
      ]).catch(e => console.error('[media_master] upsert err:', e.message));
      upserted++;
    }
    if (data.records.length < 500) break;
    offset += 500;
  }

  // kintone側で削除されたものをDBからも削除
  if (seenIds.size > 0) {
    const ids = Array.from(seenIds);
    const del = await dbQuery(
      `DELETE FROM media_master WHERE record_id NOT IN (${ids.map((_,i)=>`$${i+1}`).join(',')})`,
      ids
    );
    if (del.rowCount > 0) console.log(`[media_master] removed ${del.rowCount} stale records`);
  }
  console.log(`[kintone] media_master upserted: ${upserted}`);
  return upserted;
}

// App221（AN調査管理表）→ an_studies + an_study_media へ正規化同期
async function syncAnStudies() {
  const { dbQuery } = require('../db/index');
  if (!APP_221_TOKEN) {
    console.warn('[kintone] APP_221 token not set, skipping an_studies sync');
    return 0;
  }
  let offset = 0, upserted = 0;
  const seenIds = new Set();
  const num = (v) => (v == null || v === '') ? null : Number(v);
  const txt = (v) => {
    if (v == null) return null;
    if (Array.isArray(v)) return v.map(x => x.name || x.code || x).join(', ');
    return String(v);
  };
  const arr = (v) => Array.isArray(v) ? v.filter(Boolean) : [];

  while (true) {
    const data = await kintoneGet(221, APP_221_TOKEN, [], offset);
    if (data.code) throw new Error(`kintone error [app221]: ${data.code} - ${data.message}`);
    if (!data.records || data.records.length === 0) break;

    for (const rec of data.records) {
      const get = (k) => rec[k]?.value ?? null;
      const recordId = String(rec['$id']?.value || '');
      if (!recordId) continue;
      seenIds.add(recordId);

      // 全フィールドを data に保持
      const rawData = {};
      for (const [k, fv] of Object.entries(rec)) {
        if (k.startsWith('$')) continue;
        rawData[k] = fv?.value ?? null;
      }

      await dbQuery(`
        INSERT INTO an_studies
          (record_id, team_id, company_name, case_link, slack_link, status, priority,
           request_date, requester, must_condition, other_notes,
           work_locations, max_salary, min_salary, annual_holidays,
           employment_type, job_type, target_classification,
           jobform_url, total_effective_apps, data, kintone_updated_at, synced_at)
        VALUES ($1,'T086C06L5V0',$2,$3,$4,$5,$6, NULLIF($7,'')::date, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::jsonb, NULLIF($21,'')::timestamptz, now())
        ON CONFLICT (record_id) DO UPDATE SET
          company_name=$2, case_link=$3, slack_link=$4, status=$5, priority=$6,
          request_date=NULLIF($7,'')::date, requester=$8, must_condition=$9, other_notes=$10,
          work_locations=$11, max_salary=$12, min_salary=$13, annual_holidays=$14,
          employment_type=$15, job_type=$16, target_classification=$17,
          jobform_url=$18, total_effective_apps=$19, data=$20::jsonb,
          kintone_updated_at=NULLIF($21,'')::timestamptz, synced_at=now()
      `, [
        recordId,
        txt(get('企業名')),
        txt(get('案件情報リンク')),
        txt(get('Slack')),
        txt(get('完了チェック')),
        txt(get('優先度')),
        txt(get('依頼発生日')) || '',
        txt(get('依頼者')),
        txt(get('MUTS条件')),
        txt(get('その他特記事項')),
        arr(get('勤務地')),
        num(txt(get('上限年収'))),
        num(txt(get('下限年収'))),
        num(txt(get('年間休日'))),
        txt(get('雇用形態')),
        txt(get('職種')),
        arr(get('対象区分')),
        txt(get('求人票リンク')),
        num(txt(get('有効応募数'))),
        JSON.stringify(rawData),
        txt(get('更新日時')) || '',
      ]).catch(e => console.error('[an_studies] upsert err:', e.message));

      // media slots 1-6: 既存削除→挿入
      await dbQuery(`DELETE FROM an_study_media WHERE study_record_id=$1`, [recordId]).catch(() => {});
      for (let i = 1; i <= 6; i++) {
        const mediaName  = txt(get(`媒体名${i}`));
        const fee        = num(txt(get(`料金${i}`)));
        const expected   = num(txt(get(`応募想定${i}`)));
        // 媒体名もfeeも応募想定も空ならスロット未使用 → スキップ
        if (!mediaName && !fee && !expected) continue;
        await dbQuery(`
          INSERT INTO an_study_media
            (team_id, study_record_id, slot, media_name, cost_category, fee, duration, weekly_calc,
             responses, active_count, expected_apps, reply_rate,
             effective_apps, effective_rate, status_tags, note, an_assignee)
          VALUES ('T086C06L5V0',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        `, [
          recordId, i, mediaName,
          txt(get(`費用区分${i}`)),
          fee,
          num(txt(get(`掲載期間${i}`))),
          num(txt(get(`週計算${i}`))),
          arr(get(`対応${i}`)),
          txt(get(`アクティブ数${i}`)),
          expected,
          num(txt(get(`返信率${i}`))),
          num(txt(get(`有効応募数_${i}`))),
          num(txt(get(`有効応募率_${i}`))),
          arr(get(`ステータス${i}`)),
          txt(get(`備考${i}`)),
          txt(get(`AN担当者${i}`)),
        ]).catch(e => console.error('[an_study_media] insert err:', e.message));
      }
      upserted++;
    }
    if (data.records.length < 500) break;
    offset += 500;
  }

  // 削除されたものを除去
  if (seenIds.size > 0) {
    const ids = Array.from(seenIds);
    const del = await dbQuery(
      `DELETE FROM an_studies WHERE record_id NOT IN (${ids.map((_,i)=>`$${i+1}`).join(',')})`,
      ids
    );
    if (del.rowCount > 0) console.log(`[an_studies] removed ${del.rowCount} stale records`);
  }
  console.log(`[kintone] an_studies upserted: ${upserted}`);
  return upserted;
}

module.exports = { syncKintoneApp, syncKintonePayments, syncKintoneActivities, syncMediaMaster, syncAnStudies, APPS };
