// kintone連携 API（検索・同期）
const { dbEnsureKintoneSchema, dbUpsertKintoneRecords, dbSearchKintoneCompanies, dbGetKintoneLastSync, dbGetKintoneRecord } = require('../db/kintone');
const { syncKintoneApp, syncKintonePayments, syncKintoneActivities, syncMediaMaster, syncAnStudies, APPS } = require('./kintone-sync');
const { dbQuery } = require('../db/index');

// kintone App102 フィールド → deals カラム のマッピング定義
const KINTONE_DEAL_FIELD_MAP = {
  // 基本
  '案件名':                             'name',
  'ヨミ':                               'yomi',
  'ヨミ_2':                             'contract_type',
  'ヨミ_経過フロー':                     'yomi_flow',
  'この案件の事業':                      'business_category',
  // 担当
  '担当営業_0':                         'sales_user_id',
  '担当営業':                           'na_user_id',
  '商談獲得者':                          'acquisition_person',
  // 日付
  '受注日':                             'order_date',
  '結論日':                             'conclusion_date',
  '初回商談日_コンサルチーム':           'first_meeting_date',
  '商談獲得日_マーケチーム':             'acquisition_date',
  '流入日':                             'inflow_date',
  'IS用最終対応日付':                    'inflow_date',
  'NextAction日':                       'next_action_date',
  'NA内容':                             'next_action_content',
  '営業メモ最終日付':                    'sales_memo_last_date',
  // 金額
  '見込売り上げ_税抜き':                 'initial_fee',
  '_1ヶ月or1名_当たりの単価_税抜き':    'guarantee_salary',
  '契約月数or採用人数_税抜き':          'contract_months',
  'ヨミ_見込売り上げ_税抜き':            'yomi_expected_amount',
  'ヨミ金額_初期費用_税抜き':            'yomi_initial_fee',
  'ヨミ金額_月額費用_税抜き':            'yomi_monthly_fee',
  '初期請求費用_税抜き':                 'initial_cost',
  'ディスカウント金額':                  'discount_amount',
  // 流入
  '流入経路':                           'inflow_source',
  // BANT
  'Budget_予算':                        'bant_budget',
  'Budget_予算_概要':                   'bant_budget_memo',
  'Authority_決済権':                   'bant_authority',
  'Authority_決済権_概要':              'bant_authority_memo',
  'Needs_ニーズ':                       'bant_needs',
  'Needs_ニーズ_概要':                  'bant_needs_memo',
  'Timeframe_導入時期':                 'bant_timeframe',
  'Timeframe_導入時期_概要':            'bant_timeframe_memo',
  // 失注
  '失注理由':                           'lost_reason',
  // 契約事務／コンプラ
  '反社チェック完了':                    'antisocial_check_done',
  '反社チェック実行日':                  'antisocial_check_date',
  'リーガルチェック完了':                'legal_check_done',
  'リーガルチェック実行日':              'legal_check_date',
  '契約稟議完了日':                      'contract_approval_date',
  '契約書送付完了':                      'contract_send_done',
  '契約書送付日':                        'contract_send_date',
  '契約書送付先メルアド':                'contract_send_to_email',
  '契約書送付先宛名':                    'contract_send_to_name',
  '契約書':                             'contract_pdf',
  '契約書リンク':                        'contract_pdf_link',
  '提案書リンク':                        'proposal_link',
  // 支払
  '支払方式':                           'payment_type',
  '前払い':                             'advance_payment',
  // サブテーブル（deals.data に JSONB として保存）
  // 採用費用テーブル / 利益テーブル / 原価テーブル / RPO費用テーブル / 応募予測テーブル / 媒体選定テーブル / 事前情報テーブル / 人件費テーブル
};

// kintone_cache (App94) → customers / customer_contacts
// kintone_record_id をキーに UPSERT。会社名がマッチする既存 customer は kintone_record_id で紐付けして上書き
async function syncCustomersFromKintoneCache() {
  const teamId = 'T086C06L5V0';
  try {
    // 1) kintone_record_id 未設定で同名がある既存 customer に record_id を紐付け
    await dbQuery(`
      UPDATE customers c SET kintone_record_id = kc.record_id
      FROM kintone_cache kc
      WHERE kc.app_id='94' AND c.team_id=$1 AND c.kintone_record_id IS NULL
        AND c.name = kc.company_name
    `, [teamId]);

    // 2) 新規 customer 作成（kintone_record_id がまだ customers に無いもの）
    //    フィールドは extractValue 経由で文字列化済（複数選択や郵便番号など）
    const custIns = await dbQuery(`
      INSERT INTO customers (
        team_id, name, name_short, industry, prefecture, employee_count,
        website, postal_code, address, business_description, memo,
        inflow_date, inflow_source, inrevo_person, service_lp_url1, service_lp_url2,
        kintone_record_id
      )
      SELECT
        $1,
        kc.company_name,
        NULLIF(kc.data->>'文字列__1行_',''),
        NULLIF(kc.data->>'複数選択',''),
        NULLIF(kc.data->>'ドロップダウン_0',''),
        NULLIF(kc.data->>'ドロップダウン',''),
        NULLIF(kc.data->>'会社HPリンク',''),
        NULLIF(kc.data->>'郵便番号',''),
        NULLIF(kc.data->>'会社住所',''),
        NULLIF(kc.data->>'文字列__複数行__1',''),
        NULLIF(kc.data->>'文字列__複数行__0',''),
        NULLIF(kc.data->>'流入日','')::date,
        NULLIF(kc.data->>'流入経路',''),
        NULLIF(kc.data->>'INREVO担当者',''),
        NULLIF(kc.data->>'会社HPリンク_0',''),
        NULLIF(kc.data->>'会社HPリンク_1',''),
        kc.record_id
      FROM kintone_cache kc
      WHERE kc.app_id='94' AND kc.company_name IS NOT NULL AND kc.company_name <> ''
        AND NOT EXISTS (SELECT 1 FROM customers c WHERE c.team_id=$1 AND c.kintone_record_id=kc.record_id)
    `, [teamId]);

    // 3) 既存 customer 更新（kintone_record_id 一致）— 差分のある行のみ
    const custUpd = await dbQuery(`
      UPDATE customers c SET
        name                 = COALESCE(NULLIF(kc.company_name,''), c.name),
        name_short           = COALESCE(NULLIF(kc.data->>'文字列__1行_',''),    c.name_short),
        industry             = COALESCE(NULLIF(kc.data->>'複数選択',''),         c.industry),
        prefecture           = COALESCE(NULLIF(kc.data->>'ドロップダウン_0',''), c.prefecture),
        employee_count       = COALESCE(NULLIF(kc.data->>'ドロップダウン',''),   c.employee_count),
        website              = COALESCE(NULLIF(kc.data->>'会社HPリンク',''),     c.website),
        postal_code          = COALESCE(NULLIF(kc.data->>'郵便番号',''),         c.postal_code),
        address              = COALESCE(NULLIF(kc.data->>'会社住所',''),         c.address),
        business_description = COALESCE(NULLIF(kc.data->>'文字列__複数行__1',''), c.business_description),
        memo                 = COALESCE(NULLIF(kc.data->>'文字列__複数行__0',''), c.memo),
        inflow_date          = COALESCE(NULLIF(kc.data->>'流入日','')::date,     c.inflow_date),
        inflow_source        = COALESCE(NULLIF(kc.data->>'流入経路',''),         c.inflow_source),
        inrevo_person        = COALESCE(NULLIF(kc.data->>'INREVO担当者',''),     c.inrevo_person),
        service_lp_url1      = COALESCE(NULLIF(kc.data->>'会社HPリンク_0',''),   c.service_lp_url1),
        service_lp_url2      = COALESCE(NULLIF(kc.data->>'会社HPリンク_1',''),   c.service_lp_url2),
        updated_at           = now()
      FROM kintone_cache kc
      WHERE kc.app_id='94' AND c.team_id=$1 AND c.kintone_record_id=kc.record_id
        AND (
              c.name_short           IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'文字列__1行_',''),    c.name_short)
           OR c.industry             IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'複数選択',''),         c.industry)
           OR c.prefecture           IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'ドロップダウン_0',''), c.prefecture)
           OR c.employee_count       IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'ドロップダウン',''),   c.employee_count)
           OR c.website              IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'会社HPリンク',''),     c.website)
           OR c.postal_code          IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'郵便番号',''),         c.postal_code)
           OR c.address              IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'会社住所',''),         c.address)
           OR c.business_description IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'文字列__複数行__1',''), c.business_description)
           OR c.inflow_source        IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'流入経路',''),         c.inflow_source)
           OR c.inrevo_person        IS DISTINCT FROM COALESCE(NULLIF(kc.data->>'INREVO担当者',''),     c.inrevo_person)
        )
    `, [teamId]);

    // 4) 担当者サブテーブル → customer_contacts
    //    シンプル戦略: 同期実行時、対象 customer の既存 contacts は一旦削除 → 再投入
    //    （件数が少ない＆ kintone がマスタなので素直に置換）
    const contactSync = await dbQuery(`
      WITH parsed AS (
        SELECT c.id AS customer_id, c.team_id,
               jsonb_array_elements(
                 CASE WHEN jsonb_typeof((kc.data->>'担当者情報テーブル')::jsonb) = 'array'
                      THEN (kc.data->>'担当者情報テーブル')::jsonb
                      ELSE '[]'::jsonb END
               ) AS row
        FROM customers c
        JOIN kintone_cache kc ON kc.app_id='94' AND kc.record_id = c.kintone_record_id
        WHERE c.team_id=$1
          AND kc.data ? '担当者情報テーブル'
          AND (kc.data->>'担当者情報テーブル') NOT IN ('', 'null', '[]')
      ),
      del AS (
        DELETE FROM customer_contacts cc
        WHERE cc.team_id=$1
          AND cc.customer_id IN (SELECT DISTINCT customer_id FROM parsed)
        RETURNING 1
      )
      INSERT INTO customer_contacts (id, customer_id, team_id, last_name, first_name, furigana, position_title, department, email, phone, memo)
      SELECT gen_random_uuid()::text,
             p.customer_id, p.team_id,
             NULLIF(p.row->>'担当者名',''),       -- 姓+名を分割しない（kintoneは1フィールド）
             NULL,
             NULLIF(p.row->>'担当者名_ふりがな',''),
             NULLIF(p.row->>'役職',''),
             NULLIF(p.row->>'部署名',''),
             NULLIF(p.row->>'メールアドレス',''),
             NULLIF(p.row->>'電話番号',''),
             NULLIF(p.row->>'備考','')
      FROM parsed p
      WHERE COALESCE(NULLIF(p.row->>'担当者名',''), NULLIF(p.row->>'メールアドレス','')) IS NOT NULL
    `, [teamId]);

    console.log(`[kintone] customers sync: new+${custIns.rowCount} upd=${custUpd.rowCount} contacts_replaced=${contactSync.rowCount}`);
  } catch (e) {
    console.error('[kintone] customers sync error:', e.message);
  }
}

// kintone_cache から deals テーブルへマッピングを反映（record_id ベースの UPSERT）
// 動作:
//   1. customers: company_name でなければ新規作成
//   2. deals:     data->>'kintone_record_id' でマッチ。なければ新規作成。あれば全フィールド更新
//   3. ヨミから status を導出（受注/失注/見送り）
async function syncDealsFromKintoneCache() {
  const teamId = 'T086C06L5V0';
  try {
    // ヨミの（）以降を剥がす式 / status導出式（SQL内で共通利用）
    const YOMI = `TRIM(regexp_replace(COALESCE(kc.data->>'ヨミ',''), '[（(].*$', ''))`;
    const STATUS = `CASE ${YOMI}
        WHEN '受注' THEN 'won' WHEN '失注' THEN 'lost' WHEN '見送り' THEN 'dormant' ELSE 'active' END`;

    // App102 → deals マッピング定義（順序付き）
    //   type: 'text' | 'date' | 'numeric'
    //   kc:   kintone_cache.data の取り出し式（標準は data->>'field'。複雑なものは個別記述）
    const FIELDS = [
      // 日付
      { col: 'order_date',             kc: `kc.data->>'受注日'`,                          type: 'date' },
      { col: 'conclusion_date',        kc: `kc.data->>'結論日'`,                          type: 'date' },
      { col: 'first_meeting_date',     kc: `kc.data->>'初回商談日_コンサルチーム'`,         type: 'date' },
      { col: 'acquisition_date',       kc: `kc.data->>'商談獲得日_マーケチーム'`,           type: 'date' },
      { col: 'inflow_date',            kc: `COALESCE(NULLIF(kc.data->>'流入日',''), kc.data->>'商談獲得日_マーケチーム')`, type: 'date' },
      { col: 'next_action_date',       kc: `kc.data->>'NextAction日'`,                   type: 'date' },
      { col: 'next_action_content',    kc: `kc.data->>'NA内容'`,                          type: 'text' },
      { col: 'yomi_flow',              kc: `kc.data->>'ヨミ_経過フロー'`,                 type: 'text' },
      { col: 'sales_memo_last_date',   kc: `kc.data->>'営業メモ最終日付'`,                  type: 'date' },
      { col: 'antisocial_check_date',  kc: `kc.data->>'反社チェック実行日'`,                type: 'date' },
      { col: 'legal_check_date',       kc: `kc.data->>'リーガルチェック実行日'`,            type: 'date' },
      { col: 'contract_approval_date', kc: `kc.data->>'契約稟議完了日'`,                   type: 'date' },
      { col: 'contract_send_date',     kc: `kc.data->>'契約書送付日'`,                     type: 'date' },
      // 金額
      { col: 'initial_fee',            kc: `kc.data->>'見込売り上げ_税抜き'`,              type: 'numeric' },
      { col: 'guarantee_salary',       kc: `kc.data->>'_1ヶ月or1名_当たりの単価_税抜き'`, type: 'numeric' },
      { col: 'contract_months',        kc: `kc.data->>'契約月数or採用人数_税抜き'`,        type: 'numeric' },
      { col: 'yomi_expected_amount',   kc: `kc.data->>'ヨミ_見込売り上げ_税抜き'`,         type: 'numeric' },
      { col: 'yomi_initial_fee',       kc: `kc.data->>'ヨミ金額_初期費用_税抜き'`,         type: 'numeric' },
      { col: 'yomi_monthly_fee',       kc: `kc.data->>'ヨミ金額_月額費用_税抜き'`,         type: 'numeric' },
      { col: 'initial_cost',           kc: `kc.data->>'初期請求費用_税抜き'`,              type: 'numeric' },
      { col: 'discount_amount',        kc: `kc.data->>'ディスカウント金額'`,               type: 'numeric' },
      // テキスト
      { col: 'inflow_source',          kc: `kc.data->>'流入経路'`,                         type: 'text' },
      { col: 'contract_type',          kc: `REPLACE(COALESCE(kc.data->>'ヨミ_2',''),'採用保証','一括払い')`, type: 'text' },
      { col: 'lost_reason',            kc: `kc.data->>'失注理由'`,                         type: 'text' },
      { col: 'sales_person',           kc: `kc.data->>'担当営業_0'`,                       type: 'text' },
      { col: 'sales_user_id',          kc: `kc.data->>'担当営業_0'`,                       type: 'text' },
      { col: 'na_user_id',             kc: `kc.data->>'担当営業'`,                         type: 'text' },
      { col: 'acquisition_person',     kc: `kc.data->>'商談獲得者'`,                       type: 'text' },
      { col: 'business_category',      kc: `kc.data->>'この案件の事業'`,                   type: 'text' },
      { col: 'payment_type',           kc: `kc.data->>'支払方式'`,                         type: 'text' },
      { col: 'advance_payment',        kc: `kc.data->>'前払い'`,                           type: 'text' },
      // BANT
      { col: 'bant_budget',            kc: `kc.data->>'Budget_予算'`,                     type: 'text' },
      { col: 'bant_budget_memo',       kc: `kc.data->>'Budget_予算_概要'`,                type: 'text' },
      { col: 'bant_authority',         kc: `kc.data->>'Authority_決済権'`,                type: 'text' },
      { col: 'bant_authority_memo',    kc: `kc.data->>'Authority_決済権_概要'`,           type: 'text' },
      { col: 'bant_needs',             kc: `kc.data->>'Needs_ニーズ'`,                    type: 'text' },
      { col: 'bant_needs_memo',        kc: `kc.data->>'Needs_ニーズ_概要'`,               type: 'text' },
      { col: 'bant_timeframe',         kc: `kc.data->>'Timeframe_導入時期'`,              type: 'text' },
      { col: 'bant_timeframe_memo',    kc: `kc.data->>'Timeframe_導入時期_概要'`,         type: 'text' },
      // 契約事務
      { col: 'antisocial_check_done',  kc: `kc.data->>'反社チェック完了'`,                 type: 'text' },
      { col: 'legal_check_done',       kc: `kc.data->>'リーガルチェック完了'`,             type: 'text' },
      { col: 'contract_send_done',     kc: `kc.data->>'契約書送付完了'`,                   type: 'text' },
      { col: 'contract_send_to_email', kc: `kc.data->>'契約書送付先メルアド'`,             type: 'text' },
      { col: 'contract_send_to_name',  kc: `kc.data->>'契約書送付先宛名'`,                 type: 'text' },
      { col: 'contract_pdf',           kc: `kc.data->>'契約書'`,                           type: 'text' },
      { col: 'contract_pdf_link',      kc: `kc.data->>'契約書リンク'`,                     type: 'text' },
      { col: 'proposal_link',          kc: `kc.data->>'提案書リンク'`,                     type: 'text' },
    ];

    // 値式: 空文字を NULL に。型に応じてキャスト
    const valExpr = (f) => {
      const base = `NULLIF(${f.kc},'')`;
      if (f.type === 'date')    return `${base}::date`;
      if (f.type === 'numeric') return `${base}::numeric`;
      return base;
    };
    // UPDATE/INSERTで「空なら既存値保持」
    const coalesceExpr = (f) => `COALESCE(${valExpr(f)}, d.${f.col})`;
    const isDistinct  = (f) => `d.${f.col} IS DISTINCT FROM ${coalesceExpr(f)}`;

    // サブテーブル: deals.data の 'kintone_subtables' に格納
    const SUBTABLES = [
      '採用費用テーブル','利益テーブル','原価テーブル','RPO費用テーブル',
      '応募予測テーブル','媒体選定テーブル','事前情報テーブル','人件費テーブル',
    ];
    const subtableExpr = `
      jsonb_strip_nulls(jsonb_build_object(
        ${SUBTABLES.map(t => `'${t}', CASE
          WHEN kc.data->>'${t}' IS NULL OR kc.data->>'${t}' IN ('','null') THEN NULL
          WHEN jsonb_typeof((kc.data->>'${t}')::jsonb) = 'array' THEN (kc.data->>'${t}')::jsonb
          ELSE NULL END`).join(',\n        ')}
      ))
    `;

    // 1) customers 補完（company_name が customers に無ければ作成）
    const custIns = await dbQuery(`
      INSERT INTO customers (team_id, name)
      SELECT DISTINCT $1, kc.company_name
      FROM kintone_cache kc
      WHERE kc.app_id='102' AND kc.company_name IS NOT NULL AND kc.company_name <> ''
        AND NOT EXISTS (SELECT 1 FROM customers c WHERE c.team_id=$1 AND c.name=kc.company_name)
    `, [teamId]);

    // 2) 新規 deal 作成
    const insertCols = ['team_id','customer_id','name','yomi','status', ...FIELDS.map(f => f.col), 'data'];
    const insertVals = [
      `$1`,
      `c.id`,
      `COALESCE(NULLIF(kc.data->>'案件名',''), kc.company_name || '_' || kc.record_id)`,
      `NULLIF(${YOMI},'')`,
      STATUS,
      ...FIELDS.map(valExpr),
      `jsonb_build_object('kintone_record_id', kc.record_id, 'kintone_subtables', ${subtableExpr})`,
    ];
    const dealIns = await dbQuery(`
      INSERT INTO deals (${insertCols.join(', ')})
      SELECT ${insertVals.join(',\n             ')}
      FROM kintone_cache kc
      JOIN customers c ON c.team_id=$1 AND c.name=kc.company_name
      WHERE kc.app_id='102' AND kc.company_name IS NOT NULL AND kc.company_name <> ''
        AND NOT EXISTS (SELECT 1 FROM deals d WHERE d.team_id=$1 AND d.data->>'kintone_record_id'=kc.record_id)
    `, [teamId]);

    // 3) 既存 deal 更新（差分判定）
    const setClauses = [
      `yomi   = COALESCE(NULLIF(${YOMI},''), d.yomi)`,
      `status = ${STATUS}`,
      ...FIELDS.map(f => `${f.col} = ${coalesceExpr(f)}`),
      // dataは既存値とkintone由来サブテーブルをマージ
      `data   = COALESCE(d.data,'{}'::jsonb) || jsonb_build_object('kintone_subtables', ${subtableExpr})`,
      `updated_at = now()`,
    ];
    const distinctClauses = [
      `d.yomi          IS DISTINCT FROM COALESCE(NULLIF(${YOMI},''), d.yomi)`,
      `d.status        IS DISTINCT FROM (${STATUS})`,
      ...FIELDS.map(isDistinct),
    ];
    const dealUpd = await dbQuery(`
      UPDATE deals d SET ${setClauses.join(',\n      ')}
      FROM kintone_cache kc
      WHERE kc.app_id='102' AND d.team_id=$1 AND d.data->>'kintone_record_id'=kc.record_id
        AND ( ${distinctClauses.join('\n          OR ')} )
    `, [teamId]);

    console.log(`[kintone] deals upsert(bulk): customers+${custIns.rowCount} deals_new+${dealIns.rowCount} deals_upd=${dealUpd.rowCount}`);
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
    // customers テーブルへのフィールドマッピング（App94 → customers / customer_contacts）
    await syncCustomersFromKintoneCache();
    // deals テーブルへのフィールドマッピング
    await syncDealsFromKintoneCache();
    // kintone_payments（App170）同期
    await syncKintonePayments();
    // App103（活動履歴）同期（読み取り専用で TaskHub UI に表示）
    await syncKintoneActivities().catch(e => console.error('[kintone] activities sync error:', e.message));
    // App225（媒体マスタ）同期
    await syncMediaMaster().catch(e => console.error('[kintone] media_master sync error:', e.message));
    // App221（AN調査管理表）同期
    await syncAnStudies().catch(e => console.error('[kintone] an_studies sync error:', e.message));
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
