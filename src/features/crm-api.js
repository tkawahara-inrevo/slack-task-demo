// 顧客管理・商談管理 / 日報メンバー管理 API
const { randomUUID } = require('crypto');
const { dbQuery } = require('../db/index');

function registerDailyReportApi({ expressApp, authWithRole, slackClient }) {
  const USERGROUP_ID = process.env.DAILY_REPORT_USERGROUP_ID || process.env.CORP_SYSTEM_USERGROUP_ID || '';

  const requireCorpOrAdmin = (req, res, next) => {
    const role = req.dashboardUser?.role;
    if (role !== 'admin' && role !== 'corp') return res.status(403).json({ error: 'forbidden' });
    next();
  };

  // GET /api/dashboard/admin/daily-report/members — 一覧取得
  expressApp.get('/api/dashboard/admin/daily-report/members', authWithRole, requireCorpOrAdmin, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT * FROM daily_report_members WHERE team_id=$1 ORDER BY display_name ASC`,
        [teamId]
      );
      res.json({ members: rows, usergroupId: USERGROUP_ID });
    } catch (e) {
      console.error('[DailyReport] list error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // PATCH /api/dashboard/admin/daily-report/members/:userId — 対象フラグ切り替え
  expressApp.patch('/api/dashboard/admin/daily-report/members/:userId', authWithRole, requireCorpOrAdmin, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { isTarget } = req.body;
      await dbQuery(
        `UPDATE daily_report_members SET is_target=$3, updated_at=now() WHERE team_id=$1 AND user_id=$2`,
        [teamId, req.params.userId, !!isTarget]
      );
      res.json({ ok: true });
    } catch (e) {
      console.error('[DailyReport] toggle error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // POST /api/dashboard/admin/daily-report/sync — Slackユーザーグループから同期
  expressApp.post('/api/dashboard/admin/daily-report/sync', authWithRole, requireCorpOrAdmin, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      if (!USERGROUP_ID) return res.status(400).json({ error: 'DAILY_REPORT_USERGROUP_ID not set' });

      // ユーザーグループのメンバーを取得
      const ugRes = await slackClient.usergroups.users.list({ usergroup: USERGROUP_ID });
      const userIds = ugRes.users || [];

      // ユーザー情報を取得
      const usersRes = await slackClient.users.list({ limit: 500 });
      const userMap = {};
      for (const u of (usersRes.members || [])) {
        userMap[u.id] = {
          name: u.profile?.display_name || u.profile?.real_name || u.real_name || u.name || u.id,
          avatar: u.profile?.image_48 || null,
        };
      }

      // 既存の対象フラグを保持しつつupsert
      for (const uid of userIds) {
        const info = userMap[uid] || { name: uid, avatar: null };
        await dbQuery(`
          INSERT INTO daily_report_members (team_id, user_id, display_name, avatar_url, is_target)
          VALUES ($1, $2, $3, $4, true)
          ON CONFLICT (team_id, user_id) DO UPDATE SET
            display_name=EXCLUDED.display_name,
            avatar_url=EXCLUDED.avatar_url,
            updated_at=now()
        `, [teamId, uid, info.name, info.avatar]);
      }

      const { rows } = await dbQuery(
        `SELECT * FROM daily_report_members WHERE team_id=$1 ORDER BY display_name ASC`, [teamId]
      );
      res.json({ members: rows, synced: userIds.length });
    } catch (e) {
      console.error('[DailyReport] sync error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // GET /api/admin/daily-report/excludes — daily-report-watcher用（認証不要・外部公開）
  expressApp.get('/api/admin/daily-report/excludes', async (req, res) => {
    try {
      // team_idが複数ある場合に備え、is_target=falseのuser_idをすべて返す
      const { rows } = await dbQuery(
        `SELECT DISTINCT user_id FROM daily_report_members WHERE is_target=false`
      );
      res.json({ excludes: rows.map(r => r.user_id) });
    } catch (e) {
      console.error('[DailyReport] excludes error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}


const YOMI_OPTIONS = [
  'アポ化前', 'アポ化済商談前',
  'E 5％', 'D 15％', 'C 30％', 'B 50％', 'A 70％', 'S 90％',
  '受注', '失注',
];

const CONTRACT_TYPES = [
  '月額：コンサルのみ', '月額：実務のみ', '月額：フルコミット',
  '後払い：媒体費弊社', '後払い：媒体費クライアント',
  '採用保証：分析付き', '採用保証：人材紹介案件',
];

const PAYMENT_TYPES = [
  '月額', '月額（1st upsell）',
  '採用保証', '採用保証（1st upsell）', '採用保証（2st upsell）',
  '後払い（媒体費用INREVO持ち）', '後払い（媒体費用クライアント持ち）',
  '変動プラン',
];

const LOST_REASONS = [
  'ニーズなし', '金額NG', '競合負け', '採用予定なし',
  '多忙・リスケ不可', '人材紹介のみ', '時期が違う', '前払いNG',
  '企業年数', '外注意思なし', '上長NG',
];

function registerCrmApi({ expressApp, authWithRole }) {

  // ─── 顧客 CRUD ───────────────────────────────────────
  expressApp.get('/api/crm/customers', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const q         = req.query.q?.trim() || '';
      const salesUser = req.query.salesUser?.trim() || '';
      const yomiList  = req.query.yomiList ? req.query.yomiList.split(',').filter(Boolean) : [];
      const offset    = Number(req.query.offset) || 0;
      const limit     = Math.min(Number(req.query.limit) || 100, 500);

      let having = '';
      const params = [teamId];
      if (q) { params.push(`%${q}%`); having += ` AND (c.name ILIKE $${params.length} OR c.industry ILIKE $${params.length})`; }
      if (salesUser) { params.push(`%${salesUser}%`); having += ` AND EXISTS (SELECT 1 FROM deals dx WHERE dx.customer_id=c.id AND dx.team_id=c.team_id AND dx.sales_user_id ILIKE $${params.length})`; }
      if (yomiList.length > 0) { params.push(yomiList); having += ` AND EXISTS (SELECT 1 FROM deals dx WHERE dx.customer_id=c.id AND dx.team_id=c.team_id AND dx.yomi=ANY($${params.length}::text[]))`; }

      const { rows } = await dbQuery(`
        SELECT c.*,
               COUNT(d.id)::int AS deal_count,
               MAX(d.yomi) AS latest_yomi,
               MAX(d.updated_at) AS latest_deal_at
        FROM customers c
        LEFT JOIN deals d ON d.customer_id = c.id AND d.team_id = c.team_id
        WHERE c.team_id = $1 ${having}
        GROUP BY c.id
        ORDER BY c.updated_at DESC
        LIMIT ${limit} OFFSET ${offset}
      `, params);
      const { rows: [{ total }] } = await dbQuery(
        `SELECT COUNT(*)::int AS total FROM customers c WHERE c.team_id=$1 ${having}`,
        params
      );
      const { rows: salesRows } = await dbQuery(
        `SELECT DISTINCT sales_user_id as name FROM deals WHERE team_id=$1 AND sales_user_id IS NOT NULL
         UNION SELECT DISTINCT na_user_id FROM deals WHERE team_id=$1 AND na_user_id IS NOT NULL
         ORDER BY name`,
        [teamId]
      );
      const salesUsers = salesRows.map(r => r.name).filter(n => n && n.trim() && !n.includes('�'));
      res.json({ customers: rows, total, offset, limit, meta: { yomiOptions: YOMI_OPTIONS, contractTypes: CONTRACT_TYPES, paymentTypes: PAYMENT_TYPES, lostReasons: LOST_REASONS, salesUsers } });
    } catch (e) {
      console.error('[CRM] list customers error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.get('/api/crm/customers/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [customer] } = await dbQuery(
        `SELECT * FROM customers WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]
      );
      if (!customer) return res.status(404).json({ error: 'not_found' });
      const { rows: deals } = await dbQuery(
        `SELECT * FROM deals WHERE customer_id=$1 AND team_id=$2 ORDER BY created_at DESC`,
        [req.params.id, teamId]
      );
      res.json({ customer, deals });
    } catch (e) {
      console.error('[CRM] get customer error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.post('/api/crm/customers', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const {
        name, industry, prefecture, employeeCount, website, memo,
        inflowDate, inflowSource, nameShort, competitors, businessDescription,
        postalCode, address, serviceLpUrl1, serviceLpUrl2,
      } = req.body || {};
      if (!name?.trim()) return res.status(400).json({ error: 'name required' });
      const { rows: [row] } = await dbQuery(`
        INSERT INTO customers (id, team_id, name, industry, prefecture, employee_count, website, memo, created_by,
          inflow_date, inflow_source, name_short, competitors, business_description, postal_code, address, service_lp_url1, service_lp_url2)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING *
      `, [randomUUID(), teamId, name.trim(), industry||null, prefecture||null, employeeCount||null, website||null, memo||null, userId,
          inflowDate||null, inflowSource||null, nameShort||null, JSON.stringify(competitors||[]),
          businessDescription||null, postalCode||null, address||null, serviceLpUrl1||null, serviceLpUrl2||null]);
      res.status(201).json({ customer: row });
    } catch (e) {
      console.error('[CRM] create customer error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.patch('/api/crm/customers/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const {
        name, industry, prefecture, employeeCount, website, memo,
        inflowDate, inflowSource, nameShort, competitors, businessDescription,
        postalCode, address, serviceLpUrl1, serviceLpUrl2,
        updatedAt, force, data,
      } = req.body || {};

      // オプティミスティックロック
      if (updatedAt && !force) {
        const { rows: [cur] } = await dbQuery(`SELECT updated_at FROM customers WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]);
        if (cur && new Date(cur.updated_at).getTime() !== new Date(updatedAt).getTime()) {
          return res.status(409).json({ error:'conflict', message:'他のユーザーがこのデータを更新しました', serverUpdatedAt: cur.updated_at });
        }
      }

      const { rows: [row] } = await dbQuery(`
        UPDATE customers SET
          name=$3, industry=$4, prefecture=$5, employee_count=$6, website=$7, memo=$8,
          inflow_date=$9, inflow_source=$10, name_short=$11, competitors=$12,
          business_description=$13, postal_code=$14, address=$15, service_lp_url1=$16, service_lp_url2=$17,
          data=COALESCE($18::jsonb, data),
          updated_at=now()
        WHERE id=$1 AND team_id=$2 RETURNING *
      `, [req.params.id, teamId, name, industry||null, prefecture||null, employeeCount||null, website||null, memo||null,
          inflowDate||null, inflowSource||null, nameShort||null, JSON.stringify(competitors||[]),
          businessDescription||null, postalCode||null, address||null, serviceLpUrl1||null, serviceLpUrl2||null,
          data ? JSON.stringify(data) : null]);
      if (!row) return res.status(404).json({ error: 'not_found' });
      res.json({ customer: row });
    } catch (e) {
      console.error('[CRM] update customer error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── customer_contacts CRUD ──
  expressApp.get('/api/crm/customers/:id/contacts', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(`SELECT * FROM customer_contacts WHERE customer_id=$1 AND team_id=$2 ORDER BY sort_order, created_at`, [req.params.id, teamId]);
      res.json({ contacts: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/crm/customers/:id/contacts', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { lastName, firstName, furigana, positionTitle, department, email, phone, memo, salesProhibited } = req.body || {};
      const { rows: [row] } = await dbQuery(`
        INSERT INTO customer_contacts (id, customer_id, team_id, last_name, first_name, furigana, position_title, department, email, phone, memo, sales_prohibited)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *
      `, [randomUUID(), req.params.id, teamId, lastName||null, firstName||null, furigana||null, positionTitle||null, department||null, email||null, phone||null, memo||null, salesProhibited||false]);
      res.status(201).json({ contact: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.patch('/api/crm/contacts/:contactId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { lastName, firstName, furigana, positionTitle, department, email, phone, memo, salesProhibited } = req.body || {};
      const { rows: [row] } = await dbQuery(`
        UPDATE customer_contacts SET last_name=$3, first_name=$4, furigana=$5, position_title=$6, department=$7, email=$8, phone=$9, memo=$10, sales_prohibited=$11
        WHERE id=$1 AND team_id=$2 RETURNING *
      `, [req.params.contactId, teamId, lastName||null, firstName||null, furigana||null, positionTitle||null, department||null, email||null, phone||null, memo||null, salesProhibited||false]);
      res.json({ contact: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/crm/contacts/:contactId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM customer_contacts WHERE id=$1 AND team_id=$2`, [req.params.contactId, teamId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── 新サブテーブル CRUD (RPO費用/採用費用/人件費/応募予測) ──
  const subTables = [
    { path: 'rpo-costs',    table: 'deal_rpo_costs' },
    { path: 'hiring-costs', table: 'deal_hiring_costs' },
    { path: 'labor-costs',  table: 'deal_labor_costs' },
    { path: 'app-forecasts',table: 'deal_application_forecasts' },
  ];
  subTables.forEach(({ path, table }) => {
    expressApp.get(`/api/crm/deals/:id/${path}`, authWithRole, async (req, res) => {
      try {
        const { teamId } = req.dashboardUser;
        const { rows } = await dbQuery(`SELECT * FROM ${table} WHERE deal_id=$1 AND team_id=$2 ORDER BY sort_order, created_at`, [req.params.id, teamId]);
        res.json({ rows });
      } catch (e) { res.status(500).json({ error: 'internal' }); }
    });
    expressApp.post(`/api/crm/deals/:id/${path}`, authWithRole, async (req, res) => {
      try {
        const { teamId } = req.dashboardUser;
        const body = req.body || {};
        const cols = Object.keys(body).filter(k => !['id','deal_id','team_id','created_at'].includes(k));
        const vals = cols.map(k => body[k]);
        const id = randomUUID();
        const { rows: [row] } = await dbQuery(
          `INSERT INTO ${table} (id, deal_id, team_id${cols.length ? ','+cols.join(',') : ''}) VALUES ($1,$2,$3${cols.map((_,i)=>','+'$'+(i+4)).join('')}) RETURNING *`,
          [id, req.params.id, teamId, ...vals]
        );
        res.status(201).json({ row });
      } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
    });
    expressApp.delete(`/api/crm/${path}/:rowId`, authWithRole, async (req, res) => {
      try {
        const { teamId } = req.dashboardUser;
        await dbQuery(`DELETE FROM ${table} WHERE id=$1 AND team_id=$2`, [req.params.rowId, teamId]);
        res.json({ ok: true });
      } catch (e) { res.status(500).json({ error: 'internal' }); }
    });
  });

  // ── deal_activities CRUD ──────────────────────────────────────
  expressApp.get('/api/crm/deals/:id/activities', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      // 既存 deal_activities
      const localRes = await dbQuery(
        `SELECT * FROM deal_activities WHERE team_id=$1 AND deal_id=$2 ORDER BY created_at DESC`,
        [teamId, req.params.id]
      );
      // kintone活動履歴: deals.data->>'kintone_record_id' で紐付け
      const dealRes = await dbQuery(
        `SELECT data->>'kintone_record_id' AS rec_id FROM deals WHERE id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      const dealKintoneId = dealRes.rows[0]?.rec_id;
      let kintoneRows = [];
      if (dealKintoneId) {
        const kr = await dbQuery(
          `SELECT record_id, deal_record_id, activity_date, activity_type, assignee, content,
                  next_action_date, next_action_content, next_action_detail, next_assignee,
                  yomi_at_time, is_done, created_at, updated_at
           FROM kintone_activities
           WHERE deal_record_id=$1
           ORDER BY COALESCE(activity_date, created_at::date) DESC, created_at DESC`,
          [String(dealKintoneId)]
        );
        kintoneRows = kr.rows.map(r => ({ ...r, source: 'kintone' }));
      }
      res.json({ activities: localRes.rows, kintoneActivities: kintoneRows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/crm/deals/:id/activities', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { activityType, result, content, yomiAtTime,
              activityDate, nextActionDate, nextActionContent, nextPersonId } = req.body || {};
      if (!activityType) return res.status(400).json({ error: 'activityType required' });
      const { rows: [row] } = await dbQuery(
        `INSERT INTO deal_activities
           (id, deal_id, team_id, user_id, activity_type, result, content, yomi_at_time,
            activity_date, next_action_date, next_action_content, next_person_id, metadata)
         VALUES (gen_random_uuid()::text,$1,$2,$3,$4,$5,$6,$7,
                 NULLIF($8,'')::date, NULLIF($9,'')::date, $10, $11, '{}') RETURNING *`,
        [req.params.id, teamId, userId, activityType, result||null, content||null, yomiAtTime||null,
         activityDate||'', nextActionDate||'', nextActionContent||null, nextPersonId||null]
      );
      res.status(201).json({ activity: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/crm/deals/:id/activities/:actId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM deal_activities WHERE id=$1 AND team_id=$2`, [req.params.actId, teamId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── アクティビティ設定 (activity types / result options) ──────
  expressApp.get('/api/crm/activity-settings', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(`SELECT * FROM crm_activity_settings WHERE team_id=$1`, [teamId]);
      const row = rows[0] || {
        activity_types: ['架電','商談','メール','受電','その他'],
        result_options: ['アポ獲得','有効会話','不通','折り返し','NG','その他'],
      };
      res.json({ activityTypes: row.activity_types, resultOptions: row.result_options });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/crm/activity-settings', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { activityTypes, resultOptions } = req.body || {};
      await dbQuery(`
        INSERT INTO crm_activity_settings (team_id, activity_types, result_options)
        VALUES ($1,$2,$3)
        ON CONFLICT (team_id) DO UPDATE SET activity_types=$2, result_options=$3, updated_at=now()
      `, [teamId, JSON.stringify(activityTypes), JSON.stringify(resultOptions)]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── ステージ変換ヘルパー ─────────────────────────────────────────
  function yomiToStage(yomi, status) {
    if (status === 'won')     return '受注済';
    if (status === 'lost')    return '失注';
    if (status === 'dormant') return '見送り';
    if (yomi === 'アポ化前')         return 'リード獲得';
    if (yomi === 'アポ化済商談前')   return '初回商談待ち';
    if (['E 5％','D 15％','C 30％','B 50％','A 70％','S 90％'].includes(yomi)) return '商談中';
    return 'その他';
  }

  // ── ヘルス計算 ───────────────────────────────────────────────────
  function calcHealth(yomi, status, updatedAt, hasRecentActivity) {
    if (status === 'won')     return 100;
    if (status === 'lost')    return 0;
    if (status === 'dormant') return 0;
    const base = { 'S 90％':90,'A 70％':75,'B 50％':60,'C 30％':45,'D 15％':30,'E 5％':20,'アポ化済商談前':15,'アポ化前':10 }[yomi] || 10;
    const days = updatedAt ? Math.floor((Date.now() - new Date(updatedAt)) / 86400000) : 999;
    const penalty = Math.min(days * 2, 30);
    const bonus = hasRecentActivity ? 5 : 0;
    return Math.max(0, Math.min(100, base - penalty + bonus));
  }

  // ── 案件一覧（新UI用・ヘルス付き）──────────────────────────────
  expressApp.get('/api/crm/deals-list', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { stage, yomi, plan, q, salesUser, showDormant, quickFilter, limit: lq } = req.query;
      const scope = req.query.scope || 'all'; // 'all' | 'self'
      const offset = Number(req.query.offset) || 0;
      const limit = Math.min(Number(lq) || 100, 500);

      let where = `d.team_id=$1`;
      const params = [teamId];

      // スコープ（自分のみ）
      if (scope === 'self') {
        params.push(userId);
        where += ` AND (COALESCE(d.sales_person, d.sales_user_id)=(SELECT display_name FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$${params.length} LIMIT 1) OR d.sales_user_id=$${params.length})`;
      }

      // 見送りの表示制御
      if (showDormant === '1') {
        where += ` AND d.status IN ('active','won','lost','dormant')`;
      } else {
        where += ` AND d.status IN ('active','won','lost')`;
      }

      // ステージフィルター
      if (stage) {
        const stageToYomi = {
          'リード獲得':    `d.yomi='アポ化前'`,
          '初回商談待ち':  `d.yomi='アポ化済商談前'`,
          '商談中':        `d.yomi IN ('E 5％','D 15％','C 30％','B 50％','A 70％','S 90％')`,
          '受注済':        `d.status='won'`,
          '失注':          `d.status='lost'`,
          '見送り':        `d.status='dormant'`,
        };
        if (stageToYomi[stage]) where += ` AND (${stageToYomi[stage]})`;
      }

      // ヨミフィルター
      if (yomi) { params.push(yomi.split(',').filter(Boolean)); where += ` AND d.yomi=ANY($${params.length}::text[])`; }

      // 担当者
      if (salesUser) { params.push(salesUser); where += ` AND COALESCE(d.sales_person, d.sales_user_id)=$${params.length}`; }

      // 検索
      if (q) { params.push(`%${q}%`); where += ` AND (c.name ILIKE $${params.length} OR d.name ILIKE $${params.length})`; }

      // クイックフィルター
      if (quickFilter === 'high_priority') where += ` AND d.yomi IN ('A 70％','S 90％') AND d.status='active'`;
      if (quickFilter === 'watch')         where += ` AND d.updated_at < now() - interval '14 days' AND d.status='active' AND d.yomi NOT IN ('アポ化前','受注','失注')`;
      if (quickFilter === 'yomi_mgmt')     where += ` AND d.yomi IN ('C 30％','B 50％','A 70％','S 90％') AND d.status='active'`;

      // 総件数（フィルター適用後）
      const countRes = await dbQuery(
        `SELECT COUNT(*)::int AS total FROM deals d JOIN customers c ON c.id=d.customer_id WHERE ${where}`,
        params
      );
      const totalCount = countRes.rows[0]?.total || 0;

      // 直近アクティビティチェック用
      const { rows } = await dbQuery(`
        SELECT
          d.id, d.name, d.yomi, d.status, d.dormant_reason,
          d.contract_type, d.payment_type,
          d.initial_fee, d.monthly_fee, d.hiring_target,
          d.sales_person, d.sales_user_id, d.na_user_id,
          d.next_action_date, d.next_action_content,
          d.updated_at, d.order_date, d.first_meeting_date, d.inflow_source, d.data,
          c.id AS customer_id, c.name AS customer_name, c.industry, c.prefecture,
          (SELECT COUNT(*) FROM deal_activities da WHERE da.deal_id=d.id AND da.created_at > now() - interval '7 days')::int AS recent_activity_count,
          (SELECT content FROM deal_activities da WHERE da.deal_id=d.id ORDER BY da.created_at DESC LIMIT 1) AS latest_activity
        FROM deals d
        JOIN customers c ON c.id=d.customer_id
        WHERE ${where}
        ORDER BY d.updated_at DESC
        LIMIT ${limit} OFFSET ${offset}
      `, params);

      // ステージ付与（ヘルスは削除）
      const deals = rows.map(d => ({
        ...d,
        stage: yomiToStage(d.yomi, d.status),
        sales_person_name: d.sales_person || d.sales_user_id,
      }));

      // KPI集計
      const activeDeals = deals.filter(d => d.status === 'active');
      const kpi = {
        total:       rows.length,
        totalAmount: activeDeals.reduce((s, d) => s + Number(d.initial_fee || 0), 0),
        alertCount:  activeDeals.filter(d => d.next_action_date && new Date(d.next_action_date) < new Date()).length +
                     activeDeals.filter(d => { const days = Math.floor((Date.now() - new Date(d.updated_at)) / 86400000); return days >= 14 && !['アポ化前'].includes(d.yomi); }).length,
      };

      // 担当者一覧：crm_rep_rolesから動的取得
      const repRolesRes = await dbQuery(`SELECT rep_name FROM crm_rep_roles WHERE team_id=$1 ORDER BY rep_name`, [teamId]);
      const TARGET_REPS = repRolesRes.rows.map(r => r.rep_name);

      res.json({ deals, kpi, totalCount, hasMore: offset + rows.length < totalCount, salesUsers: TARGET_REPS });
    } catch (e) {
      console.error('[CRM] deals-list error:', e);
      res.status(500).json({ error: 'internal', detail: e.message });
    }
  });

  // ── 見送りに変更 ──────────────────────────────────────────────
  expressApp.patch('/api/crm/deals/:id/dormant', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { reason, revert } = req.body || {};
      if (revert) {
        await dbQuery(`UPDATE deals SET status='active', dormant_reason=NULL WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]);
      } else {
        await dbQuery(`UPDATE deals SET status='dormant', dormant_reason=$3 WHERE id=$1 AND team_id=$2`, [req.params.id, teamId, reason || null]);
      }
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── CRM権限設定 CRUD ──────────────────────────────────────────────
  const DEFAULT_CRM_PERMISSIONS = {
    bc_team_name: 'Business Consulting',
    tabs: {
      dashboard:   { access: 'bc_and_above' },  // admin=all, bc manager=all, bc member=self, others=none
      customers:   { access: 'all' },
      yomi:        { access: 'bc_all' },         // bc member+ (any role) + admin
      performance: { access: 'bc_manager' },     // bc sub_manager/manager + admin
      settings:    { access: 'bc_manager' },
    },
  };

  // ユーザーの CRM アクセス権を返す
  expressApp.get('/api/crm/my-crm-access', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const role = req.dashboardUser.role;

      // 権限設定を取得
      const permRes = await dbQuery(`SELECT * FROM crm_permissions WHERE team_id=$1`, [teamId]);
      const perm = permRes.rows[0] || {};
      const cfg = { ...DEFAULT_CRM_PERMISSIONS, ...(perm.config || {}) };
      const bcTeamName = perm.bc_team_name || cfg.bc_team_name;

      // admin は常に全アクセス・全スコープ
      if (role === 'admin') {
        return res.json({
          isBC: true, isBCManager: true, scope: 'all', role,
          tabs: Object.fromEntries(Object.keys(cfg.tabs).map(t => [t, { visible: true, scope: 'all' }])),
          bcTeamName,
        });
      }

      // BC所属チェック（dash_team_members → dash_teams で名前確認）
      const bcRes = await dbQuery(`
        SELECT 1 FROM dash_team_members dtm
        JOIN dash_teams dt ON dt.id=dtm.dash_team_id AND dt.team_id=dtm.team_id
        WHERE dtm.team_id=$1 AND dtm.user_id=$2
          AND dt.name ILIKE $3
        LIMIT 1
      `, [teamId, userId, `%${bcTeamName}%`]);
      const isBC = bcRes.rows.length > 0;
      const isBCManager = isBC && ['manager','sub_manager'].includes(role);

      const tabAccess = {};
      for (const [tab, rule] of Object.entries(cfg.tabs)) {
        const acc = rule.access;
        if (acc === 'all') {
          tabAccess[tab] = { visible: true, scope: 'all' };
        } else if (acc === 'bc_all') {
          tabAccess[tab] = { visible: isBC, scope: 'all' };
        } else if (acc === 'bc_manager') {
          tabAccess[tab] = { visible: isBCManager, scope: 'all' };
        } else if (acc === 'bc_and_above') {
          if (!isBC) {
            tabAccess[tab] = { visible: false, scope: 'none' };
          } else if (isBCManager) {
            tabAccess[tab] = { visible: true, scope: 'all' };
          } else {
            tabAccess[tab] = { visible: true, scope: 'self' };
          }
        } else {
          tabAccess[tab] = { visible: false, scope: 'none' };
        }
      }

      res.json({ isBC, isBCManager, scope: isBC ? (isBCManager ? 'all' : 'self') : 'none', role, tabs: tabAccess, bcTeamName });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 権限設定 GET（admin用）
  expressApp.get('/api/crm/permissions', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(`SELECT * FROM crm_permissions WHERE team_id=$1`, [teamId]);
      const row = r.rows[0];
      res.json({
        bcTeamName: row?.bc_team_name || DEFAULT_CRM_PERMISSIONS.bc_team_name,
        tabs: { ...DEFAULT_CRM_PERMISSIONS.tabs, ...(row?.config?.tabs || {}) },
      });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // 権限設定 PUT（admin用）
  expressApp.put('/api/crm/permissions', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      if (req.dashboardUser.role !== 'admin') return res.status(403).json({ error: 'admin_required' });
      const { bcTeamName, tabs } = req.body || {};
      await dbQuery(`
        INSERT INTO crm_permissions (team_id, bc_team_name, config)
        VALUES ($1,$2,$3)
        ON CONFLICT (team_id) DO UPDATE SET bc_team_name=$2, config=$3, updated_at=now()
      `, [teamId, bcTeamName, JSON.stringify({ tabs })]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── ダッシュボード ──────────────────────────────────────────────
  expressApp.get('/api/crm/dashboard', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { salesUser, period = 'month', customMonth } = req.query; // period: 'month' | 'term' | 'custom'

      const ADDA_REF = '添田/リファラル';
      const isAddaRef = salesUser === ADDA_REF;

      // 期間設定取得
      const periodRes = await dbQuery('SELECT * FROM crm_period_settings WHERE team_id=$1', [teamId]);
      const ps = periodRes.rows[0] || {
        prev_start: '2025-08-01', prev_end: '2025-11-30',
        curr_start: '2025-12-01', curr_end: '2026-05-31',
      };

      // 今月の範囲
      const now = new Date();
      const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
      const monthEnd   = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];

      // 集計期間の選択
      let rangeStart, rangeEnd;
      if (period === 'term') {
        [rangeStart, rangeEnd] = [ps.curr_start, ps.curr_end];
      } else if (period === 'custom' && customMonth) {
        const cm = new Date(`${customMonth}-01`);
        const cy = cm.getFullYear(), cmn = cm.getMonth() + 1;
        rangeStart = `${cy}-${String(cmn).padStart(2,'0')}-01`;
        rangeEnd   = new Date(cy, cmn, 0).toISOString().split('T')[0];
      } else {
        [rangeStart, rangeEnd] = [monthStart, monthEnd];
      }
      const [prevStart, prevEnd] = [ps.prev_start, ps.prev_end];

      // 設定済み担当者（crm_rep_roles）をフラグ込みでDBから取得
      const repRolesRes = await dbQuery(`
        SELECT rep_name, role_name, monthly_target_override,
               COALESCE(is_retired, false)          AS is_retired,
               COALESCE(exclude_from_kpi, false)    AS exclude_from_kpi,
               COALESCE(monthly_to_adda_ref, false) AS monthly_to_adda_ref
        FROM crm_rep_roles WHERE team_id=$1
      `, [teamId]);
      const REP_ROLES = repRolesRes.rows;
      const CONFIGURED_REPS = REP_ROLES.map(r => r.rep_name);

      // deals 側の担当者フィルタ（yomi等のクエリ用、$2 以降を消費）
      // - 通常担当者: COALESCE(...)=$2
      // - 添田/リファラル: 設定済担当者 全員に該当しない人（NOT ILIKE 〜 AND ...）
      const personFilter = isAddaRef
        ? (CONFIGURED_REPS.length > 0
          ? ` AND (${CONFIGURED_REPS.map((_, i) => `COALESCE(d.sales_person, d.sales_user_id) NOT ILIKE $${i + 2}`).join(' AND ')})`
          : '')
        : (salesUser ? ` AND COALESCE(d.sales_person, d.sales_user_id)=$2` : '');
      const personParams = isAddaRef
        ? (CONFIGURED_REPS.length > 0 ? [teamId, ...CONFIGURED_REPS.map(r => `%${r}%`)] : [teamId])
        : (salesUser ? [teamId, salesUser] : [teamId]);

      // staff 名 → ロール設定の解決（部分一致）
      const findRoleFor = (staff) => {
        if (!staff) return null;
        for (const r of REP_ROLES) {
          if (!r.rep_name) continue;
          if (staff.includes(r.rep_name) || r.rep_name.includes(staff)) return r;
        }
        return null;
      };

      // 入金1件の振り分け先: 'bc' | 'adda_ref' | 'excluded'
      // 'bc'       … 担当者本人のBC実績として計上
      // 'adda_ref' … 添田/リファラル行へ集約
      // 'excluded' … KPIから完全除外（アライアンス扱い）
      const classifyPayment = (staff, plan, inflow) => {
        // 過渡期の特別ルール: 丸山さん × グラハム流入 = アライアンス扱い
        if ((staff || '').includes('丸山') && inflow === 'グラハム') return 'excluded';

        const role = findRoleFor(staff);
        if (role?.exclude_from_kpi)    return 'excluded';
        if (role?.is_retired)          return 'adda_ref';
        if (role?.monthly_to_adda_ref) return (plan || '').includes('月額') ? 'adda_ref' : 'bc';
        if (role)                      return 'bc';
        return 'adda_ref';
      };

      // ── 期間内の主要指標を集計するヘルパー ──
      const getMetrics = async (start, end) => {
        // deals 側のフィルタ（personFilter と同じ分岐、ただし baseP に start/end が末尾追加）
        const pf      = personFilter;
        const baseP   = [...personParams, start, end];
        const si      = baseP.length - 1; // start の $番号

        const [wonRes, meetingRes, payRowsRes] = await Promise.all([
          dbQuery(`SELECT COUNT(*)::int AS cnt, COALESCE(SUM(d.initial_fee),0)::bigint AS amount
            FROM deals d WHERE d.team_id=$1${pf}
            AND d.status='won' AND d.order_date BETWEEN $${si}::date AND $${si+1}::date`, baseP),
          dbQuery(`SELECT COUNT(*)::int AS cnt FROM deals d
            WHERE d.team_id=$1${pf}
            AND d.first_meeting_date BETWEEN $${si}::date AND $${si+1}::date`, baseP),
          dbQuery(`SELECT staff, plan, inflow_source,
                          COALESCE(amount,0)::bigint AS amount,
                          COALESCE(incentive_amount,0)::bigint AS incentive_amount
                   FROM kintone_payments
                   WHERE payment_date BETWEEN $1::date AND $2::date`,
            [start, end]),
        ]);

        // KPI集計対象: 'bc' と 'adda_ref' 両方含む（'excluded' = アライアンスのみ除外）
        // 「添田/リファラル」フィルタ選択時は adda_ref のみに絞る
        let paymentAmount = 0, incentiveAmount = 0;
        for (const p of payRowsRes.rows) {
          if (salesUser && !isAddaRef && !(p.staff || '').includes(salesUser)) continue;
          const dest = classifyPayment(p.staff, p.plan, p.inflow_source);
          if (dest === 'excluded') continue;
          if (isAddaRef && dest !== 'adda_ref') continue;
          paymentAmount   += Number(p.amount);
          incentiveAmount += Number(p.incentive_amount);
        }

        return {
          wonCount:        wonRes.rows[0]?.cnt || 0,
          wonAmount:       Number(wonRes.rows[0]?.amount || 0),
          meetingCount:    meetingRes.rows[0]?.cnt || 0,
          paymentAmount,
          incentiveAmount,
          allianceIncentive: 0,
        };
      };

      // 前月算出（custom期間の場合は1ヶ月前）
      let prevRangeStart = null, prevRangeEnd = null;
      if (period === 'term') {
        [prevRangeStart, prevRangeEnd] = [prevStart, prevEnd];
      } else if (period === 'custom' && customMonth) {
        const cm = new Date(`${customMonth}-01`);
        cm.setMonth(cm.getMonth() - 1);
        const py = cm.getFullYear(), pmn = cm.getMonth() + 1;
        prevRangeStart = `${py}-${String(pmn).padStart(2, '0')}-01`;
        prevRangeEnd   = new Date(py, pmn, 0).toISOString().split('T')[0];
      }

      const [currMetrics, prevMetrics] = await Promise.all([
        getMetrics(rangeStart, rangeEnd),
        prevRangeStart ? getMetrics(prevRangeStart, prevRangeEnd) : null,
      ]);

      // ── 担当者別テーブル ──
      const repRows = await dbQuery(`
        SELECT
          COALESCE(d.sales_person, d.sales_user_id) AS rep,
          COUNT(*) FILTER (WHERE d.status='won' AND d.order_date BETWEEN $2::date AND $3::date)::int AS won_count,
          COUNT(*) FILTER (
            WHERE d.first_meeting_date BETWEEN $2::date AND $3::date
          )::int AS meeting_count
        FROM deals d
        WHERE d.team_id=$1 AND COALESCE(d.sales_person, d.sales_user_id) IS NOT NULL
        GROUP BY 1 ORDER BY 1
      `, [teamId, rangeStart, rangeEnd]);

      // 担当者別入金額 & インセン（kintone_payments）— 全行取得して JS で振り分け
      const repPayRowsRes = await dbQuery(`
        SELECT staff, plan, inflow_source,
               COALESCE(amount,0)::bigint           AS amount,
               COALESCE(incentive_amount,0)::bigint AS incentive_amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1::date AND $2::date
      `, [rangeStart, rangeEnd]);

      // staff（kintoneの担当者名）→ 設定上の rep_name 解決
      const resolveRepName = (staff) => {
        const role = findRoleFor(staff);
        return role?.rep_name || staff;
      };

      // BC計上対象のみ rep_name 別に集計、addaRef集約分は別バケツに
      const repPayMap = {};   // { repName: { payment, incentive } }
      const addaRefAgg = { paymentAmount: 0, incentiveAmount: 0 };
      for (const p of repPayRowsRes.rows) {
        const dest = classifyPayment(p.staff, p.plan, p.inflow_source);
        if (dest === 'excluded') continue;
        if (dest === 'adda_ref') {
          addaRefAgg.paymentAmount   += Number(p.amount);
          addaRefAgg.incentiveAmount += Number(p.incentive_amount);
          continue;
        }
        // dest === 'bc'
        const repName = resolveRepName(p.staff);
        if (!repPayMap[repName]) repPayMap[repName] = { payment: 0, incentive: 0 };
        repPayMap[repName].payment   += Number(p.amount);
        repPayMap[repName].incentive += Number(p.incentive_amount);
      }

      const isConfigured = (rep) => rep && CONFIGURED_REPS.some(n => rep.includes(n) || n.includes(rep));

      // 設定済み担当者を repRows（deals 側） + repPayMap（kintone 側）から合成
      const repNameSet = new Set();
      for (const r of repRows.rows) if (isConfigured(r.rep)) repNameSet.add(resolveRepName(r.rep));
      for (const n of Object.keys(repPayMap)) repNameSet.add(n);

      // deals 側集計を rep_name 解決ベースで束ねる
      const dealsAggByRep = {};
      for (const r of repRows.rows) {
        const repName = resolveRepName(r.rep);
        if (!isConfigured(repName)) continue;
        if (!dealsAggByRep[repName]) dealsAggByRep[repName] = { wonCount: 0, meetingCount: 0 };
        dealsAggByRep[repName].wonCount     += Number(r.won_count);
        dealsAggByRep[repName].meetingCount += Number(r.meeting_count);
      }

      // 「添田/リファラル」は予約名なので normalRows には出さない（addaRefRow と重複防止）
      const normalRows = [...repNameSet]
        .filter(repName => repName !== ADDA_REF)
        .map(repName => ({
          rep:             repName,
          wonCount:        dealsAggByRep[repName]?.wonCount     || 0,
          meetingCount:    dealsAggByRep[repName]?.meetingCount || 0,
          paymentAmount:   repPayMap[repName]?.payment          || 0,
          incentiveAmount: repPayMap[repName]?.incentive        || 0,
        }));

      // 添田/リファラル行: 入金集約値 + 未設定担当者の受注/初回商談を加算
      const addaRefDealRows = repRows.rows.filter(r => !isConfigured(r.rep));
      const addaRefRow = (addaRefAgg.paymentAmount === 0 && addaRefAgg.incentiveAmount === 0 && addaRefDealRows.length === 0)
        ? null
        : {
            rep:             '添田/リファラル',
            wonCount:        addaRefDealRows.reduce((s, r) => s + Number(r.won_count), 0),
            meetingCount:    addaRefDealRows.reduce((s, r) => s + Number(r.meeting_count), 0),
            paymentAmount:   addaRefAgg.paymentAmount,
            incentiveAmount: addaRefAgg.incentiveAmount,
            isGrouped:       true,
            groupType:       'adda_ref',
          };

      const repTable = isAddaRef
        ? (addaRefRow ? [addaRefRow] : [])
        : [...normalRows, ...(addaRefRow ? [addaRefRow] : [])];

      // ── アラート（全担当者対象、フィルター不要）──
      const today = new Date().toISOString().split('T')[0];
      const [overdueRes, stagnantRes] = await Promise.all([
        dbQuery(`
          SELECT d.id, d.name, d.yomi, d.next_action_date, d.next_action_content,
                 COALESCE(d.sales_person, d.sales_user_id) AS sales_person, c.name AS customer_name
          FROM deals d JOIN customers c ON c.id=d.customer_id
          WHERE d.team_id=$1 AND d.status='active'
            AND d.next_action_date < $2::date AND d.next_action_date IS NOT NULL
          ORDER BY d.next_action_date ASC LIMIT 30
        `, [teamId, today]),
        dbQuery(`
          SELECT d.id, d.name, d.yomi, d.updated_at,
                 COALESCE(d.sales_person, d.sales_user_id) AS sales_person, c.name AS customer_name,
                 (CURRENT_DATE - d.updated_at::date)::int AS days_since_update
          FROM deals d JOIN customers c ON c.id=d.customer_id
          WHERE d.team_id=$1 AND d.status='active'
            AND d.yomi NOT IN ('アポ化前','受注','失注')
            AND d.updated_at < now() - interval '14 days'
          ORDER BY d.updated_at ASC LIMIT 30
        `, [teamId]),
      ]);

      // ── ヨミ別内訳（アクティブ）──
      const yomiRes = await dbQuery(`
        SELECT d.yomi, COUNT(*)::int AS cnt,
               COALESCE(SUM(d.initial_fee),0)::bigint AS total_initial
        FROM deals d WHERE d.team_id=$1 AND d.status='active'${personFilter}
        GROUP BY d.yomi
        ORDER BY CASE d.yomi
          WHEN 'S 90％' THEN 1 WHEN 'A 70％' THEN 2 WHEN 'B 50％' THEN 3
          WHEN 'C 30％' THEN 4 WHEN 'D 15％' THEN 5 WHEN 'E 5％' THEN 6
          WHEN 'アポ化済商談前' THEN 7 WHEN 'アポ化前' THEN 8 ELSE 9 END
      `, personParams);

      // プラン別入金内訳（フィルタ適用、JSで振り分け）
      const planRowsRes = await dbQuery(`
        SELECT plan, company, staff, inflow_source, COALESCE(amount,0)::bigint AS amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1::date AND $2::date AND amount > 0
      `, [rangeStart, rangeEnd]);

      const planAgg = {}; // plan -> { cnt: Set<company>, amount }
      const planExpected = isAddaRef ? 'adda_ref' : (salesUser ? 'bc' : null);
      for (const p of planRowsRes.rows) {
        if (salesUser && !isAddaRef && !(p.staff || '').includes(salesUser)) continue;
        const dest = classifyPayment(p.staff, p.plan, p.inflow_source);
        if (planExpected && dest !== planExpected) continue;
        if (!planExpected && dest === 'excluded') continue;
        const key = p.plan || '未設定';
        if (!planAgg[key]) planAgg[key] = { companies: new Set(), amount: 0 };
        if (p.company) planAgg[key].companies.add(p.company);
        planAgg[key].amount += Number(p.amount);
      }
      const planBreakdownRes = {
        rows: Object.entries(planAgg)
          .map(([plan, v]) => ({ plan, cnt: v.companies.size, amount: v.amount }))
          .sort((a, b) => b.amount - a.amount),
      };

      // 担当者リスト & 役職別目標 & 担当者役職マッピング
      const [salesUsersRes, roleTargetRes, repRoleRes] = await Promise.all([
        dbQuery(
          `SELECT DISTINCT COALESCE(sales_person, sales_user_id) AS name FROM deals
           WHERE team_id=$1 AND COALESCE(sales_person, sales_user_id) IS NOT NULL ORDER BY name`,
          [teamId]
        ),
        dbQuery('SELECT role_name, monthly_target FROM crm_role_targets WHERE team_id=$1', [teamId]),
        dbQuery('SELECT rep_name, role_name, monthly_target_override FROM crm_rep_roles WHERE team_id=$1', [teamId]),
      ]);

      // 役職別目標マップ
      const roleTargetMap = {};
      for (const r of roleTargetRes.rows) roleTargetMap[r.role_name] = Number(r.monthly_target || 0);

      // 担当者別手動設定マップ
      const repRepRoleMap = {};
      for (const r of repRoleRes.rows) repRepRoleMap[r.rep_name] = r;

      const repTargetMap = {};
      const repRoleInferred = {}; // フロントに渡す役職（手動設定のみ。Slack自動推定は廃止）
      // KPI管理対象の担当者（BC担当）= crm_rep_roles 設定済 − 除外/退職 − 添田/リファラル予約名
      const TARGET_REPS_SERVER = REP_ROLES
        .filter(r => !r.exclude_from_kpi && !r.is_retired && r.rep_name !== ADDA_REF)
        .map(r => r.rep_name);
      let teamTarget = 0;
      for (const rep of TARGET_REPS_SERVER) {
        const repRole  = repRepRoleMap[rep];
        const override = repRole?.monthly_target_override;
        const roleName = repRole?.role_name || '役職無し';
        repRoleInferred[rep] = roleName;

        const effective = override != null ? Number(override) : (roleTargetMap[roleName] || 0);
        repTargetMap[rep] = effective;
        teamTarget += effective;
      }

      res.json({
        period, rangeStart, rangeEnd,
        prevStart, prevEnd,
        curr: { ...currMetrics, allianceIncentive: currMetrics.allianceIncentive },
        prev: prevMetrics,
        repTable,
        repTargetMap,
        repRoleInferred,
        teamTarget,
        termTargetOverride: ps.term_target != null ? Number(ps.term_target) : null,
        targetReps: [...TARGET_REPS_SERVER, ADDA_REF], // KPI担当者リスト + 添田/リファラル
        planBreakdown: planBreakdownRes.rows,
        yomiBreakdown: yomiRes.rows,
        overdueAlerts: overdueRes.rows,
        stagnantAlerts: stagnantRes.rows,
        salesUsers: salesUsersRes.rows.map(r => r.name).filter(Boolean),
      });
    } catch (e) {
      console.error('[CRM] dashboard error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── ダッシュボード ドリルダウン ──────────────────────────────
  expressApp.get('/api/crm/dashboard/drilldown', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rep, type, start, end } = req.query;
      // type: 'payments' | 'won'

      if (type === 'payments') {
        // 期間内の入金を全取得 → 新ルール（is_retired / exclude_from_kpi / monthly_to_adda_ref / 流入経路）で振り分け
        const repRolesRes = await dbQuery(`
          SELECT rep_name,
                 COALESCE(is_retired, false)          AS is_retired,
                 COALESCE(exclude_from_kpi, false)    AS exclude_from_kpi,
                 COALESCE(monthly_to_adda_ref, false) AS monthly_to_adda_ref
          FROM crm_rep_roles WHERE team_id=$1
        `, [teamId]);
        const REP_ROLES = repRolesRes.rows;
        const findRoleFor = (staff) => {
          if (!staff) return null;
          for (const r of REP_ROLES) {
            if (!r.rep_name) continue;
            if (staff.includes(r.rep_name) || r.rep_name.includes(staff)) return r;
          }
          return null;
        };
        const classifyPayment = (staff, plan, inflow) => {
          if ((staff || '').includes('丸山') && inflow === 'グラハム') return 'excluded';
          const role = findRoleFor(staff);
          if (role?.exclude_from_kpi)    return 'excluded';
          if (role?.is_retired)          return 'adda_ref';
          if (role?.monthly_to_adda_ref) return (plan || '').includes('月額') ? 'adda_ref' : 'bc';
          if (role)                      return 'bc';
          return 'adda_ref';
        };

        const allRowsRes = await dbQuery(`
          SELECT kp.payment_date, kp.company, kp.plan, kp.incentive_amount, kp.amount, kp.staff, kp.inflow_source,
            CASE WHEN kp.plan LIKE '%月額%' THEN (
              SELECT COUNT(*)::int FROM kintone_payments kp2
              WHERE kp2.company = kp.company
                AND kp2.payment_date <= kp.payment_date
            ) ELSE NULL END AS month_num
          FROM kintone_payments kp
          WHERE kp.payment_date BETWEEN $1::date AND $2::date
            AND kp.incentive_amount > 0
          ORDER BY kp.payment_date DESC
        `, [start, end]);

        const rows = allRowsRes.rows.filter(r => {
          const dest = classifyPayment(r.staff, r.plan, r.inflow_source);
          if (rep === '添田/リファラル') return dest === 'adda_ref';
          if (rep === 'アライアンス')    return dest === 'excluded';
          if (dest !== 'bc') return false;
          // 個別担当者: staff に名前を含む（部分一致）
          return (r.staff || '').includes(rep);
        });
        res.json({ rows });
      } else if (type === 'won') {
        const { rows } = await dbQuery(`
          SELECT d.order_date, c.name AS customer_name,
                 d.contract_type, d.initial_fee, d.monthly_fee
          FROM deals d JOIN customers c ON c.id=d.customer_id
          WHERE d.team_id=$1
            AND COALESCE(d.sales_person, d.sales_user_id)=$2
            AND d.status='won'
            AND d.order_date BETWEEN $3::date AND $4::date
          ORDER BY d.order_date DESC
        `, [teamId, rep, start, end]);
        res.json({ rows });
      } else {
        res.status(400).json({ error: 'invalid type' });
      }
    } catch (e) {
      console.error('[CRM] drilldown error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 月別入金推移（チャート用）──────────────────────────────
  expressApp.get('/api/crm/dashboard/monthly-trend', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { salesUser, months = 12 } = req.query;
      const { rows } = await dbQuery(`
        SELECT
          TO_CHAR(payment_date, 'YYYY/MM') AS month,
          COALESCE(SUM(amount), 0)::bigint AS amount,
          COALESCE(SUM(incentive_amount), 0)::bigint AS incentive_amount,
          COUNT(*)::int AS count
        FROM kintone_payments
        WHERE payment_date >= (CURRENT_DATE - INTERVAL '${Number(months)} months')
          AND payment_date <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')
          ${salesUser ? `AND staff=$1` : ''}
        GROUP BY 1 ORDER BY 1
      `, salesUser ? [salesUser] : []);
      res.json({ rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── フィールド選択肢管理 ────────────────────────────────────────
  expressApp.get('/api/crm/field-options', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery('SELECT field_name, options FROM crm_field_options WHERE team_id=$1', [teamId]);
      const map = {};
      for (const r of rows) map[r.field_name] = r.options;
      res.json({ options: map });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/crm/field-options/:fieldName', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { options } = req.body;
      await dbQuery(
        `INSERT INTO crm_field_options (team_id, field_name, options) VALUES ($1,$2,$3)
         ON CONFLICT (team_id, field_name) DO UPDATE SET options=$3`,
        [teamId, req.params.fieldName, JSON.stringify(options)]
      );
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── カスタムフィールド CRUD ───────────────────────────────────
  expressApp.get('/api/crm/custom-fields', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { entity } = req.query; // 'customer' | 'deal'
      const { rows } = await dbQuery(
        `SELECT * FROM crm_custom_fields WHERE team_id=$1 ${entity ? 'AND entity_type=$2' : ''}
         ORDER BY entity_type, sort_order, created_at`,
        entity ? [teamId, entity] : [teamId]
      );
      res.json({ fields: rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/crm/custom-fields', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { entity_type, field_label, field_type = 'text', options = [], is_required = false } = req.body;
      const field_key = 'cf_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
      const { rows } = await dbQuery(
        `INSERT INTO crm_custom_fields (team_id, entity_type, field_key, field_label, field_type, options, is_required)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
        [teamId, entity_type, field_key, field_label, field_type, JSON.stringify(options), is_required]
      );
      res.json({ field: rows[0] });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.patch('/api/crm/custom-fields/:fieldId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { field_label, field_type, options, is_required, sort_order } = req.body;
      const sets = [], vals = [teamId, req.params.fieldId];
      if (field_label  !== undefined) { sets.push(`field_label=$${vals.length+1}`);  vals.push(field_label); }
      if (field_type   !== undefined) { sets.push(`field_type=$${vals.length+1}`);   vals.push(field_type); }
      if (options      !== undefined) { sets.push(`options=$${vals.length+1}`);      vals.push(JSON.stringify(options)); }
      if (is_required  !== undefined) { sets.push(`is_required=$${vals.length+1}`);  vals.push(is_required); }
      if (sort_order   !== undefined) { sets.push(`sort_order=$${vals.length+1}`);   vals.push(sort_order); }
      if (!sets.length) return res.json({ ok: true });
      await dbQuery(`UPDATE crm_custom_fields SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, vals);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/crm/custom-fields/:fieldId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM crm_custom_fields WHERE team_id=$1 AND id=$2`, [teamId, req.params.fieldId]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 現在のユーザー情報（名前・役職）──────────────────────────────
  expressApp.get('/api/crm/my-info', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT display_name, real_name, profile_json->>'title' AS title
         FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2`,
        [teamId, userId]
      );
      const row = rows[0] || {};
      // 表示名の日本語部分のみ抽出（例: "板金 慎太郎/Shintaro" → "板金 慎太郎"）
      const displayName = (row.display_name || row.real_name || '').split('/')[0].trim();
      res.json({ displayName, title: row.title || '' });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 成績ページ閲覧可否（admin / BC管理職以上）──────────────────
  expressApp.get('/api/crm/performance-access', authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      if (role === 'admin') return res.json({ allowed: true });
      if (role !== 'corp') return res.json({ allowed: false });
      const { rows } = await dbQuery(
        `SELECT profile_json->>'title' AS title FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2`,
        [teamId, userId]
      );
      const t = (rows[0]?.title || '').toLowerCase();
      const allowed = ['sub manager','sub chief','chief','sub expert','expert','manager'].some(k => t.includes(k));
      res.json({ allowed, title: rows[0]?.title || '' });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 担当者別役職・目標設定 ────────────────────────────────────
  expressApp.get('/api/crm/rep-roles', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery('SELECT * FROM crm_rep_roles WHERE team_id=$1', [teamId]);
      res.json({ repRoles: rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/crm/rep-roles', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { repRoles } = req.body;
      for (const r of repRoles) {
        await dbQuery(`
          INSERT INTO crm_rep_roles (team_id, rep_name, role_name, monthly_target_override,
                                     prev_role_name, prev_monthly_target_override,
                                     is_retired, exclude_from_kpi, monthly_to_adda_ref)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (team_id, rep_name) DO UPDATE
          SET role_name=$3, monthly_target_override=$4, prev_role_name=$5, prev_monthly_target_override=$6,
              is_retired=$7, exclude_from_kpi=$8, monthly_to_adda_ref=$9
        `, [teamId, r.rep_name, r.role_name || '', r.monthly_target_override || null,
            r.prev_role_name || '', r.prev_monthly_target_override || null,
            !!r.is_retired, !!r.exclude_from_kpi, !!r.monthly_to_adda_ref]);
      }
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // 行削除（担当者をリストから外す）
  expressApp.delete('/api/crm/rep-roles/:repName', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { repName } = req.params;
      await dbQuery('DELETE FROM crm_rep_roles WHERE team_id=$1 AND rep_name=$2', [teamId, repName]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 最近のアクティビティ（ダッシュボード用）──────────────────
  expressApp.get('/api/crm/dashboard/recent-activities', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const limit = Math.min(Number(req.query.limit) || 8, 30);
      const { rows } = await dbQuery(`
        SELECT da.id, da.activity_type, da.result, da.content, da.created_at,
               c.name AS customer_name,
               COALESCE(d.sales_person, d.sales_user_id) AS sales_person
        FROM deal_activities da
        JOIN deals d ON d.id = da.deal_id
        JOIN customers c ON c.id = d.customer_id
        WHERE da.team_id = $1
        ORDER BY da.created_at DESC
        LIMIT $2
      `, [teamId, limit]);
      res.json({ rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete('/api/crm/customers/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM customers WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) {
      console.error('[CRM] delete customer error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ─── 商談 CRUD ───────────────────────────────────────
  expressApp.get('/api/crm/deals', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { yomi, salesUserId, status, limit: limitQ } = req.query;
      let extra = '';
      const params = [teamId];
      if (yomi) { params.push(yomi); extra += ` AND d.yomi=$${params.length}`; }
      if (salesUserId) { params.push(salesUserId); extra += ` AND d.sales_user_id=$${params.length}`; }
      // デフォルトは進行中のみ（won/lost除外）
      if (status === 'all') { /* no filter */ }
      else if (status === 'won') { extra += ` AND d.status='won'`; }
      else if (status === 'lost') { extra += ` AND d.status='lost'`; }
      else { extra += ` AND d.status='active'`; }
      const limit = Math.min(Number(limitQ) || 500, 2000);
      const sql = `SELECT d.id, d.name, d.yomi, d.status, d.customer_id, d.sales_user_id, d.na_user_id, d.initial_fee, d.monthly_fee, d.data, d.order_date, d.updated_at, COALESCE(d.sales_person, d.sales_user_id) AS sales_person, c.name AS customer_name FROM deals d JOIN customers c ON c.id = d.customer_id WHERE d.team_id=$1${extra} ORDER BY d.updated_at DESC LIMIT ${limit}`;
      const { rows } = await dbQuery(sql, params);
      // 担当者リスト（DBの実際の値）
      const { rows: salesRows } = await dbQuery(
        `SELECT DISTINCT sales_user_id as name FROM deals WHERE team_id=$1 AND sales_user_id IS NOT NULL
         UNION SELECT DISTINCT na_user_id FROM deals WHERE team_id=$1 AND na_user_id IS NOT NULL
         ORDER BY name`,
        [teamId]
      );
      const salesUsers = salesRows.map(r => r.name).filter(n => n && !n.includes('�'));
      res.json({ deals: rows, meta: { yomiOptions: YOMI_OPTIONS, contractTypes: CONTRACT_TYPES, paymentTypes: PAYMENT_TYPES, lostReasons: LOST_REASONS, salesUsers } });
    } catch (e) {
      console.error('[CRM] list deals error:', e.message, e.stack);
      res.status(500).json({ error: 'internal', detail: e.message });
    }
  });

  expressApp.post('/api/crm/deals', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { customerId, name, yomi, contractType, paymentType, salesUserId, naUserId,
              initialFee, monthlyFee, contractMonths, hiringTarget, employmentType, memo } = req.body || {};
      if (!customerId || !name?.trim()) return res.status(400).json({ error: 'invalid_params' });
      const { rows: [row] } = await dbQuery(`
        INSERT INTO deals (id, team_id, customer_id, name, yomi, contract_type, payment_type,
          sales_user_id, na_user_id, initial_fee, monthly_fee, contract_months,
          hiring_target, employment_type, memo, created_by)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING *
      `, [randomUUID(), teamId, customerId, name.trim(),
          yomi||'アポ化前', contractType||null, paymentType||null,
          salesUserId||null, naUserId||null,
          initialFee||null, monthlyFee||null, contractMonths||null,
          hiringTarget||null, employmentType||null, memo||null, userId]);
      res.status(201).json({ deal: row });
    } catch (e) {
      console.error('[CRM] create deal error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.patch('/api/crm/deals/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, yomi, contractType, paymentType, salesUserId, naUserId,
              initialFee, monthlyFee, contractMonths, hiringTarget, employmentType,
              lostReason, status, memo, data, firstMeetingDate,
              settlementForecast, forecastConfidence,
              updatedAt, force } = req.body || {};
      const { rows: [existing] } = await dbQuery(
        `SELECT * FROM deals WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]
      );
      if (!existing) return res.status(404).json({ error: 'not_found' });

      // オプティミスティックロック：updatedAt が渡されていて force でなければ競合チェック
      if (updatedAt && !force) {
        const serverTs = new Date(existing.updated_at).getTime();
        const clientTs = new Date(updatedAt).getTime();
        if (serverTs !== clientTs) {
          return res.status(409).json({
            error: 'conflict',
            message: '他のユーザーがこのデータを更新しました',
            serverUpdatedAt: existing.updated_at,
          });
        }
      }

      const newYomi = yomi ?? existing.yomi;
      const newStatus = newYomi === '受注' ? 'won' : newYomi === '失注' ? 'lost' : (status || existing.status);
      const { rows: [row] } = await dbQuery(`
        UPDATE deals SET
          name=$3, yomi=$4, contract_type=$5, payment_type=$6,
          sales_user_id=$7, na_user_id=$8, initial_fee=$9, monthly_fee=$10,
          contract_months=$11, hiring_target=$12, employment_type=$13,
          lost_reason=$14, status=$15, memo=$16,
          data=COALESCE($17::jsonb, data),
          first_meeting_date=COALESCE($18::date, first_meeting_date),
          settlement_forecast=$19, forecast_confidence=$20,
          updated_at=now()
        WHERE id=$1 AND team_id=$2 RETURNING *
      `, [req.params.id, teamId,
          name??existing.name, newYomi,
          contractType??existing.contract_type, paymentType??existing.payment_type,
          salesUserId??existing.sales_user_id, naUserId??existing.na_user_id,
          initialFee??existing.initial_fee, monthlyFee??existing.monthly_fee,
          contractMonths??existing.contract_months, hiringTarget??existing.hiring_target,
          employmentType??existing.employment_type, lostReason??existing.lost_reason,
          newStatus, memo??existing.memo, data ? JSON.stringify(data) : null,
          firstMeetingDate||null,
          settlementForecast!==undefined ? (settlementForecast||null) : existing.settlement_forecast,
          forecastConfidence!==undefined ? (forecastConfidence||null) : existing.forecast_confidence]);

      // 受注になった場合、RPO案件を自動生成（まだなければ）
      let rpoClientId = row.data?.rpo_client_id || null;
      if (newYomi === '受注' && existing.yomi !== '受注' && !rpoClientId) {
        try {
          const { rows: [customer] } = await dbQuery(
            `SELECT * FROM customers WHERE id=$1 AND team_id=$2`, [row.customer_id, teamId]
          );
          // 既に同dealでRPO案件がないか確認（crmDealId / dealId 両方チェック）
          const { rows: existing_rpo } = await dbQuery(
            `SELECT id FROM rpo_clients WHERE team_id=$1 AND (data->>'crmDealId'=$2 OR data->>'dealId'=$2)`,
            [teamId, row.id]
          );
          if (existing_rpo.length === 0) {
            const plan = (row.contract_type || row.payment_method || '').includes('月額') ? 'monthly' : 'guarantee';
            const colorOpts = ['Ocean','Emerald','Amber','Rose','Violet','Pink','Teal','Slate'];
            const color = colorOpts[Math.floor(Math.random() * colorOpts.length)];
            const contractAmount = row.unit_price && row.guarantee_count
              ? Number(row.unit_price) * Number(row.guarantee_count)
              : row.monthly_cost && row.contract_months
                ? Number(row.monthly_cost) * Number(row.contract_months) + (Number(row.initial_cost) || 0)
                : Number(row.initial_fee) || 0;
            const { rows: [rpoClient] } = await dbQuery(`
              INSERT INTO rpo_clients (id, team_id, name, color, plan, status, phase, data, created_by)
              VALUES (gen_random_uuid()::text, $1, $2, $3, $4, 'active', 'cr', $5::jsonb, $6)
              RETURNING id
            `, [teamId, customer?.name || row.name, color, plan,
                JSON.stringify({
                  crmDealId: row.id,
                  projectInfo: {
                    hiringTarget:  row.hiring_target || 0,
                    contractAmount,
                    inrevoContact: row.sales_person || '',
                    startDate:     row.order_date ? String(row.order_date).slice(0, 10) : '',
                  },
                }),
                req.dashboardUser.userId]);
            rpoClientId = rpoClient.id;
            await dbQuery(
              `UPDATE deals SET data=jsonb_set(COALESCE(data,'{}'), '{rpo_client_id}', $3::jsonb) WHERE id=$1 AND team_id=$2`,
              [row.id, teamId, JSON.stringify(rpoClientId)]
            );
            row.data = { ...row.data, rpo_client_id: rpoClientId };
          }
        } catch (e) {
          console.error('[CRM] RPO auto-create error:', e);
        }
      }

      res.json({ deal: row, rpoClientId });
    } catch (e) {
      console.error('[CRM] update deal error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  expressApp.delete('/api/crm/deals/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM deals WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) {
      console.error('[CRM] delete deal error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 月次収支サマリー ──────────────────────────────────────────────
  expressApp.get('/api/crm/monthly-summary', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { month, salesUser } = req.query; // YYYY-MM 形式
      const targetMonth = month ? new Date(`${month}-01`) : new Date();
      const year = targetMonth.getFullYear();
      const mon = targetMonth.getMonth() + 1;
      const monthStart = `${year}-${String(mon).padStart(2,'0')}-01`;
      const monthEnd = new Date(year, mon, 0).toISOString().split('T')[0];

      // 担当者ロール（フラグ）を取得して入金を振り分ける
      const repRolesRes = await dbQuery(`
        SELECT rep_name,
               COALESCE(is_retired, false)          AS is_retired,
               COALESCE(exclude_from_kpi, false)    AS exclude_from_kpi,
               COALESCE(monthly_to_adda_ref, false) AS monthly_to_adda_ref
        FROM crm_rep_roles WHERE team_id=$1
      `, [teamId]);
      const REP_ROLES_MS = repRolesRes.rows;
      const findRoleForMS = (staff) => {
        if (!staff) return null;
        for (const r of REP_ROLES_MS) {
          if (!r.rep_name) continue;
          if (staff.includes(r.rep_name) || r.rep_name.includes(staff)) return r;
        }
        return null;
      };
      const classifyPaymentMS = (staff, plan, inflow) => {
        if ((staff || '').includes('丸山') && inflow === 'グラハム') return 'excluded';
        const role = findRoleForMS(staff);
        if (role?.exclude_from_kpi)    return 'excluded';
        if (role?.is_retired)          return 'adda_ref';
        if (role?.monthly_to_adda_ref) return (plan || '').includes('月額') ? 'adda_ref' : 'bc';
        if (role)                      return 'bc';
        return 'adda_ref';
      };

      // 今月入金（期間内の全行取得 → JS で振り分け、BC計上分のみ confirmed として扱う）
      const allPaymentsRes = await dbQuery(`
        SELECT payment_date, company, staff, plan, inflow_source,
               amount AS payment_amount,
               incentive_amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1 AND $2
          AND amount > 0
        ORDER BY payment_date
      `, [monthStart, monthEnd]);

      const ADDA_REF_MS = '添田/リファラル';
      const isAddaRefMS = salesUser === ADDA_REF_MS;
      // KPI対象 = 'bc' + 'adda_ref'（'excluded' = アライアンスのみ除外）
      // addaRef フィルタ選択時は adda_ref のみに絞る
      const paymentsRes = {
        rows: allPaymentsRes.rows.filter(p => {
          if (salesUser && !isAddaRefMS && !(p.staff || '').includes(salesUser)) return false;
          const dest = classifyPaymentMS(p.staff, p.plan, p.inflow_source);
          if (dest === 'excluded') return false;
          if (isAddaRefMS && dest !== 'adda_ref') return false;
          return true;
        }),
      };

      // 担当者フィルタ（addaRef 時は設定済担当者以外）
      let salesFilter, salesParams;
      if (isAddaRefMS) {
        const configured = REP_ROLES_MS.map(r => r.rep_name).filter(Boolean);
        if (configured.length > 0) {
          salesFilter = `AND (${configured.map((_, i) => `COALESCE(d.sales_person, d.sales_user_id) NOT ILIKE $${i + 2}`).join(' AND ')})`;
          salesParams = [teamId, ...configured.map(r => `%${r}%`)];
        } else {
          salesFilter = '';
          salesParams = [teamId];
        }
      } else if (salesUser) {
        salesFilter = `AND COALESCE(d.sales_person, d.sales_user_id)=$2`;
        salesParams = [teamId, salesUser];
      } else {
        salesFilter = '';
        salesParams = [teamId];
      }

      // 締結ほぼ確実 (yomi A or S)（担当者フィルタ対応）
      const highRes = await dbQuery(`
        SELECT d.id, d.name, d.yomi, d.contract_type, d.initial_fee, d.monthly_fee,
               d.unit_price, COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, d.settlement_forecast, d.forecast_confidence,
               c.name AS customer_name, c.id AS customer_id
        FROM deals d JOIN customers c ON c.id = d.customer_id
        WHERE d.team_id=$1 ${salesFilter}
          AND d.yomi IN ('A 70％','S 90％')
          AND d.status = 'active'
        ORDER BY d.updated_at DESC
      `, salesParams);

      // 締結多分いける (yomi B or C)（担当者フィルタ対応）
      const medRes = await dbQuery(`
        SELECT d.id, d.name, d.yomi, d.contract_type, d.initial_fee, d.monthly_fee,
               d.unit_price, COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, c.name AS customer_name, c.id AS customer_id
        FROM deals d JOIN customers c ON c.id = d.customer_id
        WHERE d.team_id=$1 ${salesFilter}
          AND d.yomi IN ('B 50％','C 30％')
          AND d.status = 'active'
        ORDER BY d.updated_at DESC
      `, salesParams);

      // KPI計算ヘルパー
      const calcKpi = (deal) => {
        const plan = String(deal.contract_type || '');
        const TAX = 1.1;
        if (plan.includes('採用保証')) {
          const amt = Number(deal.monthly_fee || deal.initial_fee || 0) * TAX;
          return Math.round(amt * 0.6);
        }
        if (plan.includes('月額')) {
          const unit = Math.round(Number(deal.unit_price || 0) * TAX);
          const init = Math.round(Number(deal.initial_fee || 0) * TAX);
          return unit + init;
        }
        return 0;
      };

      // 担当者別集計
      const staffMap = {};
      const addToStaff = (name, key, amount) => {
        const n = name || '未設定';
        if (!staffMap[n]) staffMap[n] = { name: n, confirmed: 0, confirmedIncentive: 0, high: 0, highKpi: 0, medium: 0, mediumKpi: 0, thisMonthMaybe: 0, nextMonthForecast: 0 };
        staffMap[n][key] += amount;
      };

      paymentsRes.rows.forEach(p => {
        addToStaff(p.staff || '未設定', 'confirmed', Number(p.payment_amount || 0));
        addToStaff(p.staff || '未設定', 'confirmedIncentive', Number(p.incentive_amount || 0));
      });
      highRes.rows.forEach(d => {
        const staffName = d.sales_person || d.sales_user_id || '未設定';
        const amt = Number(d.monthly_fee || d.initial_fee || 0) * 1.1;
        addToStaff(staffName, 'high', Math.round(amt));       // 売上見込み（表示用）
        addToStaff(staffName, 'highKpi', calcKpi(d));         // インセン見込み（KPI用）
        // 営業手動の締結見込み（今月可能性あり / 来月締結見込み）
        if (d.settlement_forecast === '今月可能性あり') addToStaff(staffName, 'thisMonthMaybe', Math.round(amt));
        else if (d.settlement_forecast === '来月締結見込み') addToStaff(staffName, 'nextMonthForecast', Math.round(amt));
      });
      medRes.rows.forEach(d => {
        const staffName = d.sales_person || d.sales_user_id || '未設定';
        const amt = Number(d.monthly_fee || d.initial_fee || 0) * 1.1;
        addToStaff(staffName, 'medium', Math.round(amt));     // 売上見込み（表示用）
        addToStaff(staffName, 'mediumKpi', calcKpi(d));       // インセン見込み（KPI用）
      });

      const staffSummary = Object.values(staffMap).map(s => ({
        ...s,
        total:    s.confirmed + s.high + s.medium,
        kpiTotal: (s.confirmedIncentive || 0) + (s.highKpi || 0) + (s.mediumKpi || 0),
        kpi:      (s.highKpi || 0) + (s.mediumKpi || 0), // パイプライン KPI合計（目標フォールバック用）
      })).sort((a,b) => b.total - a.total);

      const totals = staffSummary.reduce((acc, s) => ({
        confirmed:          acc.confirmed          + s.confirmed,
        confirmedIncentive: acc.confirmedIncentive + (s.confirmedIncentive || 0),
        high:               acc.high               + s.high,
        highKpi:            acc.highKpi            + (s.highKpi || 0),
        medium:             acc.medium             + s.medium,
        mediumKpi:          acc.mediumKpi          + (s.mediumKpi || 0),
        total:              acc.total              + s.total,
        kpiTotal:           acc.kpiTotal           + (s.kpiTotal || 0),
        kpi:                acc.kpi                + (s.kpi || 0),
        thisMonthMaybe:     acc.thisMonthMaybe     + (s.thisMonthMaybe || 0),
        nextMonthForecast:  acc.nextMonthForecast  + (s.nextMonthForecast || 0),
      }), { confirmed: 0, confirmedIncentive: 0, high: 0, highKpi: 0, medium: 0, mediumKpi: 0, total: 0, kpiTotal: 0, kpi: 0, thisMonthMaybe: 0, nextMonthForecast: 0 });

      res.json({
        month: `${year}-${String(mon).padStart(2,'0')}`,
        totals,
        staffSummary,
        payments: paymentsRes.rows,
        highDeals: highRes.rows,
        mediumDeals: medRes.rows,
      });
    } catch (e) {
      console.error('[CRM] monthly-summary error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 個人成績評価 ──────────────────────────────────────────────

  // ロール目標 GET/PUT
  expressApp.get('/api/crm/role-targets', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery('SELECT * FROM crm_role_targets WHERE team_id=$1 ORDER BY sort_order', [teamId]);
      res.json({ targets: r.rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/crm/role-targets', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { targets } = req.body; // [{ role_name, monthly_target, sort_order }]
      const validNames = targets.map(t => t.role_name).filter(Boolean);
      // リストに無くなった役職は削除
      if (validNames.length > 0) {
        await dbQuery(
          `DELETE FROM crm_role_targets WHERE team_id=$1 AND role_name NOT IN (${validNames.map((_, i) => `$${i+2}`).join(',')})`,
          [teamId, ...validNames]
        );
      } else {
        await dbQuery(`DELETE FROM crm_role_targets WHERE team_id=$1`, [teamId]);
      }
      for (const t of targets) {
        if (!t.role_name) continue;
        await dbQuery(`INSERT INTO crm_role_targets (team_id, role_name, monthly_target, sort_order) VALUES ($1,$2,$3,$4)
          ON CONFLICT (team_id, role_name) DO UPDATE SET monthly_target=$3, sort_order=$4`,
          [teamId, t.role_name, t.monthly_target, t.sort_order ?? 0]);
      }
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 期間設定 GET/PUT
  expressApp.get('/api/crm/period-settings', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery('SELECT * FROM crm_period_settings WHERE team_id=$1', [teamId]);
      res.json({ settings: r.rows[0] || { prev_start:'2025-08-01', prev_end:'2025-11-30', curr_start:'2025-12-01', curr_end:'2026-05-31' } });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/crm/period-settings', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { prevStart, prevEnd, currStart, currEnd, termTarget } = req.body;
      const tgt = (termTarget === '' || termTarget == null) ? null : Number(termTarget);
      await dbQuery(`INSERT INTO crm_period_settings (team_id, prev_start, prev_end, curr_start, curr_end, term_target)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (team_id) DO UPDATE SET prev_start=$2, prev_end=$3, curr_start=$4, curr_end=$5, term_target=$6, updated_at=now()`,
        [teamId, prevStart, prevEnd, currStart, currEnd, tgt]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // 個人成績計算
  expressApp.get('/api/crm/individual-performance', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { staff } = req.query;
      if (!staff) return res.status(400).json({ error: 'staff required' });

      // 設定取得（前期役職情報も取得）
      const [periodRes, roleRes, repRoleRes] = await Promise.all([
        dbQuery('SELECT * FROM crm_period_settings WHERE team_id=$1', [teamId]),
        dbQuery('SELECT * FROM crm_role_targets WHERE team_id=$1 ORDER BY sort_order', [teamId]),
        dbQuery('SELECT rep_name, role_name, prev_role_name, monthly_target_override, prev_monthly_target_override FROM crm_rep_roles WHERE team_id=$1 AND rep_name=$2', [teamId, staff]),
      ]);
      const period = periodRes.rows[0] || { prev_start:'2025-08-01', prev_end:'2025-11-30', curr_start:'2025-12-01', curr_end:'2026-05-31' };
      const roles = roleRes.rows;
      const repRole = repRoleRes.rows[0] || {};

      // 役職別目標マップ
      const roleTargetMap = {};
      for (const r of roles) roleTargetMap[r.role_name] = Number(r.monthly_target || 0);

      // 前期目標（前期役職 or 現役職にフォールバック）
      const prevRoleName = repRole.prev_role_name || repRole.role_name || '役職無し';
      const prevMonthlyTarget = repRole.prev_monthly_target_override
        ? Number(repRole.prev_monthly_target_override)
        : (roleTargetMap[prevRoleName] || 0);

      // 前期・今期のインセン合計
      const [prevRes, currRes] = await Promise.all([
        dbQuery(`SELECT SUM(incentive_amount) AS total FROM kintone_payments WHERE staff=$1 AND payment_date BETWEEN $2 AND $3 AND incentive_amount > 0`, [staff, period.prev_start, period.prev_end]),
        dbQuery(`SELECT staff, payment_date, incentive_amount, company, plan FROM kintone_payments WHERE staff=$1 AND payment_date BETWEEN $2 AND $3 AND incentive_amount > 0 ORDER BY payment_date`, [staff, period.curr_start, period.curr_end]),
      ]);
      const prevTotal = Number(prevRes.rows[0]?.total || 0);
      const currRows = currRes.rows;
      const currTotal = currRows.reduce((s, r) => s + Number(r.incentive_amount || 0), 0);

      // 月別集計（今期）
      const monthlyMap = {};
      currRows.forEach(r => {
        // DateオブジェクトはtoISOString()で安全にYYYY-MMへ変換
        const pd = r.payment_date;
        const m = pd
          ? (pd instanceof Date ? pd.toISOString() : String(pd)).substring(0, 7)
          : '不明';
        if (!monthlyMap[m]) monthlyMap[m] = 0;
        monthlyMap[m] += Number(r.incentive_amount || 0);
      });

      // 月数: 前期・今期それぞれ実際の期間で計算
      const currStart = new Date(period.curr_start);
      const currEnd = new Date(period.curr_end);
      const prevStart = new Date(period.prev_start);
      const prevEnd = new Date(period.prev_end);
      const today = new Date();
      const evalDate = today < currEnd ? today : currEnd;
      const elapsedMonths = Math.ceil((evalDate - currStart) / (30.44 * 24 * 3600 * 1000));
      const totalCurrMonths = Math.round((currEnd - currStart) / (30.44 * 24 * 3600 * 1000));
      const totalPrevMonths = Math.round((prevEnd - prevStart) / (30.44 * 24 * 3600 * 1000));

      res.json({
        staff, prevTotal, currTotal, currRows: currRows.slice(0,50), monthlyMap,
        elapsedMonths, totalCurrMonths, totalPrevMonths,
        period, roles, repRole, prevMonthlyTarget,
      });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // App170 手動同期
  expressApp.post('/api/crm/sync-payments', authWithRole, async (req, res) => {
    try {
      const { syncKintonePayments } = require('./kintone-sync');
      await syncKintonePayments();
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ── 成績対象スタッフ一覧（役職付き）────────────────────────────
  expressApp.get('/api/crm/perf-staff', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      // kintone_payments に存在するスタッフ名と Slack プロフィールを突合
      const staffRes = await dbQuery(
        `SELECT DISTINCT staff FROM kintone_payments WHERE staff IS NOT NULL ORDER BY staff`,
        []
      );
      const allStaff = staffRes.rows.map(r => r.staff);

      // Slack ディレクトリからアバター取得（役職はcrm_rep_rolesの手動設定のみを参照）
      const dirRes = await dbQuery(
        `SELECT display_name, real_name, profile_json->>'image_72' AS avatar_url
         FROM dashboard_user_directory WHERE team_id=$1 AND is_active=true`,
        [teamId]
      );
      const repRoleRes = await dbQuery(
        `SELECT rep_name, role_name FROM crm_rep_roles WHERE team_id=$1`, [teamId]
      );
      const repRoleMap = Object.fromEntries(repRoleRes.rows.map(r => [r.rep_name, r.role_name || '役職無し']));

      const result = allStaff.map(staffName => {
        const lastName = staffName.split(/[\s　]/)[0];
        const profile = dirRes.rows.find(d =>
          d.display_name?.includes(lastName) || d.real_name?.includes(lastName)
        );
        return {
          name: staffName,
          displayName: staffName.split(/[\s　]/)[0],
          role: repRoleMap[staffName] || '役職無し',
          title: null,
          avatar_url: profile?.avatar_url || null,
        };
      });

      res.json({ staff: result });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── ヨミ管理（担当者ごとのC以上進行中案件）──────────────────────
  expressApp.get('/api/crm/yomi-kanri', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(`
        SELECT d.id, d.customer_id, d.name, d.yomi, d.contract_type,
               d.initial_fee, d.monthly_fee, d.unit_price,
               COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, d.updated_at, d.sales_memo, d.memo,
               d.next_action_date, d.next_action_content,
               d.settlement_forecast, d.forecast_confidence,
               c.name AS customer_name
        FROM deals d JOIN customers c ON c.id = d.customer_id
        WHERE d.team_id=$1
          AND d.yomi IN ('C 30％','B 50％','A 70％','S 90％')
          AND d.status = 'active'
        ORDER BY
          CASE d.yomi
            WHEN 'S 90％' THEN 1 WHEN 'A 70％' THEN 2
            WHEN 'B 50％' THEN 3 WHEN 'C 30％' THEN 4 ELSE 5
          END, d.updated_at DESC
      `, [teamId]);

      // 担当者別にグルーピング
      const byStaff = {};
      for (const d of rows) {
        const name = d.sales_person || '未設定';
        if (!byStaff[name]) byStaff[name] = [];
        byStaff[name].push(d);
      }

      res.json({ byStaff, total: rows.length });
    } catch (e) {
      console.error('[CRM] yomi-kanri error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── リード管理ダッシュボード ──────────────────────────────────────
  expressApp.get('/api/crm/leads-dashboard', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { from, to } = req.query;

      // 期間デフォルト: 今期（deals の crm_period_settings を参考）
      const periodR = await dbQuery('SELECT curr_start, curr_end FROM crm_period_settings WHERE team_id=$1', [teamId]);
      const ps = periodR.rows[0] || { curr_start: '2025-12-01', curr_end: '2026-05-31' };
      const rangeFrom = from || ps.curr_start;
      const rangeTo   = to   || ps.curr_end;

      const dateFilter = `
        AND (
          (data->>'流入日' IS NOT NULL AND data->>'流入日' != '' AND (data->>'流入日')::date BETWEEN $1::date AND $2::date)
          OR
          (data->>'商談獲得日_マーケチーム' IS NOT NULL AND data->>'商談獲得日_マーケチーム' != '' AND (data->>'商談獲得日_マーケチーム')::date BETWEEN $1::date AND $2::date)
        )
      `;
      const params = [rangeFrom, rangeTo];

      const page     = Math.max(1, Number(req.query.page) || 1);
      const limit    = 50;
      const offset   = (page - 1) * limit;
      const listOnly = req.query.listOnly === '1'; // ページング用：一覧だけ返す

      // 先月比用の前期間を計算
      const rangeFromDate = new Date(rangeFrom), rangeToDate = new Date(rangeTo);
      const daysDiff = Math.round((rangeToDate - rangeFromDate) / 86400000) + 1;
      const prevFrom = new Date(rangeFromDate); prevFrom.setDate(prevFrom.getDate() - daysDiff);
      const prevTo   = new Date(rangeFromDate); prevTo.setDate(prevTo.getDate() - 1);
      const prevParams = [prevFrom.toISOString().slice(0,10), prevTo.toISOString().slice(0,10)];
      const prevDateFilter = `
        AND (
          (data->>'流入日' IS NOT NULL AND data->>'流入日' != '' AND (data->>'流入日')::date BETWEEN $1::date AND $2::date)
          OR
          (data->>'商談獲得日_マーケチーム' IS NOT NULL AND data->>'商談獲得日_マーケチーム' != '' AND (data->>'商談獲得日_マーケチーム')::date BETWEEN $1::date AND $2::date)
        )
      `;

      // マテリアライズドビューを使って高速集計
      // mv_lead_customers: 顧客ごとの基本情報（DISTINCT ON済み）
      // mv_lead_flags: 顧客ごとのフラグ（has_appo, has_lost, lost_reason）
      const customerBase = `
        base AS (
          SELECT lc.customer, lc.inflow_date, lc.source, lc.yomi,
                 lc.yomi_flow, lc.order_date, lc.rep,
                 lf.has_appo, lf.has_lost, lf.lost_reason
          FROM mv_lead_customers lc
          JOIN mv_lead_flags lf ON lf.customer = lc.customer
        )
      `;
      // 追加フィルター（担当者・流入経路・アポ化済みのみ）
      const repFilter   = req.query.rep || '';
      const srcFilter   = req.query.source_filter || '';
      const appoOnly    = req.query.appo_only === 'true';
      const extraParams = [];
      let extraCond = '';
      if (repFilter)  { extraParams.push(repFilter);  extraCond += ` AND rep = $${params.length + extraParams.length}`; }
      if (srcFilter)  { extraParams.push(srcFilter);  extraCond += ` AND source = $${params.length + extraParams.length}`; }
      if (appoOnly)   { extraCond += ` AND has_appo = TRUE`; }
      const allParams = [...params, ...extraParams];

      const customerDateFilter = `
        AND (
          (inflow_date IS NOT NULL AND inflow_date != '' AND inflow_date::date BETWEEN $1::date AND $2::date)
        )${extraCond}
      `;

      const lp = allParams.length; // LIMITのパラメータ位置計算用

      const [funnelR, sourceR, trendAllR, recentR, totalR, prevTotalR, trend12R, appoSourceR, orderSourceR, lostReasonR] = await Promise.all([
        // ファネル（顧客ベース・期間フィルター）
        dbQuery(`
          WITH ${customerBase}
          SELECT COALESCE(NULLIF(yomi,''), '不明') AS yomi, COUNT(*)::int AS cnt
          FROM base WHERE 1=1 ${customerDateFilter}
          GROUP BY yomi ORDER BY cnt DESC
        `, allParams),

        // 流入経路別（顧客ベース・アポ化率/受注率/失注率含む・文字化け除外）
        dbQuery(`
          WITH ${customerBase}
          SELECT source,
                 COUNT(*)::int AS cnt,
                 COUNT(*) FILTER (WHERE has_appo)::int AS appo_cnt,
                 COUNT(*) FILTER (WHERE order_date IS NOT NULL AND order_date != '')::int AS order_cnt,
                 COUNT(*) FILTER (WHERE has_lost)::int AS lost_cnt
          FROM base WHERE 1=1 ${customerDateFilter}
            AND source NOT LIKE '%' || chr(65533) || '%'
          GROUP BY source ORDER BY cnt DESC
        `, allParams),

        // 月次推移（顧客ベース・直近12ヶ月・アポ化数を含む）
        dbQuery(`
          WITH ${customerBase}
          SELECT TO_CHAR(inflow_date::date, 'YYYY-MM') AS month,
                 COUNT(*)::int AS cnt,
                 COUNT(*) FILTER (WHERE has_appo)::int AS appo
          FROM base WHERE inflow_date IS NOT NULL AND inflow_date != ''
            AND inflow_date::date >= (CURRENT_DATE - INTERVAL '12 months')
          GROUP BY month ORDER BY month
        `, []),

        // リード一覧（顧客ベース・ページング）
        dbQuery(`
          WITH ${customerBase}
          SELECT customer, yomi, source, rep, inflow_date
          FROM base WHERE 1=1 ${customerDateFilter}
          ORDER BY inflow_date DESC NULLS LAST
          LIMIT $${lp+1} OFFSET $${lp+2}
        `, [...allParams, limit, offset]),

        // 全カテゴリ集計（相互排他的な定義）
        dbQuery(`
          WITH ${customerBase}
          SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE has_appo = FALSE AND has_lost = FALSE)::int AS apo_before,
            COUNT(*) FILTER (WHERE has_appo = TRUE  AND has_lost = FALSE AND (order_date IS NULL OR order_date = ''))::int AS appo_active,
            COUNT(*) FILTER (WHERE has_appo = TRUE  AND has_lost = TRUE)::int AS lost_with_appo,
            COUNT(*) FILTER (WHERE has_appo = FALSE AND has_lost = TRUE)::int AS miokuri,
            COUNT(*) FILTER (WHERE order_date IS NOT NULL AND order_date != '')::int AS ordered,
            COUNT(*) FILTER (WHERE (yomi_flow IS NULL OR yomi_flow = '') AND has_lost = FALSE AND (order_date IS NULL OR order_date = ''))::int AS no_flow
          FROM base WHERE 1=1 ${customerDateFilter}
        `, allParams),

        // 前期間件数（顧客ベース）
        dbQuery(`
          WITH ${customerBase}
          SELECT COUNT(*)::int AS total FROM base
          WHERE inflow_date IS NOT NULL AND inflow_date != ''
            AND inflow_date::date BETWEEN $1::date AND $2::date
        `, prevParams),

        // 12ヶ月平均用（顧客ベース）
        dbQuery(`
          WITH ${customerBase}
          SELECT TO_CHAR(inflow_date::date, 'YYYY-MM') AS month, COUNT(*)::int AS cnt
          FROM base WHERE inflow_date IS NOT NULL AND inflow_date != ''
            AND inflow_date::date >= (CURRENT_DATE - INTERVAL '12 months')
          GROUP BY month
        `, []),

        // アポ化につながった流入経路（顧客ベース）
        dbQuery(`
          WITH ${customerBase}
          SELECT source, COUNT(*)::int AS cnt
          FROM base WHERE 1=1 ${customerDateFilter}
            AND yomi_flow LIKE '%アポ化済商談前%'
            AND source NOT LIKE '%' || chr(65533) || '%'
          GROUP BY source ORDER BY cnt DESC
        `, allParams),

        // 受注につながった流入経路（顧客ベース）
        dbQuery(`
          WITH ${customerBase}
          SELECT source, COUNT(*)::int AS cnt
          FROM base WHERE 1=1 ${customerDateFilter}
            AND order_date IS NOT NULL AND order_date != ''
            AND source NOT LIKE '%' || chr(65533) || '%'
          GROUP BY source ORDER BY cnt DESC
        `, allParams),

        // 失注理由内訳
        dbQuery(`
          WITH ${customerBase}
          SELECT COALESCE(NULLIF(lost_reason,''), '理由不明') AS reason, COUNT(*)::int AS cnt
          FROM base WHERE 1=1 ${customerDateFilter}
            AND has_lost = TRUE
          GROUP BY reason ORDER BY cnt DESC LIMIT 10
        `, allParams),
      ]);

      // ファネル集計（相互排他的な新定義）
      const tr = totalR.rows[0] || {};
      const currentTotal = tr.total       || 0;
      const appoTotal    = tr.appo_active || 0;  // アポ取得済み・継続中
      const lostTotal    = (tr.lost_with_appo || 0) + (tr.miokuri || 0);
      const periodTotal  = currentTotal;

      const funnel = [
        { label: 'アポ化前',  cnt: tr.apo_before     || 0, key: 'apo_before',     desc: 'アポ未取得・継続中' },
        { label: 'アポ化済み', cnt: tr.appo_active   || 0, key: 'appo_active',    desc: 'アポ取得済み・継続中' },
        { label: '失注',      cnt: tr.lost_with_appo || 0, key: 'lost_with_appo', desc: 'アポ取得後に失注' },
        { label: '見送り',    cnt: tr.miokuri        || 0, key: 'miokuri',        desc: 'アポ未取得で失注/見送り' },
        { label: '受注',      cnt: tr.ordered        || 0, key: 'ordered',        desc: '受注済み' },
      ];
      const noFlowTotal = tr.no_flow || 0;
      const prevTotal    = prevTotalR.rows[0]?.total || 0;
      const avg12 = trend12R.rows.length > 0
        ? Math.round(trend12R.rows.reduce((s, r) => s + r.cnt, 0) / trend12R.rows.length)
        : 0;
      // アポ化済みの12ヶ月平均（trendAllR から計算）
      const avg12Appo = trendAllR.rows.length > 0
        ? Math.round(trendAllR.rows.reduce((s, r) => s + (r.appo || 0), 0) / trendAllR.rows.length)
        : 0;

      // リスト専用（ページング用）
      if (listOnly) {
        const [recentR, totalR] = await Promise.all([
          dbQuery(`
            WITH ${customerBase}
            SELECT customer, yomi, source, rep, inflow_date
            FROM base WHERE 1=1 ${customerDateFilter}
            ORDER BY inflow_date DESC NULLS LAST
            LIMIT $3 OFFSET $4
          `, [...params, limit, offset]),
          dbQuery(`
            WITH ${customerBase}
            SELECT COUNT(*)::int AS total FROM base WHERE 1=1 ${customerDateFilter}
          `, params),
        ]);
        return res.json({ recent: recentR.rows, pagination: { page, limit, total: totalR.rows[0]?.total || 0 } });
      }

      // yomiTypeによるドリルダウン（KPIカードクリック用）
      const yomiType = req.query.yomiType;
      if (yomiType) {
        // customerBaseを使って正確な期間フィルター適用（流入日ベースで絞る）
        const ymCond =
          yomiType === 'apo_before'     ? `AND has_appo = FALSE AND has_lost = FALSE`
          : yomiType === 'appo_active'  ? `AND has_appo = TRUE AND has_lost = FALSE AND (order_date IS NULL OR order_date = '')`
          : yomiType === 'apo_got'      ? `AND has_appo = TRUE AND has_lost = FALSE`
          : yomiType === 'lost_with_appo' ? `AND has_appo = TRUE AND has_lost = TRUE`
          : yomiType === 'miokuri'      ? `AND has_appo = FALSE AND has_lost = TRUE`
          : yomiType === 'in_deal'      ? `AND yomi SIMILAR TO '(E|D|C|B|A|S) [0-9]%'`
          : yomiType === 'order'        ? `AND order_date IS NOT NULL AND order_date != ''`
          : yomiType === 'ordered'      ? `AND order_date IS NOT NULL AND order_date != ''`
          : yomiType === 'no_flow'      ? `AND (yomi_flow IS NULL OR yomi_flow = '') AND has_lost = FALSE AND (order_date IS NULL OR order_date = '')`
          : '';
        const drillR = await dbQuery(`
          WITH ${customerBase}
          SELECT customer, yomi, source, inflow_date, rep
          FROM base WHERE 1=1 ${customerDateFilter} ${ymCond}
          ORDER BY inflow_date DESC NULLS LAST
          LIMIT 200
        `, params);
        return res.json({ drilldown: drillR.rows, source: yomiType });
      }

      // ドリルダウン（source/type指定時）
      const sourceFilter = req.query.source;
      const drillType    = req.query.drillType; // 'appo' | 'order' | null
      if (sourceFilter) {
        const drillParams = [...params];
        const srcCond = sourceFilter === '不明'
          ? `AND (data->>'流入経路' IS NULL OR data->>'流入経路' = '')`
          : `AND data->>'流入経路' = $${drillParams.push(sourceFilter)}`;
        const typeCond = drillType === 'appo'
          ? `AND (data->>'ヨミ_経過フロー' LIKE '%アポ化済商談前%')`
          : drillType === 'order'
          ? `AND (data->>'受注日' IS NOT NULL AND data->>'受注日' != '')`
          : '';
        const drillR = await dbQuery(`
          SELECT
            data->>'顧客' AS customer,
            data->>'ヨミ' AS yomi,
            split_part(COALESCE(NULLIF(data->>'ヨミ_経過フロー',''), data->>'ヨミ'), ', ', 1) AS first_yomi,
            COALESCE(NULLIF(data->>'流入日',''), data->>'商談獲得日_マーケチーム') AS inflow_date,
            COALESCE(NULLIF(data->>'商談獲得者',''), data->>'担当営業_0') AS rep,
            data->>'案件名' AS deal_name
          FROM kintone_cache
          WHERE data->>'ヨミ' IS NOT NULL ${dateFilter} ${srcCond} ${typeCond}
          ORDER BY COALESCE(NULLIF(data->>'流入日',''), data->>'商談獲得日_マーケチーム') DESC NULLS LAST
          LIMIT 200
        `, drillParams);
        return res.json({ drilldown: drillR.rows, source: sourceFilter });
      }

      res.json({
        period: { from: rangeFrom, to: rangeTo },
        funnel,
        periodTotal,
        appoTotal,
        lostTotal,
        noFlowTotal,
        byLostReason: lostReasonR.rows,
        filterOptions: { reps: [], sources: [] },
        avg12Appo,
        stats: {
          current:  currentTotal,
          prev:     prevTotal,
          diffPct:  prevTotal > 0 ? Math.round((currentTotal - prevTotal) / prevTotal * 100) : null,
          avg12,
          vsAvgPct: avg12 > 0 ? Math.round((currentTotal - avg12) / avg12 * 100) : null,
        },
        bySource:      sourceR.rows,
        trend:         trendAllR.rows,
        recent:        recentR.rows,
        appoBySource:  appoSourceR.rows,
        orderBySource: orderSourceR.rows,
        pagination: { page, limit, total: currentTotal },
      });
    } catch (e) {
      console.error('[CRM] leads-dashboard error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

function registerChannelTargetsApi({ expressApp, authWithRole }) {
  // GET /api/crm/channel-performance?from=...&to=...
  expressApp.get('/api/crm/channel-performance', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { from, to } = req.query;
      const periodR = await dbQuery('SELECT curr_start, curr_end FROM crm_period_settings WHERE team_id=$1', [teamId]);
      const ps = periodR.rows[0] || { curr_start: '2025-12-01', curr_end: '2026-05-31' };
      const rangeFrom = from || ps.curr_start;
      const rangeTo   = to   || ps.curr_end;

      // 流入経路別 実績（mv_lead_customers + mv_lead_flags）
      const actualsR = await dbQuery(`
        WITH base AS (
          SELECT lc.customer, lc.inflow_date, lc.source, lc.order_date,
                 lf.has_appo
          FROM mv_lead_customers lc
          JOIN mv_lead_flags lf ON lf.customer = lc.customer
          WHERE lc.inflow_date IS NOT NULL AND lc.inflow_date != ''
            AND lc.inflow_date::date BETWEEN $1::date AND $2::date
            AND lc.source NOT LIKE '%' || chr(65533) || '%'
        )
        SELECT
          source,
          COUNT(*)::int                                              AS actual_leads,
          COUNT(*) FILTER (WHERE has_appo)::int                     AS actual_appo,
          COUNT(*) FILTER (WHERE order_date IS NOT NULL AND order_date != '')::int AS actual_orders
        FROM base
        GROUP BY source
        ORDER BY actual_leads DESC
      `, [rangeFrom, rangeTo]);

      // 受注金額（kintone_cache から期間内受注分を source 別に集計）
      const revenueR = await dbQuery(`
        SELECT
          data->>'流入経路' AS source,
          SUM(NULLIF(NULLIF(data->>'見込売り上げ_税抜き',''), '-')::numeric)::bigint AS revenue
        FROM kintone_cache
        WHERE app_id = '102'
          AND data->>'受注日' IS NOT NULL AND data->>'受注日' != ''
          AND (
            (data->>'流入日' IS NOT NULL AND data->>'流入日' != '' AND (data->>'流入日')::date BETWEEN $1::date AND $2::date)
            OR (data->>'商談獲得日_マーケチーム' IS NOT NULL AND data->>'商談獲得日_マーケチーム' != '' AND (data->>'商談獲得日_マーケチーム')::date BETWEEN $1::date AND $2::date)
          )
          AND data->>'流入経路' IS NOT NULL AND data->>'流入経路' != ''
          AND data->>'流入経路' NOT LIKE '%' || chr(65533) || '%'
        GROUP BY source
      `, [rangeFrom, rangeTo]);
      const revenueMap = {};
      for (const r of revenueR.rows) revenueMap[r.source] = Number(r.revenue) || 0;

      // チャンネル目標設定
      const targetsR = await dbQuery(
        `SELECT * FROM crm_channel_targets WHERE team_id=$1`, [teamId]
      );
      const targetMap = {};
      for (const t of targetsR.rows) targetMap[t.source] = t;

      // kintone 全期間の流入経路一覧（期間フィルターなし）
      const allSourcesR = await dbQuery(`
        SELECT DISTINCT data->>'流入経路' AS source
        FROM kintone_cache
        WHERE app_id = '102'
          AND data->>'流入経路' IS NOT NULL AND data->>'流入経路' != ''
          AND data->>'流入経路' NOT LIKE '%' || chr(65533) || '%'
        ORDER BY source
      `, []);

      // マージ: kintone全流入経路 + 期間内実績 + 目標設定
      const sources = new Set([
        ...allSourcesR.rows.map(r => r.source),
        ...actualsR.rows.map(r => r.source),
        ...targetsR.rows.map(r => r.source),
      ]);
      const rows = Array.from(sources).map(src => {
        const a = actualsR.rows.find(r => r.source === src) || {};
        const t = targetMap[src] || {};
        const leadUp   = Number(t.lead_unit_price)     || 0;
        const expAppo  = Number(t.expected_appo_count) || 0;
        return {
          source:              src,
          lead_unit_price:     leadUp,
          cost_per_month:      leadUp * expAppo,
          vendor_note:         t.vendor_note            || '',
          expected_leads:      Number(t.expected_leads)     || 0,
          expected_appo_count: expAppo,
          expected_appo_rate:  Number(t.expected_appo_rate) || 0,
          expected_order_rate: Number(t.expected_order_rate)|| 0,
          expected_unit_price: Number(t.expected_unit_price)|| 0,
          actual_leads:        Number(a.actual_leads)  || 0,
          actual_appo:         Number(a.actual_appo)   || 0,
          actual_orders:       Number(a.actual_orders) || 0,
          actual_revenue:      revenueMap[src]         || 0,
        };
      }).sort((a, b) => b.actual_leads - a.actual_leads);

      res.json({ rows, period: { from: rangeFrom, to: rangeTo } });
    } catch (e) {
      console.error('[CRM] channel-performance error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // PUT /api/crm/channel-targets  全フィールドをまとめて保存
  expressApp.put('/api/crm/channel-targets', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const {
        source,
        lead_unit_price   = 0,
        vendor_note       = '',
        expected_leads    = 0,
        expected_appo_count  = 0,
        expected_appo_rate   = 0,
        expected_order_rate  = 0,
        expected_unit_price  = 0,
      } = req.body;
      if (!source) return res.status(400).json({ error: 'source required' });
      const cost_per_month = Number(lead_unit_price) * Number(expected_appo_count);
      await dbQuery(`
        INSERT INTO crm_channel_targets
          (team_id, source, lead_unit_price, cost_per_month, vendor_note,
           expected_leads, expected_appo_count, expected_appo_rate, expected_order_rate, expected_unit_price, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
        ON CONFLICT (team_id, source) DO UPDATE SET
          lead_unit_price=EXCLUDED.lead_unit_price,
          cost_per_month=EXCLUDED.cost_per_month,
          vendor_note=EXCLUDED.vendor_note,
          expected_leads=EXCLUDED.expected_leads,
          expected_appo_count=EXCLUDED.expected_appo_count,
          expected_appo_rate=EXCLUDED.expected_appo_rate,
          expected_order_rate=EXCLUDED.expected_order_rate,
          expected_unit_price=EXCLUDED.expected_unit_price,
          updated_at=now()
      `, [teamId, source, lead_unit_price, cost_per_month, vendor_note,
          expected_leads, expected_appo_count, expected_appo_rate, expected_order_rate, expected_unit_price]);
      res.json({ ok: true });
    } catch (e) {
      console.error('[CRM] channel-targets PUT error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerCrmApi, registerDailyReportApi, registerChannelTargetsApi, YOMI_OPTIONS, CONTRACT_TYPES, PAYMENT_TYPES, LOST_REASONS };
