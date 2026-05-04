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
      } = req.body || {};
      const { rows: [row] } = await dbQuery(`
        UPDATE customers SET
          name=$3, industry=$4, prefecture=$5, employee_count=$6, website=$7, memo=$8,
          inflow_date=$9, inflow_source=$10, name_short=$11, competitors=$12,
          business_description=$13, postal_code=$14, address=$15, service_lp_url1=$16, service_lp_url2=$17,
          updated_at=now()
        WHERE id=$1 AND team_id=$2 RETURNING *
      `, [req.params.id, teamId, name, industry||null, prefecture||null, employeeCount||null, website||null, memo||null,
          inflowDate||null, inflowSource||null, nameShort||null, JSON.stringify(competitors||[]),
          businessDescription||null, postalCode||null, address||null, serviceLpUrl1||null, serviceLpUrl2||null]);
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
      const { rows } = await dbQuery(
        `SELECT * FROM deal_activities WHERE team_id=$1 AND deal_id=$2 ORDER BY created_at DESC`,
        [teamId, req.params.id]
      );
      res.json({ activities: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post('/api/crm/deals/:id/activities', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { activityType, result, content, yomiAtTime } = req.body || {};
      if (!activityType) return res.status(400).json({ error: 'activityType required' });
      const { rows: [row] } = await dbQuery(
        `INSERT INTO deal_activities (id, deal_id, team_id, user_id, activity_type, result, content, yomi_at_time, metadata)
         VALUES (gen_random_uuid()::text,$1,$2,$3,$4,$5,$6,$7,'{}') RETURNING *`,
        [req.params.id, teamId, userId, activityType, result||null, content||null, yomiAtTime||null]
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

  // ── ダッシュボード ──────────────────────────────────────────────
  expressApp.get('/api/crm/dashboard', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { salesUser, period = 'month', customMonth } = req.query; // period: 'month' | 'term' | 'custom'

      // 担当者フィルター
      const personFilter = salesUser ? ` AND COALESCE(d.sales_person, d.sales_user_id)=$2` : '';
      const personParams = salesUser ? [teamId, salesUser] : [teamId];

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

      // ── 期間内の主要指標を集計するヘルパー ──
      const getMetrics = async (start, end) => {
        const pf = salesUser ? ` AND COALESCE(d.sales_person, d.sales_user_id)=$2` : '';
        const si = salesUser ? 3 : 2;
        const baseP = salesUser ? [teamId, salesUser, start, end] : [teamId, start, end];

        const [wonRes, meetingRes, payRes] = await Promise.all([
          // 受注件数・金額
          dbQuery(`SELECT COUNT(*)::int AS cnt, COALESCE(SUM(d.initial_fee),0)::bigint AS amount
            FROM deals d WHERE d.team_id=$1${pf}
            AND d.status='won' AND d.order_date BETWEEN $${si}::date AND $${si+1}::date`, baseP),
          // 初回商談数（BCの初回商談日のみ）
          dbQuery(`SELECT COUNT(*)::int AS cnt FROM deals d
            WHERE d.team_id=$1${pf}
            AND d.first_meeting_date BETWEEN $${si}::date AND $${si+1}::date`, baseP),
          // 入金額 & インセン（両方取得）
          dbQuery(`SELECT COALESCE(SUM(kp.amount),0)::bigint AS total_amount,
                          COALESCE(SUM(kp.incentive_amount),0)::bigint AS incentive_amount
            FROM kintone_payments kp
            WHERE kp.payment_date BETWEEN $1::date AND $2::date
            ${salesUser ? `AND kp.staff=$3` : ''}`,
            salesUser ? [start, end, salesUser] : [start, end]),
        ]);

        const wonCount = wonRes.rows[0]?.cnt || 0;
        return {
          wonCount,
          wonAmount:        Number(wonRes.rows[0]?.amount || 0),
          meetingCount:     meetingRes.rows[0]?.cnt || 0,
          paymentAmount:    Number(payRes.rows[0]?.total_amount || 0),    // 入金額合計
          incentiveAmount:  Number(payRes.rows[0]?.incentive_amount || 0), // インセン合計
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

      // 担当者別入金額 & インセン（kintone_payments）
      const repPayRows = await dbQuery(`
        SELECT staff,
               COALESCE(SUM(amount),0)::bigint AS payment_amount,
               COALESCE(SUM(incentive_amount),0)::bigint AS incentive_amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1::date AND $2::date
        GROUP BY staff
      `, [rangeStart, rangeEnd]);
      const repPayMap = {};
      for (const r of repPayRows.rows) {
        repPayMap[r.staff] = { payment: Number(r.payment_amount), incentive: Number(r.incentive_amount) };
      }

      const repTable = repRows.rows.map(r => ({
        rep:              r.rep,
        wonCount:         r.won_count,
        meetingCount:     r.meeting_count,
        paymentAmount:    repPayMap[r.rep]?.payment   || 0,
        incentiveAmount:  repPayMap[r.rep]?.incentive || 0,
      }));

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

      // プラン別入金内訳（1顧客=1件でカウント）
      const planBreakdownRes = await dbQuery(`
        SELECT COALESCE(plan, '未設定') AS plan,
               COUNT(DISTINCT company)::int AS cnt,
               COALESCE(SUM(incentive_amount),0)::bigint AS amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1::date AND $2::date
          AND incentive_amount > 0
        GROUP BY 1 ORDER BY amount DESC
      `, [rangeStart, rangeEnd]);

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

      // Slack プロフィールから役職を自動推定するヘルパー（perf-staffと共通ロジック）
      const inferRoleFromTitle = (title) => {
        if (!title) return '役職無し';
        const t = title.toLowerCase();
        if (t.includes('sub expert'))  return 'Sub Expert';
        if (t.includes('expert'))      return 'Expert';
        if (t.includes('sub manager')) return 'Sub Manager';
        if (t.includes('sub chief'))   return 'Sub Chief';
        if (t.includes('chief'))       return 'Chief';
        if (t.includes('manager'))     return 'Chief';
        if (t.includes('lead'))        return 'Lead';
        return '役職無し';
      };

      // Slack ディレクトリから担当者タイトルを取得
      const slackDirRes = await dbQuery(
        `SELECT display_name, real_name, profile_json->>'title' AS title
         FROM dashboard_user_directory WHERE team_id=$1 AND is_active=true`,
        [teamId]
      );

      const repTargetMap = {};
      const repRoleInferred = {}; // フロントに渡す自動推定役職
      const TARGET_REPS_SERVER = ['山本 夏乃','板金 慎太郎','萩原 隼人','藤原 一矢','野村 尭弘'];
      let teamTarget = 0;
      for (const rep of TARGET_REPS_SERVER) {
        const repRole  = repRepRoleMap[rep];
        const override = repRole?.monthly_target_override;

        // 役職: 手動設定 > Slackプロフィール自動推定
        let roleName = repRole?.role_name || '';
        if (!roleName) {
          const lastName = rep.split(/[\s　]/)[0];
          const profile  = slackDirRes.rows.find(d =>
            d.display_name?.includes(lastName) || d.real_name?.includes(lastName)
          );
          roleName = inferRoleFromTitle(profile?.title);
        }
        repRoleInferred[rep] = roleName;

        const effective = override != null ? Number(override) : (roleTargetMap[roleName] || 0);
        repTargetMap[rep] = effective;
        teamTarget += effective;
      }

      res.json({
        period, rangeStart, rangeEnd,
        prevStart, prevEnd,
        curr: currMetrics,
        prev: prevMetrics,
        repTable,
        repTargetMap,
        repRoleInferred,
        teamTarget,
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
        const { rows } = await dbQuery(`
          SELECT payment_date, company, plan, incentive_amount, amount
          FROM kintone_payments
          WHERE staff=$1
            AND payment_date BETWEEN $2::date AND $3::date
            AND incentive_amount > 0
          ORDER BY payment_date DESC
        `, [rep, start, end]);
        res.json({ rows });
      } else if (type === 'won') {
        const { rows } = await dbQuery(`
          SELECT d.order_date, c.name AS customer_name
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
          AND payment_date <= CURRENT_DATE
          ${salesUser ? `AND staff=$1` : ''}
        GROUP BY 1 ORDER BY 1
      `, salesUser ? [salesUser] : []);
      res.json({ rows });
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
          INSERT INTO crm_rep_roles (team_id, rep_name, role_name, monthly_target_override)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (team_id, rep_name) DO UPDATE
          SET role_name=$3, monthly_target_override=$4
        `, [teamId, r.rep_name, r.role_name || '', r.monthly_target_override || null]);
      }
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
              lostReason, status, memo, data, firstMeetingDate } = req.body || {};
      const { rows: [existing] } = await dbQuery(
        `SELECT * FROM deals WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]
      );
      if (!existing) return res.status(404).json({ error: 'not_found' });

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
          firstMeetingDate||null]);

      // 受注になった場合、RPO案件を自動生成（まだなければ）
      let rpoClientId = row.data?.rpo_client_id || null;
      if (newYomi === '受注' && existing.yomi !== '受注' && !rpoClientId) {
        try {
          const { rows: [customer] } = await dbQuery(
            `SELECT * FROM customers WHERE id=$1 AND team_id=$2`, [row.customer_id, teamId]
          );
          const plan = (row.contract_type || '').includes('採用保証') ? 'guarantee' : 'monthly';
          const colorOpts = ['Ocean','Emerald','Amber','Rose','Violet','Pink','Teal','Slate'];
          const color = colorOpts[Math.floor(Math.random() * colorOpts.length)];
          const { rows: [rpoClient] } = await dbQuery(`
            INSERT INTO rpo_clients (id, team_id, name, color, plan, status, data, created_by)
            VALUES (gen_random_uuid()::text, $1, $2, $3, $4, 'active', $5::jsonb, $6)
            RETURNING id
          `, [teamId, customer?.name || row.name, color, plan,
              JSON.stringify({ dealId: row.id, customerId: row.customer_id,
                projectInfo: { hiringTarget: row.hiring_target || 0 },
                hrAssigneeName: row.na_user_id || null }),
              req.dashboardUser.userId]);
          rpoClientId = rpoClient.id;
          await dbQuery(
            `UPDATE deals SET data=jsonb_set(COALESCE(data,'{}'), '{rpo_client_id}', $3::jsonb) WHERE id=$1 AND team_id=$2`,
            [row.id, teamId, JSON.stringify(rpoClientId)]
          );
          row.data = { ...row.data, rpo_client_id: rpoClientId };
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
      const { month } = req.query; // YYYY-MM 形式
      const targetMonth = month ? new Date(`${month}-01`) : new Date();
      const year = targetMonth.getFullYear();
      const mon = targetMonth.getMonth() + 1;
      const monthStart = `${year}-${String(mon).padStart(2,'0')}-01`;
      const monthEnd = new Date(year, mon, 0).toISOString().split('T')[0];

      // 今月入金確定: kintone_payments (App170) から取得（実入金額 + インセン両方）
      const paymentsRes = await dbQuery(`
        SELECT payment_date, company, staff, plan,
               amount AS payment_amount,
               incentive_amount
        FROM kintone_payments
        WHERE payment_date BETWEEN $1 AND $2
          AND amount > 0
        ORDER BY payment_date
      `, [monthStart, monthEnd]);

      // 締結ほぼ確実 (yomi A or S) — 全進行中案件（日付フィルターなし）
      const highRes = await dbQuery(`
        SELECT d.id, d.name, d.yomi, d.contract_type, d.initial_fee, d.monthly_fee,
               d.unit_price, COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, c.name AS customer_name
        FROM deals d JOIN customers c ON c.id = d.customer_id
        WHERE d.team_id=$1
          AND d.yomi IN ('A 70％','S 90％')
          AND d.status = 'active'
        ORDER BY d.updated_at DESC
      `, [teamId]);

      // 締結多分いける (yomi B or C) — 全進行中案件
      const medRes = await dbQuery(`
        SELECT d.id, d.name, d.yomi, d.contract_type, d.initial_fee, d.monthly_fee,
               d.unit_price, COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, c.name AS customer_name
        FROM deals d JOIN customers c ON c.id = d.customer_id
        WHERE d.team_id=$1
          AND d.yomi IN ('B 50％','C 30％')
          AND d.status = 'active'
        ORDER BY d.updated_at DESC
      `, [teamId]);

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
        if (!staffMap[n]) staffMap[n] = { name: n, confirmed: 0, confirmedIncentive: 0, high: 0, highKpi: 0, medium: 0, mediumKpi: 0 };
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
      }), { confirmed: 0, confirmedIncentive: 0, high: 0, highKpi: 0, medium: 0, mediumKpi: 0, total: 0, kpiTotal: 0, kpi: 0 });

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
      for (const t of targets) {
        await dbQuery(`INSERT INTO crm_role_targets (team_id, role_name, monthly_target, sort_order) VALUES ($1,$2,$3,$4)
          ON CONFLICT (team_id, role_name) DO UPDATE SET monthly_target=$3, sort_order=$4`,
          [teamId, t.role_name, t.monthly_target, t.sort_order ?? 0]);
      }
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
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
      const { prevStart, prevEnd, currStart, currEnd } = req.body;
      await dbQuery(`INSERT INTO crm_period_settings (team_id, prev_start, prev_end, curr_start, curr_end) VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (team_id) DO UPDATE SET prev_start=$2, prev_end=$3, curr_start=$4, curr_end=$5, updated_at=now()`,
        [teamId, prevStart, prevEnd, currStart, currEnd]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // 個人成績計算
  expressApp.get('/api/crm/individual-performance', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { staff } = req.query;
      if (!staff) return res.status(400).json({ error: 'staff required' });

      // 設定取得
      const [periodRes, roleRes] = await Promise.all([
        dbQuery('SELECT * FROM crm_period_settings WHERE team_id=$1', [teamId]),
        dbQuery('SELECT * FROM crm_role_targets WHERE team_id=$1 ORDER BY sort_order', [teamId]),
      ]);
      const period = periodRes.rows[0] || { prev_start:'2025-08-01', prev_end:'2025-11-30', curr_start:'2025-12-01', curr_end:'2026-05-31' };
      const roles = roleRes.rows;

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
        const m = r.payment_date ? String(r.payment_date).substring(0, 7) : '不明';
        if (!monthlyMap[m]) monthlyMap[m] = 0;
        monthlyMap[m] += Number(r.incentive_amount || 0);
      });

      // 今期経過月数
      const currStart = new Date(period.curr_start);
      const currEnd = new Date(period.curr_end);
      const today = new Date();
      const evalDate = today < currEnd ? today : currEnd;
      const elapsedMonths = Math.ceil((evalDate - currStart) / (30.44 * 24 * 3600 * 1000));
      const totalCurrMonths = Math.round((currEnd - currStart) / (30.44 * 24 * 3600 * 1000));

      res.json({
        staff, prevTotal, currTotal, currRows: currRows.slice(0,50), monthlyMap,
        elapsedMonths, totalCurrMonths,
        period, roles,
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

      // Slack ディレクトリから名前とタイトルを取得
      const dirRes = await dbQuery(
        `SELECT display_name, real_name, profile_json->>'title' AS title, profile_json->>'image_72' AS avatar_url
         FROM dashboard_user_directory WHERE team_id=$1 AND is_active=true`,
        [teamId]
      );

      // title から役職を推定
      const inferRole = (title) => {
        if (!title) return null;
        const t = title.toLowerCase();
        if (t.includes('sub expert'))  return 'Sub Expert';
        if (t.includes('expert'))      return 'Expert';
        if (t.includes('sub manager')) return 'Sub Manager';
        if (t.includes('sub chief'))   return 'Sub Chief';
        if (t.includes('chief'))       return 'Chief';
        if (t.includes('manager'))     return 'Chief';
        if (t.includes('lead'))        return 'Lead';
        return '役職無し';
      };

      // スタッフ名でマッチング（kintone名 vs Slack表示名）
      const result = allStaff.map(staffName => {
        const lastName = staffName.split(/[\s　]/)[0]; // 姓のみで検索
        const profile = dirRes.rows.find(d =>
          d.display_name?.includes(lastName) || d.real_name?.includes(lastName)
        );
        return {
          name: staffName,
          displayName: staffName.split(/[\s　]/)[0], // 姓のみ表示
          role: inferRole(profile?.title),
          title: profile?.title || null,
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
        SELECT d.id, d.name, d.yomi, d.contract_type, d.initial_fee, d.monthly_fee,
               d.unit_price, COALESCE(d.sales_person, d.sales_user_id) AS sales_person,
               d.conclusion_date, d.updated_at, d.sales_memo, c.name AS customer_name
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
}

module.exports = { registerCrmApi, registerDailyReportApi, YOMI_OPTIONS, CONTRACT_TYPES, PAYMENT_TYPES, LOST_REASONS };
