// AN依頼管理 API + Slackリスナー + 媒体マスタ + AN調査
const { dbQuery } = require('../db/index');
const { syncMediaMaster, syncAnStudies } = require('./kintone-sync');

const AN_CHANNEL_ID = process.env.AN_CHANNEL_ID || 'C09EFPSSAF2';

// WFメッセージからAN依頼情報をパース
// Slack側で *【…】* / 【*…*】 のような bold装飾が入っても拾えるよう、まず正規化してから抽出
function parseAnRequest(text) {
  // 【】周辺の * を除去（bold装飾を吸収）
  const t = String(text || '')
    .replace(/\*+【/g, '【').replace(/】\*+/g, '】')
    .replace(/【\*+/g, '【').replace(/\*+】/g, '】');
  // ラベルから値（次の【まで or 行末まで）を取り出す
  const get = (label) => {
    const re = new RegExp(`【${label}[^】]*】\\s*\\n([^【]*)`, 's');
    const m = t.match(re);
    return m ? m[1].trim() : '';
  };
  const getMention = (label) => {
    const re = new RegExp(`【${label}[^】]*】\\s*\\n\\s*<@([A-Z0-9]+)>`, 's');
    const m = t.match(re);
    if (m) return m[1];
    const re2 = new RegExp(`【${label}[^】]*】\\s*\\n\\s*@?([^\\n<【]+)`, 's');
    const m2 = t.match(re2);
    return m2 ? m2[1].trim() : '';
  };
  const getUrl = (label) => {
    const re = new RegExp(`【${label}[^】]*】\\s*\\n\\s*<(https?://[^|>]+)`, 's');
    const m = t.match(re);
    if (m) return m[1];
    const re2 = new RegExp(`【${label}[^】]*】\\s*\\n\\s*(https?://[^\\s<【]+)`, 's');
    const m2 = t.match(re2);
    return m2 ? m2[1].trim() : '';
  };

  return {
    sales_person:  getMention('担当営業'),
    mentor:        getMention('メンター'),
    company_name:  get('会社名'),
    kintone_url:   getUrl('kintone'),
    hire_type:     get('新卒or中途'),
    request_type:  get('依頼粒度'),
    detail:        get('アナリティクスチームに依頼したい内容'),
    priority:      get('優先度'),
    hearing:       get('ヒアリング項目'),
    budget:        get('媒体予算') || get('人数に対しての媒体予算'),
  };
}

// Slack user_id → 表示名（キャッシュ）
const _userNameCache = new Map();
async function resolveUserName(teamId, idOrName) {
  if (!idOrName) return '';
  // 既に名前っぽい（U..ではない）ならそのまま
  if (!/^[A-Z0-9]+$/.test(idOrName)) return idOrName;
  const key = `${teamId}:${idOrName}`;
  if (_userNameCache.has(key)) return _userNameCache.get(key);
  try {
    const { rows: [r] } = await dbQuery(
      `SELECT COALESCE(display_name, real_name) AS name
       FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2 LIMIT 1`,
      [teamId, idOrName]
    );
    const name = (r?.name || '').split('/')[0].trim() || idOrName;
    _userNameCache.set(key, name);
    return name;
  } catch { return idOrName; }
}

// kintone URLからrecord_idを抽出してcrm_deal_idを探す
async function findCrmDealId(teamId, kintoneUrl, companyName) {
  // kintone URLからrecord番号を抽出
  const m = kintoneUrl?.match(/record=(\d+)/);
  if (m) {
    const { rows } = await dbQuery(
      `SELECT d.id FROM deals d
       JOIN customers c ON c.id = d.customer_id
       WHERE d.team_id=$1 AND d.data->>'kintone_record_id'=$2
       LIMIT 1`,
      [teamId, m[1]]
    ).catch(() => ({ rows: [] }));
    if (rows[0]) return rows[0].id;
  }
  // 会社名でフォールバック
  if (companyName) {
    const { rows } = await dbQuery(
      `SELECT d.id FROM deals d
       JOIN customers c ON c.id = d.customer_id
       WHERE d.team_id=$1 AND c.name ILIKE $2
       AND d.status != 'lost'
       ORDER BY d.updated_at DESC LIMIT 1`,
      [teamId, `%${companyName}%`]
    ).catch(() => ({ rows: [] }));
    if (rows[0]) return rows[0].id;
  }
  return null;
}

function registerAnApi({ expressApp, authWithRole, slackApp, teamId: defaultTeamId }) {

  // ── AN依頼の検出条件 ─────────────────────────────────
  // ワークフロー定型: 【会社名】と【kintone】が両方含まれる。回答返信(【AN調査結果】)は除外
  function isAnRequestMessage(text) {
    if (!text) return false;
    if (text.includes('【AN調査結果】')) return false;
    return text.includes('【会社名】') && text.includes('【kintone】');
  }

  // 1件取り込み（リスナー & backfill 共通）→ an_studies に新規作成
  async function ingestAnRequest({ teamId, channelId, messageTs, text }) {
    const parsed = parseAnRequest(text);
    // 担当営業を表示名に解決（mention抽出のため）
    const salesName = await resolveUserName(teamId, parsed.sales_person);
    const requesterDate = messageTs ? new Date(Number(messageTs.split('.')[0]) * 1000) : new Date();

    // 重複防止: 同じ slack_message_ts が既にあればスキップ
    if (messageTs) {
      const { rows: dup } = await dbQuery(
        `SELECT record_id FROM an_studies WHERE slack_message_ts=$1 LIMIT 1`, [messageTs]
      );
      if (dup.length > 0) return { inserted: false, parsed, recordId: dup[0].record_id };
    }

    // other_notes に媒体予算 / 依頼粒度 / 詳細をまとめる
    const otherNotes = [
      parsed.budget       ? `【媒体予算】\n${parsed.budget}` : '',
      parsed.request_type ? `【依頼粒度】${parsed.request_type}` : '',
      parsed.detail       ? `【その他】\n${parsed.detail}` : '',
    ].filter(Boolean).join('\n\n');

    // record_id は slack message_ts ベースで生成（同一性確保）
    const recordId = `slack-${(messageTs || Date.now()).toString().replace(/\./g,'')}`;

    const r = await dbQuery(`
      INSERT INTO an_studies
        (record_id, team_id, source, company_name, requester,
         request_date, priority, status, employment_type,
         must_condition, other_notes, slack_message_ts, slack_channel_id,
         case_link, slack_link, data, kintone_updated_at, synced_at)
      VALUES ($1,$2,'slack',$3,$4,$5::date,$6,'対応中',$7,$8,$9,$10,$11,$12,$13,$14::jsonb,now(),now())
      ON CONFLICT (record_id) DO NOTHING
      RETURNING record_id
    `, [
      recordId, teamId,
      parsed.company_name || null,
      salesName || null,
      requesterDate.toISOString().slice(0,10),
      parsed.priority || null,
      parsed.hire_type || null,
      parsed.hearing || null,
      otherNotes || null,
      messageTs || null, channelId || null,
      parsed.kintone_url || null,
      messageTs && channelId ? `https://slack.com/archives/${channelId}/p${String(messageTs).replace('.','')}` : null,
      JSON.stringify({ raw_text: text, parsed, mentor: parsed.mentor || null }),
    ]);
    return { inserted: r.rowCount > 0, parsed, recordId };
  }

  // ── Slackリスナー：AN依頼チャンネルを監視 ──────────────
  if (slackApp && AN_CHANNEL_ID) {
    slackApp.message(async ({ message }) => {
      if (message.channel !== AN_CHANNEL_ID) return;
      // edited/deleted等はスキップ。bot_message（Workflowからの依頼）は許可
      if (message.subtype && message.subtype !== 'bot_message') return;
      if (!isAnRequestMessage(message.text)) return;
      try {
        const { inserted, parsed } = await ingestAnRequest({
          teamId: defaultTeamId, channelId: message.channel, messageTs: message.ts, text: message.text,
        });
        if (inserted) console.log(`[AN] 新規依頼登録: ${parsed.company_name}`);
      } catch (e) {
        console.error('[AN] 登録エラー:', e.message);
      }
    });
    console.log(`[AN] チャンネル ${AN_CHANNEL_ID} を監視中`);
  }

  // ── 過去メッセージ取り込み（backfill） ─────────────────
  expressApp.post('/api/dashboard/an/backfill', authWithRole, async (req, res) => {
    try {
      if (!slackApp) return res.status(500).json({ error: 'Slack未設定' });
      const { teamId } = req.dashboardUser;
      const days = Math.max(1, Math.min(365, Number(req.body?.days) || 30));
      const oldest = Math.floor(Date.now()/1000) - days * 86400;

      let cursor = undefined, scanned = 0, inserted = 0;
      do {
        const r = await slackApp.client.conversations.history({
          channel: AN_CHANNEL_ID, oldest: String(oldest), limit: 200, cursor,
        });
        if (!r.ok) return res.status(500).json({ error: r.error || 'slack_history_error' });
        for (const m of r.messages || []) {
          scanned++;
          if (m.subtype && m.subtype !== 'bot_message') continue;
          if (!isAnRequestMessage(m.text)) continue;
          try {
            const { inserted: ok } = await ingestAnRequest({
              teamId, channelId: AN_CHANNEL_ID, messageTs: m.ts, text: m.text,
            });
            if (ok) inserted++;
          } catch (e) { console.error('[AN backfill]', e.message); }
        }
        cursor = r.response_metadata?.next_cursor;
      } while (cursor);

      res.json({ ok: true, scanned, inserted, days });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // ── CRUD API ──────────────────────────────────────────
  expressApp.get('/api/dashboard/an/requests', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { status } = req.query;
      const extra = status ? ` AND a.status=$2` : '';
      const { rows } = await dbQuery(
        `SELECT a.*, d.name AS deal_name, c.name AS customer_name
         FROM an_requests a
         LEFT JOIN deals d ON d.id = a.crm_deal_id
         LEFT JOIN customers c ON c.id = d.customer_id
         WHERE a.team_id=$1${extra}
         ORDER BY a.created_at DESC`,
        status ? [teamId, status] : [teamId]
      );
      // raw_text から構造化フィールドを再パース＋ユーザー名解決
      const enriched = await Promise.all(rows.map(async (r) => {
        const parsed = r.raw_text ? parseAnRequest(r.raw_text) : {};
        const sales_person_name  = await resolveUserName(teamId, parsed.sales_person || r.sales_person);
        const mentor_name        = await resolveUserName(teamId, parsed.mentor);
        return {
          ...r,
          // 優先順位: DB → 再パース。空なら再パース値で埋める
          company_name: r.company_name || parsed.company_name || null,
          sales_person: sales_person_name || r.sales_person || null,
          mentor_name: mentor_name || null,
          kintone_url: r.kintone_url || parsed.kintone_url || null,
          hire_type: parsed.hire_type || null,
          request_type: r.request_type || parsed.request_type || null,
          priority: r.priority || parsed.priority || null,
          hearing: parsed.hearing || null,
          budget: parsed.budget || null,
          detail_parsed: parsed.detail || null,
        };
      }));
      res.json({ requests: enriched });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.patch('/api/dashboard/an/requests/:id', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { answer, status, est_media_cost, est_unit_price, est_budget, est_hire_count, recommended_media } = req.body;
      const fields = [], vals = [];
      let i = 1;
      if (answer  !== undefined) { fields.push(`answer=$${i++}`);    vals.push(answer); }
      if (status  !== undefined) { fields.push(`status=$${i++}`);    vals.push(status); }
      const numOrNull = (v) => (v === '' || v == null) ? null : Number(v);
      if (est_media_cost    !== undefined) { fields.push(`est_media_cost=$${i++}`);    vals.push(numOrNull(est_media_cost)); }
      if (est_unit_price    !== undefined) { fields.push(`est_unit_price=$${i++}`);    vals.push(numOrNull(est_unit_price)); }
      if (est_budget        !== undefined) { fields.push(`est_budget=$${i++}`);        vals.push(numOrNull(est_budget)); }
      if (est_hire_count    !== undefined) { fields.push(`est_hire_count=$${i++}`);    vals.push(numOrNull(est_hire_count)); }
      if (recommended_media !== undefined) { fields.push(`recommended_media=$${i++}`); vals.push(recommended_media || null); }
      if (answer  !== undefined) {
        fields.push(`answer_by=$${i++}`); vals.push(userId);
        fields.push(`answer_at=now()`);
      }
      fields.push(`updated_at=now()`);
      vals.push(req.params.id, teamId);
      const { rows: [row] } = await dbQuery(
        `UPDATE an_requests SET ${fields.join(',')} WHERE id=$${i++} AND team_id=$${i++} RETURNING *`,
        vals
      );
      res.json({ request: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 案件に紐づくAN依頼を取得（案件詳細パネル用）
  expressApp.get('/api/dashboard/an/by-deal/:dealId', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT * FROM an_requests WHERE team_id=$1 AND crm_deal_id=$2 ORDER BY created_at DESC`,
        [teamId, req.params.dealId]
      );
      res.json({ requests: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 案件詳細からAN依頼を直接起票
  expressApp.post('/api/dashboard/an/from-deal', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { crm_deal_id, company_name, sales_person, request_type, priority, detail } = req.body || {};
      const { rows: [row] } = await dbQuery(`
        INSERT INTO an_requests (team_id, company_name, crm_deal_id, sales_person, request_type, priority, detail, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,'pending') RETURNING *
      `, [teamId, company_name||null, crm_deal_id||null, sales_person||null, request_type||null, priority||null, detail||null]);
      res.status(201).json({ request: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 回答をSlackに投稿
  expressApp.post('/api/dashboard/an/requests/:id/post-to-slack', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [anReq] } = await dbQuery(
        `SELECT * FROM an_requests WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]
      );
      if (!anReq) return res.status(404).json({ error: 'not_found' });
      if (!anReq.answer) return res.status(400).json({ error: '回答が未入力です' });
      if (!slackApp) return res.status(500).json({ error: 'Slack未設定' });

      const text = [
        `*【AN調査結果】${anReq.company_name || ''}*`,
        anReq.request_type ? `依頼内容: ${anReq.request_type}` : '',
        '',
        anReq.answer,
      ].filter(s => s !== null).join('\n');

      await slackApp.client.chat.postMessage({
        channel: AN_CHANNEL_ID,
        text,
        ...(anReq.message_ts ? { thread_ts: anReq.message_ts } : {}),
      });

      await dbQuery(
        `UPDATE an_requests SET status='answered', updated_at=now() WHERE id=$1`,
        [req.params.id]
      );
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // ─── AN依頼に紐づくRPO実績 ──────────────────────────
  expressApp.get('/api/dashboard/an/requests/:id/rpo-results', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [anReq] } = await dbQuery(
        `SELECT * FROM an_requests WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]
      );
      if (!anReq || !anReq.crm_deal_id) return res.json({ results: null });

      // CRM deal → RPO client を辿る
      const { rows: [deal] } = await dbQuery(
        `SELECT data->>'rpo_client_id' AS rpo_client_id FROM deals WHERE id=$1 AND team_id=$2`,
        [anReq.crm_deal_id, teamId]
      );
      const rpoClientId = deal?.rpo_client_id;
      if (!rpoClientId) return res.json({ results: null });

      const { rows: [rpo] } = await dbQuery(
        `SELECT name, data FROM rpo_clients WHERE id=$1 AND team_id=$2`, [rpoClientId, teamId]
      );
      if (!rpo) return res.json({ results: null });

      const media = (rpo.data?.mediaStatus || []).filter(m => m.name && (m.mediaCost > 0 || m.hiredCount > 0));
      const { rows: appRows } = await dbQuery(
        `SELECT status, COUNT(*)::int AS cnt FROM rpo_applicants WHERE rpo_client_id=$1 AND team_id=$2 GROUP BY status`,
        [rpoClientId, teamId]
      );
      const appByStatus = Object.fromEntries(appRows.map(r => [r.status, r.cnt]));
      const totalApplicants = appRows.reduce((s, r) => s + r.cnt, 0);
      const accepted = appByStatus['内定承諾'] || 0;
      const totalCost = media.reduce((s, m) => s + (Number(m.mediaCost) || 0), 0);

      res.json({ results: {
        client_name: rpo.name,
        rpo_client_id: rpoClientId,
        media,
        total_applicants: totalApplicants,
        app_by_status: appByStatus,
        accepted_count: accepted,
        total_cost: totalCost,
        hiring_target: rpo.data?.projectInfo?.hiringTarget || 0,
      }});
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ─── 媒体実績DB（an_study_media + an_studies ベース） ─────────────
  expressApp.get('/api/dashboard/an/media-stats', authWithRole, async (req, res) => {
    try {
      const { employment_type, job_type, priority, requester, period_from, period_to } = req.query;

      const filters = [`m.media_name IS NOT NULL`, `m.media_name <> ''`];
      const params = [];
      const add = (sql, val) => { params.push(val); filters.push(sql.replace('?', `$${params.length}`)); };
      if (employment_type) add(`s.employment_type = ?`, employment_type);
      if (job_type)        add(`s.job_type = ?`, job_type);
      if (priority)        add(`s.priority = ?`, priority);
      if (requester)       add(`s.requester = ?`, requester);
      if (period_from)     add(`s.request_date >= ?::date`, period_from);
      if (period_to)       add(`s.request_date <= ?::date`, period_to);

      const baseFrom = `FROM an_study_media m JOIN an_studies s ON s.record_id = m.study_record_id WHERE ${filters.join(' AND ')}`;

      // 媒体別 集計
      const { rows } = await dbQuery(`
        SELECT m.media_name,
          COUNT(*)::int AS cases,
          ROUND(AVG(NULLIF(m.fee,0)))::bigint           AS avg_fee,
          SUM(COALESCE(m.fee,0))::bigint                AS total_fee,
          ROUND(SUM(COALESCE(m.expected_apps,0))::numeric,1) AS total_expected,
          ROUND(SUM(COALESCE(m.effective_apps,0))::numeric,1) AS total_effective,
          ROUND(AVG(NULLIF(m.expected_apps,0))::numeric,1)    AS avg_expected,
          ROUND(AVG(NULLIF(m.effective_apps,0))::numeric,1)   AS avg_effective,
          ROUND(AVG(NULLIF(m.reply_rate,0))::numeric,2)       AS avg_reply_rate,
          ROUND(CASE WHEN AVG(NULLIF(m.expected_apps,0))>0
                THEN AVG(NULLIF(m.effective_apps,0))/AVG(NULLIF(m.expected_apps,0))*100
                ELSE NULL END, 1) AS forecast_accuracy_pct
        ${baseFrom}
        GROUP BY m.media_name
        ORDER BY cases DESC, total_effective DESC NULLS LAST
      `, params);

      // 媒体×雇用形態 breakdown
      const { rows: byEmp } = await dbQuery(`
        SELECT m.media_name,
               COALESCE(NULLIF(s.employment_type,''),'未設定') AS employment_type,
               COUNT(*)::int AS cases,
               ROUND(SUM(COALESCE(m.effective_apps,0))::numeric,1) AS effective,
               ROUND(AVG(NULLIF(m.fee,0)))::bigint AS avg_fee
        ${baseFrom}
        GROUP BY m.media_name, employment_type
        ORDER BY m.media_name, effective DESC NULLS LAST
      `, params);

      // 媒体×職種 breakdown
      const { rows: byJob } = await dbQuery(`
        SELECT m.media_name,
               COALESCE(NULLIF(s.job_type,''),'未設定') AS job_type,
               COUNT(*)::int AS cases,
               ROUND(SUM(COALESCE(m.effective_apps,0))::numeric,1) AS effective,
               ROUND(AVG(NULLIF(m.fee,0)))::bigint AS avg_fee
        ${baseFrom}
        GROUP BY m.media_name, job_type
        ORDER BY m.media_name, effective DESC NULLS LAST
      `, params);

      // ファセット（全体から）
      const { rows: facetEmp }  = await dbQuery(`SELECT DISTINCT employment_type FROM an_studies WHERE employment_type IS NOT NULL AND employment_type <> '' ORDER BY employment_type`);
      const { rows: facetJob }  = await dbQuery(`SELECT DISTINCT job_type FROM an_studies WHERE job_type IS NOT NULL AND job_type <> '' ORDER BY job_type`);
      const { rows: facetPrio } = await dbQuery(`SELECT DISTINCT priority FROM an_studies WHERE priority IS NOT NULL AND priority <> '' ORDER BY priority`);
      const { rows: facetReq }  = await dbQuery(`SELECT DISTINCT requester FROM an_studies WHERE requester IS NOT NULL AND requester <> '' ORDER BY requester`);

      const groupBy = (arr) => arr.reduce((m, r) => {
        (m[r.media_name] = m[r.media_name] || []).push(r); return m;
      }, {});
      const empByMedia = groupBy(byEmp);
      const jobByMedia = groupBy(byJob);

      res.json({
        stats: rows.map(r => ({
          media_name: r.media_name,
          cases: r.cases,
          total_fee: Number(r.total_fee) || 0,
          avg_fee: Number(r.avg_fee) || 0,
          total_expected: Number(r.total_expected) || 0,
          total_effective: Number(r.total_effective) || 0,
          avg_expected: Number(r.avg_expected) || 0,
          avg_effective: Number(r.avg_effective) || 0,
          avg_reply_rate: Number(r.avg_reply_rate) || 0,
          forecast_accuracy_pct: r.forecast_accuracy_pct,
          by_employment_type: (empByMedia[r.media_name] || []).map(x => ({
            employment_type: x.employment_type, cases: x.cases,
            effective: Number(x.effective)||0, avg_fee: Number(x.avg_fee)||0,
          })),
          by_job_type: (jobByMedia[r.media_name] || []).map(x => ({
            job_type: x.job_type, cases: x.cases,
            effective: Number(x.effective)||0, avg_fee: Number(x.avg_fee)||0,
          })),
        })),
        facets: {
          employment_types: facetEmp.map(r => r.employment_type),
          job_types: facetJob.map(r => r.job_type),
          priorities: facetPrio.map(r => r.priority),
          requesters: facetReq.map(r => r.requester),
        },
      });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 媒体ごとの紐づき案件リスト（予測 vs 実績）
  expressApp.get('/api/dashboard/an/media-cases', authWithRole, async (req, res) => {
    try {
      const { media_name, employment_type, job_type, priority, requester } = req.query;
      if (!media_name) return res.json({ cases: [] });
      const params = [media_name];
      const where = [`m.media_name = $1`];
      const add = (col, v) => { params.push(v); where.push(`${col} = $${params.length}`); };
      if (employment_type) add('s.employment_type', employment_type);
      if (job_type)        add('s.job_type', job_type);
      if (priority)        add('s.priority', priority);
      if (requester)       add('s.requester', requester);

      const { rows } = await dbQuery(`
        SELECT s.record_id, s.company_name, s.requester, s.priority,
               s.employment_type, s.job_type, s.request_date, s.status,
               m.expected_apps, m.effective_apps, m.reply_rate, m.fee,
               m.cost_category, m.an_assignee,
               CASE WHEN COALESCE(m.expected_apps,0) > 0
                    THEN ROUND(COALESCE(m.effective_apps,0)::numeric / m.expected_apps * 100, 1)
                    ELSE NULL END AS accuracy_pct
        FROM an_study_media m
        JOIN an_studies s ON s.record_id = m.study_record_id
        WHERE ${where.join(' AND ')}
        ORDER BY s.request_date DESC NULLS LAST
        LIMIT 200
      `, params);

      res.json({
        cases: rows.map(r => ({
          ...r,
          expected_apps: r.expected_apps != null ? Number(r.expected_apps) : null,
          effective_apps: r.effective_apps != null ? Number(r.effective_apps) : null,
          fee: r.fee != null ? Number(r.fee) : null,
          accuracy_pct: r.accuracy_pct != null ? Number(r.accuracy_pct) : null,
        })),
      });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ─── 媒体マスタ（App225） ─────────────────────────────
  expressApp.get('/api/dashboard/media-master', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const q = (req.query.q || '').trim();
      const industry = req.query.industry || '';
      const jobType  = req.query.job_type || '';
      const area     = req.query.area || '';
      const hireMethod = req.query.hire_method || '';
      const employmentType = req.query.employment_type || '';
      const minScore = Number(req.query.min_score || 0);

      const params = [teamId];
      const where = [`team_id=$1`];
      if (q) {
        params.push(`%${q}%`);
        where.push(`(name ILIKE $${params.length} OR vendor_url ILIKE $${params.length} OR notes ILIKE $${params.length})`);
      }
      if (industry)       { params.push(industry);       where.push(`$${params.length} = ANY(industries)`); }
      if (jobType)        { params.push(jobType);        where.push(`$${params.length} = ANY(job_types)`); }
      if (area)           { params.push(area);           where.push(`$${params.length} = ANY(areas)`); }
      if (hireMethod)     { params.push(hireMethod);     where.push(`$${params.length} = ANY(hire_methods)`); }
      if (employmentType) { params.push(employmentType); where.push(`$${params.length} = ANY(employment_types)`); }
      if (minScore > 0)   { params.push(minScore);       where.push(`COALESCE(recommend_score,0) >= $${params.length}`); }

      const { rows } = await dbQuery(`
        SELECT record_id, name, vendor_url, recommend_score, service_type,
               hire_methods, areas, age_targets, industries, job_types, employment_types,
               basic_billing, norma, notes, caution
        FROM media_master WHERE ${where.join(' AND ')}
        ORDER BY recommend_score DESC NULLS LAST, name
      `, params);

      // ファセット用に全レコードから distinct を集める
      const { rows: facetRows } = await dbQuery(
        `SELECT industries, job_types, areas, hire_methods, employment_types FROM media_master WHERE team_id=$1`,
        [teamId]
      );
      const uniq = (col) => [...new Set(facetRows.flatMap(r => r[col] || []))].sort();
      const facets = {
        industries: uniq('industries'),
        job_types:  uniq('job_types'),
        areas:      uniq('areas'),
        hire_methods: uniq('hire_methods'),
        employment_types: uniq('employment_types'),
      };
      res.json({ media: rows, facets, total: rows.length });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.get('/api/dashboard/media-master/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [m] } = await dbQuery(
        `SELECT * FROM media_master WHERE record_id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!m) return res.status(404).json({ error: 'not_found' });
      res.json({ media: m });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // 媒体マスタ 更新（手動編集）
  expressApp.patch('/api/dashboard/media-master/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const allowed = ['name','vendor_url','recommend_score','service_type',
        'hire_methods','areas','age_targets','industries','job_types','employment_types',
        'basic_billing','norma','notes','caution'];
      const arrFields = new Set(['hire_methods','areas','age_targets','industries','job_types','employment_types']);
      const numFields = new Set(['recommend_score']);
      const sets = [], vals = [];
      let i = 1;
      for (const k of allowed) {
        if (k in req.body) {
          let v = req.body[k];
          if (numFields.has(k)) v = (v==='' || v==null) ? null : Number(v);
          if (arrFields.has(k)) v = Array.isArray(v) ? v : (typeof v === 'string' && v.trim() ? v.split(',').map(s=>s.trim()).filter(Boolean) : []);
          sets.push(`${k}=$${i++}`); vals.push(v);
        }
      }
      if (sets.length === 0) return res.json({ ok: true });
      sets.push(`synced_at=now()`);
      vals.push(req.params.id, teamId);
      const { rows: [row] } = await dbQuery(
        `UPDATE media_master SET ${sets.join(',')} WHERE record_id=$${i++} AND team_id=$${i++} RETURNING *`, vals
      );
      res.json({ media: row });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体マスタ 新規作成
  expressApp.post('/api/dashboard/media-master', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const recordId = `local-${Date.now()}`;
      const { rows: [row] } = await dbQuery(`
        INSERT INTO media_master (record_id, team_id, name, vendor_url, recommend_score, service_type,
                                   hire_methods, areas, employment_types, industries, job_types, notes, data)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'{}'::jsonb) RETURNING *
      `, [
        recordId, teamId,
        req.body?.name || '新規媒体',
        req.body?.vendor_url || null,
        req.body?.recommend_score || null,
        req.body?.service_type || null,
        req.body?.hire_methods || [],
        req.body?.areas || [],
        req.body?.employment_types || [],
        req.body?.industries || [],
        req.body?.job_types || [],
        req.body?.notes || null,
      ]);
      res.json({ media: row });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // Slack スレッド取得（slack_message_ts/channel_id が紐付いた案件）
  // 案件詳細のCRMサマリー（顧客情報＋直近案件）
  expressApp.get('/api/dashboard/an/studies/:id/crm-summary', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      // study 取得
      const { rows: [study] } = await dbQuery(
        `SELECT company_name FROM an_studies WHERE record_id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!study?.company_name) return res.json({ customer: null, deals: [] });

      // customer 取得（kintone同期分含む）
      const { rows: [customer] } = await dbQuery(
        `SELECT id, name, name_short, industry, prefecture, employee_count, address,
                business_description, inrevo_person, inflow_source, inflow_date, website,
                memo, kintone_record_id
         FROM customers WHERE team_id=$1 AND name=$2 LIMIT 1`,
        [teamId, study.company_name]
      );
      if (!customer) return res.json({ customer: null, deals: [] });

      // 案件一覧（顧客に紐づく案件、新しい順、最大10件）
      const { rows: deals } = await dbQuery(
        `SELECT id, name, yomi, status, contract_type, sales_person,
                initial_fee, monthly_fee, order_date, conclusion_date,
                first_meeting_date, next_action_date,
                bant_budget, bant_authority, bant_needs, bant_timeframe
         FROM deals
         WHERE team_id=$1 AND customer_id=$2
         ORDER BY COALESCE(conclusion_date, order_date, first_meeting_date) DESC NULLS LAST, updated_at DESC
         LIMIT 10`,
        [teamId, customer.id]
      );

      // 担当者
      const { rows: contacts } = await dbQuery(
        `SELECT last_name, first_name, position_title, department, email, phone
         FROM customer_contacts WHERE customer_id=$1 AND team_id=$2 ORDER BY sort_order, created_at LIMIT 5`,
        [customer.id, teamId]
      );

      res.json({ customer, deals, contacts });
    } catch (e) { console.error('[AN crm-summary]', e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.get('/api/dashboard/an/studies/:id/slack-thread', authWithRole, async (req, res) => {
    try {
      if (!slackApp) return res.status(500).json({ error: 'slack_not_configured' });
      const { teamId } = req.dashboardUser;
      const { rows: [s] } = await dbQuery(
        `SELECT slack_message_ts, slack_channel_id FROM an_studies WHERE record_id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!s || !s.slack_message_ts || !s.slack_channel_id) return res.json({ messages: [] });

      const r = await slackApp.client.conversations.replies({
        channel: s.slack_channel_id,
        ts: s.slack_message_ts,
        limit: 100,
      });
      // user_id → 表示名（簡易キャッシュ）
      const messages = await Promise.all((r.messages || []).map(async (m) => {
        let user_name = null;
        if (m.user) user_name = await resolveUserName(teamId, m.user);
        return {
          ts: m.ts,
          user: m.user,
          user_name,
          bot_id: m.bot_id,
          username: m.username,
          text: m.text,
          subtype: m.subtype,
        };
      }));
      res.json({ channel_id: s.slack_channel_id, root_ts: s.slack_message_ts, messages });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 案件詳細からSlackスレッドに返信
  expressApp.post('/api/dashboard/an/studies/:id/slack-reply', authWithRole, async (req, res) => {
    try {
      if (!slackApp) return res.status(500).json({ error: 'slack_not_configured' });
      const { teamId } = req.dashboardUser;
      const text = (req.body?.text || '').trim();
      if (!text) return res.status(400).json({ error: 'text required' });
      const { rows: [s] } = await dbQuery(
        `SELECT slack_message_ts, slack_channel_id FROM an_studies WHERE record_id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!s?.slack_message_ts || !s?.slack_channel_id) return res.status(400).json({ error: 'slack_not_linked' });
      await slackApp.client.chat.postMessage({
        channel: s.slack_channel_id,
        thread_ts: s.slack_message_ts,
        text,
      });
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体マスタ 削除
  expressApp.delete('/api/dashboard/media-master/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM media_master WHERE record_id=$1 AND team_id=$2`, [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // 手動再同期
  expressApp.post('/api/dashboard/media-master/sync', authWithRole, async (req, res) => {
    try {
      const n = await syncMediaMaster();
      res.json({ ok: true, upserted: n });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ─── AN依頼/調査の統合一覧（an_studies に統一） ─────────────
  expressApp.get('/api/dashboard/an/unified', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const q = (req.query.q || '').trim();
      const status = req.query.status || ''; // 'pending' | 'done' | ''
      const params = [teamId];
      let where = `s.team_id=$1`;
      if (q) { params.push(`%${q}%`); where += ` AND (s.company_name ILIKE $${params.length} OR s.requester ILIKE $${params.length})`; }
      if (status === 'pending') where += ` AND COALESCE(s.status,'') NOT IN ('完了','対応済','クローズ')`;
      if (status === 'done')    where += ` AND s.status IN ('完了','対応済','クローズ')`;

      const { rows } = await dbQuery(`
        SELECT s.record_id AS source_id, s.company_name,
               s.request_date::timestamptz AS requested_at,
               s.status, s.priority, s.requester,
               s.job_type, s.employment_type,
               s.job_type AS request_type,
               s.source,
               (SELECT COUNT(*)::int FROM an_study_media m WHERE m.study_record_id=s.record_id) AS media_count
        FROM an_studies s WHERE ${where}
        ORDER BY s.request_date DESC NULLS LAST, s.synced_at DESC
      `, params);
      // ファセット（フィルタ選択肢用）
      const { rows: facetRows } = await dbQuery(
        `SELECT DISTINCT requester FROM an_studies WHERE team_id=$1 AND requester IS NOT NULL AND requester <> '' ORDER BY requester`,
        [teamId]
      );
      res.json({
        rows,
        counts: { total: rows.length },
        facets: { requesters: facetRows.map(r => r.requester) },
      });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 案件のstatusや特記事項を更新
  expressApp.patch('/api/dashboard/an/studies/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const allowed = ['status', 'priority', 'must_condition', 'other_notes', 'requester', 'employment_type', 'job_type'];
      const sets = [], vals = [];
      let i = 1;
      for (const k of allowed) {
        if (k in req.body) { sets.push(`${k}=$${i++}`); vals.push(req.body[k] || null); }
      }
      if (sets.length === 0) return res.json({ ok: true });
      sets.push(`synced_at=now()`);
      vals.push(req.params.id, teamId);
      const { rows: [row] } = await dbQuery(
        `UPDATE an_studies SET ${sets.join(',')} WHERE record_id=$${i++} AND team_id=$${i++} RETURNING *`, vals
      );
      res.json({ study: row });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体スロット 追加
  expressApp.post('/api/dashboard/an/studies/:id/media', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [maxSlot] } = await dbQuery(
        `SELECT COALESCE(MAX(slot),0) AS s FROM an_study_media WHERE study_record_id=$1`, [req.params.id]
      );
      const slot = (maxSlot?.s || 0) + 1;
      const { rows: [row] } = await dbQuery(`
        INSERT INTO an_study_media (team_id, study_record_id, slot, media_name)
        VALUES ($1, $2, $3, $4) RETURNING *
      `, [teamId, req.params.id, slot, req.body?.media_name || null]);
      res.json({ slot: row });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体スロット 更新
  expressApp.patch('/api/dashboard/an/study-media/:id', authWithRole, async (req, res) => {
    try {
      const allowed = ['media_name', 'cost_category', 'fee', 'duration', 'responses',
        'active_count', 'expected_apps', 'reply_rate', 'effective_apps', 'effective_rate',
        'status_tags', 'note', 'an_assignee'];
      const sets = [], vals = [];
      let i = 1;
      const numFields = new Set(['fee','duration','expected_apps','reply_rate','effective_apps','effective_rate']);
      const arrFields = new Set(['responses','status_tags']);
      for (const k of allowed) {
        if (k in req.body) {
          let v = req.body[k];
          if (numFields.has(k)) v = (v === '' || v == null) ? null : Number(v);
          if (arrFields.has(k)) v = Array.isArray(v) ? v : (typeof v === 'string' && v.trim() ? v.split(',').map(s=>s.trim()) : []);
          sets.push(`${k}=$${i++}`); vals.push(v);
        }
      }
      if (sets.length === 0) return res.json({ ok: true });
      vals.push(req.params.id);
      const { rows: [row] } = await dbQuery(
        `UPDATE an_study_media SET ${sets.join(',')} WHERE id=$${i} RETURNING *`, vals
      );
      res.json({ slot: row });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体スロット 削除
  expressApp.delete('/api/dashboard/an/study-media/:id', authWithRole, async (req, res) => {
    try {
      await dbQuery(`DELETE FROM an_study_media WHERE id=$1`, [req.params.id]);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // kintone調査 単体取得（an_studies + media_slots）
  expressApp.get('/api/dashboard/an/studies/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [s] } = await dbQuery(
        `SELECT * FROM an_studies WHERE record_id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!s) return res.status(404).json({ error: 'not_found' });
      const { rows: media } = await dbQuery(
        `SELECT * FROM an_study_media WHERE study_record_id=$1 ORDER BY slot`,
        [req.params.id]
      );
      res.json({ study: s, media });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ─── AN調査管理表（App221） ─────────────────────────────
  // 過去調査一覧（会社名でフィルタ可。media展開も同時に返す）
  expressApp.get('/api/dashboard/an/studies', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const company = (req.query.company || '').trim();
      const params = [teamId];
      let where = `s.team_id=$1`;
      if (company) { params.push(`%${company}%`); where += ` AND s.company_name ILIKE $${params.length}`; }
      const { rows } = await dbQuery(`
        SELECT s.*,
          COALESCE(json_agg(json_build_object(
            'slot', m.slot, 'media_name', m.media_name, 'cost_category', m.cost_category,
            'fee', m.fee, 'duration', m.duration, 'responses', m.responses,
            'active_count', m.active_count, 'expected_apps', m.expected_apps,
            'reply_rate', m.reply_rate, 'effective_apps', m.effective_apps,
            'effective_rate', m.effective_rate, 'status_tags', m.status_tags,
            'note', m.note, 'an_assignee', m.an_assignee
          ) ORDER BY m.slot) FILTER (WHERE m.id IS NOT NULL), '[]'::json) AS media_slots
        FROM an_studies s
        LEFT JOIN an_study_media m ON m.study_record_id = s.record_id
        WHERE ${where}
        GROUP BY s.record_id
        ORDER BY s.request_date DESC NULLS LAST, s.synced_at DESC
        LIMIT 200
      `, params);
      res.json({ studies: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体実績ダッシュボード用の概要KPI
  expressApp.get('/api/dashboard/an/dashboard-summary', authWithRole, async (req, res) => {
    try {
      const { rows: [k] } = await dbQuery(`
        SELECT
          (SELECT COUNT(*)::int FROM an_studies)                        AS total_studies,
          (SELECT COUNT(DISTINCT media_name)::int FROM an_study_media WHERE media_name IS NOT NULL AND media_name <> '')  AS unique_media,
          (SELECT COUNT(*)::int FROM an_study_media WHERE media_name IS NOT NULL)  AS total_slots,
          (SELECT COUNT(*)::int FROM an_studies WHERE status IN ('完了','対応済','クローズ')) AS done_studies,
          ROUND((SELECT AVG(fee) FROM an_study_media WHERE fee > 0))::bigint AS avg_fee,
          ROUND((SELECT
                  CASE WHEN AVG(NULLIF(expected_apps,0))>0
                       THEN AVG(NULLIF(effective_apps,0))/AVG(NULLIF(expected_apps,0))*100
                       ELSE NULL END
                FROM an_study_media), 1) AS forecast_accuracy_pct
      `);
      // 媒体別 採用数（実応募）TOP10
      const top = await dbQuery(`
        SELECT media_name,
               COUNT(*)::int AS cases,
               ROUND(SUM(COALESCE(effective_apps,0)))::int AS total_effective,
               ROUND(AVG(NULLIF(fee,0)))::bigint AS avg_fee
        FROM an_study_media
        WHERE media_name IS NOT NULL AND media_name <> ''
        GROUP BY media_name
        ORDER BY total_effective DESC NULLS LAST
        LIMIT 10
      `);
      // 業種(雇用形態) × 媒体 マトリクス（採用数ベース、雇用形態主軸）
      const matrix = await dbQuery(`
        SELECT COALESCE(NULLIF(s.employment_type,''),'未設定') AS group_key,
               m.media_name,
               COUNT(*)::int AS cases,
               ROUND(SUM(COALESCE(m.effective_apps,0)))::int AS effective
        FROM an_study_media m
        JOIN an_studies s ON s.record_id = m.study_record_id
        WHERE m.media_name IS NOT NULL AND m.media_name <> ''
        GROUP BY group_key, m.media_name
        ORDER BY group_key, effective DESC NULLS LAST
      `);
      res.json({ kpi: k, top_media: top.rows, matrix: matrix.rows });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 媒体ROI 集計（媒体名でグルーピングして応募予測精度／費用効率を集計）
  expressApp.get('/api/dashboard/an/media-roi', authWithRole, async (req, res) => {
    try {
      const { rows } = await dbQuery(`
        SELECT m.media_name,
          COUNT(*)::int                                AS cases,
          ROUND(AVG(NULLIF(m.fee,0)))::bigint          AS avg_fee,
          ROUND(SUM(COALESCE(m.fee,0)))::bigint        AS total_fee,
          ROUND(AVG(NULLIF(m.expected_apps,0)),1)      AS avg_expected,
          ROUND(AVG(NULLIF(m.effective_apps,0)),1)     AS avg_effective,
          ROUND(AVG(NULLIF(m.reply_rate,0)),2)         AS avg_reply_rate,
          ROUND(AVG(NULLIF(m.effective_rate,0)),2)     AS avg_effective_rate,
          -- 応募予測精度: 実応募 / 予測応募
          ROUND(CASE WHEN AVG(NULLIF(m.expected_apps,0)) > 0
                THEN AVG(NULLIF(m.effective_apps,0))/AVG(NULLIF(m.expected_apps,0))*100
                ELSE NULL END, 1) AS forecast_accuracy_pct
        FROM an_study_media m
        WHERE m.media_name IS NOT NULL AND m.media_name <> ''
        GROUP BY m.media_name
        HAVING COUNT(*) >= 1
        ORDER BY cases DESC, avg_effective DESC NULLS LAST
      `);
      res.json({ rows });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // 推奨媒体サジェスト（媒体マスタから候補スコア順、業種・職種・エリアで絞り込み可）
  expressApp.get('/api/dashboard/media-suggest', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const params = [teamId];
      const where = ['mm.team_id=$1'];
      if (req.query.industry)  { params.push(req.query.industry);  where.push(`$${params.length} = ANY(mm.industries)`); }
      if (req.query.job_type)  { params.push(req.query.job_type);  where.push(`$${params.length} = ANY(mm.job_types)`); }
      if (req.query.area)      { params.push(req.query.area);      where.push(`$${params.length} = ANY(mm.areas)`); }
      const { rows } = await dbQuery(`
        SELECT mm.record_id, mm.name, mm.recommend_score, mm.areas,
               COALESCE(rs.cases,0) AS past_cases,
               rs.avg_effective, rs.forecast_accuracy_pct
        FROM media_master mm
        LEFT JOIN (
          SELECT media_name,
                 COUNT(*)::int AS cases,
                 ROUND(AVG(NULLIF(effective_apps,0)),1) AS avg_effective,
                 ROUND(CASE WHEN AVG(NULLIF(expected_apps,0))>0
                       THEN AVG(NULLIF(effective_apps,0))/AVG(NULLIF(expected_apps,0))*100
                       ELSE NULL END, 1) AS forecast_accuracy_pct
          FROM an_study_media GROUP BY media_name
        ) rs ON rs.media_name = mm.name
        WHERE ${where.join(' AND ')}
        ORDER BY mm.recommend_score DESC NULLS LAST, COALESCE(rs.cases,0) DESC
        LIMIT 30
      `, params);
      res.json({ suggestions: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  expressApp.post('/api/dashboard/an/studies/sync', authWithRole, async (req, res) => {
    try {
      const n = await syncAnStudies();
      res.json({ ok: true, upserted: n });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
}

module.exports = { registerAnApi };
