// AN依頼管理 API + Slackリスナー
const { dbQuery } = require('../db/index');

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

  // 1件取り込み（リスナー & backfill 共通）
  async function ingestAnRequest({ teamId, channelId, messageTs, text }) {
    const parsed = parseAnRequest(text);
    const crmDealId = await findCrmDealId(teamId, parsed.kintone_url, parsed.company_name);
    const detail = [
      parsed.hearing ? `【ヒアリング項目】\n${parsed.hearing}` : '',
      parsed.budget  ? `【媒体予算】\n${parsed.budget}` : '',
    ].filter(Boolean).join('\n\n');

    const r = await dbQuery(`
      INSERT INTO an_requests
        (team_id, channel_id, message_ts, company_name, crm_deal_id,
         sales_person, request_type, priority, detail, raw_text, status, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',COALESCE($11, now()))
      ON CONFLICT DO NOTHING
      RETURNING id
    `, [
      teamId, channelId, messageTs,
      parsed.company_name || null,
      crmDealId || null,
      parsed.sales_person || null,
      parsed.request_type || null,
      parsed.priority || null,
      detail || parsed.detail || null,
      text,
      messageTs ? new Date(Number(messageTs.split('.')[0]) * 1000) : null,
    ]);
    return { inserted: r.rowCount > 0, parsed };
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

  // ─── 媒体実績DB（全案件横断集計 + フィルタ + ファセット） ─────────
  expressApp.get('/api/dashboard/an/media-stats', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { industry, hire_type, prefecture, size_bucket, period_from, period_to } = req.query;

      // size_bucket: small(<=5) / mid(6-10) / large(11+)
      const filters = [`rc.team_id=$1`, `rc.status != 'archived'`, `(m->>'name') IS NOT NULL`, `(m->>'name') != ''`,
        `((m->>'mediaCost')::numeric > 0 OR (m->>'hiredCount')::int > 0)`];
      const params = [teamId];
      const add = (sql, val) => { params.push(val); filters.push(sql.replace('?', `$${params.length}`)); };
      if (industry)     add(`c.industry = ?`, industry);
      if (prefecture)   add(`c.prefecture = ?`, prefecture);
      if (hire_type)    add(`d.hire_type @> ?::jsonb`, JSON.stringify([hire_type]));
      if (size_bucket === 'small') filters.push(`COALESCE((rc.data->'projectInfo'->>'hiringTarget')::int, 0) BETWEEN 1 AND 5`);
      if (size_bucket === 'mid')   filters.push(`COALESCE((rc.data->'projectInfo'->>'hiringTarget')::int, 0) BETWEEN 6 AND 10`);
      if (size_bucket === 'large') filters.push(`COALESCE((rc.data->'projectInfo'->>'hiringTarget')::int, 0) >= 11`);
      if (period_from) add(`(m->>'periodStart')::date >= ?`, period_from);
      if (period_to)   add(`(m->>'periodEnd')::date <= ?`, period_to);

      const baseTables = `
        FROM rpo_clients rc
        LEFT JOIN deals d     ON d.id = rc.data->>'crmDealId'
        LEFT JOIN customers c ON c.id = d.customer_id
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN rc.data ? 'mediaStatus' THEN rc.data->'mediaStatus' ELSE '[]'::jsonb END
        ) AS m
      `;
      const baseWhere = `WHERE ${filters.join(' AND ')}`;
      const baseFrom = `${baseTables} ${baseWhere}`;

      // 媒体別の集計
      const { rows } = await dbQuery(`
        SELECT
          m->>'name'                             AS media_name,
          COUNT(*)::int                          AS campaigns,
          SUM((m->>'mediaCost')::numeric)        AS total_cost,
          SUM((m->>'hiredCount')::int)           AS total_hired,
          AVG((m->>'mediaCost')::numeric)        AS avg_cost,
          SUM(CASE WHEN (m->>'hiredCount')::int > 0 THEN 1 ELSE 0 END)::int AS success_campaigns
        ${baseFrom}
        GROUP BY media_name
        ORDER BY total_hired DESC NULLS LAST, total_cost DESC NULLS LAST
      `, params);

      // 媒体 × 業界 のブレイクダウン
      const { rows: byInd } = await dbQuery(`
        SELECT m->>'name' AS media_name,
               COALESCE(c.industry,'未設定') AS industry,
               SUM((m->>'hiredCount')::int)    AS hired,
               SUM((m->>'mediaCost')::numeric) AS cost,
               COUNT(*)::int                   AS campaigns
        ${baseFrom}
        GROUP BY media_name, industry
        ORDER BY media_name, hired DESC NULLS LAST
      `, params);

      // 媒体 × 雇用形態 のブレイクダウン
      const { rows: byHire } = await dbQuery(`
        SELECT m->>'name' AS media_name,
               COALESCE(NULLIF(ht.value, ''), '未設定') AS hire_type,
               SUM((m->>'hiredCount')::int)    AS hired,
               SUM((m->>'mediaCost')::numeric) AS cost
        ${baseTables}
        LEFT JOIN LATERAL jsonb_array_elements_text(
          CASE WHEN d.hire_type IS NOT NULL AND jsonb_typeof(d.hire_type)='array' AND jsonb_array_length(d.hire_type)>0
               THEN d.hire_type ELSE '["未設定"]'::jsonb END
        ) AS ht(value) ON true
        ${baseWhere}
        GROUP BY media_name, hire_type
        ORDER BY media_name, hired DESC NULLS LAST
      `, params);

      // ファセット（フィルタ選択肢、フィルタ適用前の全体から）
      const { rows: facetRows } = await dbQuery(`
        SELECT DISTINCT
          c.industry, c.prefecture
        FROM rpo_clients rc
        LEFT JOIN deals d     ON d.id = rc.data->>'crmDealId'
        LEFT JOIN customers c ON c.id = d.customer_id
        WHERE rc.team_id=$1 AND rc.status != 'archived'
      `, [teamId]);
      const industries  = [...new Set(facetRows.map(r => r.industry).filter(Boolean))].sort();
      const prefectures = [...new Set(facetRows.map(r => r.prefecture).filter(Boolean))].sort();

      const groupBy = (arr, key) => arr.reduce((m, r) => {
        (m[r.media_name] = m[r.media_name] || []).push(r); return m;
      }, {});
      const indByMedia  = groupBy(byInd);
      const hireByMedia = groupBy(byHire);

      res.json({
        stats: rows.map(r => ({
          media_name: r.media_name,
          campaigns: r.campaigns,
          total_cost: Number(r.total_cost) || 0,
          total_hired: Number(r.total_hired) || 0,
          avg_cost: Math.round(Number(r.avg_cost) || 0),
          cost_per_hire: r.total_hired > 0 ? Math.round(Number(r.total_cost) / r.total_hired) : null,
          success_campaigns: r.success_campaigns,
          by_industry: (indByMedia[r.media_name] || []).map(x => ({
            industry: x.industry, hired: Number(x.hired)||0, cost: Number(x.cost)||0, campaigns: x.campaigns,
          })),
          by_hire_type: (hireByMedia[r.media_name] || []).map(x => ({
            hire_type: x.hire_type, hired: Number(x.hired)||0, cost: Number(x.cost)||0,
          })),
        })),
        facets: { industries, prefectures, hire_types: ['新卒','中途'],
          size_buckets: [['small','〜5名'],['mid','6〜10名'],['large','11名〜']] },
      });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });
}

module.exports = { registerAnApi };
