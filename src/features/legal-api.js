// 法務案件管理 API
const { dbQuery } = require('../db/index');

let _anthropic = null;
function getAnthropic() {
  if (_anthropic) return _anthropic;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY not set');
  const Anthropic = require('@anthropic-ai/sdk').default;
  _anthropic = new Anthropic({ apiKey });
  return _anthropic;
}

const LEGAL_AI_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['summary', 'next_action'],
  properties: {
    summary: { type: 'string', description: 'これまでの経緯を3-5文で簡潔に要約' },
    next_action: { type: 'string', description: '次に誰が何をすべきかを具体的に1-2文で提案' },
  },
};

function registerLegalApi({ expressApp, authWithRole }) {

  // ── 一覧 ────────────────────────────────────────────────
  expressApp.get('/api/dashboard/legal/cases', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT * FROM legal_cases WHERE team_id=$1 ORDER BY updated_at DESC`,
        [teamId]
      );
      res.json({ cases: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── 作成 ────────────────────────────────────────────────
  expressApp.post('/api/dashboard/legal/cases', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { rows: [row] } = await dbQuery(
        `INSERT INTO legal_cases (team_id, created_by) VALUES ($1, $2) RETURNING *`,
        [teamId, userId]
      );
      res.status(201).json({ case: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── 更新 ────────────────────────────────────────────────
  expressApp.patch('/api/dashboard/legal/cases/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const {
        case_name, na_date, start_date, priority, chief, ball,
        na_ledger, issue_summary, issue_details, contract_details,
        direction, thread_url, result, history, minutes,
        status_phase, current_state, ledger_url, contract_url,
        email_slack_url, minutes_url, final_result, closed_date,
      } = req.body;

      const fields = [];
      const vals   = [];
      let i = 1;
      const set = (col, val) => { if (val !== undefined) { fields.push(`${col}=$${i++}`); vals.push(val); } };

      set('case_name',        case_name);
      set('na_date',          na_date || null);
      set('start_date',       start_date || null);
      set('priority',         priority);
      set('chief',            chief);
      set('ball',             ball);
      set('na_ledger',        na_ledger);
      set('issue_summary',    issue_summary);
      set('issue_details',    issue_details);
      set('contract_details', contract_details);
      set('direction',        direction);
      set('thread_url',       thread_url);
      set('result',           result);
      set('status_phase',     status_phase);
      set('current_state',    current_state);
      set('ledger_url',       ledger_url);
      set('contract_url',     contract_url);
      set('email_slack_url',  email_slack_url);
      set('minutes_url',      minutes_url);
      set('final_result',     final_result);
      set('closed_date',      closed_date || null);
      if (history  !== undefined) { fields.push(`history=$${i++}`);  vals.push(JSON.stringify(history)); }
      if (minutes  !== undefined) { fields.push(`minutes=$${i++}`);  vals.push(JSON.stringify(minutes)); }

      if (fields.length === 0) return res.json({ ok: true });

      fields.push(`updated_at=now()`);
      vals.push(req.params.id, teamId);

      const { rows: [row] } = await dbQuery(
        `UPDATE legal_cases SET ${fields.join(',')} WHERE id=$${i++} AND team_id=$${i++} RETURNING *`,
        vals
      );
      if (!row) return res.status(404).json({ error: 'not_found' });
      res.json({ case: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── AI 要約・次アクション提案 ───────────────────────────
  expressApp.post('/api/dashboard/legal/cases/:id/ai', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows: [c] } = await dbQuery(
        `SELECT * FROM legal_cases WHERE id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      if (!c) return res.status(404).json({ error: 'not_found' });

      const historyStr = Array.isArray(c.history)
        ? c.history.map(h => `${h.date || ''} [${h.type || ''}] ${h.content || ''}`).join('\n')
        : '';
      const minutesStr = Array.isArray(c.minutes)
        ? c.minutes.map(m => `${m.date || ''} [${m.type || ''}] ${m.content || ''}`).join('\n')
        : '';

      const userPrompt = [
        `案件名: ${c.case_name || ''}`,
        `現在のステータス: ${c.status_phase || ''}`,
        `ボール: ${c.ball || ''}`,
        `担当: ${c.chief || ''}`,
        `問題該当箇所: ${c.issue_details || ''}`,
        `契約書該当箇所: ${c.contract_details || ''}`,
        `方向性: ${c.direction || ''}`,
        `現状メモ: ${c.current_state || ''}`,
        `結果: ${c.final_result || c.result || ''}`,
        '',
        '【対応履歴】',
        historyStr || '（なし）',
        '',
        '【議事録】',
        minutesStr || '（なし）',
      ].join('\n');

      const client = getAnthropic();
      const response = await client.messages.create({
        model: 'claude-haiku-4-5',
        max_tokens: 800,
        system: 'あなたは法務部のAIアシスタント。提供された案件情報・履歴・議事録から、これまでの経緯を3-5文で簡潔に要約し、次に誰が何をすべきかを具体的に1-2文で提案する。出力は構造化された日本語で。',
        messages: [{ role: 'user', content: userPrompt }],
        tools: [{
          name: 'output_summary',
          description: '案件の要約と次アクションを返す',
          input_schema: LEGAL_AI_SCHEMA,
        }],
        tool_choice: { type: 'tool', name: 'output_summary' },
      });

      const toolUse = (response.content || []).find(b => b.type === 'tool_use');
      const result = toolUse?.input || { summary: '生成に失敗しました', next_action: '再度お試しください' };
      res.json(result);
    } catch (e) {
      console.error('legal AI error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ── 削除 ────────────────────────────────────────────────
  expressApp.delete('/api/dashboard/legal/cases/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(`DELETE FROM legal_cases WHERE id=$1 AND team_id=$2`, [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });
}

module.exports = { registerLegalApi };
