// 法務案件管理 API
const { dbQuery } = require('../db/index');

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
        direction, thread_url, result, history, minutes
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
