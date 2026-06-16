// 電子決裁 一覧/詳細 API（TaskHub Web 用）
const { dbQuery } = require('../db/index');

function registerApprovalApi({ expressApp, authWithRole }) {
  // 一覧
  expressApp.get('/api/dashboard/approvals', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const status = req.query.status; // 'pending' | 'approved' | 'rejected' | undefined (all)
      const params = [teamId];
      let where = `WHERE a.team_id=$1`;
      if (status && ['pending', 'approved', 'rejected', 'cancelled'].includes(status)) {
        params.push(status);
        where += ` AND a.status=$${params.length}`;
      }
      const r = await dbQuery(
        `SELECT a.*,
                COALESCE(json_agg(json_build_object(
                  'user_id', v.user_id,
                  'order_idx', v.order_idx,
                  'status', v.status,
                  'comment', v.comment,
                  'decided_at', v.decided_at
                ) ORDER BY v.order_idx) FILTER (WHERE v.user_id IS NOT NULL), '[]'::json) AS voters
         FROM approvals a
         LEFT JOIN approval_voters v ON v.approval_id = a.id
         ${where}
         GROUP BY a.id
         ORDER BY a.created_at DESC
         LIMIT 200`,
        params
      );
      res.json({ approvals: r.rows });
    } catch (e) {
      console.error('GET /approvals error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // 詳細
  expressApp.get('/api/dashboard/approvals/:id', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(
        `SELECT * FROM approvals WHERE id=$1 AND team_id=$2`,
        [req.params.id, teamId]
      );
      const a = r.rows[0];
      if (!a) return res.status(404).json({ error: 'not_found' });
      const vr = await dbQuery(
        `SELECT * FROM approval_voters WHERE approval_id=$1 ORDER BY order_idx ASC`,
        [a.id]
      );
      res.json({ approval: a, voters: vr.rows });
    } catch (e) {
      console.error('GET /approvals/:id error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerApprovalApi };
