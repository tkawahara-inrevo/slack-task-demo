// 機能別アクセス権限管理
// - admin は常に全機能アクセス可
// - それ以外は feature_access に subject_type='user'/'role'/'slack_usergroup' で許可レコードが必要
//
// 機能カタログ（UI表示用ラベルと階層）
const FEATURES = [
  { key: 'legal',          label: '法務案件管理', desc: '法務 案件進捗管理ページ', sensitive: true },
  { key: 'crm',            label: 'CRM',         desc: 'CRM全体（顧客・案件・ヨミ）' },
  { key: 'crm.yomi',       label: 'CRM ヨミ管理', desc: 'CRMの進行中案件ヨミ別管理', parent: 'crm' },
  { key: 'crm.performance',label: 'CRM 成績',    desc: '個人成績・KPI', parent: 'crm' },
  { key: 'rpo',            label: 'RPO 案件管理', desc: 'RPO 全体' },
  { key: 'recruit',        label: '採用管理',     desc: '自社採用管理' },
  { key: 'corp',           label: 'Corp管理',     desc: 'Corporate ダッシュボード' },
];

function registerFeatureAccess({ expressApp, authWithRole, dbQuery, getUsergroupMembers }) {
  // ── 内部ヘルパー: アクセス可否 ────────────────────────────
  async function hasAccess(teamId, userId, role, featureKey) {
    if (role === 'admin') return true;
    if (!teamId || !userId || !featureKey) return false;

    // 親featureキーも検査対象に（例: crm.yomi なら crm の許可でも通す）
    const keys = [featureKey];
    const dot = featureKey.indexOf('.');
    if (dot > 0) keys.push(featureKey.slice(0, dot));

    const { rows } = await dbQuery(
      `SELECT subject_type, subject_id FROM feature_access
       WHERE team_id=$1 AND feature_key = ANY($2)`,
      [teamId, keys]
    ).catch(() => ({ rows: [] }));

    if (!rows.length) return false;

    for (const r of rows) {
      if (r.subject_type === 'user' && r.subject_id === userId) return true;
      if (r.subject_type === 'role' && r.subject_id === role) return true;
      if (r.subject_type === 'slack_usergroup' && typeof getUsergroupMembers === 'function') {
        try {
          const members = await getUsergroupMembers(teamId, r.subject_id);
          if (Array.isArray(members) && members.includes(userId)) return true;
        } catch {}
      }
    }
    return false;
  }

  // ── ミドルウェア: 単一feature_keyの保護 ──────────────────
  function requireFeatureAccess(featureKey) {
    return async (req, res, next) => {
      try {
        const { teamId, userId, role } = req.dashboardUser || {};
        if (await hasAccess(teamId, userId, role, featureKey)) return next();
        return res.status(403).json({ error: 'forbidden', feature: featureKey });
      } catch (e) {
        console.error('requireFeatureAccess error:', e);
        return res.status(500).json({ error: 'internal' });
      }
    };
  }

  // ── 自分が見られる feature 一覧 ──────────────────────────
  expressApp.get('/api/dashboard/me/feature-access', authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const map = {};
      for (const f of FEATURES) {
        map[f.key] = await hasAccess(teamId, userId, role, f.key);
      }
      res.json({ access: map });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 機能カタログ取得（admin専用） ────────────────────────
  expressApp.get('/api/dashboard/admin/feature-access/catalog', authWithRole, async (req, res) => {
    if (req.dashboardUser?.role !== 'admin') return res.status(403).json({ error: 'forbidden' });
    res.json({ features: FEATURES });
  });

  // ── 機能ごとの許可リスト取得 ─────────────────────────────
  expressApp.get('/api/dashboard/admin/feature-access/:featureKey', authWithRole, async (req, res) => {
    if (req.dashboardUser?.role !== 'admin') return res.status(403).json({ error: 'forbidden' });
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT subject_type, subject_id, created_at FROM feature_access
         WHERE team_id=$1 AND feature_key=$2 ORDER BY created_at ASC`,
        [teamId, req.params.featureKey]
      );
      res.json({ grants: rows });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 許可追加 ────────────────────────────────────────────
  expressApp.post('/api/dashboard/admin/feature-access/:featureKey', authWithRole, async (req, res) => {
    if (req.dashboardUser?.role !== 'admin') return res.status(403).json({ error: 'forbidden' });
    const { teamId } = req.dashboardUser;
    const { subject_type, subject_id } = req.body || {};
    if (!['user', 'role', 'slack_usergroup'].includes(subject_type)) {
      return res.status(400).json({ error: 'invalid_subject_type' });
    }
    if (!subject_id) return res.status(400).json({ error: 'subject_id_required' });
    try {
      await dbQuery(
        `INSERT INTO feature_access (team_id, feature_key, subject_type, subject_id)
         VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
        [teamId, req.params.featureKey, subject_type, subject_id]
      );
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // ── 許可削除 ────────────────────────────────────────────
  expressApp.delete('/api/dashboard/admin/feature-access/:featureKey', authWithRole, async (req, res) => {
    if (req.dashboardUser?.role !== 'admin') return res.status(403).json({ error: 'forbidden' });
    const { teamId } = req.dashboardUser;
    const { subject_type, subject_id } = req.query || {};
    if (!subject_type || !subject_id) return res.status(400).json({ error: 'missing_params' });
    try {
      await dbQuery(
        `DELETE FROM feature_access WHERE team_id=$1 AND feature_key=$2 AND subject_type=$3 AND subject_id=$4`,
        [teamId, req.params.featureKey, subject_type, subject_id]
      );
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  return { hasAccess, requireFeatureAccess, FEATURES };
}

module.exports = { registerFeatureAccess };
