// 権限管理API
const { dbQuery } = require('../db/index');

// 機能定義
const FEATURES = [
  { id: 'crm',              label: 'CRM（パイプライン・顧客）', category: 'ヒトトレ', scopes: ['none','all'] },
  { id: 'crm_performance',  label: 'CRM 成績閲覧',             category: 'ヒトトレ', scopes: ['none','all'] },
  { id: 'rpo',              label: 'RPO 案件管理',              category: 'ヒトトレ', scopes: ['none','own','all'] },
  { id: 'gantt',            label: '業務ガント',                category: '業務',     scopes: ['none','own','all'] },
  { id: 'ranking',          label: 'ランキング',                category: '情シス',   scopes: ['none','all'] },
  { id: 'channel_mapping',  label: 'チャンネルマッピング',      category: '情シス',   scopes: ['none','all'] },
  { id: 'daily_report',     label: '日報管理',                  category: '情シス',   scopes: ['none','all'] },
  { id: 'admin',            label: '管理設定',                  category: '情シス',   scopes: ['none','all'] },
];

const SCOPE_LABELS = { none: 'なし', own: '自分のチームのみ', all: '全件' };

// デフォルト権限（DBに設定がない場合のフォールバック）
const DEFAULT_PERMISSIONS = {
  role: {
    admin:   { crm:'all', crm_performance:'all', rpo:'all', gantt:'all', ranking:'all', channel_mapping:'all', daily_report:'all', admin:'all' },
    manager: { crm:'all', crm_performance:'all', rpo:'all', gantt:'all', ranking:'all', channel_mapping:'none', daily_report:'none', admin:'none' },
    corp:    { crm:'all', crm_performance:'none', rpo:'none', gantt:'own', ranking:'none', channel_mapping:'all', daily_report:'all', admin:'none' },
    member:  { crm:'all', crm_performance:'none', rpo:'none', gantt:'own', ranking:'none', channel_mapping:'none', daily_report:'none', admin:'none' },
  },
};

async function getPermissions(teamId) {
  const { rows } = await dbQuery(
    `SELECT subject_type, subject_id, feature, scope FROM feature_permissions WHERE team_id=$1`,
    [teamId]
  );
  // 構造化: { dept: { '営業': { crm: 'all' } }, role: { admin: { crm: 'all' } } }
  const perms = { dept: {}, role: {}, user: {} };
  for (const r of rows) {
    if (!perms[r.subject_type]) perms[r.subject_type] = {};
    if (!perms[r.subject_type][r.subject_id]) perms[r.subject_type][r.subject_id] = {};
    perms[r.subject_type][r.subject_id][r.feature] = r.scope;
  }
  return perms;
}

async function setPermission(teamId, subjectType, subjectId, feature, scope) {
  await dbQuery(`
    INSERT INTO feature_permissions (team_id, subject_type, subject_id, feature, scope)
    VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (team_id, subject_type, subject_id, feature)
    DO UPDATE SET scope=$5, updated_at=now()
  `, [teamId, subjectType, subjectId, feature, scope]);
}

function registerPermissionsApi({ expressApp, authWithRole, adminOnly }) {
  // GET /features — 機能定義一覧
  expressApp.get('/api/dashboard/admin/permissions/features', authWithRole, (req, res) => {
    res.json({ features: FEATURES, scopeLabels: SCOPE_LABELS, defaults: DEFAULT_PERMISSIONS });
  });

  // GET /permissions — 現在の権限設定一覧
  expressApp.get('/api/dashboard/admin/permissions', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      // 部署一覧（dash_teams の親チームとロール）
      const { rows: depts } = await dbQuery(
        `SELECT DISTINCT name FROM dash_teams WHERE team_id=$1 AND parent_id IS NOT NULL ORDER BY name`,
        [teamId]
      );
      // ロール一覧
      const { rows: roleRows } = await dbQuery(
        `SELECT DISTINCT role FROM dashboard_roles WHERE team_id=$1 AND role NOT IN ('member') ORDER BY role`,
        [teamId]
      );
      // ユーザー別権限（明示設定のあるもの）
      const { rows: userRows } = await dbQuery(
        `SELECT fp.subject_id, du.display_name, fp.feature, fp.scope
         FROM feature_permissions fp
         LEFT JOIN dashboard_user_directory du ON du.user_id=fp.subject_id AND du.team_id=fp.team_id
         WHERE fp.team_id=$1 AND fp.subject_type='user'`,
        [teamId]
      );
      const perms = await getPermissions(teamId);
      res.json({
        features: FEATURES, scopeLabels: SCOPE_LABELS,
        departments: depts.map(d => d.name),
        roles: ['admin','manager','corp',...roleRows.map(r => r.role).filter(r => !['admin','manager','corp'].includes(r))],
        permissions: perms,
        defaults: DEFAULT_PERMISSIONS,
        userOverrides: userRows,
      });
    } catch (e) {
      console.error('[permissions] GET error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // PATCH /permissions — 権限を更新
  expressApp.patch('/api/dashboard/admin/permissions', authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { subjectType, subjectId, feature, scope } = req.body;
      if (!subjectType || !subjectId || !feature || !scope) return res.status(400).json({ error: 'invalid' });
      await setPermission(teamId, subjectType, subjectId, feature, scope);
      res.json({ ok: true });
    } catch (e) {
      console.error('[permissions] PATCH error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerPermissionsApi, FEATURES, DEFAULT_PERMISSIONS, SCOPE_LABELS };
