// 組織・役職・所属 API（権限再設計 Phase 2-3）
// docs/permission-redesign.md
//
// Phase 2: 既存 dash_teams / dash_team_members → 新テーブル移行
// Phase 3: 読み取り専用 UI 用エンドポイント
//
// このモジュールは旧テーブル運用に影響を与えない。
// authWithRole は dashboard-api から受け取る。

function registerOrgApi({ expressApp, authWithRole, adminOnly, dbQuery }) {

  // ── ヘルパー ─────────────────────────────────────
  function teamIdOf(req) {
    return req.dashboardUser?.teamId || process.env.SLACK_TEAM_ID || 'T086C06L5V0';
  }

  // ── 1. 組織ツリー取得 ─────────────────────────
  expressApp.get('/api/admin/org/units', authWithRole, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const r = await dbQuery(
        `SELECT id, name, type, parent_id, sort_order, is_active
         FROM org_units
         WHERE team_id=$1 AND is_active=true
         ORDER BY type, sort_order, id`,
        [teamId],
      );
      res.json({ units: r.rows });
    } catch (e) {
      console.error('[org-api] /units error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 2. 役職マスター取得 ────────────────────────
  expressApp.get('/api/admin/org/positions', authWithRole, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const r = await dbQuery(
        `SELECT id, name, level, sort_order, is_active
         FROM positions
         WHERE team_id=$1 AND is_active=true
         ORDER BY level, sort_order, id`,
        [teamId],
      );
      res.json({ positions: r.rows });
    } catch (e) {
      console.error('[org-api] /positions error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 3. メンバー一覧（所属付き） ──────────────
  expressApp.get('/api/admin/org/members', authWithRole, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const r = await dbQuery(
        `SELECT
           d.user_id,
           d.display_name,
           d.real_name,
           COALESCE(
             json_agg(
               json_build_object(
                 'assignment_id', a.id,
                 'org_unit_id', a.org_unit_id,
                 'org_unit_name', u.name,
                 'org_unit_type', u.type,
                 'position_id', a.position_id,
                 'position_name', p.name,
                 'position_level', p.level,
                 'is_primary', a.is_primary,
                 'effective_from', a.effective_from,
                 'effective_to', a.effective_to
               ) ORDER BY a.is_primary DESC, p.level DESC
             ) FILTER (WHERE a.id IS NOT NULL),
             '[]'::json
           ) AS assignments
         FROM dashboard_user_directory d
         LEFT JOIN user_assignments a ON a.user_id = d.user_id
           AND a.team_id = d.team_id
           AND a.effective_to IS NULL
         LEFT JOIN org_units u ON u.id = a.org_unit_id
         LEFT JOIN positions p ON p.id = a.position_id
         WHERE d.team_id=$1 AND d.is_active=true
         GROUP BY d.user_id, d.display_name, d.real_name
         ORDER BY d.display_name`,
        [teamId],
      );
      res.json({ members: r.rows });
    } catch (e) {
      console.error('[org-api] /members error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 4. 既存データ移行（admin限定・1回実行） ──
  // POST /api/admin/org/migrate-from-legacy
  // 既存の dash_teams + dash_team_members を org_units + user_assignments にコピー。
  // 冪等: org_unit が既に存在する場合はスキップ。
  expressApp.post('/api/admin/org/migrate-from-legacy', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const createdBy = req.dashboardUser?.userId || 'system';

      // CEO unit を取得
      const ceoRes = await dbQuery(
        `SELECT id FROM org_units WHERE team_id=$1 AND type='ceo' LIMIT 1`,
        [teamId],
      );
      const ceoUnitId = ceoRes.rows[0]?.id;
      if (!ceoUnitId) {
        return res.status(500).json({ error: 'CEO unit not found' });
      }

      // メンバー役職を取得（デフォルト割当）
      const memberRes = await dbQuery(
        `SELECT id FROM positions WHERE team_id=$1 AND name='メンバー' LIMIT 1`,
        [teamId],
      );
      const memberPosId = memberRes.rows[0]?.id;
      if (!memberPosId) {
        return res.status(500).json({ error: 'メンバー position not found' });
      }

      // 既存 dash_teams を org_units に複製（既存スキップ）
      const teamsRes = await dbQuery(
        `SELECT id, name, created_at FROM dash_teams WHERE team_id=$1`,
        [teamId],
      );

      const dashTeamToOrgId = new Map(); // dash_team.id → org_units.id
      let createdUnits = 0;
      let skippedUnits = 0;

      for (const dt of teamsRes.rows) {
        // 同名 unit が既にあるかチェック
        const exist = await dbQuery(
          `SELECT id FROM org_units WHERE team_id=$1 AND name=$2 AND type IN ('dept','team') LIMIT 1`,
          [teamId, dt.name],
        );
        if (exist.rows[0]) {
          dashTeamToOrgId.set(dt.id, exist.rows[0].id);
          skippedUnits++;
          continue;
        }
        const ins = await dbQuery(
          `INSERT INTO org_units (team_id, name, type, parent_id, sort_order)
           VALUES ($1, $2, 'dept', $3, 0)
           RETURNING id`,
          [teamId, dt.name, ceoUnitId],
        );
        dashTeamToOrgId.set(dt.id, ins.rows[0].id);
        createdUnits++;
      }

      // 既存 dash_team_members を user_assignments に複製
      const membersRes = await dbQuery(
        `SELECT m.dash_team_id, m.user_id
         FROM dash_team_members m
         WHERE m.team_id=$1`,
        [teamId],
      );

      let createdAssignments = 0;
      let skippedAssignments = 0;

      for (const m of membersRes.rows) {
        const orgUnitId = dashTeamToOrgId.get(m.dash_team_id);
        if (!orgUnitId) continue;

        // 既に有効な assignment があるかチェック
        const exist = await dbQuery(
          `SELECT id FROM user_assignments
           WHERE team_id=$1 AND user_id=$2 AND org_unit_id=$3 AND effective_to IS NULL
           LIMIT 1`,
          [teamId, m.user_id, orgUnitId],
        );
        if (exist.rows[0]) { skippedAssignments++; continue; }

        // is_primary: そのユーザーの最初の assignment を primary にする
        const hasAny = await dbQuery(
          `SELECT id FROM user_assignments
           WHERE team_id=$1 AND user_id=$2 AND effective_to IS NULL LIMIT 1`,
          [teamId, m.user_id],
        );
        const isPrimary = !hasAny.rows[0];

        await dbQuery(
          `INSERT INTO user_assignments
             (team_id, user_id, org_unit_id, position_id, is_primary, created_by)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [teamId, m.user_id, orgUnitId, memberPosId, isPrimary, createdBy],
        );
        createdAssignments++;
      }

      res.json({
        ok: true,
        summary: {
          createdUnits,
          skippedUnits,
          createdAssignments,
          skippedAssignments,
        },
      });
    } catch (e) {
      console.error('[org-api] /migrate-from-legacy error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ── 5. 移行状態確認 ───────────────────────────
  expressApp.get('/api/admin/org/migration-status', authWithRole, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const [units, assignments, legacyTeams, legacyMembers] = await Promise.all([
        dbQuery(`SELECT count(*) FROM org_units WHERE team_id=$1`, [teamId]),
        dbQuery(`SELECT count(*) FROM user_assignments WHERE team_id=$1 AND effective_to IS NULL`, [teamId]),
        dbQuery(`SELECT count(*) FROM dash_teams WHERE team_id=$1`, [teamId]),
        dbQuery(`SELECT count(*) FROM dash_team_members WHERE team_id=$1`, [teamId]),
      ]);
      res.json({
        newSchema: {
          orgUnits: Number(units.rows[0].count),
          activeAssignments: Number(assignments.rows[0].count),
        },
        legacy: {
          dashTeams: Number(legacyTeams.rows[0].count),
          dashTeamMembers: Number(legacyMembers.rows[0].count),
        },
      });
    } catch (e) {
      console.error('[org-api] /migration-status error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  console.log('[org-api] registered (org units / positions / members / migration)');
}

module.exports = { registerOrgApi };
