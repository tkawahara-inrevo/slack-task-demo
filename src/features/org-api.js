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
  expressApp.get('/api/dashboard/admin/org/units', authWithRole, async (req, res) => {
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
  expressApp.get('/api/dashboard/admin/org/positions', authWithRole, async (req, res) => {
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
  expressApp.get('/api/dashboard/admin/org/members', authWithRole, async (req, res) => {
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
  expressApp.post('/api/dashboard/admin/org/migrate-from-legacy', authWithRole, adminOnly, async (req, res) => {
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
  expressApp.get('/api/dashboard/admin/org/migration-status', authWithRole, async (req, res) => {
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

  // ═══════════════════════════════════════════════════════════
  // Phase 4: 編集機能（組織ツリー・メンバー所属）
  // ═══════════════════════════════════════════════════════════

  // ── 組織ツリー編集 ─────────────────────────────

  // POST /units 新規作成
  expressApp.post('/api/dashboard/admin/org/units', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const { name, type, parent_id, sort_order = 0 } = req.body || {};
      if (!name || !type) return res.status(400).json({ error: 'name and type required' });
      if (!['division','dept','team'].includes(type)) {
        return res.status(400).json({ error: 'type must be division/dept/team' });
      }
      const r = await dbQuery(
        `INSERT INTO org_units (team_id, name, type, parent_id, sort_order)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, name, type, parent_id, sort_order`,
        [teamId, name, type, parent_id || null, sort_order],
      );
      res.json({ unit: r.rows[0] });
    } catch (e) {
      console.error('[org-api] POST /units error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // PATCH /units/:id 編集（改名・親変更・並び替え）
  expressApp.patch('/api/dashboard/admin/org/units/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      const { name, parent_id, sort_order, type } = req.body || {};

      // CEO unit は type 変更不可
      const cur = await dbQuery(
        `SELECT type FROM org_units WHERE id=$1 AND team_id=$2`,
        [id, teamId],
      );
      if (!cur.rows[0]) return res.status(404).json({ error: 'not found' });
      if (cur.rows[0].type === 'ceo' && type && type !== 'ceo') {
        return res.status(400).json({ error: 'CEO unit type is immutable' });
      }

      // 循環チェック: parent_id を自分の祖先（既に自分以下）に向けない
      if (parent_id) {
        if (parent_id === id) {
          return res.status(400).json({ error: 'cannot parent to self' });
        }
        const cycle = await dbQuery(
          `WITH RECURSIVE descendants AS (
             SELECT id FROM org_units WHERE id=$1
             UNION ALL
             SELECT u.id FROM org_units u JOIN descendants d ON u.parent_id=d.id
           )
           SELECT 1 FROM descendants WHERE id=$2`,
          [id, parent_id],
        );
        if (cycle.rows[0]) return res.status(400).json({ error: 'circular reference' });
      }

      const sets = [];
      const params = [id, teamId];
      let n = 3;
      if (name !== undefined)       { sets.push(`name=$${n++}`);       params.push(name); }
      if (parent_id !== undefined)  { sets.push(`parent_id=$${n++}`);  params.push(parent_id || null); }
      if (sort_order !== undefined) { sets.push(`sort_order=$${n++}`); params.push(sort_order); }
      if (type !== undefined && cur.rows[0].type !== 'ceo') { sets.push(`type=$${n++}`); params.push(type); }
      if (sets.length === 0) return res.json({ ok: true });
      sets.push(`updated_at=now()`);

      const r = await dbQuery(
        `UPDATE org_units SET ${sets.join(', ')}
         WHERE id=$1 AND team_id=$2
         RETURNING id, name, type, parent_id, sort_order`,
        params,
      );
      res.json({ unit: r.rows[0] });
    } catch (e) {
      console.error('[org-api] PATCH /units error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // DELETE /units/:id 論理削除
  expressApp.delete('/api/dashboard/admin/org/units/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      // CEO 削除不可
      const cur = await dbQuery(
        `SELECT type FROM org_units WHERE id=$1 AND team_id=$2`,
        [id, teamId],
      );
      if (!cur.rows[0]) return res.status(404).json({ error: 'not found' });
      if (cur.rows[0].type === 'ceo') return res.status(400).json({ error: 'CEO unit cannot be deleted' });

      // 子 unit があれば拒否
      const child = await dbQuery(
        `SELECT count(*)::int AS c FROM org_units WHERE parent_id=$1 AND is_active=true`,
        [id],
      );
      if (child.rows[0].c > 0) {
        return res.status(400).json({ error: 'has active children', children: child.rows[0].c });
      }

      // 有効な assignment があれば確認 force パラメータが必要
      const force = req.query.force === '1';
      const active = await dbQuery(
        `SELECT count(*)::int AS c FROM user_assignments
         WHERE org_unit_id=$1 AND effective_to IS NULL`,
        [id],
      );
      if (active.rows[0].c > 0 && !force) {
        return res.status(400).json({
          error: 'has active assignments',
          assignments: active.rows[0].c,
          hint: 'pass ?force=1 to terminate all assignments and delete',
        });
      }

      if (force) {
        await dbQuery(
          `UPDATE user_assignments SET effective_to=CURRENT_DATE
           WHERE org_unit_id=$1 AND effective_to IS NULL`,
          [id],
        );
      }

      await dbQuery(
        `UPDATE org_units SET is_active=false, updated_at=now() WHERE id=$1`,
        [id],
      );
      res.json({ ok: true });
    } catch (e) {
      console.error('[org-api] DELETE /units error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ── メンバー所属編集 ───────────────────────────

  // POST /assignments 新規所属追加（兼任）
  expressApp.post('/api/dashboard/admin/org/assignments', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const createdBy = req.dashboardUser?.userId || 'admin';
      const { user_id, org_unit_id, position_id, is_primary = false, effective_from } = req.body || {};
      if (!user_id || !org_unit_id || !position_id) {
        return res.status(400).json({ error: 'user_id, org_unit_id, position_id required' });
      }
      // 同じユーザー × 同 unit で有効な assignment があれば重複
      const dup = await dbQuery(
        `SELECT id FROM user_assignments
         WHERE team_id=$1 AND user_id=$2 AND org_unit_id=$3 AND effective_to IS NULL`,
        [teamId, user_id, org_unit_id],
      );
      if (dup.rows[0]) return res.status(400).json({ error: 'already assigned to this unit' });

      // is_primary=true なら既存 primary を解除
      if (is_primary) {
        await dbQuery(
          `UPDATE user_assignments SET is_primary=false
           WHERE team_id=$1 AND user_id=$2 AND effective_to IS NULL AND is_primary=true`,
          [teamId, user_id],
        );
      }

      const r = await dbQuery(
        `INSERT INTO user_assignments
           (team_id, user_id, org_unit_id, position_id, is_primary, effective_from, created_by)
         VALUES ($1, $2, $3, $4, $5, COALESCE($6, CURRENT_DATE), $7)
         RETURNING id, user_id, org_unit_id, position_id, is_primary, effective_from`,
        [teamId, user_id, org_unit_id, position_id, !!is_primary, effective_from || null, createdBy],
      );
      res.json({ assignment: r.rows[0] });
    } catch (e) {
      console.error('[org-api] POST /assignments error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // PATCH /assignments/:id 編集（昇降格・primary 変更）
  expressApp.patch('/api/dashboard/admin/org/assignments/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      const { position_id, is_primary } = req.body || {};

      const cur = await dbQuery(
        `SELECT user_id FROM user_assignments WHERE id=$1 AND team_id=$2`,
        [id, teamId],
      );
      if (!cur.rows[0]) return res.status(404).json({ error: 'not found' });
      const userId = cur.rows[0].user_id;

      // is_primary=true なら他の primary を解除
      if (is_primary === true) {
        await dbQuery(
          `UPDATE user_assignments SET is_primary=false
           WHERE team_id=$1 AND user_id=$2 AND id!=$3 AND effective_to IS NULL`,
          [teamId, userId, id],
        );
      }

      const sets = [];
      const params = [id];
      let n = 2;
      if (position_id !== undefined) { sets.push(`position_id=$${n++}`); params.push(position_id); }
      if (is_primary !== undefined)  { sets.push(`is_primary=$${n++}`);  params.push(!!is_primary); }
      if (sets.length === 0) return res.json({ ok: true });

      const r = await dbQuery(
        `UPDATE user_assignments SET ${sets.join(', ')}
         WHERE id=$1
         RETURNING id, user_id, org_unit_id, position_id, is_primary`,
        params,
      );
      res.json({ assignment: r.rows[0] });
    } catch (e) {
      console.error('[org-api] PATCH /assignments error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // DELETE /assignments/:id 所属終了（effective_to セット、履歴は残す）
  expressApp.delete('/api/dashboard/admin/org/assignments/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      const effectiveTo = req.query.to || null; // YYYY-MM-DD 指定可、なければ今日

      await dbQuery(
        `UPDATE user_assignments
         SET effective_to=COALESCE($3::date, CURRENT_DATE)
         WHERE id=$1 AND team_id=$2 AND effective_to IS NULL`,
        [id, teamId, effectiveTo],
      );
      res.json({ ok: true });
    } catch (e) {
      console.error('[org-api] DELETE /assignments error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ═══════════════════════════════════════════════════════════
  // Phase 4 続き: 役職マスター編集
  // ═══════════════════════════════════════════════════════════

  expressApp.post('/api/dashboard/admin/org/positions', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const { name, level, sort_order = 0 } = req.body || {};
      if (!name || level == null) return res.status(400).json({ error: 'name and level required' });
      const r = await dbQuery(
        `INSERT INTO positions (team_id, name, level, sort_order)
         VALUES ($1, $2, $3, $4)
         RETURNING id, name, level, sort_order`,
        [teamId, name, Number(level), Number(sort_order)],
      );
      res.json({ position: r.rows[0] });
    } catch (e) {
      console.error('[org-api] POST /positions error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  expressApp.patch('/api/dashboard/admin/org/positions/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      const { name, level, sort_order } = req.body || {};
      const sets = [];
      const params = [id, teamId];
      let n = 3;
      if (name !== undefined)       { sets.push(`name=$${n++}`);       params.push(name); }
      if (level !== undefined)      { sets.push(`level=$${n++}`);      params.push(Number(level)); }
      if (sort_order !== undefined) { sets.push(`sort_order=$${n++}`); params.push(Number(sort_order)); }
      if (sets.length === 0) return res.json({ ok: true });
      const r = await dbQuery(
        `UPDATE positions SET ${sets.join(', ')} WHERE id=$1 AND team_id=$2
         RETURNING id, name, level, sort_order`,
        params,
      );
      if (!r.rows[0]) return res.status(404).json({ error: 'not found' });
      res.json({ position: r.rows[0] });
    } catch (e) {
      console.error('[org-api] PATCH /positions error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  expressApp.delete('/api/dashboard/admin/org/positions/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      // 有効な assignment があれば拒否
      const used = await dbQuery(
        `SELECT count(*)::int AS c FROM user_assignments
         WHERE position_id=$1 AND effective_to IS NULL`, [id],
      );
      if (used.rows[0].c > 0) {
        return res.status(400).json({ error: 'position in use', assignments: used.rows[0].c });
      }
      await dbQuery(
        `UPDATE positions SET is_active=false WHERE id=$1 AND team_id=$2`,
        [id, teamId],
      );
      res.json({ ok: true });
    } catch (e) {
      console.error('[org-api] DELETE /positions error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ═══════════════════════════════════════════════════════════
  // Phase 4 続き: 権限グラント CRUD
  // ═══════════════════════════════════════════════════════════

  // 機能カタログ（feature-access.js のと統合）
  const FEATURE_CATALOG = [
    { key: 'crm',                label: 'CRM 全体',           category: 'crm' },
    { key: 'crm.yomi',           label: 'CRM ヨミ管理',       category: 'crm' },
    { key: 'crm.performance',    label: 'CRM 成績',           category: 'crm' },
    { key: 'rpo',                label: 'RPO 案件管理',       category: 'rpo' },
    { key: 'recruit',            label: '採用管理',           category: 'recruit' },
    { key: 'legal',              label: '法務案件管理',       category: 'legal' },
    { key: 'corp',               label: 'Corp 管理',          category: 'corp' },
    { key: 'org.unit.edit',      label: '組織編集（部署/チーム）', category: 'admin' },
    { key: 'org.member.assign',  label: 'メンバー異動・所属変更', category: 'admin' },
    { key: 'org.position.edit',  label: '役職マスター編集',   category: 'admin' },
    { key: 'org.permission.edit', label: '権限グラント編集',  category: 'admin' },
  ];

  expressApp.get('/api/dashboard/admin/org/feature-catalog', authWithRole, async (req, res) => {
    res.json({ features: FEATURE_CATALOG });
  });

  expressApp.get('/api/dashboard/admin/org/permission-grants', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const r = await dbQuery(
        `SELECT g.*,
           CASE g.subject_type
             WHEN 'org_unit' THEN (SELECT name FROM org_units WHERE id=g.subject_id)
             WHEN 'position' THEN (SELECT name FROM positions WHERE id=g.subject_id)
             WHEN 'user'     THEN (SELECT display_name FROM dashboard_user_directory WHERE user_id=g.subject_id::text LIMIT 1)
           END AS subject_label
         FROM permission_grants g
         WHERE g.team_id=$1
         ORDER BY g.feature_key, g.effect, g.subject_type`,
        [teamId],
      );
      res.json({ grants: r.rows });
    } catch (e) {
      console.error('[org-api] GET /permission-grants error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  expressApp.post('/api/dashboard/admin/org/permission-grants', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const grantedBy = req.dashboardUser?.userId || 'admin';
      const { feature_key, subject_type, subject_id, composite_json, effect = 'allow' } = req.body || {};
      if (!feature_key || !subject_type) {
        return res.status(400).json({ error: 'feature_key and subject_type required' });
      }
      if (!['user','org_unit','position','composite'].includes(subject_type)) {
        return res.status(400).json({ error: 'invalid subject_type' });
      }
      if (!['allow','deny'].includes(effect)) {
        return res.status(400).json({ error: 'invalid effect' });
      }
      const r = await dbQuery(
        `INSERT INTO permission_grants
           (team_id, feature_key, subject_type, subject_id, composite_json, effect, granted_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING *`,
        [teamId, feature_key, subject_type, subject_id || null,
         composite_json ? JSON.stringify(composite_json) : null, effect, grantedBy],
      );
      res.json({ grant: r.rows[0] });
    } catch (e) {
      console.error('[org-api] POST /permission-grants error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  expressApp.delete('/api/dashboard/admin/org/permission-grants/:id', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const id = Number(req.params.id);
      await dbQuery(
        `DELETE FROM permission_grants WHERE id=$1 AND team_id=$2`, [id, teamId],
      );
      res.json({ ok: true });
    } catch (e) {
      console.error('[org-api] DELETE /permission-grants error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ═══════════════════════════════════════════════════════════
  // Phase 5: 既存ロール/CRM権限 → permission_grants へ移行
  // ═══════════════════════════════════════════════════════════

  expressApp.post('/api/dashboard/admin/org/migrate-permissions', authWithRole, adminOnly, async (req, res) => {
    try {
      const teamId = teamIdOf(req);
      const grantedBy = req.dashboardUser?.userId || 'admin';

      const summary = { admins: 0, corps: 0, featureAccessCopied: 0, skipped: 0 };

      // 1) dashboard_roles.role='admin' → org.permission.edit allow user
      const adminRows = await dbQuery(
        `SELECT user_id FROM dashboard_roles WHERE team_id=$1 AND role='admin'`, [teamId],
      );
      for (const r of adminRows.rows) {
        const dup = await dbQuery(
          `SELECT 1 FROM permission_grants
           WHERE team_id=$1 AND feature_key='org.permission.edit'
             AND subject_type='user' AND subject_id::text=$2 AND effect='allow'`,
          [teamId, r.user_id],
        );
        if (dup.rows[0]) { summary.skipped++; continue; }
        // user_id は TEXT だが subject_id は INTEGER. user の場合は subject_id=null + composite_json でユーザーID格納
        await dbQuery(
          `INSERT INTO permission_grants
             (team_id, feature_key, subject_type, composite_json, effect, granted_by)
           VALUES ($1, 'org.permission.edit', 'user', $2, 'allow', $3)`,
          [teamId, JSON.stringify({ user_id: r.user_id }), grantedBy],
        );
        summary.admins++;
      }

      // 2) dashboard_roles.role='corp' → corp feature allow user
      const corpRows = await dbQuery(
        `SELECT user_id FROM dashboard_roles WHERE team_id=$1 AND role='corp'`, [teamId],
      );
      for (const r of corpRows.rows) {
        const dup = await dbQuery(
          `SELECT 1 FROM permission_grants
           WHERE team_id=$1 AND feature_key='corp' AND subject_type='user'
             AND composite_json->>'user_id'=$2 AND effect='allow'`,
          [teamId, r.user_id],
        );
        if (dup.rows[0]) { summary.skipped++; continue; }
        await dbQuery(
          `INSERT INTO permission_grants
             (team_id, feature_key, subject_type, composite_json, effect, granted_by)
           VALUES ($1, 'corp', 'user', $2, 'allow', $3)`,
          [teamId, JSON.stringify({ user_id: r.user_id }), grantedBy],
        );
        summary.corps++;
      }

      // 3) 既存 feature_access を permission_grants にコピー（あれば）
      const fa = await dbQuery(
        `SELECT to_regclass('public.feature_access') IS NOT NULL AS exists`,
      );
      if (fa.rows[0].exists) {
        const rows = await dbQuery(
          `SELECT feature_key, subject_type, subject_id FROM feature_access WHERE team_id=$1`,
          [teamId],
        );
        for (const fr of rows.rows) {
          // subject_id を適切な型に
          const subjectIdNum = /^\d+$/.test(String(fr.subject_id)) ? Number(fr.subject_id) : null;
          const composite = subjectIdNum == null ? { raw: String(fr.subject_id) } : null;
          const dup = await dbQuery(
            `SELECT 1 FROM permission_grants
             WHERE team_id=$1 AND feature_key=$2 AND subject_type=$3
               AND (subject_id=$4 OR (composite_json->>'raw')=$5)
               AND effect='allow'`,
            [teamId, fr.feature_key, fr.subject_type, subjectIdNum, String(fr.subject_id)],
          );
          if (dup.rows[0]) { summary.skipped++; continue; }
          await dbQuery(
            `INSERT INTO permission_grants
               (team_id, feature_key, subject_type, subject_id, composite_json, effect, granted_by)
             VALUES ($1, $2, $3, $4, $5, 'allow', $6)`,
            [teamId, fr.feature_key, fr.subject_type, subjectIdNum,
             composite ? JSON.stringify(composite) : null, grantedBy],
          );
          summary.featureAccessCopied++;
        }
      }

      res.json({ ok: true, summary });
    } catch (e) {
      console.error('[org-api] /migrate-permissions error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  console.log('[org-api] registered (units/positions/members/migration + edit + permissions)');
}

module.exports = { registerOrgApi };
