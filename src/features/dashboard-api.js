const crypto = require("crypto");
const { randomUUID } = require("crypto");
const { registerRpoApi } = require("./rpo-api");
const { registerKintoneApi } = require("./kintone-api");
const { registerDriveApi } = require("./drive-api");
const { registerCrmApi, registerDailyReportApi } = require("./crm-api");
const { registerChannelMappingApi } = require("./channel-mapping");
const { registerPermissionsApi } = require("./permissions");
const { registerRankingApi } = require("./ranking");

// ================================
// Dashboard API + Token Auth
// ================================

const STAGE_LABELS = {
  mk: 'MK（アポ取り）',
  bc: 'BC（商談中）',
  contracted: '受注済',
  hr: 'HR分析中',
  direction: 'ディレクション',
  cs: 'CS（スカウト）',
  completed: '完了',
  lost: '失注',
};

const authTokens = new Map();

const TOKEN_TTL_MS = 60 * 60 * 1000;                  // 1時間（マジックリンク・使い捨て）
const SESSION_COOKIE_MAX_AGE = 10 * 365 * 24 * 60 * 60 * 1000; // 10年（事実上永続）

// トークン期限切れチェック（マジックリンクのみ）
function cleanupExpiredTokens() {
  const now = Date.now();
  for (const [k, v] of authTokens) {
    if (now - v.createdAt > TOKEN_TTL_MS) authTokens.delete(k);
  }
}
setInterval(cleanupExpiredTokens, 30 * 60 * 1000);

function generateToken(teamId, userId) {
  const token = crypto.randomBytes(32).toString("hex");
  const sessionId = crypto.randomBytes(32).toString("hex");
  authTokens.set(token, { teamId, userId, createdAt: Date.now(), sessionId });
  return token;
}

// token を消費して { teamId, userId, sessionId } を返す（まだDBには書かない）
function consumeToken(token) {
  cleanupExpiredTokens();
  const entry = authTokens.get(token);
  if (!entry) return null;
  if (Date.now() - entry.createdAt > TOKEN_TTL_MS) {
    authTokens.delete(token);
    return null;
  }
  authTokens.delete(token);
  return { teamId: entry.teamId, userId: entry.userId, sessionId: entry.sessionId };
}

// ================================
// Register API routes
// ================================
function registerDashboardApi(deps) {
  const {
    expressApp,
    slackClient,
    dbQuery,
    getUserDisplayName,
    dbGetDashboardRole,
    dbSetDashboardRole,
    dbListDashboardAdmins,
    dbCreateDashTeam,
    dbListDashTeams,
    dbGetDashTeam,
    dbDeleteDashTeam,
    dbUpdateDashTeam,
    dbUpdateDashTeamFull,
    dbAddDashTeamMember,
    dbUpdateDashTeamMemberRole,
    dbGetUserSlackTitle,
    dbUserHasAdminTeamRole,
    dbGetUserDashTeamRoles,
    dbGetDashTeamSubtree,
    dbSetUserDirectoryActive,
    dbRemoveDashTeamMember,
    dbListDashTeamMembers,
    dbListDashTeamMembersWithProfile,
    dbGetUserDashTeams,
    dbListDashboardVisibleUsers,
    dbListDashboardVisibleTeams,
    dbReplaceDashboardVisibleUsers,
    dbReplaceDashboardVisibleTeams,
    dbUpsertDashboardUserDirectoryMember,
    dbListDashboardUserDirectory,
    dbGetDashboardDirectoryMember,
    dbListWorkloadItems,
    dbGetWorkloadItem,
    dbCreateWorkloadItem,
    dbUpdateWorkloadItem,
    dbDeleteWorkloadItem,
    dbListWorkloadCells,
    dbSetWorkloadCells,
    dbCopyWorkloadMonth,
    dbListWorkloadCategories,
    dbUpsertWorkloadCategory,
    dbUpdateWorkloadCategory,
    dbDeleteWorkloadCategory,
    dbCreateProject,
    dbListProjects,
    dbGetProject,
    dbDeleteProject,
    dbUpdateProject,
    dbAddProjectTask,
    dbRemoveProjectTask,
    dbListProjectTasks,
    // Task operations (same DB functions as Slack app)
    dbGetTaskById,
    dbUpdateStatus,
    dbUpdateTaskEditableFields,
    dbUpdateBroadcastCounts,
    dbListTaskComments,
    dbInsertTaskComment,
    dbCreateTask,
    dbInsertTaskTargets,
    dbListTargetUserIds,
    randomUUID,
    // Integrations
    dbCreateIntegration,
    dbListIntegrations,
    dbGetIntegration,
    dbUpdateIntegration,
    dbDeleteIntegration,
    dbListFieldMappings,
    dbSetFieldMappings,
    dbCreateSyncLog,
    dbUpdateSyncLog,
    dbListSyncLogs,
    getSubteamIdMap,
    getUsergroupMembers,
    updateUsergroupMembers,
    // CRM
    dbCreateClient,
    dbListClients,
    dbGetClient,
    dbUpdateClient,
    dbDeleteClient,
    dbCreateDeal,
    dbListDeals,
    dbGetDeal,
    dbUpdateDeal,
    dbDeleteDeal,
    dbAddDealMember,
    dbRemoveDealMember,
    dbListDealMembers,
    dbIsDealMember,
    dbCreateDealActivity,
    dbListDealActivities,
    dbDeleteDealActivity,
    dbCreateDealPayment,
    dbListDealPayments,
    dbUpdateDealPayment,
    dbDeleteDealPayment,
    dbAddDealTask,
    dbRemoveDealTask,
    dbListDealTasks,
    dbCreateDeliverable,
    dbListDeliverables,
    dbUpdateDeliverable,
    dbDeleteDeliverable,
    dbCreateClientContact,
    dbListClientContacts,
    dbUpdateClientContact,
    dbDeleteClientContact,
    dbCreateDealPosition,
    dbListDealPositions,
    dbUpdateDealPosition,
    dbDeleteDealPosition,
    dbCreateDealMediaPlan,
    dbListDealMediaPlans,
    dbUpdateDealMediaPlan,
    dbDeleteDealMediaPlan,
    dbCreateCalcDef,
    dbListCalcDefs,
    dbUpdateCalcDef,
    dbDeleteCalcDef,
    dbPipelineSummary,
    upsertThreadCard,
    buildThreadCardBlocks,
    dbListPersonalFilters,
    dbGetPersonalFilterMemberIds,
  } = deps;

  const kintone = require("./kintone-connector");

  // ================================
  // セッション管理（DB永続化）
  // ================================
  async function dbCreateSession(sessionId, teamId, userId) {
    await dbQuery(
      `INSERT INTO dashboard_sessions (session_id, team_id, user_id, created_at, last_seen_at)
       VALUES ($1, $2, $3, NOW(), NOW())
       ON CONFLICT (session_id) DO NOTHING`,
      [sessionId, teamId, userId],
    );
  }

  async function validateSessionFromDb(sessionId) {
    if (!sessionId) return null;
    const res = await dbQuery(
      `SELECT team_id, user_id FROM dashboard_sessions WHERE session_id = $1`,
      [sessionId],
    ).catch(() => null);
    const row = res?.rows?.[0];
    if (!row) return null;
    return { teamId: row.team_id, userId: row.user_id };
  }

  // ================================
  // Middleware
  // ================================
  function authMiddleware(req, res, next) {
    const sessionId =
      req.cookies?.dashboard_session ||
      (req.headers.authorization || "").replace("Bearer ", "");
    validateSessionFromDb(sessionId)
      .then((user) => {
        if (!user) { res.status(401).json({ error: "unauthorized" }); return; }
        req.dashboardUser = user;
        next();
      })
      .catch(() => res.status(401).json({ error: "unauthorized" }));
  }

  // Slackプロフィールのtitleからシステムロールを決定する
  // 優先順: Sub Manager/Sub Expert → manager, Manager → manager,
  //         Sub Chief → sub_chief, Chief → chief, Lead → lead, その他 → member
  function roleTitleFromSlack(title) {
    if (!title) return 'member';
    if (/sub\s+(manager|expert)/i.test(title)) return 'manager';
    if (/\bmanager\b/i.test(title)) return 'manager';
    if (/sub\s*chief/i.test(title)) return 'sub_chief';
    if (/\bchief\b/i.test(title)) return 'chief';
    if (/\blead\b/i.test(title)) return 'lead';
    return 'member';
  }

  // admin check middleware（管理設定・インテグレーション等の高権限操作用）
  function adminOnly(req, res, next) {
    if (req.dashboardUser?.role !== "admin") {
      return res.status(403).json({ error: "admin_required" });
    }
    next();
  }

  // admin または IT チーム向け（管理者権限・チャンネルマッピング・Slackメンション管理）
  function adminOrITOnly(req, res, next) {
    const role = req.dashboardUser?.role;
    if (role !== "admin" && role !== "it") {
      return res.status(403).json({ error: "admin_or_it_required" });
    }
    next();
  }

  // admin または Personnel チーム向け（採用管理）
  function adminOrPersonnelOnly(req, res, next) {
    const role = req.dashboardUser?.role;
    if (role !== "admin" && role !== "personnel") {
      return res.status(403).json({ error: "admin_or_personnel_required" });
    }
    next();
  }

  // チーフ以上（admin / manager / chief）向けミドルウェア（チーム設定操作用）
  function chiefOrAbove(req, res, next) {
    const role = req.dashboardUser?.role;
    if (!['admin', 'manager', 'chief'].includes(role)) {
      return res.status(403).json({ error: "insufficient_role" });
    }
    next();
  }

  // ロールキャッシュ（TTL: 5分）。ロール変更後は最大5分で反映される。
  const roleCache = new Map(); // `${teamId}:${userId}` → { role, expiresAt }
  setInterval(() => {
    const now = Date.now();
    for (const [k, v] of roleCache) {
      if (now >= v.expiresAt) roleCache.delete(k);
    }
  }, 5 * 60 * 1000).unref();

  // Enhance authMiddleware to attach role.
  // dashboard_roles に admin が明示設定されている場合はそれを優先。
  // それ以外はSlackプロフィールのtitleから自動判定する。
  async function authWithRole(req, res, next) {
    authMiddleware(req, res, async () => {
      try {
        const { teamId, userId } = req.dashboardUser;
        const cacheKey = `${teamId}:${userId}`;
        const cached = roleCache.get(cacheKey);
        if (cached && Date.now() < cached.expiresAt) {
          req.dashboardUser.role = cached.role;
          return next();
        }
        const dbRole = await dbGetDashboardRole(teamId, userId);
        let role;
        if (dbRole === 'admin') {
          role = 'admin';
        } else if (dbRole === 'corp') {
          role = 'corp';
        } else {
          // Corporateチームに所属していれば corp 扱い
          const { rows: corpRows } = await dbQuery(`
            SELECT 1 FROM dash_team_members dtm
            JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
            JOIN dash_teams parent ON parent.id = dt.parent_id AND parent.team_id = dt.team_id
            WHERE dtm.team_id = $1 AND dtm.user_id = $2
              AND parent.name ILIKE '%corporate%'
            LIMIT 1
          `, [teamId, userId]);
          if (corpRows.length > 0) {
            role = 'corp';
          } else {
            // IT チーム所属チェック
            const { rows: itRows } = await dbQuery(`
              SELECT 1 FROM dash_team_members dtm
              JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
              WHERE dtm.team_id = $1 AND dtm.user_id = $2
                AND (dt.name ILIKE '%IT%' OR dt.name ILIKE '%情シス%')
              LIMIT 1
            `, [teamId, userId]);
            if (itRows.length > 0) {
              role = 'it';
            } else {
              // Personnel（人事）チーム所属チェック
              const { rows: persRows } = await dbQuery(`
                SELECT 1 FROM dash_team_members dtm
                JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
                WHERE dtm.team_id = $1 AND dtm.user_id = $2
                  AND (dt.name ILIKE '%personnel%' OR dt.name ILIKE '%人事%' OR dt.name ILIKE '%HR%')
                LIMIT 1
              `, [teamId, userId]);
              if (persRows.length > 0) {
                role = 'personnel';
              } else {
                const title = await dbGetUserSlackTitle(teamId, userId);
                role = roleTitleFromSlack(title);
              }
            }
          }
        }
        roleCache.set(cacheKey, { role, expiresAt: Date.now() + 5 * 60 * 1000 });
        req.dashboardUser.role = role;
        next();
      } catch (e) {
        console.error("authWithRole error:", e);
        res.status(500).json({ error: "internal" });
      }
    });
  }

  // Helper: get visible user_ids for non-admin.
  // Role-based visibility:
  //   manager   → all users in their team's full subtree (team + all descendants)
  //   chief / sub_chief / lead / member → all users in their direct team(s)
  async function getVisibleUserIds(teamId, userId) {
    // Explicit visibility overrides (manually set by admin)
    const [explicitUsers, explicitTeams] = await Promise.all([
      dbListDashboardVisibleUsers(teamId, userId),
      dbListDashboardVisibleTeams(teamId, userId),
    ]);
    if (explicitUsers.length || explicitTeams.length) {
      const visible = new Set([userId, ...explicitUsers.map((row) => row.visible_user_id)]);
      for (const row of explicitTeams) {
        const members = await dbListDashTeamMembers(teamId, row.visible_dash_team_id);
        for (const member of members) visible.add(member.user_id);
      }
      return Array.from(visible);
    }

    // Role-based: collect all team IDs the user should see based on their role
    const teamRoles = await dbGetUserDashTeamRoles(teamId, userId);
    if (!teamRoles.length) return [userId];

    const teamIdsToShow = new Set();
    for (const { id: tId, role } of teamRoles) {
      if (role === 'manager') {
        // See all teams in the subtree rooted at this team
        const subtree = await dbGetDashTeamSubtree(teamId, tId);
        for (const sid of subtree) teamIdsToShow.add(sid);
      } else {
        // chief / sub_chief / lead / member: just their direct team
        teamIdsToShow.add(tId);
        // Also include child teams (existing inherited membership behaviour)
        const allAccessible = await dbGetUserDashTeams(teamId, userId);
        for (const t of allAccessible) teamIdsToShow.add(t.id);
      }
    }

    const allMembers = new Set([userId]);
    for (const tid of teamIdsToShow) {
      const members = await dbListDashTeamMembers(teamId, tid);
      for (const m of members) allMembers.add(m.user_id);
    }
    return Array.from(allMembers);
  }

  async function getVisibilityConfig(teamId, userId) {
    const [visibleUsers, visibleTeams] = await Promise.all([
      dbListDashboardVisibleUsers(teamId, userId),
      dbListDashboardVisibleTeams(teamId, userId),
    ]);
    return {
      visibleUserIds: visibleUsers.map((row) => row.visible_user_id),
      visibleDashTeamIds: visibleTeams.map((row) => row.visible_dash_team_id),
    };
  }

  function parseMonthKey(input) {
    const raw = String(input || '').trim();
    return /^\d{4}-\d{2}$/.test(raw) ? raw : null;
  }

  async function canAccessDashTeam(teamId, userId, role, dashTeamId) {
    if (!dashTeamId) return false;
    if (role === 'admin') {
      const team = await dbGetDashTeam(teamId, dashTeamId);
      return !!team;
    }
    const teams = await dbGetUserDashTeams(teamId, userId);
    return teams.some((team) => team.id === dashTeamId);
  }

  async function syncDashboardUserDirectory(teamId) {
    let cursor = '';
    let count = 0;
    do {
      const resp = await slackClient.users.list({
        limit: 200,
        cursor,
        team_id: teamId,
      });
      const members = Array.isArray(resp?.members) ? resp.members : [];
      for (const member of members) {
        if (!member?.id || member.is_bot || member.id === 'USLACKBOT') continue;
        await dbUpsertDashboardUserDirectoryMember(teamId, member);
        count += 1;
      }
      cursor = resp?.response_metadata?.next_cursor || '';
    } while (cursor);
    return count;
  }

  // --- Token exchange ---
  expressApp.get("/dashboard/auth", async (req, res) => {
    const { token } = req.query;
    if (!token) return res.status(400).send("Missing token");
    const result = consumeToken(token);
    if (!result) {
      return res.status(401).send(
        "<html><body><h2>リンクの有効期限が切れています</h2>" +
        "<p>Slackで <code>/dashboard</code> を再度実行してください。</p></body></html>"
      );
    }
    await dbCreateSession(result.sessionId, result.teamId, result.userId);
    res.cookie("dashboard_session", result.sessionId, {
      httpOnly: true,
      sameSite: "lax",
      maxAge: SESSION_COOKIE_MAX_AGE,
      secure: process.env.NODE_ENV === "production",
    });
    // セッションIDをURLに乗せてリダイレクト（ネイティブブラウザで同じURLを開くと自動認証）
    res.redirect(`/dashboard?auth=${result.sessionId}`);
  });

  // ================================
  // セッション移行（URLトークン経由の自動引き継ぎ）
  // ================================
  const transferTokens = new Map(); // token → { teamId, userId, sessionId, expires }
  // 定期クリーンアップ
  setInterval(() => { for (const [k, v] of transferTokens) if (v.expires < Date.now()) transferTokens.delete(k); }, 60000);

  // セッションIDをURLから受け取り、DBで検証してcookieをセット（ネイティブブラウザへの引き継ぎ）
  expressApp.get('/api/auth/adopt', async (req, res) => {
    try {
      const { token, redirect = '/dashboard' } = req.query;
      if (!token) return res.redirect('/dashboard');
      // DBでセッションが存在するか検証
      const { rows } = await dbQuery(
        'SELECT team_id, user_id FROM dashboard_sessions WHERE session_id = $1',
        [token]
      );
      if (!rows[0]) return res.redirect('/dashboard');
      // 有効なセッションIDをcookieとしてセット
      res.cookie('dashboard_session', token, {
        httpOnly: true, sameSite: 'lax',
        maxAge: SESSION_COOKIE_MAX_AGE,
        secure: process.env.NODE_ENV === 'production',
      });
      res.redirect(decodeURIComponent(redirect));
    } catch (e) {
      console.error('[auth/adopt error]', e);
      res.redirect('/dashboard');
    }
  });

  // セッションID返却（adoptエンドポイントがDBで検証できるよう、セッションIDをそのまま返す）
  expressApp.post('/api/auth/transfer-token', authMiddleware, async (req, res) => {
    try {
      const sessionId = req.cookies?.dashboard_session || (req.headers.authorization || '').replace('Bearer ', '');
      if (!sessionId) return res.status(401).json({ error: 'no session' });
      res.json({ token: sessionId });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // トークン引き換え → セッションcookieを発行してリダイレクト
  expressApp.get('/api/auth/transfer', async (req, res) => {
    const { token, redirect = '/dashboard/crm' } = req.query;
    const data = transferTokens.get(token);
    if (!data || data.expires < Date.now()) {
      return res.send('<html><body style="font-family:sans-serif;text-align:center;padding:60px"><h2>リンクの有効期限が切れています</h2><p>アプリに戻って再度お試しください。</p></body></html>');
    }
    transferTokens.delete(token);
    // 既存セッションを再利用
    res.cookie('dashboard_session', data.sessionId, {
      httpOnly: true, sameSite: 'lax',
      maxAge: SESSION_COOKIE_MAX_AGE,
      secure: process.env.NODE_ENV === 'production',
    });
    res.redirect(redirect);
  });

  // ================================
  // General APIs (role-aware)
  // ================================

  // --- /me (with role) ---
  expressApp.get("/api/dashboard/me", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const name = await getUserDisplayName(teamId, userId);
      const teams = await dbGetUserDashTeams(teamId, userId);
      res.json({ teamId, userId, displayName: name, role, dashTeams: teams });
    } catch (e) {
      console.error("dashboard /me error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /summary (team-scoped for non-admin; scope=self for personal view) ---
  expressApp.get("/api/dashboard/summary", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;

      if (req.query.scope === "self") {
        const [statusRes, overdueRes] = await Promise.all([
          dbQuery(
            `SELECT status, COUNT(*)::int AS count FROM tasks WHERE team_id=$1 AND assignee_id=$2 AND status != 'cancelled' GROUP BY status`,
            [teamId, userId]
          ),
          dbQuery(
            `SELECT COUNT(*)::int AS overdue FROM tasks WHERE team_id=$1 AND assignee_id=$2 AND due_date < CURRENT_DATE AND status NOT IN ('done','cancelled')`,
            [teamId, userId]
          ),
        ]);
        const summary = {};
        for (const row of statusRes.rows) summary[row.status] = parseInt(row.count, 10);
        summary._overdue = parseInt(overdueRes.rows[0]?.overdue || 0, 10);
        return res.json({ summary });
      }

      let scopeWhere = "";
      let params = [teamId];

      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        scopeWhere = "AND assignee_id = ANY($2)";
        params.push(visible);
      }

      const q = `
        SELECT status, COUNT(*)::int AS count
        FROM tasks
        WHERE team_id = $1 ${scopeWhere}
        GROUP BY status
        ORDER BY status;
      `;
      const result = await dbQuery(q, params);
      const summary = {};
      for (const row of result.rows) summary[row.status] = row.count;
      res.json({ summary });
    } catch (e) {
      console.error("dashboard /summary error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /tasks (team-scoped for non-admin) ---
  expressApp.get("/api/dashboard/tasks", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const status = req.query.status || null;
      const assignee = req.query.assignee || null;
      const projectId = req.query.project || null;
      const overdue = req.query.overdue === "1" || req.query.overdue === "true";
      const usergroupId = req.query.usergroup || null;
      const dashTeamParam = req.query.dashTeam || null;
      const personalFilterId = req.query.personalFilter || null;
      const sortParam = req.query.sort || '';
      const page = Math.max(1, parseInt(req.query.page, 10) || 1);
      const limit = Math.min(2000, Math.max(1, parseInt(req.query.limit, 10) || 50));
      const offset = (page - 1) * limit;

      const conditions = ["t.team_id = $1"];
      const params = [teamId];
      let idx = 2;
      let joinClause = "";

      const assignees = req.query.assignees ? req.query.assignees.split(',').filter(Boolean) : null;
      if (assignees?.length) {
        const p = idx++;
        conditions.push(
          `(t.assignee_id = ANY($${p}) OR (t.task_type='broadcast' AND EXISTS ` +
          `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = ANY($${p}))))`
        );
        params.push(assignees);
      } else if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        const p = idx++;
        conditions.push(
          `(t.assignee_id = ANY($${p}) OR (t.task_type='broadcast' AND EXISTS ` +
          `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = ANY($${p}))))`
        );
        params.push(visible);
      }
      if (status) {
        conditions.push(`t.status = $${idx++}`);
        params.push(status);
      }
      if (assignee) {
        const p = idx++;
        conditions.push(
          `(t.assignee_id = $${p} OR (t.task_type='broadcast' AND EXISTS ` +
          `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = $${p})))`
        );
        params.push(assignee);
      }
      if (projectId) {
        joinClause = `JOIN project_tasks pt ON pt.task_id = t.id AND pt.team_id = t.team_id`;
        conditions.push(`pt.project_id = $${idx++}`);
        params.push(projectId);
      }
      if (overdue) {
        conditions.push(`t.due_date < CURRENT_DATE`);
        conditions.push(`t.status NOT IN ('done','cancelled')`);
      }
      if (usergroupId) {
        const members = await getUsergroupMembers(teamId, usergroupId);
        if (members.length > 0) {
          const p = idx++;
          conditions.push(
            `(t.assignee_id = ANY($${p}) OR (t.task_type='broadcast' AND EXISTS ` +
            `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = ANY($${p}))))`
          );
          params.push(members);
        } else {
          conditions.push("false");
        }
      }
      if (dashTeamParam) {
        // 部署指定時は子チームも含む全メンバーを対象
        const subtreeIds = await dbGetDashTeamSubtree(teamId, dashTeamParam);
        const allTeamIds = [dashTeamParam, ...subtreeIds];
        const memberRows = await dbQuery(
          `SELECT DISTINCT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id = ANY($2)`,
          [teamId, allTeamIds]
        );
        const memberIds = memberRows.rows.map(r => r.user_id);
        if (memberIds.length > 0) {
          const p = idx++;
          conditions.push(
            `(t.assignee_id = ANY($${p}) OR (t.task_type='broadcast' AND EXISTS ` +
            `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = ANY($${p}))))`
          );
          params.push(memberIds);
        } else {
          conditions.push("false");
        }
      }
      if (personalFilterId) {
        const pfMembers = await dbGetPersonalFilterMemberIds(teamId, personalFilterId);
        if (pfMembers.length > 0) {
          const p = idx++;
          conditions.push(
            `(t.assignee_id = ANY($${p}) OR t.requester_user_id = ANY($${p}) OR ` +
            `(t.task_type='broadcast' AND EXISTS ` +
            `(SELECT 1 FROM task_targets tt WHERE tt.task_id::text=t.id AND tt.team_id=t.team_id AND tt.user_id = ANY($${p}))))`
          );
          params.push(pfMembers);
        } else {
          conditions.push("false");
        }
      }

      const where = conditions.join(" AND ");

      const [countResult, dataResult] = await Promise.all([
        dbQuery(`SELECT COUNT(*)::int AS total FROM tasks t ${joinClause} WHERE ${where}`, params),
        dbQuery(
          `SELECT t.id, t.title, t.status, t.due_date, t.assignee_id, t.assignee_label,
                  t.requester_user_id, t.task_type, t.created_at, t.completed_count, t.total_count
           FROM tasks t ${joinClause}
           WHERE ${where}
           ORDER BY ${
             sortParam === 'due_date_asc'  ? '(t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC' :
             sortParam === 'due_date_desc' ? '(t.due_date IS NULL) DESC, t.due_date DESC, t.created_at DESC' :
             't.created_at DESC'
           }
           LIMIT $${idx++} OFFSET $${idx++}`,
          [...params, limit, offset],
        ),
      ]);

      // Resolve assigneeDisplayName for each task
      const tasksWithNames = await Promise.all(
        dataResult.rows.map(async (t) => {
          let assigneeDisplayName = null;
          if (t.task_type === 'broadcast') {
            assigneeDisplayName = t.assignee_label || null;
          } else if (t.assignee_id) {
            assigneeDisplayName = await getUserDisplayName(teamId, t.assignee_id);
          }
          return { ...t, assigneeDisplayName };
        }),
      );

      // ガント用: broadcast タスクに対象メンバーの user_id と完了済み user_id を付与
      if (assignees?.length) {
        const broadcastIds = tasksWithNames.filter(t => t.task_type === 'broadcast').map(t => t.id);
        if (broadcastIds.length > 0) {
          const [ttRes, compRes] = await Promise.all([
            dbQuery(
              `SELECT task_id::text AS task_id, user_id FROM task_targets WHERE team_id=$1 AND task_id::text = ANY($2) AND user_id = ANY($3)`,
              [teamId, broadcastIds, assignees]
            ),
            dbQuery(
              `SELECT task_id::text AS task_id, user_id FROM task_completions WHERE team_id=$1 AND task_id::text = ANY($2)`,
              [teamId, broadcastIds]
            ),
          ]);
          const targetMap = {};
          for (const row of ttRes.rows) {
            if (!targetMap[row.task_id]) targetMap[row.task_id] = [];
            targetMap[row.task_id].push(row.user_id);
          }
          const completedMap = {};
          for (const row of compRes.rows) {
            if (!completedMap[row.task_id]) completedMap[row.task_id] = [];
            completedMap[row.task_id].push(row.user_id);
          }
          for (const t of tasksWithNames) {
            if (t.task_type === 'broadcast') {
              t.target_user_ids = targetMap[t.id] || [];
              t.completed_user_ids = completedMap[t.id] || [];
            }
          }
        }
      }

      res.json({
        tasks: tasksWithNames,
        total: countResult.rows[0]?.total || 0,
        page,
        limit,
      });
    } catch (e) {
      console.error("dashboard /tasks error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /personal-filters ---
  expressApp.get("/api/dashboard/personal-filters", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const filters = await dbListPersonalFilters(teamId, userId);
      res.json({ filters });
    } catch (e) {
      console.error("dashboard /personal-filters error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /usergroups ---
  expressApp.get("/api/dashboard/usergroups", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const idToHandle = await getSubteamIdMap(teamId);
      const usergroups = await Promise.all(
        Array.from(idToHandle.entries()).map(async ([id, handle]) => {
          const memberIds = await getUsergroupMembers(teamId, id);
          return { id, handle, memberIds };
        }),
      );
      res.json({ usergroups });
    } catch (e) {
      console.error("dashboard /usergroups error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Slack グループ管理 (admin) ---

  // 全グループ＋メンバー一覧（プロフィール付き）
  expressApp.get("/api/dashboard/admin/slack-groups", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const idToHandle = await getSubteamIdMap(teamId);

      // 全ユーザープロフィールをディレクトリから取得
      const allUsers = await dbListDashboardUserDirectory(teamId, { limit: 2000 });
      const userMap = Object.fromEntries(allUsers.map(u => [u.user_id, u]));

      const groups = await Promise.all(
        Array.from(idToHandle.entries()).map(async ([id, handle]) => {
          const memberIds = await getUsergroupMembers(teamId, id);
          const members = memberIds.map(uid => {
            const u = userMap[uid];
            return {
              user_id: uid,
              display_name: u?.display_name || uid,
              real_name: u?.real_name || uid,
              avatar_url: u?.profile_json?.image_72 || null,
              title: u?.profile_json?.title || null,
            };
          });
          return { id, handle, members };
        })
      );

      // ユーザーごとのグループ一覧マップも返す
      const userGroups = {};
      for (const g of groups) {
        for (const m of g.members) {
          if (!userGroups[m.user_id]) userGroups[m.user_id] = [];
          userGroups[m.user_id].push({ id: g.id, handle: g.handle });
        }
      }

      res.json({ groups, userGroups, users: allUsers.filter(u => u.is_active && (u.profile_json?.email || '').endsWith('@inrevo.jp')).map(u => ({
        user_id: u.user_id,
        display_name: u.display_name,
        real_name: u.real_name,
        avatar_url: u.profile_json?.image_72 || null,
        title: u.profile_json?.title || null,
      })) });
    } catch (e) {
      console.error("admin /slack-groups error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // グループのメンバーを更新（追加 or 削除）
  expressApp.patch("/api/dashboard/admin/slack-groups/:groupId/members", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { groupId } = req.params;
      const { action, userId } = req.body; // action: 'add' | 'remove'
      if (!action || !userId) return res.status(400).json({ error: "action and userId required" });

      const current = await getUsergroupMembers(teamId, groupId);
      let updated;
      if (action === 'add') {
        updated = current.includes(userId) ? current : [...current, userId];
      } else {
        updated = current.filter(id => id !== userId);
      }
      await updateUsergroupMembers(teamId, groupId, updated);
      res.json({ ok: true, members: updated });
    } catch (e) {
      if (e.code === "cannot_make_group_empty") {
        return res.status(400).json({ error: "cannot_make_group_empty", message: "グループに最低1名必要なため削除できません" });
      }
      console.error("admin /slack-groups/:id/members error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Slackグループルール ---

  expressApp.get("/api/dashboard/admin/slack-group-rules", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery("SELECT * FROM slack_group_rules WHERE team_id=$1 ORDER BY created_at ASC", [teamId]);
      res.json({ rules: r.rows });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.post("/api/dashboard/admin/slack-group-rules", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, category, groupIds, deptName } = req.body;
      if (!name?.trim()) return res.status(400).json({ error: "name required" });
      const id = randomUUID();
      const r = await dbQuery(
        "INSERT INTO slack_group_rules (id, team_id, name, title_pattern, category, group_ids, dept_name) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *",
        [id, teamId, name.trim(), '', category || 'role', JSON.stringify(groupIds || []), deptName || null]
      );
      res.json({ rule: r.rows[0] });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.put("/api/dashboard/admin/slack-group-rules/:id", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, category, groupIds, deptName } = req.body;
      const r = await dbQuery(
        "UPDATE slack_group_rules SET name=$3, category=$4, group_ids=$5, dept_name=$6 WHERE id=$1 AND team_id=$2 RETURNING *",
        [req.params.id, teamId, name.trim(), category || 'role', JSON.stringify(groupIds || []), deptName || null]
      );
      res.json({ rule: r.rows[0] });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.delete("/api/dashboard/admin/slack-group-rules/:id", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery("DELETE FROM slack_group_rules WHERE id=$1 AND team_id=$2", [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // ルール適用プレビュー
  expressApp.post("/api/dashboard/admin/slack-group-rules/preview", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const rulesRes = await dbQuery("SELECT * FROM slack_group_rules WHERE team_id=$1", [teamId]);
      const rules = rulesRes.rows;
      if (rules.length === 0) return res.json({ changes: [] });

      // 全グループ取得
      const idToHandle = await getSubteamIdMap(teamId);
      // ルールで管理対象のグループID集合
      const managedGroupIds = new Set(rules.flatMap(r => r.group_ids || []));

      // 全ユーザー（@inrevo.jp のみ）
      const allUsers = await dbListDashboardUserDirectory(teamId, { limit: 2000 });
      const targetUsers = allUsers.filter(u => u.is_active && (u.profile_json?.email || '').endsWith('@inrevo.jp'));

      // 各グループの現メンバー取得
      const groupMembers = {};
      for (const gid of managedGroupIds) {
        groupMembers[gid] = new Set(await getUsergroupMembers(teamId, gid));
      }

      const changes = [];
      for (const user of targetUsers) {
        const title = (user.profile_json?.title || '').toLowerCase();
        // マッチするルールを探す
        const matchedRules = rules.filter(r => title.includes(r.title_pattern.toLowerCase()));
        const shouldBeIn = new Set(matchedRules.flatMap(r => r.group_ids || []));

        const toAdd = [...shouldBeIn].filter(gid => !groupMembers[gid]?.has(user.user_id));
        const toRemove = [...managedGroupIds].filter(gid => !shouldBeIn.has(gid) && groupMembers[gid]?.has(user.user_id));

        if (toAdd.length > 0 || toRemove.length > 0) {
          changes.push({
            user_id: user.user_id,
            display_name: user.display_name,
            real_name: user.real_name,
            avatar_url: user.profile_json?.image_72 || null,
            title: user.profile_json?.title || '',
            add: toAdd.map(gid => ({ id: gid, handle: idToHandle.get(gid) || gid })),
            remove: toRemove.map(gid => ({ id: gid, handle: idToHandle.get(gid) || gid })),
          });
        }
      }
      res.json({ changes });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // ルール適用実行
  expressApp.post("/api/dashboard/admin/slack-group-rules/apply", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { changes } = req.body; // previewと同じ形式
      if (!Array.isArray(changes)) return res.status(400).json({ error: "changes required" });

      // グループIDごとに変更を集約
      const groupDeltas = {}; // groupId -> { add: Set, remove: Set }
      for (const c of changes) {
        for (const g of (c.add || [])) {
          if (!groupDeltas[g.id]) groupDeltas[g.id] = { add: new Set(), remove: new Set() };
          groupDeltas[g.id].add.add(c.user_id);
        }
        for (const g of (c.remove || [])) {
          if (!groupDeltas[g.id]) groupDeltas[g.id] = { add: new Set(), remove: new Set() };
          groupDeltas[g.id].remove.add(c.user_id);
        }
      }

      for (const [groupId, delta] of Object.entries(groupDeltas)) {
        const current = await getUsergroupMembers(teamId, groupId);
        const updated = [...new Set([
          ...current.filter(uid => !delta.remove.has(uid)),
          ...delta.add,
        ])];
        await updateUsergroupMembers(teamId, groupId, updated);
      }

      res.json({ ok: true, applied: Object.keys(groupDeltas).length });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 変更ウィザード: 1人のユーザーに対して特定ルールを適用する差分を返す
  // カテゴリ内の他ルールのグループはすべて外す
  expressApp.post("/api/dashboard/admin/slack-group-rules/change-preview", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { userId, toRuleId } = req.body;
      if (!userId || !toRuleId) return res.status(400).json({ error: "userId and toRuleId required" });

      const rulesRes = await dbQuery("SELECT * FROM slack_group_rules WHERE team_id=$1", [teamId]);
      const rules = rulesRes.rows;
      const toRule = rules.find(r => r.id === toRuleId);
      if (!toRule) return res.status(404).json({ error: "rule not found" });

      const idToHandle = await getSubteamIdMap(teamId);

      // 同カテゴリの全ルールのグループが管理対象
      const sameCategory = rules.filter(r => r.category === toRule.category);
      const managedInCategory = new Set(sameCategory.flatMap(r => r.group_ids || []));

      // 現在の所属グループ（管理対象のみ）
      const currentInManaged = [];
      for (const gid of managedInCategory) {
        const members = await getUsergroupMembers(teamId, gid);
        if (members.includes(userId)) currentInManaged.push(gid);
      }

      const toGroupIds = new Set(toRule.group_ids || []);
      const toAdd = [...toGroupIds].filter(gid => !currentInManaged.includes(gid));
      const toRemove = currentInManaged.filter(gid => !toGroupIds.has(gid));

      res.json({
        toRule: { id: toRule.id, name: toRule.name, category: toRule.category },
        add: toAdd.map(gid => ({ id: gid, handle: idToHandle.get(gid) || gid })),
        remove: toRemove.map(gid => ({ id: gid, handle: idToHandle.get(gid) || gid })),
      });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.post("/api/dashboard/admin/slack-group-rules/change-apply", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { userId, add, remove } = req.body;
      if (!userId) return res.status(400).json({ error: "userId required" });

      for (const g of (remove || [])) {
        const current = await getUsergroupMembers(teamId, g.id);
        await updateUsergroupMembers(teamId, g.id, current.filter(id => id !== userId));
      }
      for (const g of (add || [])) {
        const current = await getUsergroupMembers(teamId, g.id);
        if (!current.includes(userId)) {
          await updateUsergroupMembers(teamId, g.id, [...current, userId]);
        }
      }
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // --- グループタブ表示設定 ---
  expressApp.get("/api/dashboard/admin/slack-group-visibility", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery("SELECT group_id, hidden_tabs FROM slack_group_tab_visibility WHERE team_id=$1", [teamId]);
      const map = Object.fromEntries(r.rows.map(row => [row.group_id, row.hidden_tabs || []]));
      res.json({ visibility: map });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.put("/api/dashboard/admin/slack-group-visibility/:groupId", authWithRole, adminOrITOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { groupId } = req.params;
      const { hiddenTabs } = req.body; // array like ['role','dept','auto']
      await dbQuery(
        `INSERT INTO slack_group_tab_visibility (team_id, group_id, hidden_tabs)
         VALUES ($1, $2, $3)
         ON CONFLICT (team_id, group_id) DO UPDATE SET hidden_tabs=$3`,
        [teamId, groupId, JSON.stringify(hiddenTabs || [])]
      );
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // ═══════════════════════════════════════════════
  // 採用管理（自社）実技テスト
  // ═══════════════════════════════════════════════

  // 設定 GET/PUT
  expressApp.get("/api/dashboard/admin/recruitment/settings", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      res.json({ settings: r.rows[0] || {} });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.put("/api/dashboard/admin/recruitment/settings", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { templateSpreadsheetId, gasEndpointUrl, notifyChannelId, notifyMentionUserId, webhookSecret,
              fromEmail, emailSubject, emailBody, totalScore, importSheetUrl } = req.body;
      await dbQuery(`
        INSERT INTO recruitment_settings (team_id, template_spreadsheet_id, gas_endpoint_url, notify_channel_id, notify_mention_user_id, webhook_secret, from_email, email_subject, email_body, total_score, import_sheet_url, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
        ON CONFLICT (team_id) DO UPDATE SET
          template_spreadsheet_id=$2, gas_endpoint_url=$3, notify_channel_id=$4,
          notify_mention_user_id=$5, webhook_secret=$6,
          from_email=$7, email_subject=$8, email_body=$9, total_score=$10,
          import_sheet_url=$11, updated_at=now()
      `, [teamId, templateSpreadsheetId||null, gasEndpointUrl||null, notifyChannelId||null, notifyMentionUserId||null, webhookSecret||null,
          fromEmail||null, emailSubject||null, emailBody||null, totalScore||null, importSheetUrl||null]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 候補者 一覧/追加/削除
  expressApp.get("/api/dashboard/admin/recruitment/candidates", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery("SELECT * FROM recruitment_candidates WHERE team_id=$1 ORDER BY created_at DESC", [teamId]);
      res.json({ candidates: r.rows });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.post("/api/dashboard/admin/recruitment/candidates", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, email } = req.body;
      if (!name?.trim() || !email?.trim()) return res.status(400).json({ error: "name and email required" });
      const id = randomUUID();
      const r = await dbQuery(
        "INSERT INTO recruitment_candidates (id, team_id, name, email) VALUES ($1,$2,$3,$4) RETURNING *",
        [id, teamId, name.trim(), email.trim()]
      );
      res.json({ candidate: r.rows[0] });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.delete("/api/dashboard/admin/recruitment/candidates/:id", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery("DELETE FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [req.params.id, teamId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // スプレッドシートから候補者取り込み（GAS経由: A列=名前, B列=メアド, C列に取り込み済みフラグ）
  expressApp.post("/api/dashboard/admin/recruitment/import-sheet", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { spreadsheetUrl } = req.body;
      if (!spreadsheetUrl) return res.status(400).json({ error: "spreadsheetUrl required" });

      const match = spreadsheetUrl.match(/\/d\/([a-zA-Z0-9-_]+)/);
      if (!match) return res.status(400).json({ error: "スプレッドシートのURLが正しくありません" });
      const spreadsheetId = match[1];

      const settingsRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      const settings = settingsRes.rows[0];
      if (!settings?.gas_endpoint_url) return res.status(400).json({ error: "GAS URLが設定されていません" });

      // GAS に取り込みを依頼（GAS側でC列にフラグを書き込む）
      const gasRes = await fetch(settings.gas_endpoint_url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'importFromSheet',
          spreadsheetId,
          secret: settings.webhook_secret || '',
        }),
      });
      const gasData = await gasRes.json();
      if (!gasData.ok) return res.status(400).json({ error: gasData.error || 'GASエラー' });

      const added = [], skipped = [];
      for (const row of (gasData.rows || [])) {
        const name = (row.name || '').trim();
        const email = (row.email || '').trim();
        if (!name || !email) { skipped.push(row); continue; }
        const existing = await dbQuery("SELECT id FROM recruitment_candidates WHERE team_id=$1 AND email=$2", [teamId, email]);
        if (existing.rows.length > 0) { skipped.push(row); continue; }
        const r = await dbQuery(
          "INSERT INTO recruitment_candidates (id, team_id, name, email) VALUES ($1,$2,$3,$4) RETURNING *",
          [randomUUID(), teamId, name, email]
        );
        added.push(r.rows[0]);
      }
      res.json({ added, skipped: skipped.length });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 送信処理（名前・メール入力済・URLなし・pending or error の候補者に一括送信）
  expressApp.post("/api/dashboard/admin/recruitment/send", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const settingsRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      const settings = settingsRes.rows[0];
      if (!settings?.gas_endpoint_url || !settings?.template_spreadsheet_id) {
        return res.status(400).json({ error: "GASエンドポイントURLとテンプレートIDを設定してください" });
      }

      const baseUrl = process.env.APP_BASE_URL || `https://inrevo-task.com`;
      const webhookUrl = `${baseUrl}/api/dashboard/recruitment/webhook/complete`;

      const candidatesRes = await dbQuery(
        "SELECT * FROM recruitment_candidates WHERE team_id=$1 AND spreadsheet_url IS NULL AND status IN ('pending','error')",
        [teamId]
      );
      const targets = candidatesRes.rows;
      if (targets.length === 0) return res.json({ ok: true, sent: 0 });

      const results = [];
      for (const c of targets) {
        try {
          // GAS web app 呼び出し
          const gasRes = await fetch(settings.gas_endpoint_url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              candidateId: c.id,
              name: c.name,
              email: c.email,
              templateId: settings.template_spreadsheet_id,
              webhookUrl,
              secret: settings.webhook_secret || '',
            }),
          });
          const gasData = await gasRes.json();
          if (gasData.ok && gasData.spreadsheetUrl) {
            await dbQuery(
              "UPDATE recruitment_candidates SET spreadsheet_url=$1, spreadsheet_id=$2, status='sent', sent_at=now(), error_message=NULL WHERE id=$3",
              [gasData.spreadsheetUrl, gasData.spreadsheetId || null, c.id]
            );
            results.push({ id: c.id, ok: true });
          } else {
            const msg = gasData.error || 'GASエラー';
            await dbQuery("UPDATE recruitment_candidates SET status='error', error_message=$1 WHERE id=$2", [msg, c.id]);
            results.push({ id: c.id, ok: false, error: msg });
          }
        } catch (err) {
          await dbQuery("UPDATE recruitment_candidates SET status='error', error_message=$1 WHERE id=$2", [err.message, c.id]);
          results.push({ id: c.id, ok: false, error: err.message });
        }
      }
      res.json({ ok: true, sent: results.filter(r => r.ok).length, results });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 1名ずつ送信（フロントがループして呼ぶ用）
  expressApp.post("/api/dashboard/admin/recruitment/send-one", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { candidateId } = req.body;
      if (!candidateId) return res.status(400).json({ error: "candidateId required" });

      const settingsRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      const settings = settingsRes.rows[0];
      if (!settings?.gas_endpoint_url || !settings?.template_spreadsheet_id)
        return res.status(400).json({ error: "設定が不完全です" });

      const candRes = await dbQuery("SELECT * FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [candidateId, teamId]);
      const c = candRes.rows[0];
      if (!c) return res.status(404).json({ error: "not found" });

      const baseUrl = process.env.APP_BASE_URL || 'https://inrevo-task.com';
      const webhookUrl = `${baseUrl}/api/dashboard/recruitment/webhook/complete`;

      try {
        const gasRes = await fetch(settings.gas_endpoint_url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            candidateId: c.id, name: c.name, email: c.email,
            templateId: settings.template_spreadsheet_id,
            webhookUrl, secret: settings.webhook_secret || '',
            fromEmail: settings.from_email || null,
            emailSubject: settings.email_subject || null,
            emailBody: settings.email_body || null,
          }),
        });
        const gasData = await gasRes.json();
        if (gasData.ok && gasData.spreadsheetUrl) {
          await dbQuery(
            "UPDATE recruitment_candidates SET spreadsheet_url=$1, spreadsheet_id=$2, status='sent', sent_at=now(), error_message=NULL WHERE id=$3",
            [gasData.spreadsheetUrl, gasData.spreadsheetId || null, c.id]
          );
          return res.json({ ok: true, candidateId, spreadsheetUrl: gasData.spreadsheetUrl });
        } else {
          const msg = gasData.error || 'GASエラー';
          await dbQuery("UPDATE recruitment_candidates SET status='error', error_message=$1 WHERE id=$2", [msg, c.id]);
          return res.json({ ok: false, candidateId, error: msg });
        }
      } catch (err) {
        await dbQuery("UPDATE recruitment_candidates SET status='error', error_message=$1 WHERE id=$2", [err.message, c.id]);
        return res.json({ ok: false, candidateId, error: err.message });
      }
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // GASからのwebhook（テスト完了・採点結果受信）
  expressApp.post("/api/dashboard/recruitment/webhook/complete", async (req, res) => {
    try {
      const { candidateId, secret, score, scoreDetail } = req.body;
      if (!candidateId) return res.status(400).json({ error: "candidateId required" });

      // 候補者を取得してチームIDとシークレット検証
      const candRes = await dbQuery("SELECT c.*, s.webhook_secret, s.notify_channel_id, s.notify_mention_user_id FROM recruitment_candidates c LEFT JOIN recruitment_settings s ON c.team_id=s.team_id WHERE c.id=$1", [candidateId]);
      const cand = candRes.rows[0];
      if (!cand) return res.status(404).json({ error: "not found" });
      if (cand.webhook_secret && cand.webhook_secret !== secret) return res.status(403).json({ error: "invalid secret" });

      // スコア更新・完了
      const typingLevel = scoreDetail?.typing_level || scoreDetail?.q13_level || null;
      await dbQuery(
        "UPDATE recruitment_candidates SET status='completed', score=$1, score_detail=$2, typing_level=$3, completed_at=now() WHERE id=$4",
        [score ?? null, scoreDetail ? JSON.stringify(scoreDetail) : null, typingLevel, candidateId]
      );

      // Slack 通知
      if (cand.notify_channel_id) {
        try {
          const mention = cand.notify_mention_user_id ? `<@${cand.notify_mention_user_id}> ` : '';
          const text = `${mention}【採用テスト完了】*${cand.name}* さんが実技テストを完了しました\nスコア: *${score ?? '未採点'}点*\n<${cand.spreadsheet_url}|スプレッドシートを確認>`;
          const { WebClient } = require("@slack/web-api");
          const slack = new WebClient(process.env.SLACK_BOT_TOKEN);
          await slack.chat.postMessage({ channel: cand.notify_channel_id, text });
        } catch (slackErr) { console.error("Slack通知エラー:", slackErr.message); }
      }

      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // --- /members (team-scoped) ---
  expressApp.get("/api/dashboard/members", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      let scopeWhere = "";
      let params = [teamId];

      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        scopeWhere = "AND assignee_id = ANY($2)";
        params.push(visible);
      }

      const q = `
        SELECT assignee_id,
               COUNT(*)::int AS total,
               COUNT(*) FILTER (WHERE status = 'in_progress')::int AS in_progress,
               COUNT(*) FILTER (WHERE status = 'done')::int AS done,
               COUNT(*) FILTER (WHERE due_date < CURRENT_DATE AND status NOT IN ('done','cancelled'))::int AS overdue
        FROM tasks
        WHERE team_id = $1
          AND assignee_id IS NOT NULL
          AND (task_type IS NULL OR task_type = 'personal')
          ${scopeWhere}
        GROUP BY assignee_id
        ORDER BY total DESC
        LIMIT 100;
      `;
      const result = await dbQuery(q, params);

      const members = await Promise.all(
        result.rows.map(async (row) => ({
          ...row,
          displayName: await getUserDisplayName(teamId, row.assignee_id),
        })),
      );

      res.json({ members });
    } catch (e) {
      console.error("dashboard /members error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /overdue (team-scoped) ---
  expressApp.get("/api/dashboard/overdue", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      let scopeWhere = "";
      let params = [teamId];

      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        scopeWhere = "AND t.assignee_id = ANY($2)";
        params.push(visible);
      }

      const q = `
        SELECT t.id, t.title, t.status, t.due_date, t.assignee_id, t.assignee_label,
               t.requester_user_id, t.task_type, t.created_at
        FROM tasks t
        WHERE t.team_id = $1
          AND t.due_date < CURRENT_DATE
          AND t.status NOT IN ('done', 'cancelled')
          ${scopeWhere}
        ORDER BY t.due_date ASC, t.created_at ASC
        LIMIT 200;
      `;
      const result = await dbQuery(q, params);
      res.json({ tasks: result.rows, total: result.rows.length });
    } catch (e) {
      console.error("dashboard /overdue error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // ================================
  // Admin APIs
  // ================================

  // --- Roles ---
  expressApp.get("/api/dashboard/admin/roles", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT user_id, role FROM dashboard_roles WHERE team_id=$1 AND role IN ('admin','corp') ORDER BY role, updated_at`,
        [teamId]
      );
      const result = await Promise.all(
        rows.map(async (a) => ({
          ...a,
          displayName: await getUserDisplayName(teamId, a.user_id),
        })),
      );
      res.json({ admins: result.filter(r => r.role === 'admin'), corps: result.filter(r => r.role === 'corp') });
    } catch (e) {
      console.error("dashboard /admin/roles error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/roles", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { userId, role } = req.body || {};
      if (!userId || !["admin", "corp", "member"].includes(role)) {
        return res.status(400).json({ error: "invalid_params" });
      }
      await dbSetDashboardRole(teamId, userId, role);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /admin/roles error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.get("/api/dashboard/admin/visibility/:userId", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const viewerUserId = String(req.params.userId || "").trim();
      if (!viewerUserId) return res.status(400).json({ error: "userId_required" });
      const config = await getVisibilityConfig(teamId, viewerUserId);
      res.json(config);
    } catch (e) {
      console.error("dashboard GET /admin/visibility error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.put("/api/dashboard/admin/visibility/:userId", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const viewerUserId = String(req.params.userId || "").trim();
      const visibleUserIds = Array.isArray(req.body?.visibleUserIds) ? req.body.visibleUserIds : [];
      const visibleDashTeamIds = Array.isArray(req.body?.visibleDashTeamIds) ? req.body.visibleDashTeamIds : [];
      if (!viewerUserId) return res.status(400).json({ error: "userId_required" });
      await Promise.all([
        dbReplaceDashboardVisibleUsers(teamId, viewerUserId, visibleUserIds),
        dbReplaceDashboardVisibleTeams(teamId, viewerUserId, visibleDashTeamIds),
      ]);
      const config = await getVisibilityConfig(teamId, viewerUserId);
      res.json({ ok: true, ...config });
    } catch (e) {
      console.error("dashboard PUT /admin/visibility error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Teams ---
  expressApp.get("/api/dashboard/admin/teams", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const teams = await dbListDashTeams(teamId);
      // メンバー数も返す
      const result = await Promise.all(
        teams.map(async (t) => {
          const members = await dbListDashTeamMembers(teamId, t.id);
          return { ...t, memberCount: members.length };
        }),
      );
      res.json({ teams: result });
    } catch (e) {
      console.error("dashboard /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/teams", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { name, parentId, show_in_orgchart } = req.body || {};
      if (!name?.trim()) return res.status(400).json({ error: "name_required" });
      const showInOrgchart = show_in_orgchart !== false;
      const team = await dbCreateDashTeam(randomUUID(), teamId, name.trim(), userId, parentId || null, showInOrgchart);
      res.json({ team });
    } catch (e) {
      console.error("dashboard POST /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.put("/api/dashboard/admin/teams/:id", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, parentId } = req.body || {};
      if (name !== undefined && !name?.trim()) return res.status(400).json({ error: "name_required" });
      await dbUpdateDashTeamFull(teamId, req.params.id, {
        ...(name !== undefined ? { name: name.trim() } : {}),
        ...(parentId !== undefined ? { parentId } : {}),
      });
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard PUT /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/admin/teams/:id", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbDeleteDashTeam(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Team members ---
  expressApp.get("/api/dashboard/admin/teams/:id/members", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const members = await dbListDashTeamMembers(teamId, req.params.id);
      const result = await Promise.all(
        members.map(async (m) => ({
          ...m,
          displayName: await getUserDisplayName(teamId, m.user_id),
        })),
      );
      res.json({ members: result });
    } catch (e) {
      console.error("dashboard /admin/teams/:id/members error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/teams/:id/members", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { userId } = req.body || {};
      if (!userId) return res.status(400).json({ error: "userId_required" });
      await dbAddDashTeamMember(teamId, req.params.id, userId);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /admin/teams/:id/members error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.patch("/api/dashboard/admin/teams/:id/members/:userId", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { role } = req.body || {};
      if (!role) return res.status(400).json({ error: "role_required" });
      await dbUpdateDashTeamMemberRole(teamId, req.params.id, req.params.userId, role);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard PATCH team member role error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // Hide a user from org chart (set is_active=false in directory)
  expressApp.delete("/api/dashboard/admin/directory/:userId", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbSetUserDirectoryActive(teamId, req.params.userId, false);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE directory user error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/admin/teams/:id/members/:userId", authWithRole, chiefOrAbove, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbRemoveDashTeamMember(teamId, req.params.id, req.params.userId);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE team member error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Org chart (accessible to all logged-in users) ---
  expressApp.get("/api/dashboard/org-chart", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const teams = await dbListDashTeams(teamId);
      const membersMap = {};
      await Promise.all(
        teams.map(async (t) => {
          membersMap[t.id] = await dbListDashTeamMembersWithProfile(teamId, t.id);
        }),
      );
      // Users in the Slack workspace who are not in any dash_team
      const allUsers = await dbListDashboardUserDirectory(teamId, { limit: 1000 });
      const assignedIds = new Set(
        Object.values(membersMap).flatMap((ms) => ms.map((m) => m.user_id)),
      );
      const unassigned = allUsers
        .filter((u) => u.is_active && !assignedIds.has(u.user_id))
        .map((u) => ({
          user_id: u.user_id,
          display_name: u.display_name,
          real_name: u.real_name,
          title: u.profile_json?.title || null,
          avatar_url: u.profile_json?.image_72 || null,
        }));
      res.json({ teams, membersMap, unassigned });
    } catch (e) {
      console.error("dashboard /org-chart error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Admin: user mapping ---
  expressApp.get("/api/dashboard/admin/user-mapping", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const query = String(req.query.q || "");
      const members = await dbListDashboardUserDirectory(teamId, { query, limit: 300 });
      res.json({ members });
    } catch (e) {
      console.error("dashboard /admin/user-mapping error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/user-mapping/sync", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const synced = await syncDashboardUserDirectory(teamId);
      res.json({ ok: true, synced });
    } catch (e) {
      console.error("dashboard POST /admin/user-mapping/sync error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Projects ---
  expressApp.get("/api/dashboard/admin/projects", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const projects = await dbListProjects(teamId);
      res.json({ projects });
    } catch (e) {
      console.error("dashboard /admin/projects error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/projects", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { name, dashTeamId } = req.body || {};
      if (!name?.trim()) return res.status(400).json({ error: "name_required" });
      const project = await dbCreateProject(randomUUID(), teamId, name.trim(), dashTeamId || null, userId);
      res.json({ project });
    } catch (e) {
      console.error("dashboard POST /admin/projects error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.put("/api/dashboard/admin/projects/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, dashTeamId } = req.body || {};
      const project = await dbUpdateProject(teamId, req.params.id, { name, dash_team_id: dashTeamId });
      res.json({ project });
    } catch (e) {
      console.error("dashboard PUT /admin/projects error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/admin/projects/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbDeleteProject(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE /admin/projects error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Project tasks ---
  expressApp.get("/api/dashboard/projects/:id/tasks", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const tasks = await dbListProjectTasks(teamId, req.params.id);
      res.json({ tasks });
    } catch (e) {
      console.error("dashboard /projects/:id/tasks error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/projects/:id/tasks", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { taskId } = req.body || {};
      if (!taskId) return res.status(400).json({ error: "taskId_required" });
      await dbAddProjectTask(teamId, req.params.id, taskId);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /admin/projects/:id/tasks error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/admin/projects/:id/tasks/:taskId", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbRemoveProjectTask(teamId, req.params.id, req.params.taskId);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE project task error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /teams (non-admin: list own teams) ---
  expressApp.get("/api/dashboard/my-teams", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const teams = await dbGetUserDashTeams(teamId, userId);
      res.json({ teams });
    } catch (e) {
      console.error("dashboard /my-teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Workload gantt ---
  expressApp.get("/api/dashboard/workload/teams", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      if (role === "admin") {
        const teams = await dbListDashTeams(teamId);
        res.json({ teams: teams.map(t => ({ ...t, is_direct_member: true })) });
        return;
      }

      // 非admin: 自分が直接メンバーのチームのみ + その親部署（表示用）を返す
      // ※ 同じ部署内の他チームは含めない
      const directRes = await dbQuery(
        `SELECT DISTINCT dash_team_id FROM dash_team_members WHERE team_id=$1 AND user_id=$2`,
        [teamId, userId]
      );
      const directIds = Array.from(new Set(directRes.rows.map(r => r.dash_team_id)));

      if (!directIds.length) { res.json({ teams: [] }); return; }

      const directTeams = (await dbQuery(
        `SELECT * FROM dash_teams WHERE team_id=$1 AND id = ANY($2) ORDER BY name ASC`,
        [teamId, directIds]
      )).rows;

      // 親部署（parent_id を持つチームの親）を追加
      const parentIds = [...new Set(
        directTeams.filter(t => t.parent_id).map(t => t.parent_id)
          .filter(pid => !directIds.includes(pid))
      )];
      const parentRows = parentIds.length > 0
        ? (await dbQuery(`SELECT * FROM dash_teams WHERE team_id=$1 AND id = ANY($2)`, [teamId, parentIds])).rows
        : [];

      const allTeams = [
        ...directTeams.map(t => ({ ...t, is_direct_member: true })),
        ...parentRows.map(t => ({ ...t, is_direct_member: false })),
      ];

      res.json({ teams: allTeams });
    } catch (e) {
      console.error("dashboard /workload/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.get("/api/dashboard/workload/users", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const dashTeamId = String(req.query.teamId || "");
      if (!(await canAccessDashTeam(teamId, userId, role, dashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      const members = await dbListDashTeamMembers(teamId, dashTeamId);
      const hydrated = await Promise.all(
        members.map(async (member) => {
          const directory = await dbGetDashboardDirectoryMember(teamId, member.user_id);
          const fallbackName = await getUserDisplayName(teamId, member.user_id);
          return {
            user_id: member.user_id,
            added_at: member.added_at,
            display_name: directory?.display_name || fallbackName,
            real_name: directory?.real_name || fallbackName,
          };
        }),
      );
      res.json({ members: hydrated });
    } catch (e) {
      console.error("dashboard /workload/users error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.get("/api/dashboard/workload", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const dashTeamId = String(req.query.teamId || "");
      const monthKey = parseMonthKey(req.query.month) || new Date().toISOString().slice(0, 7);
      if (!(await canAccessDashTeam(teamId, userId, role, dashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      const [items, cells] = await Promise.all([
        dbListWorkloadItems(teamId, dashTeamId),
        dbListWorkloadCells(teamId, dashTeamId, monthKey),
      ]);
      res.json({ items, cells, monthKey });
    } catch (e) {
      console.error("dashboard /workload error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/workload/items", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const { dashTeamId, ownerUserId, title, category, notes, sortOrder, color, recurrenceType, recurrenceConfig, estimatedHours } = req.body || {};
      if (!title?.trim() || !dashTeamId || !ownerUserId) {
        return res.status(400).json({ error: "invalid_params" });
      }
      if (!(await canAccessDashTeam(teamId, userId, role, dashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      const item = await dbCreateWorkloadItem(teamId, {
        id: randomUUID(),
        dashTeamId,
        ownerUserId,
        title: title.trim(),
        category: category?.trim() || null,
        notes: notes?.trim() || null,
        sortOrder: Number.isFinite(Number(sortOrder)) ? Number(sortOrder) : 0,
        createdBy: userId,
        color: color || null,
        recurrenceType: recurrenceType || 'other',
        recurrenceConfig: recurrenceConfig || null,
        estimatedHours: estimatedHours != null ? Number(estimatedHours) : null,
      });
      res.json({ item });
    } catch (e) {
      console.error("dashboard POST /workload/items error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.patch("/api/dashboard/workload/items/:id", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const existing = await dbGetWorkloadItem(teamId, req.params.id);
      if (!existing) return res.status(404).json({ error: "not_found" });
      const { dashTeamId, ownerUserId, title, category, notes, sortOrder, color, recurrenceType, recurrenceConfig, dueDate, statusMemo, isDone, estimatedHours } = req.body || {};
      const targetDashTeamId = String(dashTeamId || existing.dash_team_id || "");
      if (!(await canAccessDashTeam(teamId, userId, role, targetDashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      const item = await dbUpdateWorkloadItem(teamId, req.params.id, {
        dash_team_id: targetDashTeamId,
        owner_user_id: ownerUserId !== undefined ? (ownerUserId || null) : existing.owner_user_id,
        title: typeof title === "string" ? title.trim() : existing.title,
        category: typeof category === "string" ? category.trim() : existing.category,
        notes: notes !== undefined ? (typeof notes === "string" ? notes.trim() || null : null) : existing.notes,
        sort_order: Number.isFinite(Number(sortOrder)) ? Number(sortOrder) : existing.sort_order,
        color: color !== undefined ? (color || null) : existing.color,
        recurrence_type: recurrenceType !== undefined ? recurrenceType : (existing.recurrence_type || 'other'),
        recurrence_config: recurrenceConfig !== undefined ? recurrenceConfig : existing.recurrence_config,
        due_date: dueDate !== undefined ? (dueDate || null) : existing.due_date,
        status_memo: statusMemo !== undefined ? (statusMemo || null) : existing.status_memo,
        is_done: isDone !== undefined ? !!isDone : existing.is_done,
        estimated_hours: estimatedHours !== undefined ? (estimatedHours != null ? Number(estimatedHours) : null) : existing.estimated_hours,
      });
      res.json({ item });
    } catch (e) {
      console.error("dashboard PATCH /workload/items/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/workload/items/:id", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const item = await dbGetWorkloadItem(teamId, req.params.id);
      if (!item) return res.status(404).json({ error: "not_found" });
      if (!(await canAccessDashTeam(teamId, userId, role, item.dash_team_id))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      await dbDeleteWorkloadItem(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE /workload/items/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // GET /workload/hours — 工数管理集計（人別・業務別）
  expressApp.get("/api/dashboard/workload/hours", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const dashTeamId = req.query.dashTeamId;
      const from = req.query.from; // YYYY-MM-DD
      const to   = req.query.to;   // YYYY-MM-DD
      if (!dashTeamId) return res.status(400).json({ error: "dashTeamId required" });
      if (!(await canAccessDashTeam(teamId, userId, role, dashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      // 工数あり業務のみ取得
      const { rows } = await dbQuery(`
        SELECT wi.id, wi.title, wi.category, wi.owner_user_id, wi.estimated_hours,
               wi.recurrence_type, wi.color,
               COUNT(DISTINCT wc.day_num || '-' || wc.month_key) AS active_days
        FROM workload_items wi
        LEFT JOIN workload_cells wc ON wc.item_id = wi.id AND wc.team_id = wi.team_id
          AND wc.intensity > 0
          ${from && to ? `AND (wc.month_key || '-' || LPAD(wc.day_num::text,2,'0')) BETWEEN $3 AND $4` : ''}
        WHERE wi.team_id = $1 AND wi.dash_team_id = $2
          AND wi.is_archived = false AND wi.estimated_hours IS NOT NULL
        GROUP BY wi.id
        ORDER BY wi.owner_user_id, wi.sort_order
      `, from && to ? [teamId, dashTeamId, from, to] : [teamId, dashTeamId]);

      // オーナー名を解決
      const userIds = [...new Set(rows.map(r => r.owner_user_id).filter(Boolean))];
      const nameMap = {};
      await Promise.all(userIds.map(async uid => {
        nameMap[uid] = await getUserDisplayName(teamId, uid);
      }));

      const items = rows.map(r => ({
        ...r,
        estimated_hours: r.estimated_hours != null ? Number(r.estimated_hours) : null,
        active_days: Number(r.active_days) || 0,
        daily_hours: r.estimated_hours && Number(r.active_days) > 0
          ? Math.round((Number(r.estimated_hours) / Number(r.active_days)) * 10) / 10
          : null,
        owner_name: nameMap[r.owner_user_id] || r.owner_user_id,
      }));

      res.json({ items });
    } catch (e) {
      console.error("dashboard GET /workload/hours error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.put("/api/dashboard/workload/cells", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const { itemId, monthKey, cells } = req.body || {};
      const parsedMonth = parseMonthKey(monthKey);
      if (!itemId || !parsedMonth || !Array.isArray(cells)) {
        return res.status(400).json({ error: "invalid_params" });
      }
      const item = await dbGetWorkloadItem(teamId, itemId);
      if (!item) return res.status(404).json({ error: "not_found" });
      if (!(await canAccessDashTeam(teamId, userId, role, item.dash_team_id))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      await dbSetWorkloadCells(teamId, itemId, parsedMonth, cells);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard PUT /workload/cells error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/workload/copy-prev", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const { dashTeamId, monthKey } = req.body || {};
      const parsedMonth = parseMonthKey(monthKey);
      if (!dashTeamId || !parsedMonth) {
        return res.status(400).json({ error: "invalid_params" });
      }
      if (!(await canAccessDashTeam(teamId, userId, role, dashTeamId))) {
        return res.status(403).json({ error: "team_forbidden" });
      }
      const [year, month] = parsedMonth.split("-").map(Number);
      const prev = new Date(Date.UTC(year, month - 2, 1));
      const prevMonthKey = `${prev.getUTCFullYear()}-${String(prev.getUTCMonth() + 1).padStart(2, "0")}`;
      await dbCopyWorkloadMonth(teamId, dashTeamId, prevMonthKey, parsedMonth);
      res.json({ ok: true, fromMonthKey: prevMonthKey, monthKey: parsedMonth });
    } catch (e) {
      console.error("dashboard POST /workload/copy-prev error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /workload/categories ---
  expressApp.get("/api/dashboard/workload/categories", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { dashTeamId } = req.query;
      if (!dashTeamId) return res.status(400).json({ error: "dashTeamId required" });
      const categories = await dbListWorkloadCategories(teamId, dashTeamId);
      res.json({ categories });
    } catch (e) {
      console.error("dashboard GET /workload/categories error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/workload/categories", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { dashTeamId, name, color } = req.body;
      if (!dashTeamId || !name) return res.status(400).json({ error: "dashTeamId and name required" });
      const category = await dbUpsertWorkloadCategory(teamId, { id: randomUUID(), dashTeamId, name, color: color || "#6366f1" });
      res.json({ category });
    } catch (e) {
      console.error("dashboard POST /workload/categories error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.patch("/api/dashboard/workload/categories/:id", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, color } = req.body;
      const category = await dbUpdateWorkloadCategory(teamId, req.params.id, { name, color });
      if (!category) return res.status(404).json({ error: "not_found" });
      res.json({ category });
    } catch (e) {
      console.error("dashboard PATCH /workload/categories/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/workload/categories/:id", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbDeleteWorkloadCategory(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE /workload/categories/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- /projects (non-admin can list all for filtering) ---
  expressApp.get("/api/dashboard/projects", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const projects = await dbListProjects(teamId);
      res.json({ projects });
    } catch (e) {
      console.error("dashboard /projects error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // ================================
  // Task CRUD (uses same DB functions as Slack app)
  // ================================

  // --- GET /tasks/:id (detail) ---
  expressApp.get("/api/dashboard/tasks/:id", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });

      // Resolve display names
      const [requesterName, assigneeName, comments, targetUserIds] = await Promise.all([
        task.requester_user_id ? getUserDisplayName(teamId, task.requester_user_id) : Promise.resolve(null),
        task.assignee_id ? getUserDisplayName(teamId, task.assignee_id) : Promise.resolve(null),
        dbListTaskComments(teamId, req.params.id, 100),
        task.task_type === 'broadcast' ? dbListTargetUserIds(teamId, req.params.id) : Promise.resolve([]),
      ]);

      // Resolve comment author names
      const commentsWithNames = await Promise.all(
        comments.map(async (c) => ({
          ...c,
          displayName: await getUserDisplayName(teamId, c.user_id),
        })),
      );

      // Resolve broadcast target display names
      const targets = await Promise.all(
        targetUserIds.map(async (uid) => ({
          user_id: uid,
          displayName: await getUserDisplayName(teamId, uid),
        })),
      );

      res.json({
        task: {
          ...task,
          requesterDisplayName: requesterName,
          assigneeDisplayName: assigneeName,
          targets: task.task_type === 'broadcast' ? targets : [],
        },
        comments: commentsWithNames,
      });
    } catch (e) {
      console.error("dashboard GET /tasks/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /tasks/:id/thread (Slack thread messages) ---
  expressApp.get("/api/dashboard/tasks/:id/thread", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });
      if (!task.channel_id || !task.message_ts) return res.json({ messages: [] });

      const result = await slackClient.conversations.replies({
        channel: task.channel_id,
        ts: task.message_ts,
        limit: 100,
      });

      const messages = await Promise.all(
        (result.messages || []).map(async (m) => ({
          ts: m.ts,
          user_id: m.user || null,
          displayName: m.user ? await getUserDisplayName(teamId, m.user) : 'Bot',
          text: m.text || '',
          is_root: m.ts === task.message_ts,
        }))
      );

      res.json({ messages });
    } catch (e) {
      console.error("dashboard GET /tasks/:id/thread error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- PUT /tasks/:id (update fields) ---
  expressApp.put("/api/dashboard/tasks/:id", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });

      const { title, description, due_date, assignee_id, status } = req.body || {};

      // ステータス変更
      if (status && status !== task.status) {
        await dbUpdateStatus(teamId, req.params.id, status);
      }

      // フィールド更新（title は description カラムに保存 — 既存DBの構造に合わせる）
      const patch = {};
      if (title !== undefined) patch.description = title; // tasks.description = 本文
      if (description !== undefined) patch.description = description;
      if (due_date !== undefined) patch.due_date = due_date || null;
      if (assignee_id !== undefined) {
        patch.assignee_id = assignee_id || null;
        patch.task_type = task.task_type;
      }

      // title カラムは別途更新
      if (title !== undefined && title !== task.title) {
        await dbQuery(
          `UPDATE tasks SET title=$3, updated_at=now() WHERE team_id=$1 AND id=$2`,
          [teamId, req.params.id, title],
        );
      }

      if (Object.keys(patch).length > 0) {
        await dbUpdateTaskEditableFields(teamId, req.params.id, {
          task_type: task.task_type,
          assignee_id: patch.assignee_id ?? task.assignee_id,
          assignee_label: task.assignee_label,
          assignee_dept: task.assignee_dept,
          due_date: patch.due_date !== undefined ? patch.due_date : task.due_date,
          description: patch.description !== undefined ? patch.description : task.description,
          broadcast_group_handle: task.broadcast_group_handle,
          broadcast_group_id: task.broadcast_group_id,
          total_count: task.total_count,
          completed_count: task.completed_count,
        });
      }

      const updated = await dbGetTaskById(teamId, req.params.id);
      // Web → Slack sync
      if (updated?.channel_id && updated?.message_ts) {
        try {
          const blocks = await buildThreadCardBlocks({ teamId, task: updated });
          await upsertThreadCard(slackClient, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
        } catch (e) {
          console.error("Slack sync error (PUT /tasks/:id):", e);
        }
      }
      res.json({ task: updated });
    } catch (e) {
      console.error("dashboard PUT /tasks/:id error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- PATCH /tasks/:id/status (quick status change) ---
  expressApp.patch("/api/dashboard/tasks/:id/status", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { status } = req.body || {};
      if (!status || !["in_progress", "done", "cancelled", "pending"].includes(status)) {
        return res.status(400).json({ error: "invalid_status" });
      }
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });

      const updated = await dbUpdateStatus(teamId, req.params.id, status);
      // Web → Slack sync
      if (updated?.channel_id && updated?.message_ts) {
        try {
          const blocks = await buildThreadCardBlocks({ teamId, task: updated });
          await upsertThreadCard(slackClient, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
        } catch (e) {
          console.error("Slack sync error (PATCH /tasks/:id/status):", e);
        }
      }
      res.json({ task: updated });
    } catch (e) {
      console.error("dashboard PATCH /tasks/:id/status error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /tasks/:id/complete-self (broadcast タスクを自分のみ完了) ---
  expressApp.post("/api/dashboard/tasks/:id/complete-self", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      await dbQuery(
        `INSERT INTO task_completions (task_id, team_id, user_id) VALUES ($1::uuid, $2, $3) ON CONFLICT DO NOTHING`,
        [req.params.id, teamId, userId]
      );
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /tasks/:id/complete-self error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /tasks/:id/comments ---
  expressApp.post("/api/dashboard/tasks/:id/comments", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { comment } = req.body || {};
      if (!comment?.trim()) return res.status(400).json({ error: "comment_required" });

      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });

      await dbInsertTaskComment(teamId, req.params.id, userId, comment.trim());

      const comments = await dbListTaskComments(teamId, req.params.id, 100);
      const commentsWithNames = await Promise.all(
        comments.map(async (c) => ({
          ...c,
          displayName: await getUserDisplayName(teamId, c.user_id),
        })),
      );
      res.json({ comments: commentsWithNames });
    } catch (e) {
      console.error("dashboard POST /tasks/:id/comments error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /tasks (create new task from web) ---
  expressApp.post("/api/dashboard/tasks", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { title, description, assignee_id, due_date, project_id } = req.body || {};
      if (!title?.trim()) return res.status(400).json({ error: "title_required" });

      const taskId = randomUUID();
      const created = await dbCreateTask({
        id: taskId,
        team_id: teamId,
        channel_id: null,
        message_ts: null,
        source_permalink: null,
        title: title.trim(),
        description: (description || title).trim(),
        requester_user_id: userId,
        created_by_user_id: userId,
        assignee_id: assignee_id || userId,
        assignee_label: null,
        status: "in_progress",
        due_date: due_date || null,
        requester_dept: null,
        assignee_dept: null,
        task_type: "personal",
        broadcast_group_handle: null,
        broadcast_group_id: null,
        total_count: null,
        completed_count: 0,
        notified_at: null,
      });

      // プロジェクト紐付け
      if (project_id) {
        await dbAddProjectTask(teamId, project_id, taskId);
      }

      res.json({ task: created });
    } catch (e) {
      console.error("dashboard POST /tasks error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // ================================
  // Analytics API
  // ================================

  // --- GET /analytics/member-completion ---
  // メンバー別完了率
  expressApp.get("/api/dashboard/analytics/member-completion", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const filterAssignee = req.query.assignee || null;
      const filterUsergroup = req.query.usergroup || null;
      const params = [teamId];
      const extraConds = [];
      let idx = 2;
      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        extraConds.push(`assignee_id = ANY($${idx++})`);
        params.push(visible);
      }
      if (filterAssignee) {
        extraConds.push(`assignee_id = $${idx++}`);
        params.push(filterAssignee);
      }
      if (filterUsergroup) {
        const members = await getUsergroupMembers(teamId, filterUsergroup);
        if (members.length > 0) {
          extraConds.push(`assignee_id = ANY($${idx++})`);
          params.push(members);
        } else {
          extraConds.push("false");
        }
      }
      const scopeWhere = extraConds.length ? "AND " + extraConds.join(" AND ") : "";

      const q = `
        SELECT assignee_id,
               COUNT(*)::int AS total,
               COUNT(*) FILTER (WHERE status = 'done')::int AS done,
               COUNT(*) FILTER (WHERE status = 'in_progress')::int AS in_progress,
               COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
               COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
               COUNT(*) FILTER (WHERE due_date < CURRENT_DATE AND status NOT IN ('done','cancelled'))::int AS overdue
        FROM tasks
        WHERE team_id = $1
          AND assignee_id IS NOT NULL
          ${scopeWhere}
        GROUP BY assignee_id
        ORDER BY total DESC;
      `;
      const result = await dbQuery(q, params);
      const members = await Promise.all(
        result.rows.map(async (r) => ({
          ...r,
          displayName: await getUserDisplayName(teamId, r.assignee_id),
          completion_rate: r.total > 0 ? Math.round((r.done / r.total) * 100) : 0,
        })),
      );
      res.json({ members });
    } catch (e) {
      console.error("analytics member-completion error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /analytics/due-compliance ---
  // 期限遵守率の推移（週別、過去12週）
  expressApp.get("/api/dashboard/analytics/due-compliance", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const filterAssignee = req.query.assignee || null;
      const filterUsergroup = req.query.usergroup || null;
      const params = [teamId];
      const extraConds = [];
      let idx = 2;
      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        extraConds.push(`assignee_id = ANY($${idx++})`);
        params.push(visible);
      }
      if (filterAssignee) {
        extraConds.push(`assignee_id = $${idx++}`);
        params.push(filterAssignee);
      }
      if (filterUsergroup) {
        const members = await getUsergroupMembers(teamId, filterUsergroup);
        if (members.length > 0) {
          extraConds.push(`assignee_id = ANY($${idx++})`);
          params.push(members);
        } else {
          extraConds.push("false");
        }
      }
      const scopeWhere = extraConds.length ? "AND " + extraConds.join(" AND ") : "";

      // 期限付きで完了したタスクの期限遵守を週別に集計
      const q = `
        SELECT
          date_trunc('week', updated_at)::date AS week_start,
          COUNT(*) FILTER (WHERE status = 'done')::int AS completed,
          COUNT(*) FILTER (WHERE status = 'done' AND due_date IS NOT NULL AND updated_at::date <= due_date)::int AS on_time,
          COUNT(*) FILTER (WHERE status = 'done' AND due_date IS NOT NULL AND updated_at::date > due_date)::int AS late,
          COUNT(*) FILTER (WHERE status = 'done' AND due_date IS NOT NULL)::int AS with_due
        FROM tasks
        WHERE team_id = $1
          AND status = 'done'
          AND updated_at >= CURRENT_DATE - INTERVAL '12 weeks'
          ${scopeWhere}
        GROUP BY week_start
        ORDER BY week_start ASC;
      `;
      const result = await dbQuery(q, params);
      const weeks = result.rows.map((r) => ({
        ...r,
        compliance_rate: r.with_due > 0 ? Math.round((r.on_time / r.with_due) * 100) : null,
      }));
      res.json({ weeks });
    } catch (e) {
      console.error("analytics due-compliance error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /analytics/project-progress ---
  // プロジェクト別の進捗状況
  expressApp.get("/api/dashboard/analytics/project-progress", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;
      const params = [teamId];
      let scopeWhere = "";
      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        scopeWhere = "AND t.assignee_id = ANY($2)";
        params.push(visible);
      }

      const q = `
        SELECT
          p.id AS project_id,
          p.name AS project_name,
          COUNT(t.id)::int AS total,
          COUNT(t.id) FILTER (WHERE t.status = 'done')::int AS done,
          COUNT(t.id) FILTER (WHERE t.status = 'in_progress')::int AS in_progress,
          COUNT(t.id) FILTER (WHERE t.status = 'cancelled')::int AS cancelled,
          COUNT(t.id) FILTER (WHERE t.status = 'pending')::int AS pending,
          COUNT(t.id) FILTER (WHERE t.due_date < CURRENT_DATE AND t.status NOT IN ('done','cancelled'))::int AS overdue
        FROM projects p
        LEFT JOIN project_tasks pt ON pt.project_id = p.id AND pt.team_id = p.team_id
        LEFT JOIN tasks t ON t.id = pt.task_id AND t.team_id = pt.team_id
        WHERE p.team_id = $1
          ${scopeWhere}
        GROUP BY p.id, p.name
        ORDER BY p.name ASC;
      `;
      const result = await dbQuery(q, params);
      const projects = result.rows.map((r) => ({
        ...r,
        progress_rate: r.total > 0 ? Math.round((r.done / r.total) * 100) : 0,
      }));
      res.json({ projects });
    } catch (e) {
      console.error("analytics project-progress error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // ================================
  // Integrations API (admin only)
  // ================================

  // ローカルフィールド一覧（マッピングUI用）
  expressApp.get("/api/dashboard/integrations/local-fields", authWithRole, adminOnly, (req, res) => {
    res.json({ fields: kintone.getLocalFieldOptions() });
  });

  // --- GET /integrations ---
  expressApp.get("/api/dashboard/integrations", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const integrations = await dbListIntegrations(teamId);
      res.json({ integrations });
    } catch (e) {
      console.error("integrations list error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /integrations ---
  expressApp.post("/api/dashboard/integrations", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { service_type, name, config } = req.body || {};
      if (!service_type || !name) return res.status(400).json({ error: "service_type and name required" });

      const integration = await dbCreateIntegration(teamId, {
        service_type,
        name,
        config: config || {},
        created_by: userId,
      });
      res.json({ integration });
    } catch (e) {
      console.error("integrations create error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /integrations/:id ---
  expressApp.get("/api/dashboard/integrations/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const integration = await dbGetIntegration(teamId, req.params.id);
      if (!integration) return res.status(404).json({ error: "not_found" });
      const mappings = await dbListFieldMappings(teamId, req.params.id);
      const logs = await dbListSyncLogs(teamId, req.params.id, 10);
      res.json({ integration, mappings, logs });
    } catch (e) {
      console.error("integrations get error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- PUT /integrations/:id ---
  expressApp.put("/api/dashboard/integrations/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const existing = await dbGetIntegration(teamId, req.params.id);
      if (!existing) return res.status(404).json({ error: "not_found" });

      const { name, config, enabled } = req.body || {};
      const integration = await dbUpdateIntegration(teamId, req.params.id, { name, config, enabled });
      res.json({ integration });
    } catch (e) {
      console.error("integrations update error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- DELETE /integrations/:id ---
  expressApp.delete("/api/dashboard/integrations/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbDeleteIntegration(teamId, req.params.id);
      res.json({ ok: true });
    } catch (e) {
      console.error("integrations delete error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /integrations/:id/test --- テスト接続
  expressApp.post("/api/dashboard/integrations/:id/test", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const integration = await dbGetIntegration(teamId, req.params.id);
      if (!integration) return res.status(404).json({ error: "not_found" });

      if (integration.service_type === "kintone") {
        const result = await kintone.testConnection(integration.config);
        res.json({ ok: true, detail: result });
      } else {
        res.status(400).json({ error: `unsupported service: ${integration.service_type}` });
      }
    } catch (e) {
      console.error("integrations test error:", e);
      res.json({ ok: false, error: e.message });
    }
  });

  // --- GET /integrations/:id/remote-fields --- リモートのフィールド一覧取得
  expressApp.get("/api/dashboard/integrations/:id/remote-fields", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const integration = await dbGetIntegration(teamId, req.params.id);
      if (!integration) return res.status(404).json({ error: "not_found" });

      if (integration.service_type === "kintone") {
        const fields = await kintone.getFields(integration.config);
        res.json({ fields });
      } else {
        res.status(400).json({ error: `unsupported service: ${integration.service_type}` });
      }
    } catch (e) {
      console.error("integrations remote-fields error:", e);
      res.status(500).json({ error: e.message });
    }
  });

  // --- PUT /integrations/:id/mappings --- フィールドマッピング保存
  expressApp.put("/api/dashboard/integrations/:id/mappings", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const integration = await dbGetIntegration(teamId, req.params.id);
      if (!integration) return res.status(404).json({ error: "not_found" });

      const { mappings } = req.body || {};
      if (!Array.isArray(mappings)) return res.status(400).json({ error: "mappings must be array" });

      const saved = await dbSetFieldMappings(teamId, req.params.id, mappings);
      res.json({ mappings: saved });
    } catch (e) {
      console.error("integrations mappings save error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- POST /integrations/:id/sync --- 同期実行
  expressApp.post("/api/dashboard/integrations/:id/sync", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const integration = await dbGetIntegration(teamId, req.params.id);
      if (!integration) return res.status(404).json({ error: "not_found" });
      if (!integration.enabled) return res.status(400).json({ error: "integration_disabled" });

      const mappings = await dbListFieldMappings(teamId, req.params.id);
      if (mappings.length === 0) return res.status(400).json({ error: "no_field_mappings" });

      const { direction = "both" } = req.body || {};
      const logId = await dbCreateSyncLog(teamId, req.params.id, direction);

      // 非同期で同期実行（レスポンスは即返す）
      runSync(teamId, integration, mappings, direction, logId, userId).catch((e) =>
        console.error("sync background error:", e),
      );

      res.json({ ok: true, logId, message: "同期を開始しました" });
    } catch (e) {
      console.error("integrations sync error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /integrations/:id/logs --- 同期ログ
  expressApp.get("/api/dashboard/integrations/:id/logs", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const logs = await dbListSyncLogs(teamId, req.params.id, 20);
      res.json({ logs });
    } catch (e) {
      console.error("integrations logs error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // ================================
  // 同期実行ロジック
  // ================================
  async function runSync(teamId, integration, mappings, direction, logId, userId) {
    let processed = 0, created = 0, updated = 0, failed = 0;
    try {
      if (integration.service_type === "kintone") {
        const config = integration.config;

        // --- kintone → ローカル ---
        if (direction === "both" || direction === "from_remote") {
          const records = await kintone.getRecords(config);
          for (const rec of records) {
            processed++;
            try {
              const taskData = kintone.kintoneRecordToTask(rec, mappings);
              if (!taskData.title) continue;

              // タイトルで既存タスクを検索
              const existCheck = await dbQuery(
                `SELECT id FROM tasks WHERE team_id=$1 AND title=$2 LIMIT 1`,
                [teamId, taskData.title],
              );

              if (existCheck.rows.length > 0) {
                // 既存タスク更新
                const taskId = existCheck.rows[0].id;
                const existingTask = await dbGetTaskById(teamId, taskId);
                if (taskData.status && taskData.status !== existingTask.status) {
                  await dbUpdateStatus(teamId, taskId, taskData.status);
                }
                if (taskData.due_date || taskData.description || taskData.assignee_id) {
                  await dbQuery(
                    `UPDATE tasks SET
                      due_date = COALESCE($3, due_date),
                      description = COALESCE($4, description),
                      updated_at = now()
                    WHERE team_id=$1 AND id=$2`,
                    [teamId, taskId, taskData.due_date || null, taskData.description || null],
                  );
                }
                updated++;
              } else {
                // 新規タスク作成
                const taskId = randomUUID();
                await dbCreateTask({
                  id: taskId,
                  team_id: teamId,
                  channel_id: null,
                  message_ts: null,
                  source_permalink: null,
                  title: taskData.title,
                  description: taskData.description || taskData.title,
                  requester_user_id: userId,
                  created_by_user_id: userId,
                  assignee_id: taskData.assignee_id || userId,
                  assignee_label: taskData.assignee_label || null,
                  status: taskData.status || "in_progress",
                  due_date: taskData.due_date || null,
                  requester_dept: null,
                  assignee_dept: null,
                  task_type: "personal",
                  broadcast_group_handle: null,
                  broadcast_group_id: null,
                  total_count: null,
                  completed_count: 0,
                  notified_at: null,
                });
                created++;
              }
            } catch (e) {
              console.error("sync from_remote record error:", e);
              failed++;
            }
          }
        }

        // --- ローカル → kintone ---
        if (direction === "both" || direction === "to_remote") {
          const tasksRes = await dbQuery(
            `SELECT * FROM tasks WHERE team_id=$1 ORDER BY created_at DESC LIMIT 1000`,
            [teamId],
          );

          // 既存のkintoneレコードのタイトルを取得して重複チェック
          const titleField = mappings.find((m) => m.local_field === "title")?.remote_field;
          let existingTitles = new Set();
          if (titleField) {
            try {
              const existing = await kintone.getRecords(config, { fields: [titleField] });
              existingTitles = new Set(existing.map((r) => r[titleField]?.value).filter(Boolean));
            } catch (e) {
              console.error("sync: failed to fetch existing kintone records:", e);
            }
          }

          const newRecords = [];
          for (const task of tasksRes.rows) {
            processed++;
            try {
              if (titleField && existingTitles.has(task.title)) {
                continue; // 既にkintoneにあるのでスキップ
              }
              const record = kintone.taskToKintoneRecord(task, mappings);
              newRecords.push(record);
            } catch (e) {
              console.error("sync to_remote record error:", e);
              failed++;
            }
          }

          if (newRecords.length > 0) {
            await kintone.addRecords(config, newRecords);
            created += newRecords.length;
          }
        }
      }

      await dbUpdateSyncLog(logId, {
        status: "completed",
        records_processed: processed,
        records_created: created,
        records_updated: updated,
        records_failed: failed,
        finished_at: new Date().toISOString(),
      });
      await dbUpdateIntegration(teamId, integration.id, {
        last_synced_at: new Date().toISOString(),
        last_sync_status: "success",
        last_sync_message: `処理: ${processed}, 作成: ${created}, 更新: ${updated}, 失敗: ${failed}`,
      });
    } catch (e) {
      console.error("sync error:", e);
      await dbUpdateSyncLog(logId, {
        status: "failed",
        records_processed: processed,
        records_created: created,
        records_updated: updated,
        records_failed: failed,
        error_detail: e.message,
        finished_at: new Date().toISOString(),
      });
      await dbUpdateIntegration(teamId, integration.id, {
        last_synced_at: new Date().toISOString(),
        last_sync_status: "error",
        last_sync_message: e.message,
      });
    }
  }

  // RPO案件管理API（authWithRole/adminOnlyを共有）
  registerRpoApi({ expressApp, authWithRole, adminOnly });

  // kintone連携API
  registerKintoneApi({ expressApp, authWithRole, adminOnly });

  // Google Drive連携API
  registerDriveApi({ expressApp, authWithRole });

  // 顧客・商談管理API
  registerCrmApi({ expressApp, authWithRole });

  // 日報メンバー管理API
  registerDailyReportApi({ expressApp, authWithRole, slackClient });

  // 権限管理API
  registerPermissionsApi({ expressApp, authWithRole, adminOnly });

  // チャンネルマッピングAPI
  registerChannelMappingApi({ expressApp, authWithRole, adminOnly: adminOrITOnly, slackClient });

  // Slackランキング集計API
  registerRankingApi({ expressApp, authWithRole, adminOnly, slackClient });
}

module.exports = {
  generateToken,
  registerDashboardApi,
};
