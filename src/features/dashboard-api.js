const crypto = require("crypto");
const { randomUUID } = require("crypto");
const { registerRpoApi } = require("./rpo-api");
const { registerLegalApi } = require("./legal-api");
const { registerAnApi } = require("./an-api");
const { registerKintoneApi } = require("./kintone-api");
const { registerDriveApi } = require("./drive-api");
const { registerCrmApi, registerDailyReportApi, registerChannelTargetsApi } = require("./crm-api");
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
    dbTransaction,
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

  // IT チームまたは admin（チャンネルマッピング・Slackメンション管理・日報管理・ランキング管理）
  function adminOrITOnly(req, res, next) {
    const role = req.dashboardUser?.role;
    if (role !== "admin" && role !== "it") {
      return res.status(403).json({ error: "it_required" });
    }
    next();
  }

  // Personnel チームまたは admin（採用管理）
  function adminOrPersonnelOnly(req, res, next) {
    const role = req.dashboardUser?.role;
    if (role !== "admin" && role !== "personnel") {
      return res.status(403).json({ error: "personnel_required" });
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

  // ロール判定ミドルウェア。優先順位: admin(DB明示) > corp(DB明示) > IT > Personnel > Corporate > Slackタイトル推定
  // ロールはキャッシュ(5分)で保持。ロール変更後は最大5分で反映される。
  // IT・Personnel・Corporate はチーム所属名で判定しているため、チーム名変更時は要確認。
  // Cookie値の安全なパース（"view_as"）
  // { role?: string, asUserId?: string, asUserName?: string }
  function parseViewAs(req) {
    try {
      const raw = req.cookies?.view_as;
      if (!raw) return null;
      const obj = JSON.parse(decodeURIComponent(raw));
      if (!obj || typeof obj !== 'object') return null;
      const VALID = ['admin','corp','personnel','bc_manager','member'];
      if (obj.role && !VALID.includes(obj.role)) return null;
      return obj;
    } catch { return null; }
  }
  function applyViewAs(req, baseRole) {
    // adminの場合のみ view_as cookieを適用
    if (baseRole !== 'admin') return null;
    const va = parseViewAs(req);
    if (!va || (!va.role && !va.asUserId)) return null;
    const result = { viewingAs: { role: va.role || null, userId: va.asUserId || null, userName: va.asUserName || null } };
    if (va.role === 'bc_manager') {
      result.role = 'member'; result.isBcManager = true;
    } else if (va.role) {
      result.role = va.role; result.isBcManager = false;
    }
    return result;
  }

  // teamId + userId からロール+BCマネージャーを解決
  async function resolveRoleFor(teamId, userId) {
    const dbRole = await dbGetDashboardRole(teamId, userId);
    let role;
    if (dbRole === 'admin') role = 'admin';
    else if (dbRole === 'corp') role = 'corp';
    else {
      const { rows: itRows } = await dbQuery(`
        SELECT 1 FROM dash_team_members dtm
        JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
        WHERE dtm.team_id=$1 AND dtm.user_id=$2 AND (dt.name ILIKE '%IT%' OR dt.name ILIKE '%情シス%') LIMIT 1
      `, [teamId, userId]);
      if (itRows.length > 0) role = 'it';
      else {
        const { rows: persRows } = await dbQuery(`
          SELECT 1 FROM dash_team_members dtm
          JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
          WHERE dtm.team_id=$1 AND dtm.user_id=$2 AND (dt.name ILIKE '%personnel%' OR dt.name ILIKE '%人事%' OR dt.name ILIKE '%HR%') LIMIT 1
        `, [teamId, userId]);
        if (persRows.length > 0) role = 'personnel';
        else {
          const { rows: corpRows } = await dbQuery(`
            SELECT 1 FROM dash_team_members dtm
            JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
            JOIN dash_teams parent ON parent.id = dt.parent_id AND parent.team_id = dt.team_id
            WHERE dtm.team_id=$1 AND dtm.user_id=$2 AND parent.name ILIKE '%corporate%' LIMIT 1
          `, [teamId, userId]);
          if (corpRows.length > 0) role = 'corp';
          else {
            const title = await dbGetUserSlackTitle(teamId, userId);
            role = roleTitleFromSlack(title);
          }
        }
      }
    }
    const { rows: bcRows } = await dbQuery(
      `SELECT 1 FROM crm_bc_managers WHERE team_id=$1 AND user_id=$2 LIMIT 1`,
      [teamId, userId]
    ).catch(() => ({ rows: [] }));
    return { role, isBcManager: bcRows.length > 0 };
  }

  // view-as 適用後の最終的なrole/isBcManagerを決定
  // 優先順位: ① cookieの明示role > ② asUserIdの実ロール（自動取得） > ③ 元admin
  async function resolveViewAsEffective(req, baseRole, baseIsBcManager) {
    if (baseRole !== 'admin') {
      return { role: baseRole, isBcManager: baseIsBcManager, viewingAs: null };
    }
    const va = parseViewAs(req);
    if (!va || (!va.role && !va.asUserId)) {
      return { role: baseRole, isBcManager: baseIsBcManager, viewingAs: null };
    }
    let role = baseRole, isBcManager = baseIsBcManager;
    if (va.role === 'bc_manager') { role = 'member'; isBcManager = true; }
    else if (va.role)             { role = va.role; isBcManager = false; }
    else if (va.asUserId) {
      // ロール未指定 + ユーザー指定 → そのユーザーの実ロールを採用
      const resolved = await resolveRoleFor(req.dashboardUser.teamId, va.asUserId);
      role = resolved.role; isBcManager = resolved.isBcManager;
    }
    return { role, isBcManager, viewingAs: { role: va.role || null, userId: va.asUserId || null, userName: va.asUserName || null } };
  }

  async function authWithRole(req, res, next) {
    authMiddleware(req, res, async () => {
      try {
        const { teamId, userId } = req.dashboardUser;
        const cacheKey = `${teamId}:${userId}`;
        const cached = roleCache.get(cacheKey);
        if (cached && Date.now() < cached.expiresAt) {
          // キャッシュは「実ロール」のみ。view-asは毎回判定する
          const eff = await resolveViewAsEffective(req, cached.role, cached.isBcManager || false);
          req.dashboardUser.realRole    = cached.role;
          req.dashboardUser.realUserId  = userId;
          req.dashboardUser.role        = eff.role;
          req.dashboardUser.isBcManager = eff.isBcManager;
          if (eff.viewingAs?.userId) {
            req.dashboardUser.userId = eff.viewingAs.userId;
          }
          req.dashboardUser.viewingAs = eff.viewingAs;
          return next();
        }
        const { role: baseRole } = await resolveRoleFor(teamId, userId);
        let role = baseRole;
        // BCマネージャー判定（admin/role とは独立）
        const { rows: bcRows } = await dbQuery(
          `SELECT 1 FROM crm_bc_managers WHERE team_id=$1 AND user_id=$2 LIMIT 1`,
          [teamId, userId]
        ).catch(() => ({ rows: [] }));
        const isBcManager = bcRows.length > 0;

        roleCache.set(cacheKey, { role, isBcManager, expiresAt: Date.now() + 5 * 60 * 1000 });
        const eff = await resolveViewAsEffective(req, role, isBcManager);
        req.dashboardUser.realRole    = role;
        req.dashboardUser.realUserId  = userId;
        req.dashboardUser.role        = eff.role;
        req.dashboardUser.isBcManager = eff.isBcManager;
        if (eff.viewingAs?.userId) {
          req.dashboardUser.userId = eff.viewingAs.userId;
        }
        req.dashboardUser.viewingAs = eff.viewingAs;
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
  // View As（adminのみ）
  // ================================
  // 切替対象ユーザー候補一覧
  expressApp.get("/api/dashboard/view-as/users", authWithRole, async (req, res) => {
    try {
      if (req.dashboardUser.realRole !== 'admin') return res.status(403).json({ error: 'admin_only' });
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT user_id, real_name, display_name
         FROM dashboard_user_directory
         WHERE team_id=$1 AND COALESCE(real_name, display_name) IS NOT NULL
         ORDER BY real_name`,
        [teamId]
      );
      res.json({ users: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // View As セット
  expressApp.post("/api/dashboard/view-as", authWithRole, async (req, res) => {
    try {
      if (req.dashboardUser.realRole !== 'admin') return res.status(403).json({ error: 'admin_only' });
      const VALID = ['admin','corp','personnel','bc_manager','member'];
      const role = req.body?.role || null;
      const asUserId = req.body?.asUserId || null;
      const asUserName = req.body?.asUserName || null;
      if (role && !VALID.includes(role)) return res.status(400).json({ error: 'invalid_role' });
      if (!role && !asUserId) return res.status(400).json({ error: 'role_or_user_required' });
      const payload = JSON.stringify({ role, asUserId, asUserName });
      res.cookie('view_as', encodeURIComponent(payload), {
        httpOnly: false, sameSite: 'lax',
        maxAge: 24 * 60 * 60 * 1000, // 24h
        secure: process.env.NODE_ENV === 'production',
      });
      // role cacheをクリアして即時反映
      roleCache.delete(`${req.dashboardUser.teamId}:${req.dashboardUser.realUserId}`);
      res.json({ ok: true, role, asUserId, asUserName });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // View As クリア
  expressApp.post("/api/dashboard/view-as/clear", authWithRole, async (req, res) => {
    res.clearCookie('view_as');
    roleCache.delete(`${req.dashboardUser.teamId}:${req.dashboardUser.realUserId}`);
    res.json({ ok: true });
  });

  // ================================
  // 個人設定: 自動タスク化キーワード（user_task_triggers）
  // 実ユーザーで操作する（View Asのなりきり中も実ユーザー側のキーワードを操作）
  // ================================
  expressApp.get("/api/dashboard/me/task-triggers", authWithRole, async (req, res) => {
    try {
      const { teamId, realUserId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT id, keyword, enabled, created_at FROM user_task_triggers
         WHERE team_id=$1 AND user_id=$2 ORDER BY created_at`,
        [teamId, realUserId]
      );
      res.json({ triggers: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.post("/api/dashboard/me/task-triggers", authWithRole, async (req, res) => {
    try {
      const { teamId, realUserId } = req.dashboardUser;
      const keyword = String(req.body?.keyword || '').trim();
      if (!keyword) return res.status(400).json({ error: 'keyword_required' });
      if (keyword.length > 50) return res.status(400).json({ error: 'too_long' });
      const { rows: [row] } = await dbQuery(
        `INSERT INTO user_task_triggers (team_id, user_id, keyword)
         VALUES ($1, $2, $3)
         ON CONFLICT (team_id, user_id, keyword) DO UPDATE SET enabled=true
         RETURNING id, keyword, enabled, created_at`,
        [teamId, realUserId, keyword]
      );
      res.json({ trigger: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.patch("/api/dashboard/me/task-triggers/:id", authWithRole, async (req, res) => {
    try {
      const { teamId, realUserId } = req.dashboardUser;
      const enabled = !!req.body?.enabled;
      const { rows: [row] } = await dbQuery(
        `UPDATE user_task_triggers SET enabled=$3
         WHERE id=$1 AND team_id=$2 AND user_id=$4
         RETURNING id, keyword, enabled`,
        [req.params.id, teamId, enabled, realUserId]
      );
      if (!row) return res.status(404).json({ error: 'not_found' });
      res.json({ trigger: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  expressApp.delete("/api/dashboard/me/task-triggers/:id", authWithRole, async (req, res) => {
    try {
      const { teamId, realUserId } = req.dashboardUser;
      await dbQuery(
        `DELETE FROM user_task_triggers WHERE id=$1 AND team_id=$2 AND user_id=$3`,
        [req.params.id, teamId, realUserId]
      );
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ================================
  // 期限切れタスクレポート（admin限定）
  // ================================
  // 担当者ごと集約 + 明細を返す
  // 社員（@inrevo.jp）のみ対象
  async function fetchOverdueTaskData(teamId) {
    const { rows } = await dbQuery(`
      SELECT
        t.id, t.title, t.due_date, t.requester_user_id, t.assignee_id, t.assignee_label,
        t.channel_id, t.message_ts, t.source_permalink, t.task_type,
        (CURRENT_DATE - t.due_date)::int AS days_overdue,
        COALESCE(d_assignee.real_name, d_assignee.display_name, t.assignee_label) AS assignee_name,
        d_assignee.profile_json->>'email' AS assignee_email,
        COALESCE(d_requester.real_name, d_requester.display_name) AS requester_name,
        -- broadcastタスクの自分完了状況も拾うため targets を後段で処理
        ARRAY(
          SELECT tt.user_id FROM task_targets tt
          JOIN dashboard_user_directory d ON d.team_id = tt.team_id AND d.user_id = tt.user_id
          WHERE tt.task_id = t.id::uuid AND tt.team_id = t.team_id
            AND d.profile_json->>'email' ILIKE '%@inrevo.jp'
        ) AS target_user_ids
      FROM tasks t
      LEFT JOIN dashboard_user_directory d_assignee
        ON d_assignee.team_id = t.team_id AND d_assignee.user_id = t.assignee_id
      LEFT JOIN dashboard_user_directory d_requester
        ON d_requester.team_id = t.team_id AND d_requester.user_id = t.requester_user_id
      WHERE t.team_id = $1
        AND t.due_date IS NOT NULL
        AND t.due_date < CURRENT_DATE
        AND t.status != 'done'
        AND t.cancelled_at IS NULL
        AND (
          -- personal/個人タスク: 担当者が inrevo.jp なら拾う
          (t.task_type != 'broadcast' AND d_assignee.profile_json->>'email' ILIKE '%@inrevo.jp')
          -- broadcastタスク: 後段で target_user_ids が空でない場合だけ採用
          OR t.task_type = 'broadcast'
        )
      ORDER BY t.due_date ASC, t.created_at ASC
    `, [teamId]);

    // broadcastタスクは task_targets × まだ完了していない人 に展開
    const tasks = [];
    // broadcast完了済みの (task_id, user_id) を集める
    const broadcastIds = rows.filter(r => r.task_type === 'broadcast').map(r => r.id);
    let doneMap = new Map();
    if (broadcastIds.length > 0) {
      const { rows: compRows } = await dbQuery(
        `SELECT task_id::text AS task_id, user_id FROM task_completions WHERE team_id=$1 AND task_id::text = ANY($2)`,
        [teamId, broadcastIds]
      );
      for (const c of compRows) doneMap.set(`${c.task_id}:${c.user_id}`, true);
    }
    // ユーザー名キャッシュ
    const allUserIds = new Set();
    rows.forEach(r => {
      if (r.task_type === 'broadcast') r.target_user_ids?.forEach(u => allUserIds.add(u));
      else if (r.assignee_id) allUserIds.add(r.assignee_id);
    });
    let nameMap = new Map();
    if (allUserIds.size > 0) {
      const { rows: u } = await dbQuery(
        `SELECT user_id, COALESCE(real_name, display_name) AS name FROM dashboard_user_directory
         WHERE team_id=$1 AND user_id = ANY($2::text[])`,
        [teamId, [...allUserIds]]
      );
      for (const x of u) nameMap.set(x.user_id, x.name);
      // DBに無いユーザーは Slack API経由で解決（getUserDisplayName が users.info → DB upsert を行う）
      const missing = [...allUserIds].filter(uid => !nameMap.has(uid));
      if (missing.length > 0) {
        await Promise.all(missing.map(async uid => {
          try {
            const name = await getUserDisplayName(teamId, uid);
            if (name && name !== uid) nameMap.set(uid, name);
          } catch {}
        }));
      }
    }

    for (const r of rows) {
      if (r.task_type === 'broadcast') {
        for (const uid of (r.target_user_ids || [])) {
          if (doneMap.get(`${r.id}:${uid}`)) continue;
          tasks.push({
            task_id: r.id, title: r.title, due_date: r.due_date, days_overdue: r.days_overdue,
            assignee_user_id: uid, assignee_name: nameMap.get(uid) || `(社外/${uid})`,
            requester_name: r.requester_name || r.requester_user_id || '',
            permalink: r.source_permalink || '', task_type: 'broadcast',
          });
        }
      } else {
        const resolvedName =
          (r.assignee_id && nameMap.get(r.assignee_id))
          || r.assignee_name
          || r.assignee_label
          || (r.assignee_id ? `(社外/${r.assignee_id})` : '(担当者未設定)');
        tasks.push({
          task_id: r.id, title: r.title, due_date: r.due_date, days_overdue: r.days_overdue,
          assignee_user_id: r.assignee_id || '', assignee_name: resolvedName,
          requester_name: r.requester_name || r.requester_user_id || '',
          permalink: r.source_permalink || '', task_type: r.task_type,
        });
      }
    }

    // 担当者ごと集約
    const summaryMap = new Map();
    for (const t of tasks) {
      const key = t.assignee_user_id || `__label:${t.assignee_name}`;
      if (!summaryMap.has(key)) {
        summaryMap.set(key, {
          assignee_user_id: t.assignee_user_id, assignee_name: t.assignee_name,
          count: 0, oldest_due: t.due_date, max_days_overdue: t.days_overdue,
        });
      }
      const s = summaryMap.get(key);
      s.count++;
      if (t.due_date < s.oldest_due) s.oldest_due = t.due_date;
      if (t.days_overdue > s.max_days_overdue) s.max_days_overdue = t.days_overdue;
    }
    const summary = [...summaryMap.values()].sort((a,b) => b.count - a.count);
    return { summary, tasks };
  }

  // プレビューJSON
  expressApp.get("/api/dashboard/admin/overdue-report", authWithRole, adminOnly, async (req, res) => {
    try {
      const data = await fetchOverdueTaskData(req.dashboardUser.teamId);
      res.json({ ...data, generatedAt: new Date().toISOString() });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // CSV ダウンロード
  expressApp.get("/api/dashboard/admin/overdue-report.csv", authWithRole, adminOnly, async (req, res) => {
    try {
      const { tasks } = await fetchOverdueTaskData(req.dashboardUser.teamId);
      const today = new Date().toISOString().slice(0, 10);
      const headers = ['担当者', 'タスク', '期限日', '超過日数', '依頼者', 'リンク'];
      const esc = (v) => {
        const s = String(v == null ? '' : v).replace(/"/g, '""');
        return /[",\n]/.test(s) ? `"${s}"` : s;
      };
      const lines = [headers.join(',')];
      for (const t of tasks) {
        lines.push([
          esc(t.assignee_name), esc(t.title),
          esc(String(t.due_date).slice(0,10)),
          esc(`${t.days_overdue}日`),
          esc(t.requester_name), esc(t.permalink),
        ].join(','));
      }
      const csv = '﻿' + lines.join('\r\n');
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="overdue-tasks-${today}.csv"`);
      res.send(csv);
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // XLSX ダウンロード（サマリー + 明細の2シート）
  expressApp.get("/api/dashboard/admin/overdue-report.xlsx", authWithRole, adminOnly, async (req, res) => {
    try {
      const ExcelJS = require('exceljs');
      const { summary, tasks } = await fetchOverdueTaskData(req.dashboardUser.teamId);
      const today = new Date().toISOString().slice(0, 10);

      const wb = new ExcelJS.Workbook();
      wb.creator = 'Pochi'; wb.created = new Date();

      // シート1: サマリー
      const ws1 = wb.addWorksheet('担当者サマリー', { views: [{ state: 'frozen', ySplit: 1 }] });
      ws1.columns = [
        { header: '担当者',         key: 'name',  width: 24 },
        { header: '期限切れ件数',    key: 'count', width: 12 },
        { header: '最古の期限',      key: 'oldest', width: 14 },
        { header: '最大超過日数',    key: 'max',   width: 14 },
      ];
      summary.forEach(s => ws1.addRow({
        name: s.assignee_name,
        count: s.count,
        oldest: String(s.oldest_due).slice(0,10),
        max: `${s.max_days_overdue}日`,
      }));
      ws1.getRow(1).font = { bold: true };
      ws1.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF3C7' } };

      // シート2: 明細
      const ws2 = wb.addWorksheet('タスク明細', { views: [{ state: 'frozen', ySplit: 1 }] });
      ws2.columns = [
        { header: '担当者',     key: 'name',  width: 20 },
        { header: 'タスク',     key: 'title', width: 50 },
        { header: '期限日',     key: 'due',   width: 12 },
        { header: '超過日数',   key: 'over',  width: 10 },
        { header: '依頼者',     key: 'req',   width: 18 },
        { header: 'Slackリンク', key: 'link', width: 50 },
      ];
      tasks.forEach(t => ws2.addRow({
        name: t.assignee_name,
        title: (t.title || '').replace(/\n/g, ' '),
        due: String(t.due_date).slice(0,10),
        over: `${t.days_overdue}日`,
        req: t.requester_name,
        link: t.permalink ? { text: '元メッセージ', hyperlink: t.permalink } : '',
      }));
      ws2.getRow(1).font = { bold: true };
      ws2.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF3C7' } };

      const buf = await wb.xlsx.writeBuffer();
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="overdue-tasks-${today}.xlsx"`);
      res.send(Buffer.from(buf));
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ================================
  // General APIs (role-aware)
  // ================================

  // --- /me (with role) ---
  expressApp.get("/api/dashboard/me", authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role, isBcManager, realRole, realUserId, viewingAs } = req.dashboardUser;
      const name = await getUserDisplayName(teamId, userId);
      const realName = realUserId && realUserId !== userId ? await getUserDisplayName(teamId, realUserId) : null;
      const teams = await dbGetUserDashTeams(teamId, userId);
      res.json({
        teamId, userId, displayName: name,
        role, isBcManager: !!isBcManager,
        realRole, realUserId, realName,
        viewingAs: viewingAs || null,
        dashTeams: teams,
      });
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
                  t.requester_user_id, t.task_type, t.created_at, t.completed_count, t.total_count,
                  (t.message_ts IS NOT NULL) AS from_slack
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

      // assignee 単一指定: broadcast タスクの個人完了状態を付与（マイタスク一覧用）
      if (assignee && !assignees) {
        const broadcastIds = tasksWithNames.filter(t => t.task_type === 'broadcast').map(t => t.id);
        if (broadcastIds.length > 0) {
          const compRes = await dbQuery(
            `SELECT task_id::text AS task_id FROM task_completions WHERE team_id=$1 AND user_id=$2 AND task_id::text = ANY($3)`,
            [teamId, assignee, broadcastIds]
          );
          const doneSet = new Set(compRes.rows.map(r => r.task_id));
          for (const t of tasksWithNames) {
            if (t.task_type === 'broadcast') t.self_completed = doneSet.has(t.id);
          }
        }
      }

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
              fromEmail, emailSubject, emailBody, totalScore, importSheetUrl,
              personalityGasUrl, personalitySheetUrl, personalityEmailSubject, personalityEmailBody, personalityWebhookSecret } = req.body;
      await dbQuery(`
        INSERT INTO recruitment_settings (team_id, template_spreadsheet_id, gas_endpoint_url, notify_channel_id, notify_mention_user_id, webhook_secret, from_email, email_subject, email_body, total_score, import_sheet_url,
          personality_gas_url, personality_sheet_url, personality_email_subject, personality_email_body, personality_webhook_secret, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now())
        ON CONFLICT (team_id) DO UPDATE SET
          template_spreadsheet_id=$2, gas_endpoint_url=$3, notify_channel_id=$4,
          notify_mention_user_id=$5, webhook_secret=$6,
          from_email=$7, email_subject=$8, email_body=$9, total_score=$10,
          import_sheet_url=$11,
          personality_gas_url=$12, personality_sheet_url=$13,
          personality_email_subject=$14, personality_email_body=$15,
          personality_webhook_secret=$16, updated_at=now()
      `, [teamId, templateSpreadsheetId||null, gasEndpointUrl||null, notifyChannelId||null, notifyMentionUserId||null, webhookSecret||null,
          fromEmail||null, emailSubject||null, emailBody||null, totalScore||null, importSheetUrl||null,
          personalityGasUrl||null, personalitySheetUrl||null, personalityEmailSubject||null, personalityEmailBody||null, personalityWebhookSecret||null]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 候補者 一覧/追加/削除
  expressApp.get("/api/dashboard/admin/recruitment/candidates", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(
        `SELECT c.*,
                s.scheduled_at,
                s.id AS schedule_id
         FROM recruitment_candidates c
         LEFT JOIN LATERAL (
           SELECT scheduled_at, id FROM recruitment_scheduled_sends
           WHERE candidate_id = c.id AND status = 'pending'
           ORDER BY scheduled_at ASC LIMIT 1
         ) s ON true
         WHERE c.team_id = $1
         ORDER BY c.created_at DESC`,
        [teamId]
      );
      res.json({ candidates: r.rows });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  expressApp.post("/api/dashboard/admin/recruitment/candidates", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name, email, department } = req.body;
      if (!name?.trim() || !email?.trim()) return res.status(400).json({ error: "name and email required" });
      const id = randomUUID();
      const r = await dbQuery(
        "INSERT INTO recruitment_candidates (id, team_id, name, email, department) VALUES ($1,$2,$3,$4,$5) RETURNING *",
        [id, teamId, name.trim(), email.trim(), department || null]
      );
      res.json({ candidate: r.rows[0] });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 部署更新
  expressApp.patch("/api/dashboard/admin/recruitment/candidates/:id/department", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { department } = req.body;
      const r = await dbQuery(
        "UPDATE recruitment_candidates SET department=$1 WHERE id=$2 AND team_id=$3 RETURNING *",
        [department || null, req.params.id, teamId]
      );
      if (!r.rows[0]) return res.status(404).json({ error: 'not_found' });
      res.json({ candidate: r.rows[0] });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // 再採点: GASに即時採点を依頼
  expressApp.post("/api/dashboard/admin/recruitment/candidates/:id/regrade", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const candRes = await dbQuery("SELECT * FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [req.params.id, teamId]);
      const c = candRes.rows[0];
      if (!c) return res.status(404).json({ error: 'not_found' });
      if (!c.spreadsheet_id) return res.status(400).json({ error: 'スプレッドシートが未送信です' });

      const sRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      const s = sRes.rows[0];
      if (!s?.gas_endpoint_url) return res.status(400).json({ error: '実技GAS URL未設定' });

      const baseUrl = process.env.PUBLIC_BASE_URL || `https://${req.get('host')}`;
      const webhookUrl = `${baseUrl}/api/dashboard/recruitment/webhook/complete`;

      const gasRes = await fetch(s.gas_endpoint_url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'gradeNow',
          secret: s.webhook_secret || '',
          candidateId: c.id,
          spreadsheetId: c.spreadsheet_id,
          webhookUrl,
        }),
      });
      const data = await gasRes.json().catch(() => ({}));
      if (!data.ok) return res.status(500).json({ error: data.error || 'GASエラー' });
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
  });

  // MK部署等: 実技+適性を同時送付
  expressApp.post("/api/dashboard/admin/recruitment/candidates/:id/send-both", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const candRes = await dbQuery("SELECT * FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [req.params.id, teamId]);
      const c = candRes.rows[0];
      if (!c) return res.status(404).json({ error: 'not_found' });

      const sRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]);
      const s = sRes.rows[0];
      if (!s?.gas_endpoint_url) return res.status(400).json({ error: '実技GAS URL未設定' });
      if (!s?.personality_gas_url) return res.status(400).json({ error: '適性GAS URL未設定' });

      const baseUrl = process.env.PUBLIC_BASE_URL || `https://${req.get('host')}`;

      // 1) 実技テスト送付（未送付の場合のみ）
      if (!c.spreadsheet_url) {
        const webhookUrl = `${baseUrl}/api/dashboard/recruitment/webhook/complete`;
        const gasRes = await fetch(s.gas_endpoint_url, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            candidateId: c.id, name: c.name, email: c.email,
            templateId: s.template_spreadsheet_id,
            webhookUrl, secret: s.webhook_secret || '',
            fromEmail: s.from_email || null,
            emailSubject: s.email_subject || null,
            emailBody: s.email_body || null,
          }),
        });
        const data = await gasRes.json().catch(() => ({}));
        if (!data.ok) return res.status(500).json({ error: '実技送付失敗: ' + (data.error || 'unknown') });
        await dbQuery(
          "UPDATE recruitment_candidates SET spreadsheet_url=$1, spreadsheet_id=$2, status='sent', sent_at=now(), error_message=NULL WHERE id=$3",
          [data.spreadsheetUrl, data.spreadsheetId, c.id]
        );
      }

      // 2) 適性検査送付
      const pRes = await fetch(s.personality_gas_url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          secret: s.personality_webhook_secret || s.webhook_secret || '',
          action: 'sendPersonalityEmail',
          name: c.name, email: c.email,
          fromEmail: s.from_email || null,
          emailSubject: s.personality_email_subject || null,
          emailBody: s.personality_email_body || null,
        }),
      });
      const pData = await pRes.json().catch(() => ({}));
      if (!pData.ok) return res.status(500).json({ error: '適性送付失敗: ' + (pData.error || 'unknown') });

      // 3) ステージを personality に進める + 適性送付済みに
      await dbQuery(
        "UPDATE recruitment_candidates SET stage='personality', personality_status='sent', personality_sent_at=now() WHERE id=$1",
        [c.id]
      );

      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
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

      // 予約送信が入っている候補者は除外（重複送信防止）
      const scheduledRes = await dbQuery(
        `SELECT DISTINCT candidate_id FROM recruitment_scheduled_sends WHERE team_id=$1 AND status='pending'`,
        [teamId]
      );
      const scheduledIds = new Set(scheduledRes.rows.map(r => r.candidate_id));

      const candidatesRes = await dbQuery(
        "SELECT * FROM recruitment_candidates WHERE team_id=$1 AND spreadsheet_url IS NULL AND status IN ('pending','error')",
        [teamId]
      );
      const targets = candidatesRes.rows.filter(c => !scheduledIds.has(c.id));
      if (targets.length === 0) return res.json({ ok: true, sent: 0, skippedScheduled: scheduledIds.size });

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

  // ─── 予約送信 ─────────────────────────────────────
  // GET  /api/dashboard/admin/recruitment/scheduled  予約一覧
  expressApp.get("/api/dashboard/admin/recruitment/scheduled", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { rows } = await dbQuery(
        `SELECT s.*, c.name AS candidate_name, c.email AS candidate_email
         FROM recruitment_scheduled_sends s
         JOIN recruitment_candidates c ON c.id = s.candidate_id
         WHERE s.team_id = $1 AND s.status = 'pending'
         ORDER BY s.scheduled_at ASC`,
        [teamId]
      );
      res.json({ schedules: rows });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // POST /api/dashboard/admin/recruitment/scheduled  予約作成
  expressApp.post("/api/dashboard/admin/recruitment/scheduled", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { candidateIds, scheduledAt } = req.body;
      if (!candidateIds?.length || !scheduledAt) return res.status(400).json({ error: 'candidateIds and scheduledAt required' });
      const created = [];
      for (const candidateId of candidateIds) {
        const { rows } = await dbQuery(
          `INSERT INTO recruitment_scheduled_sends (team_id, candidate_id, scheduled_at)
           VALUES ($1, $2, $3) RETURNING *`,
          [teamId, candidateId, scheduledAt]
        );
        created.push(rows[0]);
      }
      res.json({ ok: true, created });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // DELETE /api/dashboard/admin/recruitment/scheduled/:id  予約キャンセル
  expressApp.delete("/api/dashboard/admin/recruitment/scheduled/:id", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbQuery(
        `UPDATE recruitment_scheduled_sends SET status='cancelled' WHERE id=$1 AND team_id=$2 AND status='pending'`,
        [req.params.id, teamId]
      );
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ─── 予約送信ワーカー（1分ごとに実行） ─────────────────
  async function processScheduledSends() {
    try {
      const { rows: due } = await dbQuery(
        `SELECT s.*, c.name, c.email, c.id AS c_id, c.spreadsheet_url AS already_sent_url,
                rs.gas_endpoint_url, rs.template_spreadsheet_id, rs.webhook_secret,
                rs.from_email, rs.email_subject, rs.email_body
         FROM recruitment_scheduled_sends s
         JOIN recruitment_candidates c ON c.id = s.candidate_id
         LEFT JOIN recruitment_settings rs ON rs.team_id = s.team_id
         WHERE s.status = 'pending' AND s.scheduled_at <= now()`
      );
      if (!due.length) return;

      const baseUrl = process.env.APP_BASE_URL || 'https://inrevo-task.com';
      const webhookUrl = `${baseUrl}/api/dashboard/recruitment/webhook/complete`;

      for (const row of due) {
        try {
          // 手動送信済みの場合はスキップ
          if (row.already_sent_url) {
            await dbQuery(`UPDATE recruitment_scheduled_sends SET status='sent', error_message='手動送信済みのためスキップ' WHERE id=$1`, [row.id]);
            continue;
          }
          if (!row.gas_endpoint_url || !row.template_spreadsheet_id) {
            await dbQuery(`UPDATE recruitment_scheduled_sends SET status='error', error_message='設定不備' WHERE id=$1`, [row.id]);
            continue;
          }
          const gasRes = await fetch(row.gas_endpoint_url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              candidateId: row.candidate_id,
              name: row.name, email: row.email,
              templateId: row.template_spreadsheet_id,
              webhookUrl, secret: row.webhook_secret || '',
              fromEmail: row.from_email || null,
              emailSubject: row.email_subject || null,
              emailBody: row.email_body || null,
            }),
          });
          const gasData = await gasRes.json();
          if (gasData.ok && gasData.spreadsheetUrl) {
            await dbQuery(
              `UPDATE recruitment_candidates SET spreadsheet_url=$1, spreadsheet_id=$2, status='sent', sent_at=now(), error_message=NULL WHERE id=$3`,
              [gasData.spreadsheetUrl, gasData.spreadsheetId || null, row.candidate_id]
            );
            await dbQuery(`UPDATE recruitment_scheduled_sends SET status='sent' WHERE id=$1`, [row.id]);
          } else {
            const msg = gasData.error || 'GASエラー';
            await dbQuery(`UPDATE recruitment_candidates SET status='error', error_message=$1 WHERE id=$2`, [msg, row.candidate_id]);
            await dbQuery(`UPDATE recruitment_scheduled_sends SET status='error', error_message=$1 WHERE id=$2`, [msg, row.id]);
          }
        } catch (err) {
          await dbQuery(`UPDATE recruitment_scheduled_sends SET status='error', error_message=$1 WHERE id=$2`, [err.message, row.id]).catch(() => {});
        }
      }
    } catch (e) { console.error('[scheduled-sends] worker error:', e); }
  }
  setInterval(processScheduledSends, 60 * 1000);

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
          const mention = cand.notify_mention_user_id
            ? cand.notify_mention_user_id.split(',').map(id => `<@${id.trim()}>`).join(' ') + ' '
            : '';
          const scoreText = (score != null) ? `${score}/10点` : '未採点';
          const typingRaw = scoreDetail?.q13_raw ?? cand.score_detail?.q13_raw;
          const typingInfo = typingLevel ? `\nタイピング: *${typingLevel}*${typingRaw != null ? `（${typingRaw}）` : ''}` : '';
          const text = `${mention}【採用テスト完了】*${cand.name}* さんが実技テストを完了しました\nスコア: *${scoreText}*${typingInfo}\n<${cand.spreadsheet_url}|スプレッドシートを確認>`;
          await slackClient.chat.postMessage({ channel: cand.notify_channel_id, text });
          console.log(`[採用通知] 送信完了: ${cand.name} → ${cand.notify_channel_id}`);
        } catch (slackErr) {
          console.error("[採用通知] Slack送信エラー:", slackErr.message);
        }
      }

      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: "internal" }); }
  });

  // ── 候補者ステージ更新 ──────────────────────────────────────────────────────
  expressApp.patch("/api/dashboard/admin/recruitment/candidates/:id/stage", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { stage } = req.body;
      if (!stage) return res.status(400).json({ error: 'stage required' });
      const { rows: [row] } = await dbQuery(
        "UPDATE recruitment_candidates SET stage=$1 WHERE id=$2 AND team_id=$3 RETURNING *",
        [stage, req.params.id, teamId]
      );
      if (!row) return res.status(404).json({ error: 'not_found' });
      res.json({ candidate: row });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── 適性診断 ──────────────────────────────────────────────────────────────────

  // 手動完了
  expressApp.post("/api/dashboard/admin/personality/complete", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { candidateId } = req.body;
      await dbQuery(
        "UPDATE recruitment_candidates SET personality_status='completed', personality_completed_at=now() WHERE id=$1 AND team_id=$2",
        [candidateId, teamId]
      );
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // 送付
  expressApp.post("/api/dashboard/admin/personality/send", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { candidateId } = req.body;
      const [candRes, settingsRes] = await Promise.all([
        dbQuery("SELECT * FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [candidateId, teamId]),
        dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]),
      ]);
      const c = candRes.rows[0];
      const s = settingsRes.rows[0];
      if (!c) return res.status(404).json({ error: 'not_found' });
      if (!s?.personality_gas_url) return res.status(400).json({ error: 'GASのURLを設定してください' });

      const gasRes = await fetch(s.personality_gas_url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          secret: s.personality_webhook_secret || s.webhook_secret || '',
          action: 'sendPersonalityEmail',
          name: c.name, email: c.email,
          fromEmail: s.from_email || null,
          emailSubject: s.personality_email_subject || null,
          emailBody: s.personality_email_body || null,
        }),
      });
      const gasData = await gasRes.json();
      if (!gasData.ok) return res.status(500).json({ error: gasData.error || 'GASエラー' });

      await dbQuery("UPDATE recruitment_candidates SET personality_status='sent', personality_sent_at=now() WHERE id=$1", [candidateId]);
      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // PDF生成（Drive URLをキャッシュ保存 → プレビューURL変換して返す）
  expressApp.post("/api/dashboard/admin/personality/pdf", authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { candidateId, forceRegenerate } = req.body;
      const [candRes, settingsRes] = await Promise.all([
        dbQuery("SELECT * FROM recruitment_candidates WHERE id=$1 AND team_id=$2", [candidateId, teamId]),
        dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [teamId]),
      ]);
      const c = candRes.rows[0];
      const s = settingsRes.rows[0];
      if (!c) return res.status(404).json({ error: 'not_found' });

      // キャッシュ済みURLがあれば再利用
      if (c.personality_pdf_url && !forceRegenerate) {
        const previewUrl = driveUrlToPreview(c.personality_pdf_url);
        return res.json({ ok: true, pdfUrl: c.personality_pdf_url, previewUrl });
      }

      if (!s?.personality_gas_url) return res.status(400).json({ error: 'GASのURLを設定してください' });

      const gasRes = await fetch(s.personality_gas_url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          secret: s.personality_webhook_secret || s.webhook_secret || '',
          action: 'generatePdf',
          candidateId: c.id, name: c.name,
        }),
      });
      const rawText = await gasRes.text();
      let gasData;
      try { gasData = JSON.parse(rawText); }
      catch { return res.status(500).json({ error: `GASがHTMLを返しました。ウェブアプリとして再デプロイしてURLを更新してください。(status: ${gasRes.status})` }); }
      if (!gasData.ok) return res.status(500).json({ error: gasData.error || 'PDF生成エラー' });

      // URLをDBにキャッシュ保存
      const pdfUrl = gasData.pdfUrl;
      await dbQuery("UPDATE recruitment_candidates SET personality_pdf_url=$1 WHERE id=$2", [pdfUrl, candidateId]).catch(() => {});

      const previewUrl = driveUrlToPreview(pdfUrl);
      res.json({ ok: true, pdfUrl, previewUrl });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  function driveUrlToPreview(url) {
    if (!url) return null;
    const m = url.match(/\/file\/d\/([^/]+)/);
    if (m) return `https://drive.google.com/file/d/${m[1]}/preview`;
    return url;
  }

  // フォーム回答webhook（GASのonFormSubmitから呼ばれる）
  expressApp.post("/api/dashboard/recruitment/webhook/personality", async (req, res) => {
    try {
      const { secret, email, name, action, responses, submittedAt } = req.body;
      if (action !== 'personalityCompleted') return res.status(400).json({ error: 'unknown_action' });

      // 名前正規化（全角スペース→半角、前後空白除去、スペース除去）
      const normName = (n) => (n || '').replace(/　/g, ' ').trim();
      const compactName = (n) => normName(n).replace(/\s+/g, '');

      // ① メールで検索
      let candRes = await dbQuery(
        `SELECT c.*, s.webhook_secret, s.personality_webhook_secret, s.notify_channel_id, s.notify_mention_user_id
         FROM recruitment_candidates c LEFT JOIN recruitment_settings s ON c.team_id=s.team_id
         WHERE LOWER(c.email)=LOWER($1) AND c.personality_status='sent' LIMIT 1`,
        [email]
      );
      // ② 名前の完全一致（スペース正規化後）
      if (!candRes.rows[0] && name) {
        const { rows } = await dbQuery(
          `SELECT c.*, s.webhook_secret, s.personality_webhook_secret, s.notify_channel_id, s.notify_mention_user_id
           FROM recruitment_candidates c LEFT JOIN recruitment_settings s ON c.team_id=s.team_id
           WHERE c.personality_status='sent'`,
          []
        );
        const formName = compactName(name);
        // スペース除去後の完全一致 → 部分一致の順で探す
        let matched = rows.find(r => compactName(r.name) === formName);
        if (!matched) matched = rows.find(r => compactName(r.name).includes(formName) || formName.includes(compactName(r.name)));
        if (matched) candRes = { rows: [matched] };
      }
      const cand = candRes.rows[0];
      if (!cand) {
        console.log(`[適性診断webhook] 候補者が見つかりません: email=${email}, name=${name}`);
        return res.status(404).json({ error: 'candidate_not_found' });
      }

      const expectedSecret = cand.personality_webhook_secret || cand.webhook_secret || '';
      if (expectedSecret && expectedSecret !== secret) return res.status(403).json({ error: 'invalid_secret' });

      await dbQuery(
        "UPDATE recruitment_candidates SET personality_status='completed', personality_completed_at=now() WHERE id=$1",
        [cand.id]
      );

      // Slack通知（PDF生成してボタン付きで送信）
      if (cand.notify_channel_id) {
        try {
          const { WebClient } = require('@slack/web-api');
          const wc = new WebClient(process.env.SLACK_BOT_TOKEN);
          const mention = cand.notify_mention_user_id
            ? cand.notify_mention_user_id.split(',').map(u => `<@${u.trim()}>`).join(' ')
            : '';

          // PDFを生成してURLを取得
          let pdfUrl = null;
          try {
            const settingsRes = await dbQuery("SELECT * FROM recruitment_settings WHERE team_id=$1", [cand.team_id]);
            const s = settingsRes.rows[0];
            if (s?.personality_gas_url) {
              const gasRes = await fetch(s.personality_gas_url, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  secret: s.personality_webhook_secret || s.webhook_secret || '',
                  action: 'generatePdf', candidateId: cand.id, name: cand.name,
                }),
              });
              const rawText = await gasRes.text();
              const gasData = JSON.parse(rawText);
              if (gasData.ok && gasData.pdfUrl) {
                pdfUrl = gasData.pdfUrl;
                await dbQuery("UPDATE recruitment_candidates SET personality_pdf_url=$1 WHERE id=$2", [pdfUrl, cand.id]).catch(() => {});
              }
            }
          } catch (pdfErr) { console.error('[適性診断通知] PDF生成エラー:', pdfErr.message); }

          const blocks = [
            {
              type: 'section',
              text: { type: 'mrkdwn', text: `${mention}【適性診断完了】*${cand.name}* さんが適性診断に回答しました` },
            },
            ...(pdfUrl ? [{
              type: 'actions',
              elements: [{
                type: 'button',
                text: { type: 'plain_text', text: '📄 結果PDFを開く' },
                url: pdfUrl,
                style: 'primary',
              }],
            }] : []),
          ];

          await wc.chat.postMessage({
            channel: cand.notify_channel_id,
            text: `${mention}【適性診断完了】${cand.name} さんが適性診断に回答しました`,
            blocks,
          });
        } catch (slackErr) { console.error('[適性診断通知] Slack送信エラー:', slackErr.message); }
      }

      res.json({ ok: true });
    } catch (e) { console.error(e); res.status(500).json({ error: 'internal' }); }
  });

  // ── HRMOS採用 CSVインポート & アナリティクス ──────────────────────────────────
  let hrmosUpload;
  try {
    const multer = require('multer');
    hrmosUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });
  } catch {
    hrmosUpload = { single: () => (req, res, next) => res.status(503).json({ error: 'csv_import_unavailable' }) };
  }

  // HRMOSのCSVエクスポートはタブ区切り(TSV)・Shift-JISで出力される。
  // 下記パーサーはクォート内の改行（レジュメ・備考欄）も正しく処理する。
  // split('\n')だと複数行フィールドで壊れるため、1文字ずつ処理する方式を採用。
  function hrmosParseCSV(text, delim) {
    const s = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const rows = [];
    let row = [], field = '', inQ = false;

    for (let i = 0; i <= s.length; i++) {
      const c = i < s.length ? s[i] : '\n';

      if (inQ) {
        if (c === '"') {
          if (s[i + 1] === '"') { field += '"'; i++; }
          else { inQ = false; }
        } else {
          field += c;
        }
      } else {
        if (c === '"') { inQ = true; }
        else if (c === delim) { row.push(field); field = ''; }
        else if (c === '\n') {
          row.push(field); field = '';
          if (row.some(f => f.trim())) rows.push(row);
          row = [];
        } else {
          field += c;
        }
      }
    }
    return rows;
  }

  // HRMOSはTSV（タブ区切り）でエクスポートするが、将来CSV変更の可能性もあるため自動判定する。
  function hrmosDetectDelim(text) {
    const first = text.slice(0, 2000);
    const tabs   = (first.match(/\t/g) || []).length;
    const commas = (first.match(/,/g) || []).length;
    return tabs > commas ? '\t' : ',';
  }

  async function hrmosImportCsv(teamId, raw) {
    const { randomUUID } = require('crypto');
    const delim = hrmosDetectDelim(raw);
    const rows = hrmosParseCSV(raw, delim);
    if (rows.length < 2) throw new Error('empty_csv');
    console.log(`[HRMOS] delim=${delim === '\t' ? 'TAB' : 'COMMA'}`);

    // ヘッダー行を自動検索（メタ行がある場合も対応）
    let headerRowIdx = rows.findIndex(row => row.some(c => c.includes('応募ID') || c.includes('応募者ID')));
    if (headerRowIdx === -1) headerRowIdx = 0; // fallback
    const headers = rows[headerRowIdx].map(h => h.trim());
    const dataRows = rows.slice(headerRowIdx + 1);

    // ASCII-safeなログ（Buffer経由でバイナリログ問題を回避）
    const safeHeaders = headers.slice(0, 10).map(h => Buffer.from(h).toString('base64')).join(',');
    console.log(`[HRMOS] headerRow=${headerRowIdx} cols=${headers.length} dataRows=${dataRows.length} enc_headers_b64=${safeHeaders}`);

    const idx = (names) => {
      for (const n of names) {
        const i = headers.findIndex(h => h.includes(n));
        if (i !== -1) return i;
      }
      return -1;
    };

    const col = {
      appId:        idx(['応募ID']),
      jobId:        idx(['求人ID']),
      jobName:      idx(['求人名']),
      posName:      idx(['選考ポジション名', 'ポジション名']),
      appliedDate:  idx(['応募日']),
      name:         idx(['氏名', '名前']),
      source:       idx(['応募経路']),
      sourceDetail: idx(['応募経路詳細']),
      label:        idx(['ラベル']),
      status:       idx(['選考ステータス', 'ステータス']),
      offerDate:    idx(['内定日']),
      joinDate:     idx(['入社日']),
      declineDate:  idx(['辞退日']),
    };
    console.log('[HRMOS採用] col indices:', JSON.stringify(col));

    const get = (cols, i) => i >= 0 ? (cols[i] || '').trim() || null : null;
    const toDate = v => {
      if (!v) return null;
      const jp = v.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
      if (jp) return `${jp[1]}-${jp[2].padStart(2,'0')}-${jp[3].padStart(2,'0')}`;
      const d = new Date(v.replace(/\//g, '-'));
      return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
    };

    // トランザクション内で全削除 → 一括INSERT（フルリプレイス）
    let imported = 0, skipped = 0, errors = [];
    await dbTransaction(async (client) => {
      await client.query('DELETE FROM hrmos_applicants WHERE team_id = $1', [teamId]);
      for (let i = 0; i < dataRows.length; i++) {
        const cols = dataRows[i];
        const appId = (get(cols, col.appId) || '').slice(0, 200) || null;
        const vals = [
          teamId, appId,
          get(cols, col.jobId), get(cols, col.jobName), get(cols, col.posName),
          toDate(get(cols, col.appliedDate)),
          get(cols, col.name), get(cols, col.source), get(cols, col.sourceDetail),
          get(cols, col.label), get(cols, col.status),
          toDate(get(cols, col.offerDate)), toDate(get(cols, col.joinDate)), toDate(get(cols, col.declineDate)),
        ];
        try {
          await client.query(`
            INSERT INTO hrmos_applicants
              (id, team_id, app_id, job_id, job_name, position_name, applied_date,
               applicant_name, source, source_detail, label, status,
               offer_date, join_date, decline_date, imported_at)
            VALUES (gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
          `, vals);
          imported++;
        } catch (e) {
          errors.push({ line: headerRowIdx + i + 2, error: e.message.slice(0, 100) });
          skipped++;
        }
      }
    });
    console.log(`[HRMOS採用] import done: ${imported}件 imported, ${skipped}件 skipped (full replace)`);
    return { ok: true, imported, skipped, errors: errors.slice(0, 10), colsFound: Object.fromEntries(Object.entries(col).filter(([,v]) => v >= 0)) };
  }

  // POST /api/dashboard/admin/hrmos-recruitment/import  — CSVファイルアップロード
  expressApp.post('/api/dashboard/admin/hrmos-recruitment/import', authWithRole, adminOrPersonnelOnly, hrmosUpload.single('file'), async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      if (!req.file) return res.status(400).json({ error: 'file_required' });
      const buf = req.file.buffer;
      // HRMOSエクスポートはShift-JISが多いが、GoogleシートやOS設定によって変わるため
      // UTF-8 BOM / UTF-16 / Shift-JIS / UTF-8 の4パターンを試し、既知ヘッダーとのマッチ数が
      // 最も多いエンコーディングを採用する。
      const raw = (() => {
        const iconv = (() => { try { return require('iconv-lite'); } catch { return null; } })();
        const KNOWN = ['応募ID','求人名','応募日','氏名','応募経路','ラベル','選考ステータス'];
        const score = (text) => KNOWN.filter(h => text.slice(0, 3000).includes(h)).length;
        const candidates = [];
        // UTF-8 BOM あり
        if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF)
          candidates.push(buf.slice(3).toString('utf-8'));
        // UTF-16 LE BOM あり
        if (buf[0] === 0xFF && buf[1] === 0xFE && iconv)
          candidates.push(iconv.decode(buf.slice(2), 'UTF-16LE'));
        // UTF-16 BE BOM あり
        if (buf[0] === 0xFE && buf[1] === 0xFF && iconv)
          candidates.push(iconv.decode(buf.slice(2), 'UTF-16BE'));
        // Shift-JIS
        if (iconv) candidates.push(iconv.decode(buf, 'Shift_JIS'));
        // UTF-8 (BOM なし / フォールバック)
        candidates.push(buf.toString('utf-8'));
        const scored = candidates.map(t => ({ t, s: score(t) }));
        const best = scored.reduce((a, b) => b.s > a.s ? b : a);
        console.log('[HRMOS] encoding scores:', scored.map(c => c.s).join(','), 'best:', best.s);
        return best.t;
      })();
      const result = await hrmosImportCsv(teamId, raw);
      res.json(result);
    } catch (e) {
      if (e.message === 'empty_csv') return res.status(400).json({ error: 'empty_csv' });
      console.error('[HRMOS採用] import error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // POST /api/dashboard/admin/hrmos-recruitment/import-sheet  — Google Sheetsから取り込み
  expressApp.post('/api/dashboard/admin/hrmos-recruitment/import-sheet', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { spreadsheetUrl } = req.body;
      if (!spreadsheetUrl) return res.status(400).json({ error: 'spreadsheetUrl required' });

      // スプレッドシートIDを抽出
      const idMatch = spreadsheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
      if (!idMatch) return res.status(400).json({ error: 'invalid_url' });
      const fileId = idMatch[1];

      // Drive API でCSVとしてエクスポート
      const { google } = require('googleapis');
      const path = require('path');
      const KEY_PATH = path.join(__dirname, '../../drive-service-account.json');
      const auth = new google.auth.GoogleAuth({
        keyFile: KEY_PATH,
        scopes: ['https://www.googleapis.com/auth/drive.readonly'],
      });
      const drive = google.drive({ version: 'v3', auth });

      const exportRes = await drive.files.export(
        { fileId, mimeType: 'text/csv', supportsAllDrives: true },
        { responseType: 'text' }
      );
      const raw = exportRes.data;
      if (!raw || raw.length < 10) return res.status(400).json({ error: 'empty_sheet' });

      const result = await hrmosImportCsv(teamId, raw);
      res.json(result);
    } catch (e) {
      console.error('[HRMOS採用] sheet import error:', e.message);
      if (e.code === 404) return res.status(404).json({ error: 'sheet_not_found', message: 'スプレッドシートが見つからないか、サービスアカウントに共有されていません' });
      if (e.code === 403) return res.status(403).json({ error: 'sheet_permission', message: 'スプレッドシートへのアクセス権がありません。サービスアカウントに共有してください' });
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // GET /api/dashboard/admin/hrmos-recruitment/analytics  — 集計データ
  expressApp.get('/api/dashboard/admin/hrmos-recruitment/analytics', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { from, to, granularity = 'month', jobName } = req.query;

      const conditions = ['team_id = $1'];
      const params = [teamId];
      if (from)    { conditions.push(`applied_date >= $${params.length + 1}`); params.push(from); }
      if (to)      { conditions.push(`applied_date <= $${params.length + 1}`); params.push(to); }
      if (jobName) { conditions.push(`COALESCE(job_name,'不明') = $${params.length + 1}`); params.push(jobName); }
      const where = conditions.join(' AND ');

      // 粒度別のSQLフォーマット
      const trendExpr = {
        day:   `TO_CHAR(applied_date, 'YYYY-MM-DD')`,
        '3day': `TO_CHAR(DATE '2000-01-01' + ((applied_date - DATE '2000-01-01') / 3) * 3, 'YYYY-MM-DD')`,
        week:  `TO_CHAR(date_trunc('week', applied_date::timestamp), 'YYYY-MM-DD')`,
        month: `TO_CHAR(applied_date, 'YYYY-MM')`,
      }[granularity] || `TO_CHAR(applied_date, 'YYYY-MM')`;

      const [totalR, byJobR, bySourceR, byLabelR, byStatusR, trendR, latestImportR] = await Promise.all([
        dbQuery(`SELECT COUNT(*)::int AS total, COUNT(DISTINCT job_name)::int AS unique_jobs FROM hrmos_applicants WHERE ${where}`, params),
        dbQuery(`
          SELECT COALESCE(job_name, '不明') AS name, COUNT(*)::int AS cnt
          FROM hrmos_applicants WHERE ${where}
          GROUP BY job_name ORDER BY cnt DESC LIMIT 20
        `, params),
        dbQuery(`
          SELECT COALESCE(source, '不明') AS name, COUNT(*)::int AS cnt
          FROM hrmos_applicants WHERE ${where}
          GROUP BY source ORDER BY cnt DESC
        `, params),
        dbQuery(`
          SELECT COALESCE(label, '（なし）') AS name, COUNT(*)::int AS cnt
          FROM hrmos_applicants WHERE ${where}
          GROUP BY label ORDER BY cnt DESC
        `, params),
        dbQuery(`
          SELECT COALESCE(status, '不明') AS name, COUNT(*)::int AS cnt
          FROM hrmos_applicants WHERE ${where}
          GROUP BY status ORDER BY cnt DESC
        `, params),
        dbQuery(`
          SELECT ${trendExpr} AS period, COUNT(*)::int AS cnt
          FROM hrmos_applicants WHERE ${where} AND applied_date IS NOT NULL
          GROUP BY period ORDER BY period
        `, params),
        dbQuery(`SELECT MAX(imported_at) AS latest FROM hrmos_applicants WHERE team_id = $1`, [teamId]),
      ]);

      res.json({
        total: totalR.rows[0]?.total ?? 0,
        uniqueJobs: totalR.rows[0]?.unique_jobs ?? 0,
        byJob: byJobR.rows,
        bySource: bySourceR.rows,
        byLabel: byLabelR.rows,
        byStatus: byStatusR.rows,
        trend: trendR.rows,
        granularity,
        latestImport: latestImportR.rows[0]?.latest ?? null,
      });
    } catch (e) {
      console.error('[HRMOS採用] analytics error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // GET /api/dashboard/admin/hrmos-recruitment/applicants  — ドリルダウン用応募者一覧
  expressApp.get('/api/dashboard/admin/hrmos-recruitment/applicants', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { from, to, label, status, job_name, source } = req.query;

      const conditions = ['team_id = $1'];
      const params = [teamId];
      const add = (col, val) => {
        conditions.push(`${col} = $${params.length + 1}`);
        params.push(val);
      };
      if (from) { conditions.push(`applied_date >= $${params.length + 1}`); params.push(from); }
      if (to)   { conditions.push(`applied_date <= $${params.length + 1}`); params.push(to); }
      if (label)    add('COALESCE(label, \'（なし）\')', label);
      if (status)   add('COALESCE(status, \'不明\')', status);
      if (job_name) add('COALESCE(job_name, \'不明\')', job_name);
      if (source)   add('COALESCE(source, \'不明\')', source);

      const r = await dbQuery(`
        SELECT applicant_name, job_name, position_name, applied_date,
               source, source_detail, label, status,
               offer_date, join_date, decline_date
        FROM hrmos_applicants
        WHERE ${conditions.join(' AND ')}
        ORDER BY applied_date DESC NULLS LAST
        LIMIT 500
      `, params);

      res.json({ applicants: r.rows, total: r.rows.length });
    } catch (e) {
      console.error('[HRMOS採用] applicants error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });

  // GET /api/dashboard/admin/hrmos-recruitment/summary  — インポート状況
  expressApp.get('/api/dashboard/admin/hrmos-recruitment/summary', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(`
        SELECT COUNT(*)::int AS total, MAX(imported_at) AS latest_import,
               MIN(applied_date) AS earliest_date, MAX(applied_date) AS latest_date
        FROM hrmos_applicants WHERE team_id = $1
      `, [teamId]);
      res.json(r.rows[0] || { total: 0, latest_import: null });
    } catch (e) {
      res.status(500).json({ error: 'internal' });
    }
  });

  // GET/PUT /admin/hrmos-recruitment/sheet-settings  — スプシURL保存
  expressApp.get('/api/dashboard/admin/hrmos-recruitment/sheet-settings', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(`SELECT sheet_url FROM hrmos_recruitment_settings WHERE team_id=$1`, [teamId]);
      res.json({ sheetUrl: r.rows[0]?.sheet_url || '' });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.put('/api/dashboard/admin/hrmos-recruitment/sheet-settings', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { sheetUrl } = req.body;
      await dbQuery(`
        INSERT INTO hrmos_recruitment_settings (team_id, sheet_url, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (team_id) DO UPDATE SET sheet_url=$2, updated_at=now()
      `, [teamId, sheetUrl || '']);
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  // POST /admin/hrmos-recruitment/sync  — 保存済みURLからワンクリック取り込み
  expressApp.post('/api/dashboard/admin/hrmos-recruitment/sync', authWithRole, adminOrPersonnelOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const r = await dbQuery(`SELECT sheet_url FROM hrmos_recruitment_settings WHERE team_id=$1`, [teamId]);
      const sheetUrl = r.rows[0]?.sheet_url;
      if (!sheetUrl) return res.status(400).json({ error: 'sheet_url_not_set', message: 'スプシURLが保存されていません' });

      const idMatch = sheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
      if (!idMatch) return res.status(400).json({ error: 'invalid_url' });
      const fileId = idMatch[1];

      // 公開シートなら認証なしで取得可能
      let raw;
      try {
        raw = await new Promise((resolve, reject) => {
          const https = require('https');
          const url = `https://docs.google.com/spreadsheets/d/${fileId}/export?format=csv`;
          const req2 = https.get(url, res2 => {
            if (res2.statusCode !== 200) return reject(Object.assign(new Error(`HTTP ${res2.statusCode}`), { code: 'HTTP_ERR' }));
            const chunks = [];
            res2.on('data', c => chunks.push(c));
            res2.on('end', () => {
              const body = Buffer.concat(chunks);
              const preview = body.slice(0, 300).toString();
              if (preview.includes('<html') || preview.includes('<!DOCTYPE') || preview.includes('Sign in')) {
                return reject(Object.assign(new Error('sheet_private'), { code: 'PRIVATE' }));
              }
              resolve(body.toString('utf-8'));
            });
          });
          req2.on('error', reject);
          req2.end();
        });
      } catch (fetchErr) {
        if (fetchErr.code === 'PRIVATE') {
          // 非公開 → Drive API を試みる
          try {
            const { google } = require('googleapis');
            const path = require('path');
            const auth = new google.auth.GoogleAuth({
              keyFile: path.join(__dirname, '../../drive-service-account.json'),
              scopes: ['https://www.googleapis.com/auth/drive.readonly'],
            });
            const drive = google.drive({ version: 'v3', auth });
            const exportRes = await drive.files.export({ fileId, mimeType: 'text/csv', supportsAllDrives: true }, { responseType: 'text' });
            raw = exportRes.data;
          } catch {
            return res.status(403).json({ error: 'sheet_private', message: 'スプレッドシートが非公開です。「リンクを知っている全員が閲覧可能」に設定するか、サービスアカウントに共有してください。' });
          }
        } else {
          return res.status(500).json({ error: 'fetch_failed', message: fetchErr.message });
        }
      }

      const result = await hrmosImportCsv(teamId, raw);
      res.json(result);
    } catch (e) {
      console.error('[HRMOS採用] sync error:', e);
      res.status(500).json({ error: 'internal', message: e.message });
    }
  });

  // ── Google Calendar OAuth ──────────────────────────────────────────
  const gcalClient = (() => {
    const { google } = require('googleapis');
    return new google.auth.OAuth2(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      'https://inrevo-task.com/api/google/oauth/callback'
    );
  })();

  const GCAL_SCOPES = [
    'https://www.googleapis.com/auth/calendar.readonly',
    'https://www.googleapis.com/auth/calendar.events.readonly',
  ];

  const getGcalAuth = async (teamId) => {
    const r = await dbQuery('SELECT * FROM google_oauth_tokens WHERE team_id=$1', [teamId]);
    if (!r.rows[0]?.refresh_token) return null;
    const auth = new (require('googleapis').google.auth.OAuth2)(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      'https://inrevo-task.com/api/google/oauth/callback'
    );
    auth.setCredentials({
      access_token: r.rows[0].access_token,
      refresh_token: r.rows[0].refresh_token,
      expiry_date: Number(r.rows[0].expiry_date),
    });
    // トークン自動更新時に保存
    auth.on('tokens', async (tokens) => {
      await dbQuery(
        `UPDATE google_oauth_tokens SET access_token=$2, expiry_date=$3, updated_at=now() WHERE team_id=$1`,
        [teamId, tokens.access_token, tokens.expiry_date]
      ).catch(() => {});
    });
    return auth;
  };

  // GET /api/google/oauth/start — OAuth認証開始（admin限定）
  expressApp.get('/api/google/oauth/start', authWithRole, (req, res) => {
    if (req.dashboardUser.role !== 'admin') return res.status(403).json({ error: 'admin_only' });
    const url = gcalClient.generateAuthUrl({
      access_type: 'offline',
      scope: GCAL_SCOPES,
      prompt: 'consent',
      state: req.dashboardUser.teamId,
    });
    res.redirect(url);
  });

  // GET /api/google/oauth/callback — OAuthコールバック
  expressApp.get('/api/google/oauth/callback', async (req, res) => {
    try {
      const { code, state: teamId } = req.query;
      if (!code || !teamId) return res.status(400).send('Bad request');
      const { tokens } = await gcalClient.getToken(code);
      await dbQuery(`
        INSERT INTO google_oauth_tokens (team_id, access_token, refresh_token, expiry_date, updated_at)
        VALUES ($1,$2,$3,$4,now())
        ON CONFLICT (team_id) DO UPDATE SET
          access_token=$2, refresh_token=COALESCE($3, google_oauth_tokens.refresh_token),
          expiry_date=$4, updated_at=now()
      `, [teamId, tokens.access_token, tokens.refresh_token || null, tokens.expiry_date || null]);
      res.send('<script>window.close(); window.opener && window.opener.location.reload();</script><p>✅ 認証完了！このタブを閉じてください。</p>');
    } catch (e) {
      console.error('[GCal] callback error:', e.message);
      res.status(500).send('認証エラー: ' + e.message);
    }
  });

  // GET /api/dashboard/my-calendar — 今日の自分の予定
  expressApp.get('/api/dashboard/my-calendar', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const auth = await getGcalAuth(teamId);
      if (!auth) return res.json({ events: [], connected: false });

      const { google } = require('googleapis');
      const calendar = google.calendar({ version: 'v3', auth });

      const jstNow = new Date(Date.now() + 9 * 60 * 60 * 1000);
      const todayJst = jstNow.toISOString().slice(0, 10);
      const timeMin = new Date(todayJst + 'T00:00:00+09:00').toISOString();
      const timeMax = new Date(todayJst + 'T23:59:59+09:00').toISOString();

      // 自分のメールアドレスを取得
      const dirR = await dbQuery(
        `SELECT profile_json->>'email' AS email FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2`,
        [teamId, userId]
      );
      const email = dirR.rows[0]?.email;

      const r = await calendar.events.list({
        calendarId: email || 'primary',
        timeMin, timeMax,
        singleEvents: true,
        orderBy: 'startTime',
        maxResults: 20,
      });

      const events = (r.data.items || []).map(e => ({
        id: e.id,
        title: e.summary || '（タイトルなし）',
        start: e.start?.dateTime || e.start?.date,
        end: e.end?.dateTime || e.end?.date,
        allDay: !e.start?.dateTime,
        location: e.location || null,
        meetUrl: e.hangoutLink || null,
      }));

      res.json({ events, connected: true });
    } catch (e) {
      console.error('[GCal] my-calendar error:', e.message);
      res.json({ events: [], connected: true, error: e.message });
    }
  });

  // GET /api/dashboard/gcal-status — 連携状態確認
  expressApp.get('/api/dashboard/gcal-status', authWithRole, async (req, res) => {
    try {
      const r = await dbQuery('SELECT updated_at FROM google_oauth_tokens WHERE team_id=$1', [req.dashboardUser.teamId]);
      res.json({ connected: !!r.rows[0], updatedAt: r.rows[0]?.updated_at || null });
    } catch { res.json({ connected: false }); }
  });

  // GET /api/dashboard/team-calendar-detail — チームメンバーの出勤+タスク+カレンダー
  // Chief以上またはadminのみ。自分が所属するチームのメンバーを対象とする。
  const CHIEF_ROLES = new Set(['Chief','Sub Chief','Manager','Expert','Sub Expert']);
  expressApp.get('/api/dashboard/team-calendar-detail', authWithRole, async (req, res) => {
    try {
      const { teamId, userId, role } = req.dashboardUser;

      // 権限チェック: adminまたはCRMでChief以上
      if (role !== 'admin') {
        const repR = await dbQuery('SELECT role_name FROM crm_rep_roles WHERE team_id=$1 AND rep_name IN (SELECT display_name FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2)', [teamId, userId]);
        const repRole = repR.rows[0]?.role_name || '';
        if (!CHIEF_ROLES.has(repRole)) return res.json({ canView: false });
      }

      // 自分が所属するチーム＋その子チームのメンバーを取得（再帰）
      const myTeamsR = await dbQuery(`
        SELECT dtm.dash_team_id FROM dash_team_members dtm WHERE dtm.team_id=$1 AND dtm.user_id=$2
      `, [teamId, userId]);
      const myTeamIds = myTeamsR.rows.map(r => r.dash_team_id);
      if (!myTeamIds.length) return res.json({ canView: true, members: [] });

      // 子チームも含めてすべてのチームIDを取得
      const allTeamIdsR = await dbQuery(`
        WITH RECURSIVE subtree AS (
          SELECT id FROM dash_teams WHERE id=ANY($1) AND team_id=$2
          UNION ALL
          SELECT dt.id FROM dash_teams dt JOIN subtree s ON dt.parent_id=s.id WHERE dt.team_id=$2
        )
        SELECT id FROM subtree
      `, [myTeamIds, teamId]);
      const allTeamIds = allTeamIdsR.rows.map(r => r.id);

      const membersR = await dbQuery(`
        SELECT DISTINCT dtm.user_id, d.display_name, d.profile_json->>'image_48' AS avatar_url,
               d.profile_json->>'email' AS email
        FROM dash_team_members dtm
        JOIN dashboard_user_directory d ON d.team_id=dtm.team_id AND d.user_id=dtm.user_id
        WHERE dtm.team_id=$1 AND dtm.dash_team_id=ANY($2) AND dtm.user_id != $3
          AND d.is_active=true
        ORDER BY d.display_name
      `, [teamId, allTeamIds, userId]);
      const members = membersR.rows;
      if (!members.length) return res.json({ canView: true, members: [] });

      // 打刻状況
      const jstNow = new Date(Date.now() + 9 * 60 * 60 * 1000);
      const todayJst = jstNow.toISOString().slice(0, 10);
      const dayStart = new Date(todayJst + 'T00:00:00+09:00');
      const stampsR = await dbQuery(`
        SELECT DISTINCT ON (slack_user_id, stamp_type) slack_user_id, stamp_type
        FROM hrmos_stamps WHERE team_id=$1 AND stamped_at>=$2 AND ok=true
        ORDER BY slack_user_id, stamp_type, stamped_at ASC
      `, [teamId, dayStart.toISOString()]);
      const clockedIn  = new Set(stampsR.rows.filter(s => s.stamp_type === 1).map(s => s.slack_user_id));
      const clockedOut = new Set(stampsR.rows.filter(s => s.stamp_type === 2).map(s => s.slack_user_id));

      // タスク数
      const taskR = await dbQuery(`SELECT assignee_id, COUNT(*)::int AS cnt FROM tasks WHERE team_id=$1 AND status='in_progress' GROUP BY assignee_id`, [teamId]);
      const taskMap = {};
      for (const r of taskR.rows) taskMap[r.assignee_id] = r.cnt;

      // カレンダー（連携済み時のみ）
      const auth = await getGcalAuth(teamId);
      let calMap = {};
      if (auth) {
        const { google } = require('googleapis');
        const calendar = google.calendar({ version: 'v3', auth });
        const timeMin = dayStart.toISOString();
        const timeMax = new Date(todayJst + 'T23:59:59+09:00').toISOString();
        // 今週の範囲
        const weekStart = new Date(jstNow); weekStart.setDate(jstNow.getDate() - jstNow.getDay() + 1); weekStart.setHours(0,0,0,0);
        const weekEnd   = new Date(weekStart); weekEnd.setDate(weekStart.getDate() + 6); weekEnd.setHours(23,59,59,999);

        await Promise.all(members.filter(m => m.email).map(async (m) => {
          try {
            const todayR = await calendar.events.list({ calendarId: m.email, timeMin, timeMax, singleEvents: true, orderBy: 'startTime', maxResults: 10 });
            const todayEvents = (todayR.data.items || []).filter(e => e.start?.dateTime).map(e => ({
              title: e.summary || '（タイトルなし）',
              start: e.start.dateTime,
              end: e.end.dateTime,
            }));
            const nowIso = new Date().toISOString();
            const currentEvent = todayEvents.find(e => e.start <= nowIso && e.end >= nowIso) || null;
            calMap[m.user_id] = { todayEvents, currentEvent };
          } catch { calMap[m.user_id] = { todayEvents: [], currentEvent: null }; }
        }));
      }

      const result = members.map(m => ({
        userId:      m.user_id,
        displayName: m.display_name,
        avatarUrl:   m.avatar_url,
        clockedIn:   clockedIn.has(m.user_id),
        clockedOut:  clockedOut.has(m.user_id),
        taskCount:   taskMap[m.user_id] || 0,
        ...(calMap[m.user_id] || { todayEvents: [], currentEvent: null }),
      }));

      res.json({ canView: true, members: result, calendarConnected: !!auth });
    } catch (e) {
      console.error('[team-calendar] error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── HRMOS勤怠: 自分の今日の打刻状況 ────────────────────────────────
  expressApp.get('/api/dashboard/my-attendance', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      // JSTの本日0時をUTCで計算
      const jstNow = new Date(Date.now() + 9 * 60 * 60 * 1000);
      const todayJst = jstNow.toISOString().slice(0, 10);
      const dayStart = new Date(todayJst + 'T00:00:00+09:00');

      const r = await dbQuery(`
        SELECT stamp_type, stamped_at, ok, error_reason
        FROM hrmos_stamps
        WHERE team_id=$1 AND slack_user_id=$2 AND stamped_at >= $3
        ORDER BY stamped_at ASC
      `, [teamId, userId, dayStart.toISOString()]);

      const clockIn  = r.rows.find(s => s.stamp_type === 1);
      const clockOut = r.rows.find(s => s.stamp_type === 2);
      res.json({ clockIn: clockIn || null, clockOut: clockOut || null, today: todayJst });
    } catch (e) {
      console.error('[勤怠] my-attendance error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── Slack WF → HRMOS打刻 webhook（認証不要、secret付きURL）────────
  expressApp.post('/api/hrmos/stamp', async (req, res) => {
    try {
      const secret = req.query.secret || req.body.secret;
      if (!secret || secret !== process.env.HRMOS_WEBHOOK_SECRET) {
        return res.status(403).json({ error: 'forbidden' });
      }
      // Slack WFが送るuser_idとstamp_type(1=出勤,2=退勤)
      const slackUserId = req.body.user_id;
      const stampType   = Number(req.body.stamp_type) || 1;
      if (!slackUserId) return res.status(400).json({ error: 'user_id required' });

      // teamIdをDBから取得
      const teamR = await dbQuery('SELECT DISTINCT team_id FROM tasks LIMIT 1');
      const teamId = teamR.rows[0]?.team_id || '';

      const { stampAttendance } = require('./ieyasu');
      const result = await stampAttendance(slackClient, slackUserId, stampType);

      await dbQuery(
        `INSERT INTO hrmos_stamps (id, team_id, slack_user_id, stamp_type, ok, hrmos_user_id, error_reason)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6)`,
        [teamId, slackUserId, stampType, result.ok, result.userId || null, result.ok ? null : result.reason]
      ).catch(e => console.error('[HRMOS WF] DB記録失敗:', e.message));

      console.log(`[HRMOS WF] stamp ${stampType === 1 ? '出勤' : '退勤'}: user=${slackUserId} ok=${result.ok}`);
      res.json({ ok: result.ok, reason: result.ok ? null : result.reason });
    } catch (e) {
      console.error('[HRMOS WF] error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── HRMOS勤怠: チームの今日の打刻状況（hrmos_stampsベース）────────
  expressApp.get('/api/dashboard/team-report-status', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;

      // JSTの本日0時
      const jstNow = new Date(Date.now() + 9 * 60 * 60 * 1000);
      const todayJst = jstNow.toISOString().slice(0, 10);
      const dayStart = new Date(todayJst + 'T00:00:00+09:00');

      // 対象メンバー取得（avatarはdashboard_user_directoryから）
      const membersR = await dbQuery(`
        SELECT m.user_id, m.display_name,
               d.profile_json->>'image_48' AS avatar_url
        FROM daily_report_members m
        LEFT JOIN dashboard_user_directory d
          ON d.team_id = m.team_id AND d.user_id = m.user_id
        WHERE m.team_id=$1 AND m.is_target=true
        ORDER BY m.display_name
      `, [teamId]);
      const members = membersR.rows;

      // 進行中タスク数（personal）
      const taskR = await dbQuery(`
        SELECT assignee_id, COUNT(*)::int AS cnt
        FROM tasks
        WHERE team_id=$1 AND status='in_progress'
        GROUP BY assignee_id
      `, [teamId]);
      const taskMap = {};
      for (const r of taskR.rows) taskMap[r.assignee_id] = r.cnt;

      // 今日の打刻状況をDBから取得
      const stampsR = await dbQuery(`
        SELECT DISTINCT ON (slack_user_id, stamp_type) slack_user_id, stamp_type, stamped_at, ok
        FROM hrmos_stamps
        WHERE team_id=$1 AND stamped_at >= $2 AND ok=true
        ORDER BY slack_user_id, stamp_type, stamped_at ASC
      `, [teamId, dayStart.toISOString()]);

      const stampedIn  = new Set(stampsR.rows.filter(s => s.stamp_type === 1).map(s => s.slack_user_id));
      const stampedOut = new Set(stampsR.rows.filter(s => s.stamp_type === 2).map(s => s.slack_user_id));
      const stampTimes = {};
      for (const s of stampsR.rows) {
        if (!stampTimes[s.slack_user_id]) stampTimes[s.slack_user_id] = {};
        stampTimes[s.slack_user_id][s.stamp_type] = s.stamped_at;
      }

      const status = members.map(m => ({
        userId: m.user_id,
        displayName: m.display_name,
        avatarUrl: m.avatar_url,
        clockedIn:  stampedIn.has(m.user_id),
        clockedOut: stampedOut.has(m.user_id),
        taskCount:  taskMap[m.user_id] || 0,
      }));

      res.json({
        today: todayJst,
        members: status,
        summary: {
          total:      members.length,
          clockedIn:  status.filter(s => s.clockedIn).length,
          clockedOut: status.filter(s => s.clockedOut).length,
        },
      });
    } catch (e) {
      console.error('[勤怠] team-report-status error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
  });

  // ── 未確認メンション ────────────────────────────────────────────────────────
  expressApp.delete('/api/dashboard/my-mentions/:id', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      await dbQuery(
        `UPDATE user_mentions SET dismissed_at=now() WHERE id=$1 AND team_id=$2 AND mentioned_user_id=$3`,
        [req.params.id, teamId, userId]
      );
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: 'internal' }); }
  });

  expressApp.get('/api/dashboard/my-mentions', authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

      const r = await dbQuery(`
        SELECT m.id, m.channel_id, m.message_ts, m.thread_ts_root, m.sender_user_id,
               m.text_preview, m.created_at, m.mention_type,
               d.display_name AS sender_name,
               d.profile_json->>'image_48' AS sender_avatar
        FROM user_mentions m
        LEFT JOIN dashboard_user_directory d
          ON d.team_id = m.team_id AND d.user_id = m.sender_user_id
        WHERE m.team_id = $1
          AND m.mentioned_user_id = $2
          AND m.dismissed_at IS NULL
          AND m.created_at >= $3
        ORDER BY m.created_at DESC
        LIMIT 30
      `, [teamId, userId, cutoff]);

      res.json({ mentions: r.rows });
    } catch (e) {
      console.error('[mentions] error:', e.message);
      res.status(500).json({ error: 'internal' });
    }
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

  // --- Team members (public read, for assignee filter, includes sub-teams) ---
  expressApp.get("/api/dashboard/teams/:id/members", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const subtreeIds = await dbGetDashTeamSubtree(teamId, req.params.id);
      const allTeamIds = [req.params.id, ...subtreeIds];
      const rows = await dbQuery(
        `SELECT DISTINCT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id = ANY($2)`,
        [teamId, allTeamIds]
      );
      res.json({ memberIds: rows.rows.map(r => r.user_id) });
    } catch (e) {
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Team members (admin) ---
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
      if (!task.channel_id || !task.message_ts) return res.json({ messages: [], nameMap: {}, channel: null });

      // チャンネル情報を取得
      let channel = null;
      try {
        const chInfo = await slackClient.conversations.info({ channel: task.channel_id });
        const ch = chInfo?.channel;
        channel = {
          id: task.channel_id,
          name: ch?.name || ch?.name_normalized || task.channel_id,
          is_private: !!ch?.is_private,
        };
      } catch { channel = { id: task.channel_id, name: task.channel_id, is_private: false }; }

      // ページネーションで全件取得するヘルパー
      const fetchReplies = async (ts) => {
        let msgs = [];
        let cur;
        do {
          const r = await slackClient.conversations.replies({
            channel: task.channel_id,
            ts,
            limit: 200,
            ...(cur ? { cursor: cur } : {}),
          });
          msgs = msgs.concat(r.messages || []);
          cur = r.response_metadata?.next_cursor || null;
        } while (cur);
        return msgs;
      };

      let rawMessages = await fetchReplies(task.message_ts);

      // message_tsが返信のtsだった場合、thread_tsで再フェッチしてスレッド全体を取得
      const firstMsg = rawMessages[0];
      if (firstMsg?.thread_ts && firstMsg.thread_ts !== task.message_ts) {
        rawMessages = await fetchReplies(firstMsg.thread_ts);
      }

      // メッセージ本文・blocksからユーザーIDを収集
      // ワークフロー/Block Kitメッセージは text(fallback)に表示名が入りIDは blocks内に格納される
      const mentionRe = /<@([^|>]+)(?:\|[^>]+)?>/g;
      const bareMentionRe = /@(U[A-Z0-9]{6,})/g;
      const allUserIds = new Set(rawMessages.map(m => m.user).filter(Boolean));
      for (const m of rawMessages) {
        // text フィールドから
        for (const match of (m.text || '').matchAll(mentionRe)) allUserIds.add(match[1]);
        for (const match of (m.text || '').matchAll(bareMentionRe)) allUserIds.add(match[1]);
        // blocks フィールドから (workflow/rich_text の user_id を抽出)
        if (m.blocks) {
          const blocksJson = JSON.stringify(m.blocks);
          for (const match of blocksJson.matchAll(/"user_id":"([^"]+)"/g)) allUserIds.add(match[1]);
        }
      }

      // ユーザー名・アバター解決: DB優先（dashboard_user_directory）→ getUserDisplayName（キャッシュ/API）
      const userIds = [...allUserIds];
      const dbRows = userIds.length > 0
        ? (await dbQuery(
            `SELECT user_id, display_name, profile_json->>'image_72' AS avatar_url FROM dashboard_user_directory WHERE team_id=$1 AND user_id = ANY($2)`,
            [teamId, userIds]
          ).then(r => r.rows))
        : [];

      const nameMap = {};
      const avatarMap = {};
      // 1) DB に存在する分を先に解決（最も信頼性が高い）
      for (const row of dbRows) {
        if (row.display_name) nameMap[row.user_id] = row.display_name.split('/')[0].trim();
        if (row.avatar_url)   avatarMap[row.user_id] = row.avatar_url;
      }
// 2) DB未登録の分を getUserDisplayName（プリフェッチキャッシュ/users.info）で解決し DB に保存
      const missingIds = userIds.filter(uid => !nameMap[uid]);
      await Promise.all(missingIds.map(async uid => {
        try {
          const info = await slackClient.users.info({ user: uid });
          const u = info?.user;
          if (u) {
            await dbUpsertDashboardUserDirectoryMember(teamId, u).catch(() => {});
            const profile = u.profile || {};
            const name = profile.display_name_normalized || profile.display_name || u.real_name || null;
            if (name) nameMap[uid] = name.split('/')[0].trim();
            const avatar = profile.image_72 || null;
            if (avatar) avatarMap[uid] = avatar;
          }
        } catch {
          // users.info 失敗 → getUserDisplayName のキャッシュを試みる
          const cached = await getUserDisplayName(teamId, uid).catch(() => null);
          if (cached && cached !== uid) nameMap[uid] = cached.split('/')[0].trim();
        }
      }));

      // ユーザーグループ名: 起動時キャッシュから解決（API呼び出しなし）
      const subteamIdMap = await getSubteamIdMap(teamId).catch(() => new Map());
      // フロント用: { subteamId → handle }
      const subteamMap = Object.fromEntries(subteamIdMap);

      // Botメッセージの bot_profile を補完
      const botDisplayMap = {};
      for (const m of rawMessages) {
        if (!m.user && m.bot_id && m.bot_profile) {
          botDisplayMap[`bot:${m.bot_id}`] = {
            name: m.bot_profile.name || 'Bot',
            avatar: m.bot_profile.icons?.image_72 || m.bot_profile.icons?.image_48 || null,
          };
        }
      }

      const rootTs = rawMessages[0]?.ts;
      const messages = rawMessages.map(m => {
        const botKey = !m.user && m.bot_id ? `bot:${m.bot_id}` : null;
        const botInfo = botKey ? botDisplayMap[botKey] : null;
        return {
          ts: m.ts,
          user_id: m.user || botKey || null,
          displayName: m.user ? (nameMap[m.user] || m.user) : (botInfo?.name || 'Bot'),
          avatar_url: m.user ? (avatarMap[m.user] || null) : (botInfo?.avatar || null),
          text: m.text || '',
          is_root: m.ts === rootTs,
        };
      });

      res.json({ messages, nameMap, subteamMap, channel });
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

  // --- POST /tasks/:id/notify-overdue (期限切れ通知をSlackスレッドに投稿) ---
  expressApp.post("/api/dashboard/tasks/:id/notify-overdue", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });
      if (!task.channel_id || !task.message_ts) return res.status(400).json({ error: "no_slack_thread" });

      const senderName = await getUserDisplayName(teamId, userId).then(n => n.split('/')[0].trim()).catch(() => '管理者');
      const dueStr = task.due_date ? new Date(task.due_date).toLocaleDateString('ja-JP') : '不明';

      // スレッドルートのtsを取得
      let threadTs = task.message_ts;
      try {
        const r = await slackClient.conversations.replies({ channel: task.channel_id, ts: task.message_ts, limit: 1 });
        if (r.messages?.[0]?.thread_ts && r.messages[0].thread_ts !== task.message_ts) threadTs = r.messages[0].thread_ts;
      } catch {}

      await slackClient.chat.postMessage({
        channel: task.channel_id,
        thread_ts: threadTs,
        text: `⚠ *期限切れのお知らせ* (by ${senderName})\nこのタスクは期限（${dueStr}）を過ぎています。\nご確認・対応をお願いします。`,
      });

      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /tasks/:id/notify-overdue error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /tasks/:id/incomplete-targets (broadcast 未完了者一覧) ---
  expressApp.get("/api/dashboard/tasks/:id/incomplete-targets", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });

      // 全ターゲット
      const allRes = await dbQuery(
        `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2`,
        [teamId, req.params.id]
      );
      // 完了済み
      const doneRes = await dbQuery(
        `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2`,
        [teamId, req.params.id]
      );
      const doneSet = new Set(doneRes.rows.map(r => r.user_id));
      const incomplete = allRes.rows.filter(r => !doneSet.has(r.user_id));

      const users = await Promise.all(incomplete.map(async r => {
        const name = await getUserDisplayName(teamId, r.user_id).catch(() => r.user_id);
        const avatarRes = await dbQuery(
          `SELECT profile_json->>'image_72' AS avatar FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2`,
          [teamId, r.user_id]
        );
        return { user_id: r.user_id, displayName: name, avatar_url: avatarRes.rows[0]?.avatar || null };
      }));

      res.json({ users, total: allRes.rows.length, incomplete: incomplete.length });
    } catch (e) {
      console.error("dashboard GET /tasks/:id/incomplete-targets error:", e);
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

      // Slackスレッドへ投稿（Bot名義）
      if (task.channel_id && task.message_ts) {
        try {
          const senderName = await getUserDisplayName(teamId, userId).catch(() => userId);
          const jaName = senderName.split('/')[0].trim();
          // thread_tsが異なる場合（message_tsが返信のts）は実際のrootを取得
          let threadTs = task.message_ts;
          const replies = await slackClient.conversations.replies({ channel: task.channel_id, ts: task.message_ts, limit: 1 }).catch(() => null);
          if (replies?.messages?.[0]?.thread_ts && replies.messages[0].thread_ts !== task.message_ts) {
            threadTs = replies.messages[0].thread_ts;
          }
          await slackClient.chat.postMessage({
            channel: task.channel_id,
            thread_ts: threadTs,
            text: `[TaskHub コメント by ${jaName}]\n${comment.trim()}`,
          });
        } catch (e) { console.error("Failed to post comment to Slack:", e.message); }
      }

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

  // --- PATCH /tasks/:id/group (broadcast タスクのグループ変更) ---
  expressApp.patch("/api/dashboard/tasks/:id/group", authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { group_id } = req.body || {};
      if (!group_id) return res.status(400).json({ error: "group_id_required" });

      const task = await dbGetTaskById(teamId, req.params.id);
      if (!task) return res.status(404).json({ error: "not_found" });
      if (task.task_type !== 'broadcast') return res.status(400).json({ error: "not_broadcast_task" });

      // グループのハンドル名を取得
      const subteamMap = await getSubteamIdMap(teamId).catch(() => new Map());
      const handle = subteamMap.get(group_id);
      if (!handle) return res.status(400).json({ error: "invalid_group" });

      // 新しいグループのメンバーを取得
      const memberIds = await getUsergroupMembers(teamId, group_id);

      // task_targets を差し替え
      await dbDeleteTaskTargets(teamId, req.params.id);
      if (memberIds.length > 0) await dbInsertTaskTargets(teamId, req.params.id, memberIds);

      // 新メンバーのうち完了済みの数を再計算
      const compRes = memberIds.length > 0
        ? await dbQuery(`SELECT COUNT(*)::int AS c FROM task_completions WHERE team_id=$1 AND task_id=$2 AND user_id = ANY($3)`, [teamId, req.params.id, memberIds])
        : { rows: [{ c: 0 }] };
      const completedCount = compRes.rows[0]?.c || 0;

      // タスクを更新
      await dbQuery(
        `UPDATE tasks SET broadcast_group_id=$3, broadcast_group_handle=$4, assignee_label=$5, total_count=$6, completed_count=$7, updated_at=now() WHERE team_id=$1 AND id=$2`,
        [teamId, req.params.id, group_id, handle, `@${handle}`, memberIds.length, completedCount]
      );

      const updated = await dbGetTaskById(teamId, req.params.id);
      // Slackカード更新
      if (updated?.channel_id && updated?.message_ts) {
        const blocks = await buildThreadCardBlocks({ teamId, task: updated }).catch(() => null);
        if (blocks) await upsertThreadCard(slackClient, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks }).catch(() => {});
      }
      res.json({ task: updated });
    } catch (e) {
      console.error("dashboard PATCH /tasks/:id/group error:", e);
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

  // --- GET /team-overdue (配下メンバーの期限切れタスク) ---
  expressApp.get("/api/dashboard/team-overdue", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;

      const ROLE_LEVEL = { '': 0, 'member': 0, 'sub_chief': 2, 'chief': 3, 'manager': 4, 'admin': 5 };
      const HR_ID = 'b97f72bb-a6ad-4dcc-8154-42995dcd1c0a';
      // HRの直下チーム（役割別）
      const HR_NAMED_TEAMS = ['Strategy Team', 'Management Team', 'Creative Team', 'Analytics Team', 'Direction Team'];
      // CSライン（自分のみ）
      const HR_SELF_ONLY = ['Customer Success Team', 'Operation Team'];
      // チームXX（HR直下の個人チーム）
      const HR_TEAM_XX_PREFIX = 'チーム'; // 名前でHR直下チームXXを判別
      // 特定人物が追加で見るチーム
      const BROAD_TEAMS = ['Creative Team', 'Analytics Team', 'Direction Team', 'Operation Team'];

      // 自分のチーム所属とロールを取得（チーム名・親IDも）
      const myTeamsRes = await dbQuery(
        `SELECT m.dash_team_id, m.role, t.parent_id, t.name
         FROM dash_team_members m
         JOIN dash_teams t ON t.id = m.dash_team_id AND t.team_id = m.team_id
         WHERE m.team_id = $1 AND m.user_id = $2`,
        [teamId, userId]
      );
      if (myTeamsRes.rows.length === 0) return res.json({ tasks: [] });

      const targetUserIds = new Set();

      // チームごとに HR/標準ルールを個別判断（複数部署所属に対応）
      for (const myTeam of myTeamsRes.rows) {
        const name = myTeam.name;
        const myLevel = ROLE_LEVEL[myTeam.role || ''] ?? 0;
        const isHRTeam = myTeam.parent_id === HR_ID || HR_NAMED_TEAMS.includes(name) || HR_SELF_ONLY.includes(name);

        if (isHRTeam) {
          // ── HR独自ルール ──
          if (HR_SELF_ONLY.includes(name)) continue; // CS/Operation: 自分のみ

          if (HR_NAMED_TEAMS.includes(name)) {
            // Strategy/Management/Creative/Analytics/Direction: 自チームのみ
            const r = await dbQuery(`SELECT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2 AND user_id != $3`, [teamId, myTeam.dash_team_id, userId]);
            for (const row of r.rows) targetUserIds.add(row.user_id);
          } else if (myTeam.parent_id === HR_ID && name.startsWith(HR_TEAM_XX_PREFIX)) {
            // チームXX: chief/sub_chief は自チームを見る、member は見ない
            if (myLevel >= ROLE_LEVEL['sub_chief']) {
              const r = await dbQuery(`SELECT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2 AND user_id != $3`, [teamId, myTeam.dash_team_id, userId]);
              for (const row of r.rows) targetUserIds.add(row.user_id);
            }
          }
        } else {
          // ── 標準ルール ──
          const isParent = !myTeam.parent_id;
          if (isParent) {
            const r = await dbQuery(
              `SELECT DISTINCT m.user_id FROM dash_team_members m JOIN dash_teams t ON t.id=m.dash_team_id AND t.team_id=m.team_id WHERE m.team_id=$1 AND t.parent_id=$2 AND m.user_id != $3`,
              [teamId, myTeam.dash_team_id, userId]
            );
            for (const row of r.rows) targetUserIds.add(row.user_id);
          }
          if (myLevel > 0) {
            const r = await dbQuery(`SELECT user_id, role FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2 AND user_id != $3`, [teamId, myTeam.dash_team_id, userId]);
            for (const row of r.rows) {
              if ((ROLE_LEVEL[row.role || ''] ?? 0) < myLevel) targetUserIds.add(row.user_id);
            }
          }
        }
      }

      // 特例：Management の長野・長岐、Strategy の富永 → Creative/Analytics/Direction/Operation も追加
      const inManagement = myTeamsRes.rows.some(t => t.name === 'Management Team');
      const inStrategy = myTeamsRes.rows.some(t => t.name === 'Strategy Team');
      if (inManagement || inStrategy) {
        const specialCheck = await dbQuery(
          `SELECT m.user_id FROM dash_team_members m
           JOIN dashboard_user_directory d ON d.team_id=m.team_id AND d.user_id=m.user_id
           JOIN dash_teams t ON t.id=m.dash_team_id AND t.team_id=m.team_id
           WHERE m.team_id=$1 AND m.user_id=$2 AND t.name = ANY($3)
             AND (d.display_name LIKE '%長野%' OR d.display_name LIKE '%長岐%' OR d.display_name LIKE '%富永%')`,
          [teamId, userId, inManagement ? ['Management Team'] : ['Strategy Team']]
        );
        if (specialCheck.rows.length > 0) {
          const broadRes = await dbQuery(
            `SELECT DISTINCT m.user_id FROM dash_team_members m JOIN dash_teams t ON t.id=m.dash_team_id AND t.team_id=m.team_id WHERE m.team_id=$1 AND t.name = ANY($2) AND m.user_id != $3`,
            [teamId, BROAD_TEAMS, userId]
          );
          for (const r of broadRes.rows) targetUserIds.add(r.user_id);
        }
      }

      if (targetUserIds.size === 0) return res.json({ tasks: [] });

      const overdueRes = await dbQuery(
        `SELECT t.id, t.title, t.status, t.due_date, t.assignee_id, t.task_type, t.created_at
         FROM tasks t
         WHERE t.team_id=$1 AND t.assignee_id = ANY($2)
           AND t.due_date < CURRENT_DATE AND t.status NOT IN ('done','cancelled')
           AND (t.task_type IS NULL OR t.task_type='personal')
         ORDER BY t.due_date ASC, t.assignee_id LIMIT 50`,
        [teamId, [...targetUserIds]]
      );

      const tasks = await Promise.all(overdueRes.rows.map(async t => ({
        ...t,
        assigneeName: await getUserDisplayName(teamId, t.assignee_id).then(n => n.split('/')[0].trim()).catch(() => t.assignee_id),
      })));

      res.json({ tasks });
    } catch (e) {
      console.error("dashboard GET /team-overdue error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- GET /analytics/period-summary ---
  expressApp.get("/api/dashboard/analytics/period-summary", authWithRole, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const targetUser = req.query.scope === 'self' ? userId : null;

      const getStats = async (days) => {
        const params = [teamId, targetUser || userId];
        const q = `
          SELECT
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '${days} days')::int AS assigned,
            COUNT(*) FILTER (WHERE status='done' AND completed_at >= NOW() - INTERVAL '${days} days')::int AS completed,
            COUNT(*) FILTER (WHERE status='done' AND completed_at >= NOW() - INTERVAL '${days} days'
                             AND due_date IS NOT NULL AND completed_at::date <= due_date)::int AS on_time,
            COUNT(*) FILTER (WHERE status='done' AND completed_at >= NOW() - INTERVAL '${days} days'
                             AND due_date IS NOT NULL)::int AS with_due,
            COALESCE(ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - created_at))/86400)
              FILTER (WHERE status='done' AND completed_at >= NOW() - INTERVAL '${days} days')), 0)::int AS avg_days
          FROM tasks WHERE team_id=$1 AND assignee_id=$2
        `;
        const r = await dbQuery(q, params);
        const row = r.rows[0] || {};
        const on_time = row.on_time || 0, with_due = row.with_due || 0;
        return {
          assigned: row.assigned || 0,
          completed: row.completed || 0,
          compliance_rate: with_due > 0 ? Math.round(on_time / with_due * 100) : null,
          avg_days: row.avg_days || 0,
        };
      };

      const [d7, d30] = await Promise.all([getStats(7), getStats(30)]);
      res.json({ d7, d30 });
    } catch (e) {
      console.error("analytics period-summary error:", e);
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

  // RPO案件管理API
  registerRpoApi({ expressApp, authWithRole, adminOnly });

  // 法務案件管理API
  registerLegalApi({ expressApp, authWithRole });

  // AN依頼管理API
  registerAnApi({ expressApp, authWithRole, slackApp: deps.boltApp, teamId: process.env.SLACK_TEAM_ID || 'T086C06L5V0' });

  // kintone連携API
  registerKintoneApi({ expressApp, authWithRole, adminOnly });

  // Google Drive連携API
  registerDriveApi({ expressApp, authWithRole });

  // 顧客・商談管理API
  registerCrmApi({ expressApp, authWithRole });

  // 日報メンバー管理API
  registerDailyReportApi({ expressApp, authWithRole, slackClient });

  // チャンネル別目標設定API
  registerChannelTargetsApi({ expressApp, authWithRole });

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
