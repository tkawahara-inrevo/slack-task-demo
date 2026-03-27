const crypto = require("crypto");
const { randomUUID } = require("crypto");

// ================================
// Dashboard API + Token Auth
// ================================

const authTokens = new Map();
const sessions = new Map();

const TOKEN_TTL_MS = 60 * 60 * 1000;       // 1時間（マジックリンク）
const SESSION_TTL_MS = 24 * 60 * 60 * 1000; // 24時間（セッション）

function cleanupExpired() {
  const now = Date.now();
  for (const [k, v] of authTokens) {
    if (now - v.createdAt > TOKEN_TTL_MS) authTokens.delete(k);
  }
  for (const [k, v] of sessions) {
    if (now - v.createdAt > SESSION_TTL_MS) sessions.delete(k);
  }
}
setInterval(cleanupExpired, 30 * 60 * 1000);

function generateToken(teamId, userId) {
  const token = crypto.randomBytes(32).toString("hex");
  const sessionId = crypto.randomBytes(16).toString("hex");
  authTokens.set(token, { teamId, userId, createdAt: Date.now(), sessionId });
  return token;
}

function exchangeToken(token) {
  cleanupExpired();
  const entry = authTokens.get(token);
  if (!entry) return null;
  if (Date.now() - entry.createdAt > TOKEN_TTL_MS) {
    authTokens.delete(token);
    return null;
  }
  authTokens.delete(token);
  const { teamId, userId, sessionId } = entry;
  sessions.set(sessionId, { teamId, userId, createdAt: Date.now() });
  return { teamId, userId, sessionId };
}

function validateSession(sessionId) {
  if (!sessionId) return null;
  cleanupExpired();
  const s = sessions.get(sessionId);
  if (!s) return null;
  if (Date.now() - s.createdAt > SESSION_TTL_MS) {
    sessions.delete(sessionId);
    return null;
  }
  return { teamId: s.teamId, userId: s.userId };
}

// ================================
// Middleware
// ================================
function authMiddleware(req, res, next) {
  const sessionId =
    req.cookies?.dashboard_session ||
    (req.headers.authorization || "").replace("Bearer ", "");
  const user = validateSession(sessionId);
  if (!user) return res.status(401).json({ error: "unauthorized" });
  req.dashboardUser = user;
  next();
}

// ================================
// Register API routes
// ================================
function registerDashboardApi(deps) {
  const {
    expressApp,
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
    dbAddDashTeamMember,
    dbRemoveDashTeamMember,
    dbListDashTeamMembers,
    dbGetUserDashTeams,
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
  } = deps;

  const kintone = require("./kintone-connector");

  // admin check middleware
  function adminOnly(req, res, next) {
    if (req.dashboardUser?.role !== "admin") {
      return res.status(403).json({ error: "admin_required" });
    }
    next();
  }

  // Enhance authMiddleware to attach role
  async function authWithRole(req, res, next) {
    authMiddleware(req, res, async () => {
      try {
        const { teamId, userId } = req.dashboardUser;
        const role = await dbGetDashboardRole(teamId, userId);
        req.dashboardUser.role = role;
        next();
      } catch (e) {
        console.error("authWithRole error:", e);
        res.status(500).json({ error: "internal" });
      }
    });
  }

  // Helper: get visible user_ids for non-admin
  async function getVisibleUserIds(teamId, userId) {
    const teams = await dbGetUserDashTeams(teamId, userId);
    if (!teams.length) return [userId]; // チーム未所属なら自分だけ
    const allMembers = new Set();
    for (const t of teams) {
      const members = await dbListDashTeamMembers(teamId, t.id);
      for (const m of members) allMembers.add(m.user_id);
    }
    allMembers.add(userId);
    return Array.from(allMembers);
  }

  // --- Token exchange ---
  expressApp.get("/dashboard/auth", (req, res) => {
    const { token } = req.query;
    if (!token) return res.status(400).send("Missing token");
    const result = exchangeToken(token);
    if (!result) {
      return res.status(401).send(
        "<html><body><h2>リンクの有効期限が切れています</h2>" +
        "<p>Slackで <code>/dashboard</code> を再度実行してください。</p></body></html>"
      );
    }
    res.cookie("dashboard_session", result.sessionId, {
      httpOnly: true,
      sameSite: "lax",
      maxAge: SESSION_TTL_MS,
      secure: process.env.NODE_ENV === "production",
    });
    res.redirect("/dashboard");
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

  // --- /summary (team-scoped for non-admin) ---
  expressApp.get("/api/dashboard/summary", authWithRole, async (req, res) => {
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
      const page = Math.max(1, parseInt(req.query.page, 10) || 1);
      const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 50));
      const offset = (page - 1) * limit;

      const conditions = ["t.team_id = $1"];
      const params = [teamId];
      let idx = 2;
      let joinClause = "";

      if (role !== "admin") {
        const visible = await getVisibleUserIds(teamId, userId);
        conditions.push(`t.assignee_id = ANY($${idx++})`);
        params.push(visible);
      }
      if (status) {
        conditions.push(`t.status = $${idx++}`);
        params.push(status);
      }
      if (assignee) {
        conditions.push(`t.assignee_id = $${idx++}`);
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
          conditions.push(`t.assignee_id = ANY($${idx++})`);
          params.push(members);
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
           ORDER BY t.created_at DESC
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
      const admins = await dbListDashboardAdmins(teamId);
      const result = await Promise.all(
        admins.map(async (a) => ({
          ...a,
          displayName: await getUserDisplayName(teamId, a.user_id),
        })),
      );
      res.json({ admins: result });
    } catch (e) {
      console.error("dashboard /admin/roles error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.post("/api/dashboard/admin/roles", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { userId, role } = req.body || {};
      if (!userId || !["admin", "member"].includes(role)) {
        return res.status(400).json({ error: "invalid_params" });
      }
      await dbSetDashboardRole(teamId, userId, role);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard POST /admin/roles error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  // --- Teams ---
  expressApp.get("/api/dashboard/admin/teams", authWithRole, adminOnly, async (req, res) => {
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

  expressApp.post("/api/dashboard/admin/teams", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId, userId } = req.dashboardUser;
      const { name } = req.body || {};
      if (!name?.trim()) return res.status(400).json({ error: "name_required" });
      const team = await dbCreateDashTeam(randomUUID(), teamId, name.trim(), userId);
      res.json({ team });
    } catch (e) {
      console.error("dashboard POST /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.put("/api/dashboard/admin/teams/:id", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { name } = req.body || {};
      if (!name?.trim()) return res.status(400).json({ error: "name_required" });
      const team = await dbUpdateDashTeam(teamId, req.params.id, name.trim());
      res.json({ team });
    } catch (e) {
      console.error("dashboard PUT /admin/teams error:", e);
      res.status(500).json({ error: "internal" });
    }
  });

  expressApp.delete("/api/dashboard/admin/teams/:id", authWithRole, adminOnly, async (req, res) => {
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
  expressApp.get("/api/dashboard/admin/teams/:id/members", authWithRole, adminOnly, async (req, res) => {
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

  expressApp.post("/api/dashboard/admin/teams/:id/members", authWithRole, adminOnly, async (req, res) => {
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

  expressApp.delete("/api/dashboard/admin/teams/:id/members/:userId", authWithRole, adminOnly, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      await dbRemoveDashTeamMember(teamId, req.params.id, req.params.userId);
      res.json({ ok: true });
    } catch (e) {
      console.error("dashboard DELETE team member error:", e);
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
      res.json({ task: updated });
    } catch (e) {
      console.error("dashboard PATCH /tasks/:id/status error:", e);
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
}

module.exports = {
  generateToken,
  registerDashboardApi,
};
