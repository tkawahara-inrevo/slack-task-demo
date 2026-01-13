
require("dotenv").config();
const { App } = require("@slack/bolt");
const { Pool } = require("pg");
const { randomUUID } = require("crypto");
const cron = require("node-cron");
const ExcelJS = require("exceljs");
const fs = require("fs");
const os = require("os");
const path = require("path");

// ================================
// Slack Bolt App
// ================================
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
});

// ================================
// Postgres
// ================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSL === "true" ? { rejectUnauthorized: false } : undefined,
});

// ================================
// Helpers
// ================================
function safeJsonParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function getTeamIdFromBody(body) {
  return (
    body?.team?.id ||
    body?.team_id ||
    body?.team?.team_id ||
    body?.authorizations?.[0]?.team_id ||
    null
  );
}

function getUserIdFromBody(body) {
  return body?.user?.id || body?.user_id || body?.user?.user_id || null;
}


// 通知抑止：@mk 等を表示したいが、メンション通知は飛ばしたくない（※全社タスク発行時は例外でメンションを有効にする）
function noMention(s) {
  if (!s) return "";
  return String(s).replace(/@/g, "＠");
}


function excludeUsers(base, removeList) {
  const rm = new Set(removeList || []);
  return (base || []).filter((u) => u && !rm.has(u));
}

// Home再描画を少しずつ投げる（スマホの反映遅延対策）
async function publishHomeForUsers(client, teamId, userIds, intervalMs = 200) {
  const uniq = Array.from(new Set((userIds || []).filter(Boolean)));
  for (let i = 0; i < uniq.length; i++) {
    const u = uniq[i];
    setTimeout(() => {
      publishHome({ client, teamId, userId: u }).catch(() => {});
    }, i * intervalMs);
  }
}

// 期限を YYYY/MM/DD のみにする
function formatDueDateOnly(due) {
  if (!due) return "未設定";
  if (typeof due === "string") {
    const m = due.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return `${m[1]}/${m[2]}/${m[3]}`;
    const d = new Date(due);
    if (!Number.isNaN(d.getTime())) {
      const y = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${y}/${mm}/${dd}`;
    }
    return due;
  }
  if (due instanceof Date) {
    const y = due.getFullYear();
    const mm = String(due.getMonth() + 1).padStart(2, "0");
    const dd = String(due.getDate()).padStart(2, "0");
    return `${y}/${mm}/${dd}`;
  }
  return String(due);
}

function slackDateYmd(due) {
  if (!due) return null;
  if (typeof due === "string") {
    const m = due.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
    const d = new Date(due);
    if (!Number.isNaN(d.getTime())) {
      const y = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${y}-${mm}-${dd}`;
    }
    return null;
  }
  if (due instanceof Date) {
    const y = due.getFullYear();
    const mm = String(due.getMonth() + 1).padStart(2, "0");
    const dd = String(due.getDate()).padStart(2, "0");
    return `${y}-${mm}-${dd}`;
  }
  return null;
}

function generateTitleCandidate(text, maxLen = 22) {
  if (!text) return "（タスク）";
  let s = String(text);

  s = s.replace(/\r\n/g, "\n");
  s = s.replace(/https?:\/\/\S+/g, "");
  s = s.replace(/<@[A-Z0-9]+>/g, "");
  s = s.replace(/<#[A-Z0-9]+\|[^>]+>/g, "");
  s = s.replace(/:[a-z0-9_+-]+:/gi, "");
  s = s.replace(/<!subteam\^[A-Z0-9]+(\|[^>]+)?>/g, ""); // usergroup token

  s = s.replace(/[【】\[\]（）()]/g, " ");
  s = s.replace(/\s+/g, " ").trim();

  s = s.replace(/^(すみません|恐縮ですが|お疲れ様です|取り急ぎ|ごめん|失礼|お願い|至急|急ぎ)\s*/g, "");

  const cut = s.split(/[\n。！？!?]/)[0].trim();
  let title = cut || s;
  title = title.replace(/(お願いします|ください|してもらえますか|して下さい|お願いします。?)$/g, "").trim();
  title = title.replace(/^@\S+\s*/, "");

  if (!title) title = "（タスク）";
  if (title.length > maxLen) title = title.slice(0, maxLen) + "…";
  return title;
}

async function dbQuery(text, params) {
  const client = await pool.connect();
  try {
    return await client.query(text, params);
  } finally {
    client.release();
  }
}

// ================================
// Slack text prettifier: <!subteam^ID> -> @handle
// ================================
const subteamCache = new Map(); // teamId -> { at, idToHandle: Map }
const SUBTEAM_CACHE_MS = 60 * 60 * 1000;

async function getSubteamIdMap(teamId) {
  const now = Date.now();
  const cached = subteamCache.get(teamId);
  if (cached && now - cached.at < SUBTEAM_CACHE_MS) return cached.idToHandle;

  const res = await app.client.usergroups.list({ include_users: false });
  const map = new Map();
  for (const g of res.usergroups || []) {
    if (g?.id && g?.handle) map.set(g.id, String(g.handle).replace(/^@/, ""));
  }
  subteamCache.set(teamId, { at: now, idToHandle: map });
  return map;
}

async function prettifySlackText(text, teamId) {
  if (!text) return "";
  const idToHandle = await getSubteamIdMap(teamId);

  let out = String(text).replace(/<!subteam\^([A-Z0-9]+)>/g, (m, id) => {
    const h = idToHandle.get(id);
    return h ? `@${h}` : m;
  });

  out = out.replace(/<!subteam\^([A-Z0-9]+)\|@?([^>]+)>/g, (m, id, handle) => {
    const h = idToHandle.get(id) || handle;
    return h ? `@${String(h).replace(/^@/, "")}` : m;
  });

  return out;
}


// ================================
// Slack text prettifier (user): <@UXXXX> -> @display_name (for modal readability)
// Note: Plain text inputs in modals do NOT render mrkdwn mentions, so we replace them ourselves.
// ================================
async function prettifyUserMentions(text, teamId) {
  if (!text) return "";

  const ids = Array.from(
    new Set(
      Array.from(String(text).matchAll(/<@([A-Z0-9]+)(?:\|[^>]+)?>/g)).map((m) => m[1])
    )
  );
  if (!ids.length) return String(text);

  const idToName = {};
  for (const uid of ids) {
    const name = await getUserDisplayName(teamId, uid);
    idToName[uid] = (name && String(name).trim()) ? String(name).trim() : uid;
  }

  return String(text).replace(/<@([A-Z0-9]+)(?:\|[^>]+)?>/g, (m, uid) => {
    const nm = idToName[uid] || uid;
    return `@${String(nm).replace(/^@/, "")}`;
  });
}



// ================================
// User display name cache (for assignee labels)
// ================================
const userNameCache = new Map(); // `${teamId}:${userId}` -> { at, name }
const USER_CACHE_MS = 60 * 60 * 1000;

async function getUserDisplayName(teamId, userId) {
  const key = `${teamId}:${userId}`;
  const cached = userNameCache.get(key);
  if (cached && Date.now() - cached.at < USER_CACHE_MS) return cached.name;

  try {
    const res = await app.client.users.info({ user: userId });
    const u = res?.user;
    const name =
      (u?.profile?.display_name && u.profile.display_name.trim()) ||
      (u?.real_name && u.real_name.trim()) ||
      (u?.name && String(u.name).trim()) ||
      userId;
    userNameCache.set(key, { at: Date.now(), name });
    return name;
  } catch (_) {
    return userId;
  }
}
// ================================
// Departments (A): "*-all" usergroups are department masters
// ================================
const DEPT_ALL_HANDLES = (process.env.DEPT_ALL_HANDLES || "")
  .split(",")
  .map((s) => s.trim().replace(/^@/, ""))
  .filter(Boolean);

const DEPT_PRIORITY = (process.env.DEPT_PRIORITY || "")
  .split(",")
  .map((s) => s.trim().replace(/^@/, ""))
  .filter(Boolean);

const DEPT_CACHE_TTL_MS = Number(process.env.DEPT_CACHE_TTL_SEC || "3600") * 1000;

const deptUserCache = new Map(); // `${teamId}:${userId}` -> { dept_key, dept_handle, at }
const deptGroupCache = new Map(); // teamId -> { at, deptKeys: string[], membersByDeptKey: Map }

function deptKeyFromAllHandle(handle) {
  const h = String(handle || "").replace(/^@/, "");
  return h.endsWith("-all") ? h.slice(0, -4) : h;
}

async function dbGetUserDept(teamId, userId) {
  const q = `
    SELECT team_id, user_id, dept_key, dept_handle, updated_at
    FROM user_departments
    WHERE user_id=$1
    ORDER BY (team_id=$2) DESC, updated_at DESC
    LIMIT 1;
  `;
  const res = await dbQuery(q, [userId, teamId]);
  return res.rows[0] || null;
}

async function dbUpsertUserDept(teamId, userId, dept_key, dept_handle) {
  const q = `
    INSERT INTO user_departments (team_id, user_id, dept_key, dept_handle, updated_at)
    VALUES ($1,$2,$3,$4, now())
    ON CONFLICT (user_id)
    DO UPDATE SET team_id=EXCLUDED.team_id, dept_key=EXCLUDED.dept_key, dept_handle=EXCLUDED.dept_handle, updated_at=now()
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, userId, dept_key, dept_handle]);
  return res.rows[0] || null;
}

async function fetchDeptGroups(teamId) {
  const now = Date.now();
  const cached = deptGroupCache.get(teamId);
  if (cached && now - cached.at < DEPT_CACHE_TTL_MS) return cached;

  const res = await app.client.usergroups.list({ include_users: false });
  const groups = (res.usergroups || [])
    .filter((g) => g?.id && g?.handle)
    .map((g) => ({ id: g.id, handle: String(g.handle).replace(/^@/, "") }));

  // 部署代表を決める（A）：DEPT_ALL_HANDLES があればそれだけ、なければ "*-all" を全部
  const deptHandles = (DEPT_ALL_HANDLES.length ? DEPT_ALL_HANDLES : groups.map((g) => g.handle).filter((h) => h.endsWith("-all")))
    .filter((h) => groups.some((g) => g.handle === h));

  const uniqHandles = Array.from(new Set(deptHandles)).sort((a, b) => a.localeCompare(b));

  const idByHandle = new Map(groups.map((g) => [g.handle, g.id]));
  const membersByDeptKey = new Map();

  for (const handle of uniqHandles) {
    const id = idByHandle.get(handle);
    if (!id) continue;
    try {
      const usersRes = await app.client.usergroups.users.list({ usergroup: id });
      const users = usersRes.users || [];
      const deptKey = deptKeyFromAllHandle(handle);
      membersByDeptKey.set(deptKey, new Set(users));
    } catch (e) {
      console.error("usergroups.users.list error:", e?.data || e);
    }
  }

  // 優先順位（複数所属のときの決定）
  const deptKeys = Array.from(membersByDeptKey.keys());
  let orderedKeys = deptKeys.slice().sort((a, b) => a.localeCompare(b));
  if (DEPT_PRIORITY.length) {
    const set = new Set(deptKeys);
    orderedKeys = [];
    for (const k of DEPT_PRIORITY) if (set.has(k)) orderedKeys.push(k);
    for (const k of deptKeys.sort((a, b) => a.localeCompare(b))) if (!orderedKeys.includes(k)) orderedKeys.push(k);
  }

  // insertion order を priority 順に整える
  const rebuilt = new Map();
  for (const k of orderedKeys) rebuilt.set(k, membersByDeptKey.get(k));
  const finalMembers = new Map();
  for (const [k, v] of rebuilt.entries()) if (v) finalMembers.set(k, v);

  const next = { at: now, deptKeys: orderedKeys, membersByDeptKey: finalMembers };
  deptGroupCache.set(teamId, next);
  return next;
}

async function resolveDeptForUser(teamId, userId) {
  if (!userId) return null;

  const memKey = `${teamId}:${userId}`;
  const mem = deptUserCache.get(memKey);
  if (mem && Date.now() - mem.at < DEPT_CACHE_TTL_MS) return mem.dept_key;

  try {
    const row = await dbGetUserDept(teamId, userId);
    if (row?.dept_key) {
      deptUserCache.set(memKey, { dept_key: row.dept_key, dept_handle: row.dept_handle, at: Date.now() });
      return row.dept_key;
    }
  } catch (_) {}

  const { deptKeys, membersByDeptKey } = await fetchDeptGroups(teamId);

  for (const deptKey of deptKeys) {
    const set = membersByDeptKey.get(deptKey);
    if (set && set.has(userId)) {
      const dept_key = deptKey;
      const dept_handle = `@${deptKey}`;
      try { await dbUpsertUserDept(teamId, userId, dept_key, dept_handle); } catch (_) {}
      deptUserCache.set(memKey, { dept_key, dept_handle, at: Date.now() });
      return dept_key;
    }
  }

  return null;
}

function deptLabel(dept_key) {
  if (!dept_key) return "未設定";
  return noMention(`@${dept_key}`);
}

async function listDeptKeys(teamId) {
  const { deptKeys } = await fetchDeptGroups(teamId);
  return deptKeys.slice();
}

// ================================
// Broadcast env helpers
// ================================
const BROADCAST_VIEWER_USER_IDS = (process.env.BROADCAST_VIEWER_USER_IDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// ================================
// DB: Tasks (+broadcast)
// ================================
async function dbCreateTask(task) {
  const q = `
    INSERT INTO tasks (
      id, team_id, channel_id, message_ts, source_permalink,
      title, description,
      requester_user_id, created_by_user_id,
      assignee_id, assignee_label,
      status, due_date,
      requester_dept, assignee_dept,
      task_type, broadcast_group_handle, broadcast_group_id,
      total_count, completed_count,
      notified_at,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,
      $8,$9,
      $10,$11,
      $12,$13,
      $14,$15,
      $16,$17,$18,
      $19,$20,
      $21,
      now(), now()
    )
    RETURNING *;
  `;
  const params = [
    task.id,
    task.team_id,
    task.channel_id,
    task.message_ts,
    task.source_permalink,
    task.title,
    task.description,
    task.requester_user_id,
    task.created_by_user_id,
    task.assignee_id ?? null,
    task.assignee_label ?? null,
    task.status,
    task.due_date,
    task.requester_dept ?? null,
    task.assignee_dept ?? null,
    task.task_type ?? "personal",
    task.broadcast_group_handle ?? null,
    task.broadcast_group_id ?? null,
    task.total_count ?? null,
    task.completed_count ?? 0,
    task.notified_at ?? null,
  ];
  const res = await dbQuery(q, params);
  return res.rows[0];
}

async function dbGetTaskById(teamId, taskId) {
  const q = `SELECT * FROM tasks WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0] || null;
}

async function dbListTasksForAssignee(teamId, assigneeId, status, limit = 10) {
  // Phase2: 閲覧は誰でも可能（担当者で絞らない）
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND status=$2 AND (task_type IS NULL OR task_type='personal')
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, [teamId, status, limit]);
  return res.rows;
}

async function dbListTasksForRequester(teamId, requesterId, status, limit = 10) {
  // Phase2: 閲覧は誰でも可能（依頼者で絞らない）
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND status=$2
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, [teamId, status, limit]);
  return res.rows;
}

// dept filter (personal only)
async function dbListTasksForAssigneeWithDept(teamId, assigneeId, status, deptKey, limit = 20) {
  // Phase2: 閲覧は誰でも可能（担当者で絞らない）
  if (!deptKey || deptKey === "all") {
    return await dbListTasksForAssignee(teamId, assigneeId, status, limit);
  }
  if (deptKey === "__none__") {
    const q = `
      SELECT * FROM tasks
      WHERE team_id=$1 AND status=$2 AND assignee_dept IS NULL AND (task_type IS NULL OR task_type='personal')
      ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
      LIMIT $3;
    `;
    const res = await dbQuery(q, [teamId, status, limit]);
    return res.rows;
  }
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND status=$2 AND assignee_dept=$3 AND (task_type IS NULL OR task_type='personal')
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, [teamId, status, deptKey, limit]);
  return res.rows;
}

async function dbListTasksForRequesterWithDept(teamId, requesterId, status, deptKey, limit = 20) {
  // Phase2: 閲覧は誰でも可能（依頼者で絞らない）
  if (!deptKey || deptKey === "all") {
    return await dbListTasksForRequester(teamId, requesterId, status, limit);
  }
  if (deptKey === "__none__") {
    const q = `
      SELECT * FROM tasks
      WHERE team_id=$1 AND status=$2 AND requester_dept IS NULL
      ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
      LIMIT $3;
    `;
    const res = await dbQuery(q, [teamId, status, limit]);
    return res.rows;
  }
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND status=$2 AND requester_dept=$3
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, [teamId, status, deptKey, limit]);
  return res.rows;
}

async function dbUpdateStatus(teamId, taskId, status) {
  const q = `
    UPDATE tasks
    SET status=$3,
        completed_at = CASE WHEN $3='done' THEN now() ELSE completed_at END,
        updated_at = now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, taskId, status]);
  return res.rows[0] || null;
}


async function dbUpdateTaskContent(teamId, taskId, patch) {
  const q = `
    UPDATE tasks
    SET
      assignee_id = COALESCE($3, assignee_id),
      assignee_dept = COALESCE($4, assignee_dept),
      due_date = $5,
      description = COALESCE($6, description),
      updated_at = now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  // due_date は「未設定にする」ケースがあるため COALESCE しない
  const res = await dbQuery(q, [
    teamId,
    taskId,
    patch?.assignee_id ?? null,
    patch?.assignee_dept ?? null,
    patch?.due_date ?? null,
    patch?.description ?? null,
  ]);
  return res.rows[0] || null;
}



async function dbCancelTask(teamId, taskId, actorUserId) {
  const q = `
    UPDATE tasks
    SET status='cancelled',
        cancelled_at=now(),
        cancelled_by_user_id=$3,
        updated_at=now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, taskId, actorUserId]);
  return res.rows[0] || null;
}

async function dbUpdateBroadcastCounts(teamId, taskId, completedCount, totalCount) {
  const q = `
    UPDATE tasks
    SET completed_count=$3,
        total_count = COALESCE(total_count, $4),
        updated_at=now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, taskId, completedCount, totalCount ?? null]);
  return res.rows[0] || null;
}

async function dbMarkBroadcastDoneIfComplete(teamId, taskId) {
  const q = `
    UPDATE tasks
    SET status='done',
        completed_at=now(),
        updated_at=now()
    WHERE team_id=$1 AND id=$2 AND status <> 'done'
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0] || null;
}

// ================================
// DB: Broadcast targets/completions/watchers
// ================================
async function dbInsertTaskTargets(teamId, taskId, userIds) {
  if (!userIds?.length) return;
  const values = [];
  const params = [];
  let i = 1;
  for (const uid of userIds) {
    params.push(taskId, teamId, uid);
    values.push(`($${i++},$${i++},$${i++})`);
  }
  const q = `
    INSERT INTO task_targets (task_id, team_id, user_id)
    VALUES ${values.join(",")}
    ON CONFLICT (task_id, user_id) DO NOTHING;
  `;
  await dbQuery(q, params);
}

async function dbInsertTaskWatchers(teamId, taskId, userIds) {
  if (!userIds?.length) return;
  const values = [];
  const params = [];
  let i = 1;
  for (const uid of userIds) {
    params.push(taskId, teamId, uid);
    values.push(`($${i++},$${i++},$${i++})`);
  }
  const q = `
    INSERT INTO task_watchers (task_id, team_id, user_id)
    VALUES ${values.join(",")}
    ON CONFLICT (task_id, user_id) DO NOTHING;
  `;
  await dbQuery(q, params);
}

async function dbIsUserTarget(teamId, taskId, userId) {
  const q = `SELECT 1 FROM task_targets WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId, userId]);
  return !!res.rows[0];
}

async function dbHasUserCompleted(teamId, taskId, userId) {
  const q = `SELECT 1 FROM task_completions WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId, userId]);
  return !!res.rows[0];
}

async function dbUpsertCompletion(teamId, taskId, userId) {
  const q = `
    INSERT INTO task_completions (task_id, team_id, user_id)
    VALUES ($1,$2,$3)
    ON CONFLICT (task_id, user_id) DO NOTHING;
  `;
  await dbQuery(q, [taskId, teamId, userId]);
}

async function dbCountTargets(teamId, taskId) {
  const q = `SELECT COUNT(*)::int AS c FROM task_targets WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0]?.c ?? 0;
}
async function dbCountCompletions(teamId, taskId) {
  const q = `SELECT COUNT(*)::int AS c FROM task_completions WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0]?.c ?? 0;
}

async function dbListBroadcastTasksForUser(teamId, userId, status, limit = 20, deptKey = "all") {
  // Phase2: 閲覧は誰でも可能（対象者/依頼者で絞らない）
  let whereDept = "";
  const params = [teamId, status, limit];
  if (deptKey && deptKey !== "all") {
    if (deptKey === "__none__") {
      whereDept = "AND t.requester_dept IS NULL";
    } else {
      whereDept = "AND t.requester_dept = $4";
      params.push(deptKey);
    }
  }
  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND t.task_type='broadcast'
      AND t.status=$2
      ${whereDept}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbListWatchedTasks(teamId, userId, status, limit = 20, deptKey = "all") {
  let whereDept = "";
  const params = [teamId, userId, status, limit];
  if (deptKey && deptKey !== "all") {
    if (deptKey === "__none__") {
      whereDept = "AND t.requester_dept IS NULL";
    } else {
      whereDept = "AND t.requester_dept = $5";
      params.push(deptKey);
    }
  }
  const q = `
    SELECT t.*
    FROM tasks t
    JOIN task_watchers tw ON tw.task_id::text = t.id AND tw.team_id=t.team_id
    WHERE t.team_id=$1
      AND tw.user_id=$2
      AND t.status=$3
      ${whereDept}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

// ================================
// DB: Thread Cards
// ================================
async function dbGetThreadCard(teamId, channelId, parentTs) {
  const q = `
    SELECT * FROM thread_cards
    WHERE team_id=$1 AND channel_id=$2 AND parent_ts=$3
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, channelId, parentTs]);
  return res.rows[0] || null;
}

async function dbUpsertThreadCard(teamId, channelId, parentTs, cardTs) {
  const existing = await dbGetThreadCard(teamId, channelId, parentTs);
  if (existing) {
    const q = `
      UPDATE thread_cards
      SET card_ts=$4, updated_at=now()
      WHERE team_id=$1 AND channel_id=$2 AND parent_ts=$3
      RETURNING *;
    `;
    const res = await dbQuery(q, [teamId, channelId, parentTs, cardTs]);
    return res.rows[0];
  } else {
    const q = `
      INSERT INTO thread_cards (id, team_id, channel_id, parent_ts, card_ts, updated_at)
      VALUES ($1,$2,$3,$4,$5, now())
      RETURNING *;
    `;
    const res = await dbQuery(q, [randomUUID(), teamId, channelId, parentTs, cardTs]);
    return res.rows[0];
  }
}

// ================================
// UI pieces
// ================================
const STATUS_OPTIONS = [
  { value: "open", text: "未着手" },
  { value: "in_progress", text: "対応中" },
  { value: "waiting", text: "確認待ち" },
  { value: "done", text: "完了" },
];

function statusLabel(s) {
  const f = STATUS_OPTIONS.find((x) => x.value === s);
  if (f) return f.text;
  if (s === "cancelled") return "取り下げ";
  return s || "-";
}

// broadcast: 進捗から状態を算出（ユーザーに status を意識させない）
function calcBroadcastStateLabel(task) {
  const status = task?.status;
  if (status === "cancelled") return "取り下げ";
  if (status === "done") return "完了";
  const total = Number(task?.total_count || 0);
  const done = Number(task?.completed_count || 0);
  if (total > 0 && done >= total) return "確認待ち";
  if (done > 0) return "対応中";
  return "未着手";
}

function calcBroadcastStateKey(task) {
  const label = calcBroadcastStateLabel(task);
  switch (label) {
    case "未着手":
      return "open";
    case "対応中":
      return "in_progress";
    case "確認待ち":
      return "waiting";
    case "取り下げ":
      return "cancelled";
    case "完了":
      return "done";
    default:
      return "open";
  }
}

function statusSelectElement(currentStatus) {
  return {
    type: "static_select",
    action_id: "status_select",
    placeholder: { type: "plain_text", text: "ステータス" },
    initial_option: (() => {
      const opt = STATUS_OPTIONS.find((o) => o.value === currentStatus) || STATUS_OPTIONS[0];
      return { text: { type: "plain_text", text: opt.text }, value: opt.value };
    })(),
    options: STATUS_OPTIONS.map((o) => ({
      text: { type: "plain_text", text: o.text },
      value: o.value,
    })),
  };
}

function assigneeDisplay(task) {
  if (task?.task_type === "broadcast") {
    // 表示は通知抑止（発行時通知は別で行う）
    return noMention(task.assignee_label || "（複数対象）");
  }
  return `<@${task.assignee_id}>`;
}

function progressLabel(task) {
  const total = Number(task.total_count || 0);
  const done = Number(task.completed_count || 0);
  if (!total) return "0/0";
  return `${done} / ${total}`;
}

async function safeEphemeral(client, channelId, userId, text) {
  try {
    await client.chat.postEphemeral({ channel: channelId, user: userId, text });
  } catch (_) {}
}

async function postDM(userId, text) {
  if (!userId) return;
  try {
    const dm = await app.client.conversations.open({ users: userId });
    const channel = dm.channel?.id;
    if (!channel) return;
    await app.client.chat.postMessage({ channel, text });
  } catch (_) {}
}


async function postRequesterConfirmDM({ teamId, taskId, requesterUserId, title }) {
  if (!requesterUserId) return;
  try {
    const dm = await app.client.conversations.open({ users: requesterUserId });
    const channel = dm.channel?.id;
    if (!channel) return;

    const value = JSON.stringify({ teamId, taskId });
    await app.client.chat.postMessage({
      channel,
      text: `🎉 全員が完了しました！「${noMention(title)}」の確認をお願いします。`,
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: `🎉 *全員が完了しました！*\n「*${noMention(title)}*」の確認をお願いします。` } },
        { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "確認完了 ✅" }, style: "primary", action_id: "confirm_broadcast_done", value }] },
      ],
    });
  } catch (e) {
    console.error("postRequesterConfirmDM error:", e?.data || e);
  }
}


// ================================
// Thread Card (upsert)
// ================================
async function upsertThreadCard(client, { teamId, channelId, parentTs, blocks }) {
  const existing = await dbGetThreadCard(teamId, channelId, parentTs);
  if (existing?.card_ts) {
    await client.chat.update({
      channel: channelId,
      ts: existing.card_ts,
      text: "タスク表示（更新）",
      blocks,
    });
    return existing.card_ts;
  }

  const res = await client.chat.postMessage({
    channel: channelId,
    thread_ts: parentTs,
    text: "タスク表示",
    blocks,
  });

  const cardTs = res?.ts;
  if (cardTs) await dbUpsertThreadCard(teamId, channelId, parentTs, cardTs);
  return cardTs;
}

// ★要望②：スレッドから完了ボタン削除（詳細からのみ）
async function buildThreadCardBlocks({ teamId, task }) {
  const src = task.source_permalink
    ? `<${task.source_permalink}|元メッセージを開く>`
    : noMention(`> ${(task.description || "").slice(0, 140)}`);

  // スレッド側の「詳細」は閲覧専用（操作は Home から）
  const payload = JSON.stringify({
    teamId,
    taskId: task.id,
    origin: "thread",
  });

  const common = [
    { type: "header", text: { type: "plain_text", text: "⏱ タスク" } },
    { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
    { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },
  ];
  //if (task.task_type !== "broadcast") {
  //  common.push({ type: "section", text: { type: "mrkdwn", text: `*対応者部署*：${deptLabel(task.assignee_dept)}` } });
  //}

  return [
    ...common,
    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: `*元メッセージ*\n${src}` } },
    { type: "divider" },
    {
      type: "actions",
      elements: [
        { type: "button", text: { type: "plain_text", text: "詳細を開く" }, action_id: "open_detail_modal", value: payload },
      ],
    },
    { type: "context", elements: [{ type: "mrkdwn", text: "✅ 操作は「詳細」画面から行います（誤操作防止）" }] },
  ];
}

// ================================
// Detail Modal（views.open）
// ================================
async function buildDetailModalView({ teamId, task, viewerUserId, origin = "home" }) {
  const srcLinesRaw = (task.description || "").split("\n").slice(0, 10).join("\n") || "（本文なし）";
  const srcLines = noMention(srcLinesRaw);

  const canCancel = task.status !== "done" && task.status !== "cancelled";
  const isBroadcast = task.task_type === "broadcast";

  // スレッドから開いた「詳細」は閲覧専用（操作は Home/一覧から）
  const isReadOnly = origin === "thread";

  // personal のステータス変更は「依頼者 or 対応者」のみ
  const canEditPersonalStatus = !isReadOnly && !isBroadcast && (viewerUserId === task.requester_user_id || viewerUserId === task.assignee_id);

  const meta = { teamId, taskId: task.id, origin };
  const blocks = [
    { type: "header", text: { type: "plain_text", text: "📘 タスク" } },
    { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
    { type: "divider" },

    { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
    { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },

    { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
  ];

  if (isBroadcast) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: `*進捗*：${progressLabel(task)}` } });
  }

  blocks.push({ type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } });
  blocks.push({ type: "divider" });

  // ステータス変更：personalのみ（ウォッチャーは不可）
  if (canEditPersonalStatus) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*ステータス変更*" }, accessory: statusSelectElement(task.status === "cancelled" ? "open" : task.status) });
    blocks.push({ type: "divider" });
  } else if (!isBroadcast && !isReadOnly) {
    blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: "👀 このタスクは閲覧のみです（ステータス変更は依頼者/対応者のみ）" }] });
    blocks.push({ type: "divider" });
  }

  // ★復活：元メッセージへ（permalinkがある場合のみ表示）
  if (task?.source_permalink) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: `🔗 <${task.source_permalink}|元メッセージへ>` } });
    blocks.push({ type: "divider" });
  }

  blocks.push({ type: "section", text: { type: "mrkdwn", text: `*タスク内容*\n\`\`\`\n${srcLines}\n\`\`\`` } });

// ★追加：タスク内容の編集（personal: 依頼者/対応者, broadcast: 依頼者のみ / thread起点は表示しない）
if (!isReadOnly) {
  const canEditTask =
    (!isBroadcast && (viewerUserId === task.requester_user_id || viewerUserId === task.assignee_id)) ||
    (isBroadcast && viewerUserId === task.requester_user_id);

  if (canEditTask) {
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          action_id: "open_edit_task_modal",
          text: { type: "plain_text", text: "内容を編集" },
          value: JSON.stringify({ teamId, taskId: task.id }),
        },
      ],
    });
  }
}




// ===== コメント表示（元メッセージの下）=====
let __comments = [];
try {
  __comments = await dbListTaskComments(teamId, task.id, 10);
} catch (e) {
  console.error("load comments error", e);
}

blocks.push({ type: "divider" });
blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🗨 コメント*" } });

if (!__comments.length) {
  blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: "（コメントなし）" }] });
} else {
  for (const c of __comments) {
    const name = await getUserDisplayName(teamId, c.user_id);
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: `*${name}*\n${noMention(c.comment)}` },
    });
  }
}

if (!isReadOnly) {
  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        action_id: "open_comment_modal",
        text: { type: "plain_text", text: "コメントを書く" },
        value: JSON.stringify({ teamId, taskId: task.id }),
      },
    ],
  });
}

blocks.push({ type: "divider" });
// ===== コメント表示ここまで =====


  // actions（スレッド起点は操作なし）
  if (!isReadOnly) {
    const base = { teamId, taskId: task.id };
    const actions = [];

    if (isBroadcast) {
      const isTarget = await dbIsUserTarget(teamId, task.id, viewerUserId);
      const already = await dbHasUserCompleted(teamId, task.id, viewerUserId);
      if (isTarget && !already && task.status !== "done" && task.status !== "cancelled") {
        actions.push({ type: "button", text: { type: "plain_text", text: "自分の完了 ✅" }, style: "primary", action_id: "complete_task", value: JSON.stringify(base) });
      } else if (isTarget && already) {
        actions.push({ type: "button", text: { type: "plain_text", text: "完了済み ✅" }, action_id: "noop", value: "noop" });
      }
      // 完了者/未完了者：依頼者/管理者向け（ウォッチャーは閲覧OKだが、操作導線は Home 側に寄せる）
      const canSeeProgressList = true; // 仕様変更：誰でも閲覧可
      if (canSeeProgressList) {
        actions.push({ type: "button", text: { type: "plain_text", text: "完了/未完了一覧" }, action_id: "open_progress_modal", value: JSON.stringify(base) });
      }

      // 依頼者の確認完了（全員完了→確認待ちのとき）
      if (task.status === "waiting" && task.requester_user_id === viewerUserId) {
        actions.push({ type: "button", text: { type: "plain_text", text: "確認完了 ✅" }, style: "primary", action_id: "confirm_broadcast_done", value: JSON.stringify(base) });
      }
    }

    if (actions.length) {
      blocks.push({ type: "actions", elements: actions });
    }

    // 取り下げは依頼者のみ
    if (canCancel && task.requester_user_id === viewerUserId) {
      blocks.push({
        type: "actions",
        elements: [{ type: "button", text: { type: "plain_text", text: "取り下げ（依頼者のみ）" }, style: "danger", action_id: "cancel_task", value: JSON.stringify(base) }],
      });
    }
  }

  return {
    type: "modal",
    callback_id: "detail_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "タスク" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

async function openDetailModal(client, { trigger_id, teamId, taskId, viewerUserId, origin = "home", isFromModal = false }) {
  const task = await dbGetTaskById(teamId, taskId);
  if (!task) return;

  const view = await buildDetailModalView({ teamId, task, viewerUserId, origin });

  // モーダル上のボタンからは views.open ではなく views.push（Slack仕様）
  if (isFromModal) {
    await client.views.push({ trigger_id, view });
    return;
  }

  await client.views.open({ trigger_id, view });
}

// watcher helper
async function dbIsWatcher(teamId, taskId, userId) {
  const q = `SELECT 1 FROM task_watchers WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId, userId]);
  return !!res.rows[0];
}

// ================================
// Home: filters (Phase3)
// ================================

// Homeの状態を保持（ユーザーごと）
// key: `${teamId}:${userId}`
const homeState = new Map();

// View種類
const HOME_VIEWS = [
  { key: "personal", label: "個人タスク" },
  { key: "broadcast", label: "全社/複数タスク" },
];

// 状態（表示範囲）
const HOME_SCOPES = [
  { key: "active", label: "未完了" }, // done以外すべて
  { key: "done", label: "完了" },
];

// broadcast: 範囲（Phase8-3）
const BROADCAST_SCOPES = [
  { key: "to_me", label: "自分あて" },
  { key: "requested_by_me", label: "自分が発行" },
  { key: "all", label: "すべて" },
];


// personal: 範囲（PhaseX）
const PERSONAL_SCOPES = [
  { key: "to_me", label: "自分が対応" },
  { key: "requested_by_me", label: "自分が発行" },
  { key: "all", label: "すべて" },
];

// 未完了 = done以外
const NON_DONE_STATUSES = ["open", "in_progress", "waiting", "cancelled"];

function getHomeState(teamId, userId) {
  const k = `${teamId}:${userId}`;
  const s =
    homeState.get(k) || {
      viewKey: "personal",
      scopeKey: "active",
      personalScopeKey: "to_me",
      assigneeUserId: userId,
      deptKey: "all",
      broadcastScopeKey: "to_me",
    };
  return s;
}


function setHomeState(teamId, userId, next) {
  const k = `${teamId}:${userId}`;
  homeState.set(k, { ...getHomeState(teamId, userId), ...next });
}

function homeViewSelectElement(activeKey) {
  const cur = HOME_VIEWS.find((v) => v.key === activeKey) || HOME_VIEWS[0];
  return {
    type: "static_select",
    action_id: "home_view_select",
    initial_option: { text: { type: "plain_text", text: cur.label }, value: cur.key },
    options: HOME_VIEWS.map((v) => ({ text: { type: "plain_text", text: v.label }, value: v.key })),
  };
}

function homeScopeSelectElement(scopeKey) {
  const cur = HOME_SCOPES.find((s) => s.key === scopeKey) || HOME_SCOPES[0];
  return {
    type: "static_select",
    action_id: "home_scope_select",
    initial_option: { text: { type: "plain_text", text: cur.label }, value: cur.key },
    options: HOME_SCOPES.map((s) => ({ text: { type: "plain_text", text: s.label }, value: s.key })),
  };
}

function broadcastScopeSelectElement(scopeKey) {
  const cur = BROADCAST_SCOPES.find((s) => s.key === scopeKey) || BROADCAST_SCOPES[0];
  return {
    type: "static_select",
    action_id: "home_broadcast_scope_select",
    initial_option: { text: { type: "plain_text", text: cur.label }, value: cur.key },
    options: BROADCAST_SCOPES.map((s) => ({ text: { type: "plain_text", text: s.label }, value: s.key })),
  };
}

function personalScopeSelectElement(scopeKey) {
  const cur = PERSONAL_SCOPES.find((s) => s.key === scopeKey) || PERSONAL_SCOPES[0];
  return {
    type: "static_select",
    action_id: "home_personal_scope_select",
    initial_option: { text: { type: "plain_text", text: cur.label }, value: cur.key },
    options: PERSONAL_SCOPES.map((s) => ({ text: { type: "plain_text", text: s.label }, value: s.key })),
  };
}

function deptSelectElement(currentDeptKey, deptKeys) {
  const options = [
    { text: { type: "plain_text", text: "すべて" }, value: "all" },
    { text: { type: "plain_text", text: "未設定" }, value: "__none__" },
    ...deptKeys.map((k) => ({ text: { type: "plain_text", text: `@${k}` }, value: k })),
  ];

  const text =
    currentDeptKey === "all"
      ? "すべて"
      : currentDeptKey === "__none__"
        ? "未設定"
        : `@${currentDeptKey}`;

  return {
    type: "static_select",
    action_id: "home_dept_select",
    initial_option: { text: { type: "plain_text", text }, value: currentDeptKey || "all" },
    options,
  };
}

// personal: 担当者（任意） + 担当部署（ユーザーグループ） + 状態（done以外/ done）
async function dbListPersonalTasksByAssigneeFiltered(teamId, assigneeId, statuses, deptKey = "all", limit = 30) {
  // 未完了/完了の判定は statuses で渡す（未完了=done以外すべて）
  const params = [teamId, statuses, limit];
  const where = [];

  // 担当者が指定されている場合はそれを優先（※部署×担当者の厳密ルールは後で検討）
  if (assigneeId) {
    params.push(assigneeId);
    where.push(`AND t.assignee_id = $${params.length}`);
  } else if (deptKey && deptKey !== "all") {
    // 担当部署＝Slackユーザーグループ（@mk など）に所属するユーザーのタスク
    if (deptKey === "__none__") {
      // 部署未設定の意味付けは今は使わない（0件）
      return [];
    }
    const { membersByDeptKey } = await fetchDeptGroups(teamId);
    const membersSet = membersByDeptKey.get(deptKey);
    const members = membersSet ? Array.from(membersSet) : [];
    if (!members.length) return [];
    params.push(members);
    where.push(`AND t.assignee_id = ANY($${params.length}::text[])`);
  }

  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND (t.task_type IS NULL OR t.task_type='personal')
      AND t.status = ANY($2::text[])
      ${where.join(" ")}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbListBroadcastTasksByStatuses(teamId, statuses, deptKey = "all", limit = 30) {
  const params = [teamId, statuses, limit];
  let whereDept = "";
  if (deptKey && deptKey !== "all") {
    if (deptKey === "__none__") {
      whereDept = "AND t.requester_dept IS NULL";
    } else {
      whereDept = "AND t.requester_dept = $4";
      params.push(deptKey);
    }
  }
  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND t.task_type='broadcast'
      AND t.status = ANY($2::text[])
      ${whereDept}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

// Phase8-3: broadcast 範囲フィルタ（依頼部署フィルタは廃止）
async function dbListBroadcastTasksByStatusesWithScope(teamId, statuses, scopeKey, viewerUserId, limit = 30) {
  const params = [teamId, statuses, limit];
  let joinTargets = "";
  let whereScope = "";

  if (scopeKey === "to_me") {
    // 対象者に自分を含む
    joinTargets = "JOIN task_targets tt ON tt.task_id::text = t.id AND tt.team_id=t.team_id";
    whereScope = "AND tt.user_id = $4";
    params.push(viewerUserId);
  } else if (scopeKey === "requested_by_me") {
    // 依頼者が自分
    whereScope = "AND t.requester_user_id = $4";
    params.push(viewerUserId);
  } else {
    // all: no scope filter
  }

  const q = `
    SELECT t.*
    FROM tasks t
    ${joinTargets}
    WHERE t.team_id=$1
      AND t.task_type='broadcast'
      AND t.status = ANY($2::text[])
      ${whereScope}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

// PhaseX: personal 範囲フィルタ（to_me / requested_by_me / all）
async function dbListPersonalTasksByStatusesWithScope(teamId, statuses, scopeKey, viewerUserId, limit = 60) {
  const params = [teamId, statuses, limit];
  let whereScope = "";

  if (scopeKey === "to_me") {
    whereScope = "AND t.assignee_id = $4";
    params.push(viewerUserId);
  } else if (scopeKey === "requested_by_me") {
    whereScope = "AND t.requester_user_id = $4";
    params.push(viewerUserId);
  } else {
    // all: no scope filter
  }

  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND (t.task_type IS NULL OR t.task_type='personal')
      AND t.status = ANY($2::text[])
      ${whereScope}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}



async function fetchListTasks({ teamId, viewType, userId, status, limit, deptKey }) {
  if (viewType === "requested") {
    return await dbListTasksForRequesterWithDept(teamId, userId, status, deptKey, limit);
  }
  if (viewType === "broadcast") {
    return await dbListBroadcastTasksForUser(teamId, userId, status, limit, deptKey);
  }
  return await dbListTasksForAssigneeWithDept(teamId, userId, status, deptKey, limit);
}

function taskLineForHome(task, viewKey) {
  // 既存表示文言は維持しつつ、「元メッセージへ」リンクだけ追加（source_permalink がある場合のみ）
  let base = "";
  if (viewKey === "broadcast") {
    base = `*${noMention(task.title)}*
期限：${formatDueDateOnly(task.due_date)} / 進捗：${progressLabel(task)} / 依頼者：<@${task.requester_user_id}>`;
  } else {
    // personal
    base = `*${noMention(task.title)}*
期限：${formatDueDateOnly(task.due_date)} / 依頼者：<@${task.requester_user_id}>`;
  }

  if (task?.source_permalink) {
    base += `
🔗 <${task.source_permalink}|元メッセージへ>`;
  }
  return base;
}


async function publishHome({ client, teamId, userId }) {
  const st = getHomeState(teamId, userId);
  const deptKeys = await listDeptKeys(teamId);

  const statuses = st.scopeKey === "done" ? ["done"] : NON_DONE_STATUSES;

  const blocks = [];

  // 表示
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*表示*" },
    accessory: homeViewSelectElement(st.viewKey),
  });

  // 範囲（personal/broadcast）
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*範囲*" },
    accessory:
      st.viewKey === "broadcast"
        ? broadcastScopeSelectElement(st.broadcastScopeKey || "to_me")
        : personalScopeSelectElement(st.personalScopeKey || "to_me"),
  });

  // 状態（未完了/完了）
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*状態*" },
    accessory: homeScopeSelectElement(st.scopeKey),
  });

  // 範囲=すべて のときだけ、検索UIを出す（personalのみ）
  if (st.viewKey === "personal" && (st.personalScopeKey || "to_me") === "all") {
    // 担当部署
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: "*担当部署*" },
      accessory: deptSelectElement(st.deptKey || "all", deptKeys),
    });

    // 担当者（空欄=全員対象）
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: "*担当者*" },
      accessory: {
        type: "external_select",
        action_id: "home_person_assignee_select",
        placeholder: { type: "plain_text", text: "担当者を検索" },
        min_query_length: 0,
        ...(st.assigneeUserId
          ? (() => {
              const u = (homeUserListCache.get(teamId)?.users || []).find((x) => x.id === st.assigneeUserId);
              return u
                ? { initial_option: { text: { type: "plain_text", text: u.name }, value: u.id } }
                : {};
            })()
          : {}),
      },
    });
  }

  blocks.push({ type: "divider" });


  // Phase8-5: 操作ボタン配置調整（担当者クリア＋フィルタリセットを横並び）
  blocks.push({
    type: "actions",
    elements: [
      ...(st.viewKey === "personal" && (st.personalScopeKey || "to_me") === "all"
        ? [
            {
              type: "button",
              action_id: "home_person_assignee_clear",
              text: { type: "plain_text", text: "担当者クリア" },
              value: "clear",
            },
          ]
        : []),
      {
        type: "button",
        action_id: "home_reset_filters",
        text: { type: "plain_text", text: "リセット" },
        value: "reset",
      },
      ...(st.viewKey === "personal" && (st.personalScopeKey || "to_me") === "all"
        ? [
            {
              type: "button",
              action_id: "gantt_export",
              text: { type: "plain_text", text: "ガント出力" },
              value: JSON.stringify({
                teamId,
                userId,
                viewKey: st.viewKey,
                scopeKey: st.scopeKey,
                deptKey: st.deptKey || "all",
                assigneeUserId: st.assigneeUserId || null,
              }),
            },
          ]
        : []),
    ],
  });


  blocks.push({ type: "divider" });

  // データ取得
  let tasks = [];
  if (st.viewKey === "broadcast") {
    tasks = await dbListBroadcastTasksByStatusesWithScope(teamId, statuses, st.broadcastScopeKey || "to_me", userId, 60);
  } else {
    const scope = st.personalScopeKey || "to_me";
    if (scope === "all") {
      const assigneeId = st.assigneeUserId || null;
      tasks = await dbListPersonalTasksByAssigneeFiltered(teamId, assigneeId, statuses, st.deptKey || "all", 60);
    } else {
      tasks = await dbListPersonalTasksByStatusesWithScope(teamId, statuses, scope, userId, 60);
    }
  }

  // 表示：未完了はステータス別に分ける（doneはまとめ）
  if (st.scopeKey === "done") {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*✅ 完了*" } });
    if (!tasks.length) {
      blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: "（完了なし）" }] });
    } else {
      for (const t of tasks) {
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: taskLineForHome(t, st.viewKey) },
          accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
        });
          // タスクごとの区切り（薄めの罫線：dividerではなくテキストで差を付ける）
          blocks.push({
            type: "context",
            elements: [{ type: "mrkdwn", text: "────────────────────────" }],
          });
      }
    }
  } else {
    const by = (s) => tasks.filter((t) => (st.viewKey === "broadcast" ? calcBroadcastStateKey(t) : t.status) === s);
    const sections = [
      { status: "open", title: "*🟦 未着手*" },
      { status: "in_progress", title: "*🟨 対応中*" },
      { status: "waiting", title: "*🟧 確認待ち*" },
      { status: "cancelled", title: "*🟥 取り下げ*" },
    ];

    for (const sec of sections) {
      const items = by(sec.status);
      blocks.push({ type: "section", text: { type: "mrkdwn", text: sec.title } });
      if (!items.length) {
        blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: "（なし）" }] });
      } else {
        for (const t of items) {
          blocks.push({
            type: "section",
            text: { type: "mrkdwn", text: taskLineForHome(t, st.viewKey) },
            accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
          });
          // タスクごとの区切り（薄めの罫線：dividerではなくテキストで差を付ける）
          blocks.push({
            type: "context",
            elements: [{ type: "mrkdwn", text: "────────────────────────" }],
          });
        }
      }
      blocks.push({ type: "divider" });
    }
  }

  await client.views.publish({
    user_id: userId,
    view: {
      type: "home",
      callback_id: "home",
      blocks,
    },
  });
}

// ================================
// Broadcast: usergroup options (external_multi_select)
// ================================
async function searchUsergroups(query) {
  const res = await app.client.usergroups.list({ include_users: false });
  const groups = (res.usergroups || [])
    .filter((g) => g?.id && g?.handle)
    .map((g) => ({ id: g.id, handle: String(g.handle).replace(/^@/, "") }));

  const q = String(query || "").toLowerCase().trim();
  const filtered = !q
    ? groups
    : groups.filter((g) => g.handle.toLowerCase().includes(q));

  // 上限はSlack推奨に合わせて適当に絞る
  return filtered.slice(0, 100);
}

app.options("assignee_groups_select", async ({ ack, payload }) => {
  try {
    const q = payload?.value || "";
    const groups = await searchUsergroups(q);
    await ack({
      options: groups.map((g) => ({
        text: { type: "plain_text", text: `@${g.handle}` },
        value: g.id,
      })),
    });
  } catch (e) {
    console.error("options error:", e?.data || e);
    await ack({ options: [] });
  }
});

// ================================
// Home personal assignee (external_select) options
// - If deptKey != "all": only members in that dept (usergroup) are candidates
// - If deptKey == "all": A案として、未入力時は候補を出さない（検索して選ぶ）
// ================================
const HOME_USERLIST_CACHE_MS = 5 * 60 * 1000;
const homeUserListCache = new Map(); // teamId -> { at, users: [{id, name}] }

async function listUsersCached(teamId) {
  const now = Date.now();
  const cached = homeUserListCache.get(teamId);
  if (cached && now - cached.at < HOME_USERLIST_CACHE_MS) return cached.users;

  const res = await app.client.users.list();
  const users = (res.members || [])
    .filter((u) => u && !u.deleted && !u.is_bot)
    .map((u) => {
      const name =
        (u.profile?.display_name && u.profile.display_name.trim()) ||
        (u.real_name && u.real_name.trim()) ||
        (u.name && String(u.name).trim()) ||
        u.id;
      return { id: u.id, name };
    });

  homeUserListCache.set(teamId, { at: now, users });
  return users;
}

app.options("home_person_assignee_select", async ({ ack, body, payload }) => {
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user?.id;
    const st = getHomeState(teamId, userId);
    const deptKey = st?.deptKey || "all";

    const q = String(payload?.value || "").trim().toLowerCase();
    // 初期候補：未入力でも上位5件を返す（担当部署があればその所属から、なければ全員から）
    const allUsers = await listUsersCached(teamId);

    // dept 絞り込み用の許可集合（null=絞り込みなし）
    let allowed = null;
    if (deptKey && deptKey !== "all" && deptKey !== "__none__") {
      const { membersByDeptKey } = await fetchDeptGroups(teamId);
      const set = membersByDeptKey.get(deptKey);
      allowed = set ? new Set(Array.from(set)) : new Set();
    } else if (deptKey === "__none__") {
      // 未設定を実用にしていないため候補なし
      await ack({ options: [] });
      return;
    }

    const filtered = allUsers
      .filter((u) => {
        if (allowed && !allowed.has(u.id)) return false;
        if (!q) return true; // dept指定時は空検索でも候補を出す
        return u.name.toLowerCase().includes(q);
      })
      .sort((a,b)=>{ if(a.id===userId) return -1; if(b.id===userId) return 1; return a.name.localeCompare(b.name); }).slice(0, q ? 100 : 5)
      .map((u) => ({
        text: { type: "plain_text", text: u.name },
        value: u.id,
      }));

    await ack({ options: filtered });
  } catch (e) {
    console.error("home_person_assignee_select options error:", e?.data || e);
    await ack({ options: [] });
  }
});



app.event("app_home_opened", async ({ event, client, body }) => {
  try {
    const teamId = body.team_id || body.team?.id || event.team;
    const userId = event.user;
    // Phase8-4: Homeの検索条件を保持（初回のみ初期化）
    const k = `${teamId}:${userId}`;
    if (!homeState.has(k)) {
      setHomeState(teamId, userId, { viewKey: "personal", scopeKey: "active", personalScopeKey: "to_me", assigneeUserId: userId, deptKey: "all", broadcastScopeKey: "to_me" });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("app_home_opened error:", e?.data || e);
  }
});

// ================================
// Shortcut: Message -> Task create modal
// ================================
app.shortcut("create_task_from_message", async ({ shortcut, ack, client }) => {
  await ack();

  try {
    const teamId = shortcut.team?.id || shortcut.team_id;
    const channelId = shortcut.channel?.id || "";
    const msgTs = shortcut.message?.ts || "";
    const rawText = shortcut.message?.text || "";
    const requesterUserId = shortcut.message?.user || "";

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);
    const titleCandidate = generateTitleCandidate(prettyText);

    await client.views.open({
      trigger_id: shortcut.trigger_id,
      view: {
        type: "modal",
        callback_id: "task_modal",
        private_metadata: JSON.stringify({
          teamId,
          channelId,
          msgTs,
          requesterUserId,
          messageText: rawText,
          messageTextPretty: prettyText,
        }),
        title: { type: "plain_text", text: "タスク作成" },
        submit: { type: "plain_text", text: "決定" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks: [
          { type: "input", block_id: "title", label: { type: "plain_text", text: "タイトル（自動候補）" }, element: { type: "plain_text_input", action_id: "title_input", initial_value: titleCandidate } },
          { type: "input", block_id: "desc", label: { type: "plain_text", text: "詳細（元メッセージ全文）" }, element: { type: "plain_text_input", action_id: "desc_input", multiline: true, initial_value: prettyText || "" } },

          // 対応者（個人：複数OK）
          {
            type: "input",
            optional: true,
            block_id: "assignee_users",
            label: { type: "plain_text", text: "対応者（個人・複数OK）" },
            element: { type: "multi_users_select", action_id: "assignee_users_select", placeholder: { type: "plain_text", text: "ユーザーを選択" } },
          },

          // 対応者（グループ：@ALL-xxx / @mk-all 等）
          {
            type: "input",
            optional: true,
            block_id: "assignee_groups",
            label: { type: "plain_text", text: "対応者（グループ：@ALL-xxx / @mk-all など）" },
            element: {
              type: "multi_external_select",
              action_id: "assignee_groups_select",
              placeholder: { type: "plain_text", text: "ユーザーグループを検索" },
              min_query_length: 0,
            },
          },

          { type: "input", block_id: "due", label: { type: "plain_text", text: "期限" }, element: { type: "datepicker", action_id: "due_date", placeholder: { type: "plain_text", text: "日付を選択" } } },
          { type: "input", block_id: "status", label: { type: "plain_text", text: "ステータス" }, element: statusSelectElement("open") },

          { type: "context", elements: [{ type: "mrkdwn", text: "💡 対象が1人なら「個人タスク」、2人以上またはグループ指定なら「全社/複数タスク」になります。" }] },
        ],
      },
    });
  } catch (e) {
    console.error("shortcut error:", e?.data || e);
  }
});

// ================================
// Modal submit: create task -> DB -> thread + ephemeral
// ================================
async function expandTargetsFromGroups(teamId, groupIds) {
  if (!groupIds?.length) return { users: new Set(), groupHandles: [], groupIdToHandle: new Map() };

  const idToHandle = await getSubteamIdMap(teamId);
  const groupHandles = [];
  const groupIdToHandle = new Map();

  const users = new Set();
  for (const gid of groupIds) {
    try {
      const handle = idToHandle.get(gid) || gid;
      groupIdToHandle.set(gid, handle);
      groupHandles.push(handle);
      const usersRes = await app.client.usergroups.users.list({ usergroup: gid });
      for (const uid of usersRes.users || []) users.add(uid);
    } catch (e) {
      console.error("expandTargetsFromGroups error:", e?.data || e);
    }
  }
  return { users, groupHandles, groupIdToHandle };
}

function uniq(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

app.view("task_modal", async ({ ack, body, view, client }) => {

  try {
    const meta = safeJsonParse(view.private_metadata || "{}") || {};
    const actorUserId = body.user.id;

    const teamId = meta.teamId || body.team?.id || body.team_id;
    const channelId = meta.channelId || "";
    const parentTs = meta.msgTs || "";

    const title = view.state.values.title?.title_input?.value?.trim() || "（無題タスク）";
    const description =
      view.state.values.desc?.desc_input?.value?.trim() ||
      meta.messageTextPretty ||
      meta.messageText ||
      "";

    const selectedUsers = view.state.values.assignee_users?.assignee_users_select?.selected_users || [];
    const selectedGroupOptions = view.state.values.assignee_groups?.assignee_groups_select?.selected_options || [];
    const selectedGroupIds = selectedGroupOptions.map((o) => o?.value).filter(Boolean);

    const due = view.state.values.due?.due_date?.selected_date || null;
    const status = view.state.values.status?.status_select?.selected_option?.value || "open";
    const requesterUserId = meta.requesterUserId || actorUserId;


    if (!selectedUsers.length && !selectedGroupIds.length) {

      // Phase8-2: 対応者（個人 or グループ）必須。モーダル内エラー表示で送信をブロックする

      await ack({

        response_action: "errors",

        errors: {

          assignee_users: "対応者（個人 or グループ）を1つ以上選んでください",

          assignee_groups: "対応者（個人 or グループ）を1つ以上選んでください",

        },

      });

      return;

    }

    // Phase8-2: バリデーション通過後にack（このハンドラ内でackは1回のみ）
    await ack();

    // Expand group members
    const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(teamId, selectedGroupIds);

    // targets = selectedUsers + groupUsers
    const targets = new Set();
    for (const u of selectedUsers) targets.add(u);
    for (const u of groupUsers) targets.add(u);

    const targetList = Array.from(targets);

    const isPersonal = (targetList.length === 1) && (selectedGroupIds.length === 0);
    const taskType = isPersonal ? "personal" : "broadcast";

    // label for display (no mention)
// - broadcastは「選択された対象（個人/グループ）」だけをラベル化（グループの全員は展開しない）
// - メンション通知を避けるため、表示は noMention() を通す
const labelParts = [];
for (const gidHandle of groupHandles) labelParts.push(`@${String(gidHandle).replace(/^@/, "")}`);
for (const u of selectedUsers) {
  const name = await getUserDisplayName(teamId, u);
  labelParts.push(`@${name}`);
}
const assigneeLabelRaw = labelParts.join(" ");

    // dept resolve (A): requester + (personalのみ assignee)
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const personalAssigneeId = isPersonal ? targetList[0] : null;
    const assigneeDept = isPersonal ? await resolveDeptForUser(teamId, personalAssigneeId) : null;

    let permalink = "";
    if (channelId && parentTs) {
      try {
        const r = await client.chat.getPermalink({ channel: channelId, message_ts: parentTs });
        permalink = r?.permalink || "";
      } catch (_) {}
    }

    const taskId = randomUUID();

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId || null,
      message_ts: parentTs || null,
      source_permalink: permalink || null,
      title,
      description,
      requester_user_id: requesterUserId,
      created_by_user_id: actorUserId,
      assignee_id: personalAssigneeId,
      assignee_label: assigneeLabelRaw || null,
      status,
      due_date: due,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
      task_type: taskType,
      broadcast_group_handle: groupHandles.length ? `@${groupHandles[0]}` : null,
      broadcast_group_id: selectedGroupIds.length ? selectedGroupIds[0] : null,
      total_count: taskType === "broadcast" ? targetList.length : null,
      completed_count: 0,
      notified_at: null,
    });

    // broadcast: snapshot targets
    if (taskType === "broadcast") {
      await dbInsertTaskTargets(teamId, taskId, targetList);
      // 안전派：DBに 저장된 targets 수로 total_count を確定
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;
    }

    // Create feedback (no auto detail modal)
    try {
      const payload = JSON.stringify({ teamId, taskId });
      await client.chat.postEphemeral({
        channel: channelId || body.user.id,
        user: body.user.id,
        text: "✅ タスクを作成しました",
        blocks: [
          { type: "section", text: { type: "mrkdwn", text: "✅ *タスクを作成しました*（必要なら詳細を開けます）" } },
          { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "詳細を開く" }, action_id: "open_detail_modal", value: payload }] },
        ],
      });
    } catch (_) {}

    // broadcast creation notify: allow mention (only once)
    if (taskType === "broadcast" && channelId) {
      try {
        const mentionParts = [];
        // usergroups: ensure mention works using subteam token
        const idToHandle = await getSubteamIdMap(teamId);
        for (const gid of selectedGroupIds) {
          const handle = idToHandle.get(gid);
          if (handle) mentionParts.push(`<!subteam^${gid}|@${handle}>`);
        }
        // users: normal mention
        for (const u of selectedUsers) mentionParts.push(`<@${u}>`);
        const mentionText = mentionParts.join(" ");
        await client.chat.postMessage({
          channel: channelId,
          thread_ts: parentTs || undefined,
          text: `📣 全社/複数タスクが発行されました！ ${mentionText}`,
        });
      } catch (e) {
        if (e?.data?.error === "not_in_channel") {
          await safeEphemeral(client, channelId, actorUserId, "🥺 このチャンネルにボットが参加してないよ…！ `/invite @アプリ名` してから試してね✨");
        } else {
          console.error("broadcast notify error:", e?.data || e);
        }
      }
    }

    // thread card
    if (channelId && parentTs) {
      try {
        const blocks = await buildThreadCardBlocks({ teamId, task: created });
        await upsertThreadCard(client, { teamId, channelId, parentTs, blocks });
      } catch (e) {
        if (e?.data?.error === "not_in_channel") {
          await safeEphemeral(client, channelId, actorUserId, "🥺 このチャンネルにボットが参加してないよ…！ `/invite @アプリ名` してから試してね✨");
        } else {
          console.error("thread card error:", e?.data || e);
        }
      }
    }

    // Home refresh（スマホ反映対策：関係者＋対象者へ再描画）
    publishHomeForUsers(client, teamId, [actorUserId, requesterUserId, ...targetList]);
} catch (e) {
    console.error("view submit error:", e?.data || e);
  }
});

// ================================
// Actions
// ================================
app.action("open_detail_modal", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;
  const origin = p.origin || "home";
  if (!teamId || !taskId) return;

  try {
    await openDetailModal(client, { trigger_id: body.trigger_id, teamId, taskId, viewerUserId: body.user.id, origin, isFromModal: body.view?.type === "modal" });
  } catch (e) {
    console.error("open_detail_modal error:", e?.data || e);
  }
});

app.action("noop", async ({ ack }) => {
  await ack();
});

// Home: mode change

app.action("home_view_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "personal";

    // view切替時：personalなら担当者を自分に戻す（初期値に寄せる）
    if (selected === "personal") {
      setHomeState(teamId, userId, { viewKey: "personal", personalScopeKey: "to_me", deptKey: "all", assigneeUserId: userId });
    } else {
      setHomeState(teamId, userId, { viewKey: "broadcast", broadcastScopeKey: "to_me" });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_view_select error:", e?.data || e);
  }
});

app.action("home_person_assignee_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selectedUser = body.actions?.[0]?.selected_option?.value || userId;

    setHomeState(teamId, userId, { assigneeUserId: selectedUser });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_person_assignee_select error:", e?.data || e);
  }
});

// personal: 担当者クリア（空欄=全員対象）
app.action("home_person_assignee_clear", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    setHomeState(teamId, userId, { assigneeUserId: null });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_person_assignee_clear error:", e?.data || e);
  }
});

app.action("home_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "active";

    setHomeState(teamId, userId, { scopeKey: selected });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_scope_select error:", e?.data || e);
  }
});

app.action("home_dept_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "all";

    setHomeState(teamId, userId, { deptKey: selected });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_dept_select error:", e?.data || e);
  }
});

// Home: broadcast 範囲 change（Phase8-3）
app.action("home_broadcast_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    setHomeState(teamId, userId, { broadcastScopeKey: selected });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_broadcast_scope_select error:", e?.data || e);
  }
});

// Home: フィルタをリセット（Phase8-4）
app.action("home_reset_filters", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    setHomeState(teamId, userId, {
      viewKey: "personal",
      scopeKey: "active",
      personalScopeKey: "to_me",
      assigneeUserId: userId,
      deptKey: "all",
      broadcastScopeKey: "to_me",
    });

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_reset_filters error:", e?.data || e);
  }
});

// personal: 範囲（自分が対応/自分が発行/すべて）
app.action("home_personal_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    if (selected === "all") {
      // すべて：検索UIが有効（dept/assignee）
      setHomeState(teamId, userId, { personalScopeKey: "all" });
    } else {
      // すべて以外：隠れフィルタ事故を防ぐため検索条件をリセット
      setHomeState(teamId, userId, {
        personalScopeKey: selected,
        deptKey: "all",
        assigneeUserId: userId,
      });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_personal_scope_select error:", e?.data || e);
  }
});


// ================================
// Phase9: Gantt export (personal only)
// ================================

// JST date-only helpers
function jstYmdParts(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return null;
  const parts = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(dt);
  const y = parts.find((p) => p.type === "year")?.value;
  const m = parts.find((p) => p.type === "month")?.value;
  const dd = parts.find((p) => p.type === "day")?.value;
  if (!y || !m || !dd) return null;
  return { y: Number(y), m: Number(m), d: Number(dd) };
}

function jstDateOnly(d) {
  const p = jstYmdParts(d);
  if (!p) return null;
  return new Date(`${String(p.y).padStart(4, "0")}-${String(p.m).padStart(2, "0")}-${String(p.d).padStart(2, "0")}T00:00:00+09:00`);
}

function addDays(dateObj, days) {
  const d = new Date(dateObj.getTime());
  d.setDate(d.getDate() + days);
  return d;
}

function startOfWeekMonday(dateObj) {
  const d = new Date(dateObj.getTime());
  const day = d.getDay(); // 0=Sun
  const diff = (day === 0 ? -6 : 1 - day); // back to Monday
  d.setDate(d.getDate() + diff);
  return d;
}

function endOfWeekSunday(dateObj) {
  const mon = startOfWeekMonday(dateObj);
  return addDays(mon, 6);
}

function formatMd(d) {
  const p = jstYmdParts(d);
  if (!p) return "";
  return `${String(p.m).padStart(2, "0")}/${String(p.d).padStart(2, "0")}`;
}

function formatYmd(d) {
  const p = jstYmdParts(d);
  if (!p) return "";
  return `${p.y}/${String(p.m).padStart(2, "0")}/${String(p.d).padStart(2, "0")}`;
}

function isBefore(a, b) {
  return a.getTime() < b.getTime();
}
function isAfter(a, b) {
  return a.getTime() > b.getTime();
}
function clampDate(d, min, max) {
  if (isBefore(d, min)) return min;
  if (isAfter(d, max)) return max;
  return d;
}


// ガント出力用：Homeと同じフィルタ（担当者/担当部署/状態）を反映しつつ、期限ありのタスクのみ対象
async function dbListPersonalTasksForGantt(teamId, { assigneeId = null, deptKey = "all", statuses = ["open", "in_progress", "waiting"] } = {}) {
  const params = [teamId, statuses];
  const where = [];

  // 担当者が指定されている場合はそれを優先
  if (assigneeId) {
    params.push(assigneeId);
    where.push(`AND t.assignee_id = $${params.length}`);
  } else if (deptKey && deptKey !== "all") {
    // 担当部署（ユーザーグループ）に所属するユーザーのタスク
    if (deptKey === "__none__") return [];
    const { membersByDeptKey } = await fetchDeptGroups(teamId);
    const set = membersByDeptKey && typeof membersByDeptKey.get === "function" ? membersByDeptKey.get(deptKey) : null;
    const members = set ? Array.from(set) : [];
    if (!members.length) return [];
    params.push(members);
    where.push(`AND t.assignee_id = ANY($${params.length}::text[])`);
  }

  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND (t.task_type IS NULL OR t.task_type='personal')
      AND t.status = ANY($2::text[])
      AND t.due_date IS NOT NULL
      ${where.join(" ")}
    ORDER BY t.due_date ASC, t.created_at ASC;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function generateGanttXlsx({ teamId, tasks, windowStart, windowEnd }) {
  const ExcelJS = require("exceljs");
  const fs = require("fs");
  const os = require("os");
  const path = require("path");

  const wb = new ExcelJS.Workbook();
  wb.creator = "Slack Task App";
  wb.created = new Date();

  const ws = wb.addWorksheet("ガント");

  // ==== 表示枠（日単位）====
  // 「S列くらいまで」に収めるため、14日分の表示枠にする（A〜Eが属性、F〜Sが日別ガント）
  // windowStart/windowEnd は「出力日±2週間」で受け取っているが、日別はその中心付近を切り出す
  const totalDays = 14;
  // windowStart〜windowEnd の中央を、totalDays に収める
  const mid = addDays(windowStart, Math.floor((diffDays(windowStart, windowEnd) + 1) / 2));
  const ganttStart = addDays(mid, -Math.floor(totalDays / 2));
  const ganttEnd = addDays(ganttStart, totalDays - 1);

  // 列定義
  const dayHeaders = [];
  for (let i = 0; i < totalDays; i++) {
    const d = addDays(ganttStart, i);
    dayHeaders.push({
      header: formatMdDay(d), // 例: 01/12(月)
      key: `d${i}`,
      width: 4
    });
  }

  ws.columns = [
    { header: "タスク名", key: "title", width: 44 },
    { header: "依頼者", key: "requester", width: 18 },
    { header: "対応者", key: "assignee", width: 18 },
    { header: "作成", key: "created", width: 12 },
    { header: "期限", key: "due", width: 12 },
    { header: "状態", key: "status", width: 14 },
    { header: "遅延", key: "delay", width: 10 },
    ...dayHeaders,
  ];

  // ヘッダー装飾（色つけ）
  const headerRow = ws.getRow(1);
  headerRow.height = 20;
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.alignment = { vertical: "middle", horizontal: "center" };
  headerRow.eachCell((cell) => {
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FF2F5597" }, // 濃いめブルー
    };
    cell.border = {
      top: { style: "thin", color: { argb: "FF1F1F1F" } },
      left: { style: "thin", color: { argb: "FF1F1F1F" } },
      bottom: { style: "thin", color: { argb: "FF1F1F1F" } },
      right: { style: "thin", color: { argb: "FF1F1F1F" } },
    };
  });

  // フィルタ（依頼者/対応者/状態/遅延）
  ws.autoFilter = {
    from: { row: 1, column: 1 },
    to: { row: 1, column: 7 },
  };

  // 罫線（薄め）
  const thinBorder = {
    top: { style: "thin", color: { argb: "FFD9D9D9" } },
    left: { style: "thin", color: { argb: "FFD9D9D9" } },
    bottom: { style: "thin", color: { argb: "FFD9D9D9" } },
    right: { style: "thin", color: { argb: "FFD9D9D9" } },
  };

  // データ行
  for (const t of tasks) {
    const requesterId = t.requester_user_id || t.requester_id || "";
    const assigneeId = t.assignee_id || "";

    const requesterName = requesterId ? `@${await getUserDisplayName(teamId, requesterId)}` : "";
    const assigneeName = assigneeId ? `@${await getUserDisplayName(teamId, assigneeId)}` : "";

    const created = jstDateOnly(new Date(t.created_at));
    const due = jstDateOnly(new Date(t.due_date));
    const isDelayed = due < jstDateOnly(new Date());

    const rowData = {
      title: t.title || "",
      requester: requesterName,
      assignee: assigneeName,
      created: created ? formatYmd(created) : "",
      due: due ? formatYmd(due) : "",
      status: statusToJa(t.status),
      delay: isDelayed ? "遅延" : "",
    };

    // ガント（日ごと）
    for (let i = 0; i < totalDays; i++) {
      const d = addDays(ganttStart, i);
      // created〜due の範囲を塗る（当日含む）
      const on = (created <= d) && (d <= due);
      rowData[`d${i}`] = on ? "■" : "";
    }

    const r = ws.addRow(rowData);

    // 行の見やすさ
    r.height = 18;
    r.alignment = { vertical: "middle" };
    r.eachCell((cell, colNumber) => {
      cell.border = thinBorder;

      if (colNumber >= 8) {
        cell.alignment = { vertical: "middle", horizontal: "center" };
        cell.font = { bold: true };
      }
    });

    // 遅延を目立たせる
    const delayCell = r.getCell(7);
    if (rowData.delay) {
      delayCell.font = { bold: true, color: { argb: "FFC00000" } };
    }
  }

  // 先頭行固定
  ws.views = [{ state: "frozen", ySplit: 1 }];

  // 情報シート
  const info = wb.addWorksheet("INFO");
  info.columns = [
    { header: "項目", key: "k", width: 18 },
    { header: "値", key: "v", width: 60 },
  ];
  info.getRow(1).font = { bold: true };
  info.addRow({ k: "出力日(JST)", v: formatYmd(jstDateOnly(new Date())) });
  info.addRow({ k: "ガント表示(日)", v: `${formatYmd(ganttStart)} 〜 ${formatYmd(ganttEnd)}（${totalDays}日）` });
  info.addRow({ k: "基準(仕様)", v: "personalのみ / open,in_progress,waiting / dueなし除外 / created_at〜due_date / 遅延=due<今日(JST)" });

  // ファイル保存
  const p = getJstParts(new Date());
  const ymd = `${p.y}${String(p.m).padStart(2, "0")}${String(p.d).padStart(2, "0")}`;
  const filename = `ガント_${ymd}.xlsx`;
  const tmpDir = os.tmpdir();
  const filePath = path.join(tmpDir, `${filename}`);

  await wb.xlsx.writeFile(filePath);
  return { filePath, filename };
}

// ---- Excel用フォーマット helper ----
function getJstParts(date){
  const dtf = new Intl.DateTimeFormat("ja-JP", { timeZone: "Asia/Tokyo", year: "numeric", month: "2-digit", day: "2-digit" });
  const parts = dtf.formatToParts(date);
  const y = parseInt(parts.find(p=>p.type==="year")?.value||"0",10);
  const m = parseInt(parts.find(p=>p.type==="month")?.value||"0",10);
  const d = parseInt(parts.find(p=>p.type==="day")?.value||"0",10);
  return { y, m, d };
}

function diffDays(a, b) {
  const ms = 24 * 60 * 60 * 1000;
  const aa = new Date(a.getFullYear(), a.getMonth(), a.getDate());
  const bb = new Date(b.getFullYear(), b.getMonth(), b.getDate());
  return Math.round((bb - aa) / ms);
}
function formatMdDay(d) {
  const p = getJstParts(d);
  const w = ["日", "月", "火", "水", "木", "金", "土"][new Date(d.getFullYear(), d.getMonth(), d.getDate()).getDay()];
  return `${String(p.m).padStart(2, "0")}/${String(p.d).padStart(2, "0")}(${w})`;
}
function statusToJa(status) {
  switch (status) {
    case "open": return "未着手";
    case "in_progress": return "対応中";
    case "waiting": return "確認待ち";
    case "done": return "完了";
    case "cancelled": return "取り下げ";
    default: return String(status || "");
  }
}



async function uploadToUserDM({ client, userId, filePath, filename, initialComment }) {
  const dm = await client.conversations.open({ users: userId });
  const channel = dm.channel?.id;
  if (!channel) throw new Error("DM channel not found");

  // Try v2 first
  try {
    await client.files.uploadV2({
      channel_id: channel,
      file: fs.createReadStream(filePath),
      filename,
      title: filename,
      initial_comment: initialComment || "",
    });
    return;
  } catch (e) {
    // fall back
    try {
      await client.files.upload({
        channels: channel,
        file: fs.createReadStream(filePath),
        filename,
        title: filename,
        initial_comment: initialComment || "",
      });
      return;
    } catch (e2) {
      throw e2;
    }
  }
}

// Home: ガント出力（Phase9）
app.action("gantt_export", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user?.id;

    // Window: today ±14 days, snapped to Monday start / Sunday end (JST)
    const today = jstDateOnly(new Date());
    const rawStart = addDays(today, -14);
    const rawEnd = addDays(today, 14);
    const windowStart = startOfWeekMonday(rawStart);
    const windowEnd = endOfWeekSunday(rawEnd);


    const action = (body.actions && body.actions[0]) || {};
    const payload = safeJsonParse(action.value || "{}") || {};
    const st = getHomeState(teamId, userId);

    const viewKey = payload.viewKey || st.viewKey || "personal";
    if (viewKey !== "personal") {
      await postDM(userId, "📭 ガント出力：personal タスクのみ対象です（Homeの「表示」を個人タスクにしてから実行してね）。");
      return;
    }

    const deptKey = payload.deptKey ?? st.deptKey ?? "all";
    const assigneeId = payload.assigneeUserId ?? st.assigneeUserId ?? null;
    const scopeKey = payload.scopeKey || st.scopeKey || "active";

    const statuses = scopeKey === "done" ? ["done"] : ["open", "in_progress", "waiting"];

    const tasks = await dbListPersonalTasksForGantt(teamId, { assigneeId, deptKey, statuses });
    if (!tasks.length) {
      const label = scopeKey === "done" ? "完了" : "未着手/対応中/確認待ち";
      await postDM(userId, `📭 ガント出力：対象の personal タスク（${label} & 期限あり）が0件でした。`);
      return;
    }

const { filePath, filename } = await generateGanttXlsx({ teamId, tasks, windowStart, windowEnd });

    await uploadToUserDM({
      client,
      userId,
      filePath,
      filename,
      initialComment: `📎 ガントを出力しました（personalのみ / ${formatYmd(windowStart)}〜${formatYmd(windowEnd)} / JST）`,
    });

    // clean up
    try { fs.unlinkSync(filePath); } catch (_) {}
  } catch (e) {
    console.error("gantt_export error:", e?.data || e);
    const userId = body.user?.id;
    await postDM(userId, "⚠️ ガント出力でエラーが発生しました。管理者に連絡してください。");
  }
});


// Home: open list modal
app.action("open_list_modal_from_home", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const viewType = p.viewType || "assigned";
  const userId = p.userId || body.user.id;
  const status = p.status || "open";
  const deptKey = p.deptKey || "all";

  await client.views.open({
    trigger_id: body.trigger_id,
    view: await buildListModalView({ teamId, viewType, userId, status, deptKey }),
  });
});

// list modal: status filter
app.action("list_filter_select", async ({ ack, body, action, client }) => {
  await ack();
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = meta.teamId || body.team?.id || body.team_id;
  const viewType = meta.viewType || "assigned";
  const userId = meta.userId || body.user.id;
  const deptKey = meta.deptKey || "all";
  const nextStatus = action?.selected_option?.value || "open";

  const nextView = await buildListModalView({ teamId, viewType, userId, status: nextStatus, deptKey });

  await client.views.update({
    view_id: body.view.id,
    hash: body.view.hash,
    view: nextView,
  });
});

// list modal: dept filter
app.action("dept_filter_select", async ({ ack, body, action, client }) => {
  await ack();
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = meta.teamId || body.team?.id || body.team_id;
  const viewType = meta.viewType || "assigned";
  const userId = meta.userId || body.user.id;
  const status = meta.status || "open";
  const nextDept = action?.selected_option?.value || "all";

  const nextView = await buildListModalView({ teamId, viewType, userId, status, deptKey: nextDept });

  await client.views.update({
    view_id: body.view.id,
    hash: body.view.hash,
    view: nextView,
  });
});

// list modal -> detail (same modal)
app.action("open_detail_in_list", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const p = safeJsonParse(action.value || "{}") || {};
    const teamId = p.teamId || body.team?.id || body.team_id;
    const taskId = p.taskId;

    const listMeta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const returnState = {
      viewType: listMeta.viewType || "assigned",
      userId: listMeta.userId || body.user.id,
      status: listMeta.status || "open",
      deptKey: listMeta.deptKey || "all",
    };

    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    const nextView = await buildListDetailView({ teamId, task, returnState, viewerUserId: body.user.id });
    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view: nextView });
  } catch (e) {
    console.error("open_detail_in_list error:", e?.data || e);
  }
});

app.action("back_to_list", async ({ ack, body, client }) => {
  await ack();
  try {
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const teamId = meta.teamId || body.team?.id || body.team_id;
    const returnState = meta.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };

    const listView = await buildListModalView({
      teamId,
      viewType: returnState.viewType,
      userId: returnState.userId,
      status: returnState.status,
      deptKey: returnState.deptKey,
    });

    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view: listView });
  } catch (e) {
    console.error("back_to_list error:", e?.data || e);
  }
});

// complete (detail only) - personal: status done / broadcast: per-user completion + recount
app.action("complete_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;

  if (!teamId || !taskId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    if (task.task_type === "broadcast") {
      const userId = getUserIdFromBody(body);

      const isTarget = await dbIsUserTarget(teamId, taskId, userId);
      if (!isTarget) {
        await safeEphemeral(client, task.channel_id || body.user.id, userId, "🥺 このタスクの対象者じゃないみたい…！");
        return;
      }

      await dbUpsertCompletion(teamId, taskId, userId);

      // 안전派：再集計
      const total = task.total_count || (await dbCountTargets(teamId, taskId));
      const doneCount = await dbCountCompletions(teamId, taskId);

      const updatedCounts = await dbUpdateBroadcastCounts(teamId, taskId, doneCount, total);

      // 全員完了（= 依頼者の確認待ちへ）
      if (doneCount >= total && total > 0) {
        const fresh = await dbGetTaskById(teamId, taskId);
        if (fresh && !["waiting", "done", "cancelled"].includes(fresh.status)) {
          await dbUpdateStatus(teamId, taskId, "waiting");
        }
        // 依頼者へ通知（1回だけ）
        if (fresh && !fresh.notified_at) {
          await dbQuery(`UPDATE tasks SET notified_at=now() WHERE team_id=$1 AND id=$2 AND notified_at IS NULL`, [teamId, taskId]);
          await postRequesterConfirmDM({ teamId, taskId, requesterUserId: fresh.requester_user_id, title: fresh.title });
        }
      }

// スレッドカード更新（進捗表示更新）
      if (task.channel_id && task.message_ts) {
        const refreshed = await dbGetTaskById(teamId, taskId);
        if (refreshed) {
          const blocks = await buildThreadCardBlocks({ teamId, task: refreshed });
          await upsertThreadCard(client, { teamId, channelId: refreshed.channel_id, parentTs: refreshed.message_ts, blocks });
        }
      }

      // modal refresh
      if (body.view?.id) {
        const refreshed = await dbGetTaskById(teamId, taskId);
        if (refreshed) {
          if (body.view.callback_id === "list_detail_modal") {
            const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
            const returnState = meta2.returnState || { viewType: "assigned", userId, status: "open", deptKey: "all" };
            await client.views.update({
              view_id: body.view.id,
              hash: body.view.hash,
              view: await buildListDetailView({ teamId, task: refreshed, returnState, viewerUserId: userId }),
            });
          } else {
            await client.views.update({
              view_id: body.view.id,
              hash: body.view.hash,
              view: await buildDetailModalView({ teamId, task: refreshed, viewerUserId: body.user.id }),
            });
          }
        }
      }

      // Home refresh（スマホ反映対策：関係者へまとめて再描画）
      publishHomeForUsers(client, teamId, [userId, task.requester_user_id]);
return;
    }

    // personal
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState, viewerUserId: body.user.id }),
      });
      return;
    }

    if (updated.channel_id && updated.message_ts) {
      // スレッドカードは完了ボタンが無いので、表示だけ更新
      const doneBlocks = [
        { type: "header", text: { type: "plain_text", text: "✅ 完了しました" } },
        { type: "section", text: { type: "mrkdwn", text: `*${noMention(updated.title)}*\nタスクを完了にしました✨` } },
      ];
      await upsertThreadCard(client, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks: doneBlocks });
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      const refreshed = await dbGetTaskById(teamId, taskId);
      if (refreshed) {
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildDetailModalView({ teamId, task: refreshed, viewerUserId: body.user.id }),
        });
      }
    }

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      publishHomeForUsers(client, teamId, [body.user.id], 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, [body.user.id], 200);
      }, 200);
    } catch (_) {}
  } catch (e) {
    console.error("complete_task error:", e?.data || e);
  }
});


// broadcast: requester confirms after all targets completed (waiting -> done)
app.action("confirm_broadcast_done", async ({ ack, body, action, client }) => {
  await ack();

  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;
  if (!teamId || !taskId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    // only requester can confirm
    if (task.requester_user_id !== body.user.id) {
      await safeEphemeral(client, task.channel_id || body.user.id, body.user.id, "🥺 確認完了できるのは依頼者だけだよ…！");
      return;
    }

    if (task.task_type !== "broadcast") return;
    if (task.status !== "waiting") {
      await safeEphemeral(client, task.channel_id || body.user.id, body.user.id, "まだ確認待ち状態じゃないよ…！");
      return;
    }

    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    // thread card update
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      await upsertThreadCard(client, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
    }

    // refresh open modal if any
    if (body.view?.id) {
      if (body.view.callback_id === "list_detail_modal") {
        const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
        const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildListDetailView({ teamId, task: updated, returnState, viewerUserId: body.user.id }),
        });
      } else if (body.view.callback_id === "detail_modal") {
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildDetailModalView({ teamId, task: updated, viewerUserId: body.user.id }),
        });
      }
    }
    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      publishHomeForUsers(client, teamId, [body.user.id], 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, [body.user.id], 200);
      }, 200);
    } catch (_) {}

    // best effort: update original DM message if action came from DM
    if (body.channel?.id && body.message?.ts) {
      try {
        await client.chat.update({
          channel: body.channel.id,
          ts: body.message.ts,
          text: "✅ 確認完了しました",
          blocks: [
            { type: "section", text: { type: "mrkdwn", text: `✅ *確認完了しました*\n「*${noMention(updated.title)}*」を完了にしました。` } },
          ],
        });
      } catch (_) {}
    }
  } catch (e) {
    console.error("confirm_broadcast_done error:", e?.data || e);
  }
});

app.action("cancel_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;

  if (!teamId || !taskId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    if (task.requester_user_id !== body.user.id) {
      await safeEphemeral(client, task.channel_id || body.user.id, body.user.id, "🥺 取り下げできるのは依頼者だけだよ…！");
      return;
    }

    const cancelled = await dbCancelTask(teamId, taskId, body.user.id);
    if (!cancelled) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: cancelled, returnState, viewerUserId: body.user.id }),
      });
      return;
    }

    if (cancelled.channel_id && cancelled.message_ts) {
      const blocks = [
        { type: "header", text: { type: "plain_text", text: "🚫 取り下げました" } },
        { type: "section", text: { type: "mrkdwn", text: `*${noMention(cancelled.title)}*\n依頼者により取り下げられました。` } },
      ];
      await upsertThreadCard(client, { teamId, channelId: cancelled.channel_id, parentTs: cancelled.message_ts, blocks });
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildDetailModalView({ teamId, task: cancelled, viewerUserId: body.user.id }),
      });
    }

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      publishHomeForUsers(client, teamId, [body.user.id], 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, [body.user.id], 200);
      }, 200);
    } catch (_) {}
  } catch (e) {
    console.error("cancel_task error:", e?.data || e);
  }
});

app.action("status_select", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const teamId = meta.teamId || body.team?.id || body.team_id;
    const taskId = meta.taskId;
    const nextStatus = action?.selected_option?.value;

    if (!teamId || !taskId || !nextStatus) return;

    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    // broadcast はステータス手動変更しない（検収フローで自動遷移）
    if (task.task_type === "broadcast") {
      await safeEphemeral(client, task.channel_id || body.user.id, body.user.id, "🥺 複数タスクのステータスは自動で進むよ（全員完了→確認待ち→依頼者の確認完了）");
      return;
    }

    // personal：ウォッチャー等はステータス変更不可（依頼者 or 対応者のみ）
    const actor = body.user.id;
    if (task.requester_user_id !== actor && task.assignee_id !== actor) {
      await safeEphemeral(client, task.channel_id || body.user.id, actor, "🥺 ステータス変更できるのは依頼者か対応者だけだよ…！");
      return;
    }

    const updated = await dbUpdateStatus(teamId, taskId, nextStatus);
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState, viewerUserId: body.user.id }),
      });
      return;
    }

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({ teamId, task: updated, viewerUserId: body.user.id }),
    });

    // スレッドカード：表示更新
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      await upsertThreadCard(client, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
    }

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      publishHomeForUsers(client, teamId, [body.user.id], 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, [body.user.id], 200);
      }, 200);
    } catch (_) {}
  } catch (e) {
    console.error("status_select error:", e?.data || e);
  }
});

// progress modal: MVP placeholder (実装は後で拡張しやすいように入口だけ)
app.action("open_progress_modal", async ({ ack, body, action, client }) => {
  await ack();

  // value から取れないケースがあるので、modal meta も参照する（堅牢化）
  const p = safeJsonParse(action?.value || "{}") || {};
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = p.teamId || meta.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId || meta.taskId;
  if (!teamId || !taskId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;
    if (task.task_type !== "broadcast") return;

    // 仕様変更：誰でも閲覧可（依頼者・対応者・対象者・ウォッチャー・その他）
    // targets / completions
    const targetsRes = await dbQuery(
      `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2 ORDER BY user_id`,
      [teamId, taskId]
    );
    const completionsRes = await dbQuery(
      `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2 ORDER BY user_id`,
      [teamId, taskId]
    );

    const targets = (targetsRes.rows || []).map((r) => r.user_id).filter(Boolean);
    const doneSet = new Set((completionsRes.rows || []).map((r) => r.user_id).filter(Boolean));

    const done = targets.filter((u) => doneSet.has(u));
    const todo = targets.filter((u) => !doneSet.has(u));

    const total = targets.length;
    const doneCount = done.length;

    const listText = (arr, emptyText) => {
      if (!arr.length) return emptyText;
      const MAX = 50;
      const head = arr.slice(0, MAX).map((u) => `• <@${u}>`).join("\n");
      const more = arr.length > MAX ? `\n…ほか ${arr.length - MAX} 名` : "";
      return `${head}${more}`;
    };

    const meta2 = { teamId, taskId, origin: "progress" };

    const view = {
      type: "modal",
      callback_id: "progress_modal",
      private_metadata: JSON.stringify(meta2),
      title: { type: "plain_text", text: "完了状況" },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        {
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "← 詳細に戻る" },
              action_id: "back_to_detail_from_progress",
              value: JSON.stringify({ teamId, taskId }),
            },
          ],
        },
        { type: "header", text: { type: "plain_text", text: "📊 完了/未完了一覧" } },
        { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
        { type: "divider" },
        { type: "section", text: { type: "mrkdwn", text: `進捗：*${doneCount} / ${total}*` } },
        { type: "divider" },

        { type: "section", text: { type: "mrkdwn", text: `✅ *完了済み（${done.length}）*` } },
        { type: "section", text: { type: "mrkdwn", text: listText(done, "（まだいません）") } },
        { type: "divider" },

        { type: "section", text: { type: "mrkdwn", text: `⏳ *未完了（${todo.length}）*` } },
        { type: "section", text: { type: "mrkdwn", text: listText(todo, "（全員完了！🎉）") } },
      ],
    };

    // modal 上からの遷移は push を優先（挙動が安定）
    if (body.view?.id) {
      await client.views.push({ trigger_id: body.trigger_id, view });
    } else {
      await client.views.open({ trigger_id: body.trigger_id, view });
    }
  } catch (e) {
    console.error("open_progress_modal error:", e?.data || e);
  }
});

app.action("back_to_detail_from_progress", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const p = safeJsonParse(action.value || "{}") || {};
    const teamId = p.teamId || body.team?.id || body.team_id;
    const taskId = p.taskId;
    if (!teamId || !taskId || !body.view?.id) return;

    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({ teamId, task, viewerUserId: body.user.id, origin: "home" }),
    });
  } catch (e) {
    console.error("back_to_detail_from_progress error:", e?.data || e);
  }
});

// ================================
// Due notify (09:00 JST) - personal tasks only (broadcastは完了トラッキングのため別通知設計にする想定)
// ================================
function todayJstYmd() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

async function notifyUserDM(userId, task, roleLabel) {
  if (!userId) return;

  const dm = await app.client.conversations.open({ users: userId });
  const channel = dm.channel?.id;
  if (!channel) return;

  const due = String(task.due_date || "").replaceAll("-", "/");
  const text =
    `⏰ 期限リマインド（${roleLabel}）\n` +
    `・タイトル：${noMention(task.title)}\n` +
    `・期限：${due}\n` +
    `・ステータス：${task.status}\n`;

  await app.client.chat.postMessage({ channel, text });
}

async function runDueNotifyJob() {
  const today = todayJstYmd();

  const q = `
    SELECT *
    FROM tasks
    WHERE due_date = $1
      AND status NOT IN ('done','cancelled')
      AND (notified_at IS NULL)
      AND (task_type IS NULL OR task_type='personal')
    ORDER BY created_at ASC
    LIMIT 500;
  `;
  const tasks = (await dbQuery(q, [today])).rows;

  for (const t of tasks) {
    try {
      await notifyUserDM(t.requester_user_id, t, "依頼者");
      await notifyUserDM(t.assignee_id, t, "対応者");
      await dbQuery(`UPDATE tasks SET notified_at = now() WHERE team_id=$1 AND id=$2`, [t.team_id, t.id]);
    } catch (e) {
      console.error("notify error:", e?.data || e);
    }
  }

  console.log(`[notify] done. today=${today} count=${tasks.length}`);
}

cron.schedule(
  "0 9 * * *",
  () => {
    runDueNotifyJob().catch((e) => console.error("runDueNotifyJob error:", e?.data || e));
  },
  { timezone: "Asia/Tokyo" }
);

if (process.env.RUN_NOTIFY_NOW === "true") {
  runDueNotifyJob().catch(console.error);
}




// ================================
// Edit Task modal
// ================================
app.action("open_edit_task_modal", async ({ ack, body, action, client }) => {
  await ack();

  const meta = safeJsonParse(action.value || "{}") || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const taskId = meta.taskId;
  const viewerUserId = getUserIdFromBody(body);

  if (!teamId || !taskId || !viewerUserId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    const isBroadcast = task.task_type === "broadcast";
    const canEditTask =
      (!isBroadcast && (viewerUserId === task.requester_user_id || viewerUserId === task.assignee_id)) ||
      (isBroadcast && viewerUserId === task.requester_user_id);

    if (!canEditTask) return;

    const initDue = slackDateYmd(task.due_date);

    const blocks = [];

    // personal のみ：対応者を変更できる（broadcastは対象者集合の整合性があるため変更しない）
    if (!isBroadcast) {
      blocks.push({
        type: "input",
        block_id: "assignee",
        label: { type: "plain_text", text: "対応者" },
        element: {
          type: "users_select",
          action_id: "assignee_user",
          initial_user: task.assignee_id,
        },
      });
    }

    blocks.push({
      type: "input",
      block_id: "due",
      optional: true,
      label: { type: "plain_text", text: "期限" },
      element: {
        type: "datepicker",
        action_id: "due_date",
        ...(initDue ? { initial_date: initDue } : {}),
        placeholder: { type: "plain_text", text: "日付を選択" },
      },
    });

    blocks.push({
      type: "input",
      block_id: "content",
      label: { type: "plain_text", text: "タスク内容" },
      element: {
        type: "plain_text_input",
        action_id: "content_text",
        multiline: true,
        initial_value: task.description || "",
      },
    });

    await client.views.push({
      trigger_id: body.trigger_id,
      view: {
        type: "modal",
        callback_id: "edit_task_modal",
        private_metadata: JSON.stringify({ teamId, taskId }),
        title: { type: "plain_text", text: "タスク編集" },
        submit: { type: "plain_text", text: "保存" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks,
      },
    });
  } catch (e) {
    console.error("open_edit_task_modal error:", e?.data || e);
  }
});

app.view("edit_task_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const taskId = meta.taskId;
  const actorUserId = getUserIdFromBody(body);

  const nextAssignee = view.state.values.assignee?.assignee_user?.selected_user || null;
  const nextDue = view.state.values.due?.due_date?.selected_date || null;
  const nextContent = (view.state.values.content?.content_text?.value || "").trim();

  if (!nextContent) {
    await ack({ response_action: "errors", errors: { content: "タスク内容を入力してください" } });
    return;
  }

  // ① まず軽い画面へ差し替え（hash_conflict回避）
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "edit_task_modal_saving",
      title: { type: "plain_text", text: "保存中..." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [{ type: "section", text: { type: "mrkdwn", text: "更新しています。" } }],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";
    const canEditTask =
      (!isBroadcast && (actorUserId === before.requester_user_id || actorUserId === before.assignee_id)) ||
      (isBroadcast && actorUserId === before.requester_user_id);

    if (!canEditTask) return;

    // personal だけ対応者変更を許可（broadcastは対象者集合の整合性があるため変更しない）
    let patchAssigneeId = null;
    let patchAssigneeDept = null;
    if (!isBroadcast) {
      if (!nextAssignee) {
        // users_select なので通常は入るが、万一の保険
        return;
      }
      patchAssigneeId = nextAssignee;
      try {
        patchAssigneeDept = await resolveDeptForUser(teamId, nextAssignee);
      } catch (_) {}
    }

    const updated = await dbUpdateTaskContent(teamId, taskId, {
      assignee_id: patchAssigneeId,
      assignee_dept: patchAssigneeDept,
      due_date: nextDue,
      description: nextContent,
    });
    if (!updated) return;

    // スレッドカード更新 + 変更通知（証跡）
    if (updated.channel_id && updated.message_ts) {
      const cardBlocks = await buildThreadCardBlocks({ teamId, task: updated });
      await upsertThreadCard(client, {
        teamId,
        channelId: updated.channel_id,
        parentTs: updated.message_ts,
        blocks: cardBlocks,
      });

      // 変更点を作る（証跡用）
      const changes = [];
      if (!isBroadcast && before.assignee_id && updated.assignee_id && before.assignee_id !== updated.assignee_id) {
        changes.push(`• *対応者*：<@${before.assignee_id}> → <@${updated.assignee_id}>`);
      }
      if (String(before.due_date || "") !== String(updated.due_date || "")) {
        changes.push(`• *期限*：${formatDueDateOnly(before.due_date)} → ${formatDueDateOnly(updated.due_date)}`);
      }
      if ((before.description || "") !== (updated.description || "")) {
        changes.push("• *タスク内容*：変更あり");
      }
      const changesText = changes.length ? changes.join("\n") : "• 変更点：軽微な更新";

      const beforeDesc = noMention(String(before.description || "").slice(0, 400));
      const afterDesc = noMention(String(updated.description || "").slice(0, 400));

      const blocks = [
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: `:鉛筆_2: *タスク内容が更新されました*\n更新者：<@${actorUserId}>\n*変更点*\n${changesText}`,
          },
        },
      ];

      if ((before.description || "") !== (updated.description || "")) {
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: `*変更前*\n\`\`\`\n${beforeDesc}\n\`\`\`\n*変更後*\n\`\`\`\n${afterDesc}\n\`\`\`` },
        });
      }

      await client.chat.postMessage({
        channel: updated.channel_id,
        thread_ts: updated.message_ts,
        text: "タスク内容が更新されました",
        blocks,
      });
    }

    // Home再描画（操作者のみ）
    try {
      publishHomeForUsers(client, teamId, [actorUserId], 200);
      setTimeout(() => publishHomeForUsers(client, teamId, [actorUserId], 200), 200);
    } catch (_) {}

    // ② 最後に「更新後の詳細」を表示（不安解消・hash_conflict回避）
    try {
            const detailView = await buildDetailModalView({
        teamId,
        task: updated,
        viewerUserId: actorUserId,
        origin: "home",
      });

      // ✅ コメント保存と同じ考え方：
      // - まず「前の詳細モーダル」を最新内容で更新しておく
      // - いま表示中（保存中/保存完了）のモーダルは「保存しました✅」だけにする
      //   → 閉じると、更新済みの詳細モーダルに戻る（古いモーダルが残らない）
      const prevViewId = body?.view?.previous_view_id;

      // ① 前の詳細モーダルを更新（あれば）
      if (prevViewId) {
        try {
          await client.views.update({
            view_id: prevViewId,
            view: detailView,
          });
        } catch (e) {
          console.error("update previous detail view error:", e?.data || e);
        }
      }

      // ② 現在のモーダルは「保存しました✅」最小UI
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_task_modal_done",
          title: { type: "plain_text", text: "保存しました✅" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [{ type: "section", text: { type: "mrkdwn", text: "タスク内容を更新しました。" } }],
        },
      });
    } catch (_) {}
  } catch (e) {
    console.error("edit_task_modal submit error:", e?.data || e);

    // 失敗画面（最小UI）
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_task_modal_error",
          title: { type: "plain_text", text: "保存できませんでした" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [{ type: "section", text: { type: "mrkdwn", text: "もう一度お試しください。" } }],
        },
      });
    } catch (_) {}
  }
});

// ================================
// DB: Task comments
// ================================


// ================================
// Comment modal
// ================================






// ================================
// DB: Task comments
// ================================
async function dbListTaskComments(teamId, taskId, limit = 10) {
  const q = `
    SELECT user_id, comment, created_at
    FROM task_comments
    WHERE team_id=$1 AND task_id=$2
    ORDER BY created_at ASC
    LIMIT $3;
  `;
  const res = await dbQuery(q, [teamId, taskId, limit]);
  return res.rows || [];
}

async function dbInsertTaskComment(teamId, taskId, userId, comment) {
  const q = `
    INSERT INTO task_comments (id, team_id, task_id, user_id, comment)
    VALUES ($1,$2,$3,$4,$5);
  `;
  await dbQuery(q, [randomUUID(), teamId, taskId, userId, String(comment || "").trim()]);
}

// ================================
// Comment modal
// ================================
app.action("open_comment_modal", async ({ ack, body, action, client }) => {
  await ack();

  const meta = safeJsonParse(action.value || "{}") || {};

  // 親（詳細モーダル）を更新するために保持（閉じた時に古いモーダルへ戻らないようにする）
  meta.parent_view_id = body.view?.id || null;
  meta.parent_view_type = body.view?.type || null;


  // 詳細モーダル上からは push が正解（モーダル二重 open は不可）
  await client.views.push({
    trigger_id: body.trigger_id,
    view: {
      type: "modal",
      callback_id: "comment_modal",
      private_metadata: JSON.stringify(meta),
      title: { type: "plain_text", text: "コメント" },
      submit: { type: "plain_text", text: "投稿" },
      close: { type: "plain_text", text: "キャンセル" },
      blocks: [
        {
          type: "input",
          block_id: "comment",
          label: { type: "plain_text", text: "コメント内容" },
          element: { type: "plain_text_input", action_id: "body", multiline: true },
        },
      ],
    },
  });
});

app.view("comment_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};
  const comment = view.state.values.comment?.body?.value?.trim() || "";

  if (!comment) {
    await ack({
      response_action: "errors",
      errors: { comment: "コメントを入力してください" },
    });
    return;
  }

  // ① まず3秒以内に軽い画面へ差し替え（確実にUIを落とさない）
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "comment_modal_saving",
      title: { type: "plain_text", text: "コメント" },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [{ type: "section", text: { type: "mrkdwn", text: "💾 保存中…" } }],
    },
  });

  try {
    // ② 重い処理は ack 後にやる
    await dbInsertTaskComment(meta.teamId, meta.taskId, body.user.id, comment);

    const task = await dbGetTaskById(meta.teamId, meta.taskId);
    if (!task) return;

    // ③ 親（詳細モーダル）を更新して、コメントモーダルは「投稿完了」表示にする
    // こうすると、閉じた時に古い詳細モーダルが出てくる問題を防げる
    if (meta.parent_view_id && meta.parent_view_type === "modal") {
      await client.views.update({
        view_id: meta.parent_view_id,
        view: await buildDetailModalView({
          teamId: meta.teamId,
          task,
          viewerUserId: body.user.id,
          origin: "home",
        }),
      });
    }

    // コメントモーダル側は完了メッセージ（自動で詳細に戻さない）
    await client.views.update({
      view_id: view.id,
      view: {
        type: "modal",
        callback_id: "comment_modal_done",
        title: { type: "plain_text", text: "コメント" },
        close: { type: "plain_text", text: "閉じる" },
        blocks: [
          { type: "section", text: { type: "mrkdwn", text: "✅ 投稿しました！「閉じる」で詳細画面に戻れます。" } },
        ],
      },
    });

  } catch (e) {
    console.error("comment_modal post-save error:", e?.data || e);
    // 失敗表示だけ更新（任意）
    try {
      await client.views.update({
        view_id: view.id,
        view: {
          type: "modal",
          callback_id: "comment_modal_error",
          title: { type: "plain_text", text: "コメント" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [{ type: "section", text: { type: "mrkdwn", text: "🥺 保存に失敗しました…" } }],
        },
      });
    } catch (_) {}
  }
});


// ================================
// Start
// ================================
(async () => {
  const port = process.env.PORT ? Number(process.env.PORT) : 3000;
  await app.start(port);
  console.log(`⚡️ Slack app is running on port ${port}`);
})();