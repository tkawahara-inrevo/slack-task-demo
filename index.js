require("dotenv").config();
const { App, ExpressReceiver } = require("@slack/bolt");
const express = require("express");
const { Pool } = require("pg");
const { randomUUID } = require("crypto");
const cron = require("node-cron");

// ================================
// Slack Bolt App (+ custom webhook endpoint)
// ================================
const receiver = new ExpressReceiver({
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  // Slack側のRequest URLがデフォルト(/slack/events等)のままなら、ここは指定不要
});

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  receiver,
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

  s = s.replace(
    /^(すみません|恐縮ですが|お疲れ様です|取り急ぎ|ごめん|失礼|お願い|至急|急ぎ)\s*/g,
    "",
  );

  const cut = s.split(/[\n。！？!?]/)[0].trim();
  let title = cut || s;
  title = title
    .replace(
      /(お願いします|ください|してもらえますか|して下さい|お願いします。?)$/g,
      "",
    )
    .trim();
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
      Array.from(String(text).matchAll(/<@([A-Z0-9]+)(?:\|[^>]+)?>/g)).map(
        (m) => m[1],
      ),
    ),
  );
  if (!ids.length) return String(text);

  const idToName = {};
  for (const uid of ids) {
    const name = await getUserDisplayName(teamId, uid);
    idToName[uid] = name && String(name).trim() ? String(name).trim() : uid;
  }

  return String(text).replace(/<@([A-Z0-9]+)(?:\|[^>]+)?>/g, (m, uid) => {
    const nm = idToName[uid] || uid;
    return `@${String(nm).replace(/^@/, "")}`;
  });
}

// ================================
// User icon url cache (for assignee avatar in lists)
// ================================
const userIconCache = new Map(); // `${teamId}:${userId}` -> { at, url }
const USER_ICON_CACHE_MS = 60 * 60 * 1000;

async function getUserIconUrl(teamId, userId) {
  if (!teamId || !userId) return null;

  const key = `${teamId}:${userId}`;
  const cached = userIconCache.get(key);
  if (cached && Date.now() - cached.at < USER_ICON_CACHE_MS) return cached.url;

  try {
    const res = await app.client.users.info({ user: userId });
    const u = res?.user;
    const url =
      u?.profile?.image_24 ||
      u?.profile?.image_32 ||
      u?.profile?.image_48 ||
      null;

    userIconCache.set(key, { at: Date.now(), url });
    return url;
  } catch (_) {
    return null;
  }
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

const DEPT_CACHE_TTL_MS =
  Number(process.env.DEPT_CACHE_TTL_SEC || "3600") * 1000;

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
  const deptHandles = (
    DEPT_ALL_HANDLES.length
      ? DEPT_ALL_HANDLES
      : groups.map((g) => g.handle).filter((h) => h.endsWith("-all"))
  ).filter((h) => groups.some((g) => g.handle === h));

  const uniqHandles = Array.from(new Set(deptHandles)).sort((a, b) =>
    a.localeCompare(b),
  );

  const idByHandle = new Map(groups.map((g) => [g.handle, g.id]));
  const membersByDeptKey = new Map();

  for (const handle of uniqHandles) {
    const id = idByHandle.get(handle);
    if (!id) continue;
    try {
      const usersRes = await app.client.usergroups.users.list({
        usergroup: id,
      });
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
    for (const k of deptKeys.sort((a, b) => a.localeCompare(b)))
      if (!orderedKeys.includes(k)) orderedKeys.push(k);
  }

  // insertion order を priority 順に整える
  const rebuilt = new Map();
  for (const k of orderedKeys) rebuilt.set(k, membersByDeptKey.get(k));
  const finalMembers = new Map();
  for (const [k, v] of rebuilt.entries()) if (v) finalMembers.set(k, v);

  const next = {
    at: now,
    deptKeys: orderedKeys,
    membersByDeptKey: finalMembers,
  };
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
      deptUserCache.set(memKey, {
        dept_key: row.dept_key,
        dept_handle: row.dept_handle,
        at: Date.now(),
      });
      return row.dept_key;
    }
  } catch (_) {}

  const { deptKeys, membersByDeptKey } = await fetchDeptGroups(teamId);

  for (const deptKey of deptKeys) {
    const set = membersByDeptKey.get(deptKey);
    if (set && set.has(userId)) {
      const dept_key = deptKey;
      const dept_handle = `@${deptKey}`;
      try {
        await dbUpsertUserDept(teamId, userId, dept_key, dept_handle);
      } catch (_) {}
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

// 既に同一メッセージからタスク化されていないか（リアクション導線の重複防止）
async function dbGetTaskBySource(teamId, channelId, messageTs) {
  const q = `
    SELECT *
    FROM tasks
    WHERE team_id=$1
      AND channel_id=$2
      AND message_ts=$3
    ORDER BY created_at DESC
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, channelId, messageTs]);
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

async function dbListTasksForRequester(
  teamId,
  requesterId,
  status,
  limit = 10,
) {
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
async function dbListTasksForAssigneeWithDept(
  teamId,
  assigneeId,
  status,
  deptKey,
  limit = 20,
) {
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

async function dbListTasksForRequesterWithDept(
  teamId,
  requesterId,
  status,
  deptKey,
  limit = 20,
) {
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

async function dbUpdateBroadcastCounts(
  teamId,
  taskId,
  completedCount,
  totalCount,
) {
  const q = `
    UPDATE tasks
    SET completed_count=$3,
        total_count = COALESCE(total_count, $4),
        updated_at=now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    teamId,
    taskId,
    completedCount,
    totalCount ?? null,
  ]);
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

async function dbListTargetUserIds(teamId, taskId) {
  const q = `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return (res.rows || []).map((r) => r.user_id).filter(Boolean);
}

async function dbListBroadcastTasksForUser(
  teamId,
  userId,
  status,
  limit = 20,
  deptKey = "all",
) {
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
    const res = await dbQuery(q, [
      randomUUID(),
      teamId,
      channelId,
      parentTs,
      cardTs,
    ]);
    return res.rows[0];
  }
}

// ================================
// UI pieces
// ================================
const STATUS_OPTIONS = [
  { value: "open", text: "進行中" },
  { value: "in_progress", text: "進行中" },
  { value: "waiting", text: "確認待ち" },
  { value: "done", text: "完了" },
];

function statusLabel(s) {
  if (s === "open" || s === "in_progress") return "進行中";
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

function statusSelectElement(currentStatus) {
  // ★①：ユーザーに見せる選択肢は3つだけ（進行中/確認待ち/完了）
  // open は「進行中」として扱い、選択時は in_progress に寄せる
  const STATUS_SELECT_OPTIONS = [
    { value: "in_progress", text: "進行中" },
    { value: "waiting", text: "確認待ち" },
    { value: "done", text: "完了" },
  ];

  const normalized = currentStatus === "open" ? "in_progress" : currentStatus;
  const cur =
    STATUS_SELECT_OPTIONS.find((o) => o.value === normalized) ||
    STATUS_SELECT_OPTIONS[0];

  return {
    type: "static_select",
    action_id: "status_select",
    placeholder: { type: "plain_text", text: "ステータス" },
    initial_option: {
      text: { type: "plain_text", text: cur.text },
      value: cur.value,
    },
    options: STATUS_SELECT_OPTIONS.map((o) => ({
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

async function notifyTaskSimpleDM(
  userId,
  task,
  headerText = "✅ 完了になったよ",
) {
  if (!userId || !task?.team_id || !task?.id) return;

  try {
    const dm = await app.client.conversations.open({ users: userId });
    const channel = dm.channel?.id;
    if (!channel) return;

    const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });

    await app.client.chat.postMessage({
      channel,
      text: `${headerText}: ${noMention(task.title)}`,
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: `${headerText}` } },
        {
          type: "section",
          text: { type: "mrkdwn", text: `*${noMention(task.title)}*` },
        },
        {
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "詳細を開く" },
              action_id: "open_detail_modal",
              value: payload,
            },
          ],
        },
      ],
    });
  } catch (_) {}
}

async function postRequesterConfirmDM({
  teamId,
  taskId,
  requesterUserId,
  title,
}) {
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
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: `🎉 *全員が完了しました！*\n「*${noMention(title)}*」の確認をお願いします。`,
          },
        },
        {
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "確認完了 ✅" },
              style: "primary",
              action_id: "confirm_broadcast_done",
              value,
            },
          ],
        },
      ],
    });
  } catch (e) {
    console.error("postRequesterConfirmDM error:", e?.data || e);
  }
}

// ================================
// Thread Card (upsert)
// ================================
async function upsertThreadCard(
  client,
  { teamId, channelId, parentTs, threadTs = null, blocks },
) {
  // parentTs は「カードの一意キー」（= 1メッセージ1回の判定に使う）
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

  // threadTs は「投稿先のスレッド親」（未指定なら parentTs と同じ）
  const postThreadTs = threadTs || parentTs;

  const res = await client.chat.postMessage({
    channel: channelId,
    thread_ts: postThreadTs,
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
    {
      type: "section",
      text: { type: "mrkdwn", text: `*${noMention(task.title)}*` },
    },
    { type: "divider" },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` },
    },
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*期限*：${formatDueDateOnly(task.due_date)}`,
      },
    },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` },
    },
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*ステータス*：${statusLabel(task.status)}`,
      },
    },
  ];
  //if (task.task_type !== "broadcast") {
  //  common.push({ type: "section", text: { type: "mrkdwn", text: `*対応者部署*：${deptLabel(task.assignee_dept)}` } });
  //}

  return [
    ...common,
    { type: "divider" },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*元メッセージ*\n${src}` },
    },
    { type: "divider" },
    {
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "詳細を開く" },
          action_id: "open_detail_modal",
          value: payload,
        },
      ],
    },
    {
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "✅ 操作は「詳細」画面から行います（誤操作防止）",
        },
      ],
    },
  ];
}

// ================================
// Detail Modal（views.open）
// ================================
async function buildDetailModalView({
  teamId,
  task,
  viewerUserId,
  origin = "home",
}) {
  // ----------------------------
  // Slack mrkdwn text は 3000文字制限
  // ここ（詳細モーダルの「タスク内容」）で超えがちなので安全に切る
  // ----------------------------
  const raw = String(task.description || "").replace(/\r\n/g, "\n");

  // まず「最大行数」
  const MAX_LINES = 10;
  let srcLinesRaw = raw.split("\n").slice(0, MAX_LINES).join("\n") || "（本文なし）";

  // メンション抑止 + コードフェンス混入対策（``` があると表示崩れやすい）
  let srcLines = noMention(srcLinesRaw).replace(/```/g, "´´´");

  // 次に「最大文字数」：見出しや ``` の分を引いて余裕を持たせる
  const MAX_DETAIL_CHARS = 2600;
  if (srcLines.length > MAX_DETAIL_CHARS) {
    srcLines = srcLines.slice(0, MAX_DETAIL_CHARS) + "\n…";
  }

  const isBroadcast = task.task_type === "broadcast";


  // ★統一方針A：origin による readOnly 分岐はしない（どこから開いても同じ詳細）
  // 操作可否は「権限（依頼者/対応者/対象者）」だけで決める

  // personal 完了権限（依頼者 or 対応者）
  const canCompletePersonal =
    !isBroadcast &&
    (viewerUserId === task.requester_user_id ||
      viewerUserId === task.assignee_id);

  const meta = { teamId, taskId: task.id, origin };

  const blocks = [
    {
      type: "section",
      text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` },
    },
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*期限*：${formatDueDateOnly(task.due_date)}`,
      },
    },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` },
    },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` },
    },
    { type: "divider" },
  ];

  // ✅ 期限変更ボタン：broadcast のときだけ（personalは「内容を編集」に期限があるため不要）
  if (task.status !== "cancelled" && task.task_type === "broadcast") {
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          action_id: "open_edit_due_modal",
          text: { type: "plain_text", text: "期限を変更 🗓️" },
          value: JSON.stringify({ teamId, taskId: task.id, origin }),
        },
      ],
    });
    blocks.push({ type: "divider" });
  }

// personal：完了 / 未完了に戻す（権限者のみ）
if (!isBroadcast) {
  if (canCompletePersonal && task.status !== "cancelled") {
    if (task.status === "done") {
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "未完了に戻す ↩️" },
            action_id: "reopen_task",
            value: JSON.stringify({ teamId, taskId: task.id }),
            confirm: {
              title: { type: "plain_text", text: "確認" },
              text: { type: "mrkdwn", text: "このタスクを*未完了*に戻します。" },
              confirm: { type: "plain_text", text: "戻す" },
              deny: { type: "plain_text", text: "やめる" },
            },
          },
        ],
      });
      blocks.push({ type: "divider" });
    } else {
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "完了 ✅" },
            style: "primary",
            action_id: "complete_task",
            value: JSON.stringify({ teamId, taskId: task.id }),
          },
        ],
      });
      blocks.push({ type: "divider" });
    }
  } else if (!canCompletePersonal) {
    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "👀 このタスクは閲覧のみです（完了操作は依頼者/対応者のみ）",
        },
      ],
    });
    blocks.push({ type: "divider" });
  }
}


  // 元メッセージへ（permalinkがある場合のみ表示）
  if (task?.source_permalink) {
    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text: `🔗 <${task.source_permalink}|元メッセージへ>`,
      },
    });
    blocks.push({ type: "divider" });
  }

  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: `*タスク内容*\n\`\`\`\n${srcLines}\n\`\`\`` },
  });

  // 内容編集ボタン
  // personal：依頼者 or 対応者
  // broadcast：依頼者のみ
  {
    const canEditTask =
      (!isBroadcast &&
        (viewerUserId === task.requester_user_id ||
          viewerUserId === task.assignee_id)) ||
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

  // ===== コメント表示 =====
  let __comments = [];
  try {
    __comments = await dbListTaskComments(teamId, task.id, 10);
  } catch (e) {
    console.error("load comments error", e);
  }

  blocks.push({ type: "divider" });
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*🗨 コメント*" },
  });

   // ✅ コメント日時表示（created_at）
  // - サーバーのTZに依存するとズレるので、表示は Asia/Tokyo(JST) 固定にする
  // - Slack上で読みやすいように「YYYY/MM/DD HH:mm」形式にする
  function formatCommentCreatedAtJst(createdAt) {
    if (!createdAt) return null;

    const d = createdAt instanceof Date ? createdAt : new Date(createdAt);
    if (Number.isNaN(d.getTime())) return null;

    try {
      // ✅ JST固定でフォーマット
      const parts = new Intl.DateTimeFormat("ja-JP", {
        timeZone: "Asia/Tokyo",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).formatToParts(d);

      const get = (type) => parts.find((p) => p.type === type)?.value || "";
      return `${get("year")}/${get("month")}/${get("day")} ${get("hour")}:${get("minute")}`;
    } catch (_) {
      // Intl が使えない環境向けの保険（基本はここには来ない想定）
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${y}/${m}/${dd} ${hh}:${mm}`;
    }
  }

  if (!__comments.length) {
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: "（コメントなし）" }],
    });
  } else {
    for (const c of __comments) {
      const name = await getUserDisplayName(teamId, c.user_id);
      const at = formatCommentCreatedAtJst(c.created_at);

      // ✅ 表示：名前 + 日時（あれば）→ 本文
      const header = at ? `*${name}*  _(${at})_` : `*${name}*`;
      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: `${header}\n${c.comment}` },
      });
    }
  }

  // コメントを書く：統一方針Aなので、起点で隠さず常に出す（安全ならOK）
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

  blocks.push({ type: "divider" });
  // ===== コメント表示ここまで =====

  // ===== broadcast 操作（誤操作防止版）=====
  if (isBroadcast) {
    const isTarget = await dbIsUserTarget(teamId, task.id, viewerUserId);
    const already = await dbHasUserCompleted(teamId, task.id, viewerUserId);

    // ① 自分の完了（対象者だけ / 既に完了してたら非活性表示）
    if (
      isTarget &&
      !already &&
      task.status !== "done" &&
      task.status !== "cancelled"
    ) {
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "自分だけ完了 ✅" },
            style: "primary",
            action_id: "complete_task",
            value: JSON.stringify({ teamId, taskId: task.id }),
          },
        ],
      });
    } else if (isTarget && already) {
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "自分は完了済み ✅" },
            action_id: "noop",
            value: "noop",
          },
        ],
      });
    }

    // ② 進捗一覧（誰でも閲覧可）
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "完了/未完了一覧" },
          action_id: "open_progress_modal",
          value: JSON.stringify({ teamId, taskId: task.id }),
        },
      ],
    });

    // ③ 全体を完了（強制）＋ 取り下げ（依頼者のみ）
    if (task.status !== "done" && task.status !== "cancelled") {
      const elems = [
        {
          type: "button",
          text: { type: "plain_text", text: "全体を完了（強制）⚠️" },
          style: "primary",
          action_id: "confirm_broadcast_done",
          value: JSON.stringify({ teamId, taskId: task.id }),
          confirm: {
            title: { type: "plain_text", text: "確認" },
            text: {
              type: "mrkdwn",
              text: "⚠️ 未完了の人がいても、このタスクを*完了*にします。",
            },
            confirm: { type: "plain_text", text: "完了にする" },
            deny: { type: "plain_text", text: "やめる" },
          },
        },
      ];

      if (task.requester_user_id === viewerUserId) {
        elems.push({
          type: "button",
          text: { type: "plain_text", text: "取り下げ" },
          style: "danger",
          action_id: "cancel_task",
          value: JSON.stringify({ teamId, taskId: task.id }),
          confirm: {
            title: { type: "plain_text", text: "確認" },
            text: { type: "mrkdwn", text: "このタスクを*取り下げ*ます。" },
            confirm: { type: "plain_text", text: "取り下げる" },
            deny: { type: "plain_text", text: "やめる" },
          },
        });
      }

      blocks.push({ type: "actions", elements: elems });
    }
  }
  // ===== broadcast 操作ここまで =====

  return {
    type: "modal",
    callback_id: "detail_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "タスク" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}


async function openDetailModal(
  client,
  {
    trigger_id,
    teamId,
    taskId,
    viewerUserId,
    origin = "home",
    isFromModal = false,
  },
) {
  const task = await dbGetTaskById(teamId, taskId);
  if (!task) return;

  const view = await buildDetailModalView({
    teamId,
    task,
    viewerUserId,
    origin,
  });

  // モーダル上のボタンからは views.open ではなく views.push（Slack仕様）
  if (isFromModal) {
    await client.views.push({ trigger_id, view });
    return;
  }

  await client.views.open({ trigger_id, view });
}

// watcher helper

// ================================
// Home: filters (Phase3)
// ================================

// Homeの状態を保持（ユーザーごと）
// key: `${teamId}:${userId}`
const homeState = new Map();

const HOME_SCOPES = [
  { key: "active", label: "状態: 未完了" },
  { key: "done", label: "状態: 完了" },
  { key: "cancelled", label: "状態: 取り下げ" },
];

const BROADCAST_SCOPES = [
  { key: "to_me", label: "範囲: 自分あて" },
  { key: "requested_by_me", label: "範囲: 自分が発行" },
  { key: "all", label: "範囲: すべて" },
];

// 未完了 = done以外
const ACTIVE_STATUSES = ["open", "in_progress", "waiting"];
const DONE_STATUSES = ["done"];
const CANCELLED_STATUSES = ["cancelled"];

function getHomeState(teamId, userId) {
  const k = `${teamId}:${userId}`;
  const s = homeState.get(k) || {
    viewKey: "all",
    scopeKey: "active",
    personalScopeKey: "to_me",
    assigneeUserId: userId,
    deptKey: "all",
    broadcastScopeKey: "to_me",

    // ★既存：Home「もっと見る」展開状態（範囲=すべて の時だけUIで使う）
    homeMore: {
      overdue: false,
      today: false,
    },

    // ★追加：Home「畳む（開く/閉じる）」状態（もっと見るとは別）
    homeFold: {
      overdue: false,
      today: false,
      later: false,
    },
  };

  // 後方互換：昔のstateに homeMore が無い場合に備える
  const homeMore = {
    overdue: !!s?.homeMore?.overdue,
    today: !!s?.homeMore?.today,
  };

  // 後方互換：昔のstateに homeFold が無い場合に備える
  const homeFold = {
    overdue: !!s?.homeFold?.overdue,
    today: !!s?.homeFold?.today,
    later: !!s?.homeFold?.later,
  };

  // ★表示は常に「すべて」に固定（personal/broadcastの切替を使わない）
  // ★範囲は broadcastScopeKey を共通キーとして使う
  return {
    ...s,
    viewKey: "all",
    broadcastScopeKey: s.broadcastScopeKey || "to_me",
    personalScopeKey: s.broadcastScopeKey || s.personalScopeKey || "to_me",
    homeMore,
    homeFold,
  };
}

function setHomeState(teamId, userId, next) {
  const k = `${teamId}:${userId}`;

  // ★viewKey は固定、範囲は broadcastScopeKey に統一
  const prev = getHomeState(teamId, userId);

  const merged = {
    ...prev,
    ...next,
    viewKey: "all",
  };

  // homeMore はネストなので shallow merge だと壊れる → 明示的にmerge
  merged.homeMore = {
    ...(prev.homeMore || { overdue: false, today: false }),
    ...(next.homeMore || {}),
  };

  // homeFold もネストなので明示的にmerge
  merged.homeFold = {
    ...(prev.homeFold || { overdue: false, today: false, later: false }),
    ...(next.homeFold || {}),
  };

  if (merged.broadcastScopeKey) {
    merged.personalScopeKey = merged.broadcastScopeKey;
  }

  homeState.set(k, merged);
}

function homeScopeSelectElement(scopeKey) {
  const cur = HOME_SCOPES.find((s) => s.key === scopeKey) || HOME_SCOPES[0];
  return {
    type: "static_select",
    action_id: "home_scope_select",
    initial_option: {
      text: { type: "plain_text", text: cur.label },
      value: cur.key,
    },
    options: HOME_SCOPES.map((s) => ({
      text: { type: "plain_text", text: s.label },
      value: s.key,
    })),
  };
}

function broadcastScopeSelectElement(scopeKey) {
  const cur =
    BROADCAST_SCOPES.find((s) => s.key === scopeKey) || BROADCAST_SCOPES[0];
  return {
    type: "static_select",
    action_id: "home_broadcast_scope_select",
    initial_option: {
      text: { type: "plain_text", text: cur.label },
      value: cur.key,
    },
    options: BROADCAST_SCOPES.map((s) => ({
      text: { type: "plain_text", text: s.label },
      value: s.key,
    })),
  };
}

function deptSelectElement(currentDeptValue, currentDeptText) {
  const text = currentDeptText ? `部署: ${currentDeptText}` : "部署: すべて";
  const value = currentDeptValue || "all";
  const initial =
    value === "all" || value === "__none__"
      ? { text: { type: "plain_text", text }, value }
      : currentDeptText
        ? { text: { type: "plain_text", text }, value }
        : null;

  return {
    type: "external_select",
    action_id: "home_dept_select",
    placeholder: { type: "plain_text", text: "部署（@グループ）を検索" },
    min_query_length: 0,
    ...(initial ? { initial_option: initial } : {}),
  };
}

// personal: 担当者（任意） + 担当部署（ユーザーグループ） + 状態（done以外/ done）

async function dbListBroadcastTasksByStatuses(
  teamId,
  statuses,
  deptKey = "all",
  limit = 30,
) {
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

async function dbListBroadcastTasksByStatusesWithScope(
  teamId,
  statuses,
  scopeKey,
  viewerUserId,
  limit = 30,
) {
  const params = [teamId, statuses, limit];
  let joinTargets = "";
  let whereScope = "";

  // ★未完了一覧（doneを含まない）なら、自分が完了済みのbroadcastを除外
  let joinCompletions = "";
  let whereNotCompleted = "";
  const wantsNotCompleted = !(statuses || []).includes("done");

  if (scopeKey === "to_me") {
    // 対象者に自分を含む
    joinTargets =
      "JOIN task_targets tt ON tt.task_id::text = t.id AND tt.team_id=t.team_id";
    whereScope = "AND tt.user_id = $4";
    params.push(viewerUserId);

    // ★自分が完了済みなら「自分あて未完了」には出さない
    if (wantsNotCompleted) {
      joinCompletions =
        "LEFT JOIN task_completions tc ON tc.task_id::text = t.id AND tc.team_id=t.team_id AND tc.user_id = $4";
      whereNotCompleted = "AND tc.user_id IS NULL";
    }
  } else if (scopeKey === "requested_by_me") {
    // 依頼者が自分
    whereScope = "AND t.requester_user_id = $4";
    params.push(viewerUserId);
  } else {
    // all: no scope filter
  }

  const q = `
    SELECT x.*
    FROM (
      SELECT DISTINCT ON (t.id) t.*
      FROM tasks t
      ${joinTargets}
      ${joinCompletions}
      WHERE t.team_id=$1
        AND t.task_type='broadcast'
        AND t.status = ANY($2::text[])
        ${whereScope}
        ${whereNotCompleted}
      ORDER BY
        t.id,
        (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    ) x
    ORDER BY (x.due_date IS NULL) ASC, x.due_date ASC, x.created_at DESC
    LIMIT $3;
  `;

  const res = await dbQuery(q, params);
  return res.rows;
}

// PhaseX: personal 範囲フィルタ（to_me / requested_by_me / all）
async function dbListPersonalTasksByStatusesWithScope(
  teamId,
  statuses,
  scopeKey,
  viewerUserId,
  limit = 60,
) {
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

function taskLineForHome(task, viewKey) {
  const rawDesc = String(task.description || "")
    .replace(/\r\n/g, "\n")
    .trim();

  let preview = rawDesc || String(task.title || "（本文なし）");

  // 改行を整理
  preview = preview.replace(/\n{3,}/g, "\n\n");

  // ★ まず「最大5行」に制限
  const MAX_LINES = 5;
  const lines = preview.split("\n");
  if (lines.length > MAX_LINES) {
    preview = lines.slice(0, MAX_LINES).join("\n") + "\n…";
  }

  // ★ 次に「最大文字数」に制限
  const MAX_PREVIEW_CHARS = 100;
  if (preview.length > MAX_PREVIEW_CHARS) {
    preview = preview.slice(0, MAX_PREVIEW_CHARS) + "…";
  }

  preview = noMention(preview);

  if (!preview) preview = noMention(String(task.title || "（本文なし）"));
  return preview;
}


async function publishHome({ client, teamId, userId }) {
  const st = getHomeState(teamId, userId);
  const statuses =
    st.scopeKey === "done"
      ? DONE_STATUSES
      : st.scopeKey === "cancelled"
        ? CANCELLED_STATUSES
        : ACTIVE_STATUSES;

  const blocks = [];

  // ✅ フィルタは1行（actions）にまとめる：機能はそのまま
  const rangeKey0 = st.broadcastScopeKey || "to_me";
  const isAllRange0 = rangeKey0 === "all";

  // actions 内の select は placeholder が欲しいので、ここで付ける（既存functionは触らない）
  const rangeSelect = {
    ...broadcastScopeSelectElement(rangeKey0),
    placeholder: { type: "plain_text", text: "範囲" },
  };

  const stateSelect = {
    ...homeScopeSelectElement(st.scopeKey),
    placeholder: { type: "plain_text", text: "状態" },
  };

  // 範囲=すべて のときだけ部署も同じ行に出す
  if (isAllRange0) {
    const deptValue = st.deptKey || "all";
    let deptText =
      deptValue === "all"
        ? "すべて"
        : deptValue === "__none__"
          ? "未設定"
          : null;
    if (!isAllRange0) deptText = null; // 保険（ここは来ない）

    if (!deptText && deptValue) {
      const idToHandle = await getSubteamIdMap(teamId);
      const h = idToHandle.get(deptValue);
      deptText = h ? `@${h}` : "部署（@グループ）を検索";
    }

    const deptSelect = deptSelectElement(deptValue, deptText);

    blocks.push({
      type: "actions",
      elements: [rangeSelect, deptSelect, stateSelect],
    });
  } else {
    blocks.push({
      type: "actions",
      elements: [rangeSelect, stateSelect],
    });
  }

  blocks.push({ type: "divider" });


  // データ取得
  let tasks = [];

  // 混在ソート（due_date昇順 → created_at降順、due無しは最後）
  const toTime = (d) => {
    if (!d) return null;
    const dt = d instanceof Date ? d : new Date(d);
    return Number.isNaN(dt.getTime()) ? null : dt.getTime();
  };

  const cmp = (a, b) => {
    const at = toTime(a.due_date);
    const bt = toTime(b.due_date);
    if (at === null && bt !== null) return 1;
    if (at !== null && bt === null) return -1;
    if (at !== null && bt !== null && at !== bt) return at - bt;

    const ac = toTime(a.created_at);
    const bc = toTime(b.created_at);
    if (ac !== null && bc !== null && ac !== bc) return bc - ac;

    return String(b.id || "").localeCompare(String(a.id || ""));
  };

  // ★新：表示は常に「すべて」（personal + broadcast 混在）
  const rangeKey = st.broadcastScopeKey || "to_me";
  const deptKey = st.deptKey || "all";

  // personal は範囲で絞る（to_me / requested_by_me / all）
  const personalScope =
    rangeKey === "to_me" || rangeKey === "requested_by_me" ? rangeKey : "all";
  let personalTasks = await dbListPersonalTasksByStatusesWithScope(
    teamId,
    statuses,
    personalScope,
    userId,
    60,
  );

  // broadcast は範囲で絞る（to_me は JOIN、requested_by_me は requester、all は JOINなし）
  let broadcastTasks =
    rangeKey === "to_me" || rangeKey === "requested_by_me"
      ? await dbListBroadcastTasksByStatusesWithScope(
          teamId,
          statuses,
          rangeKey,
          userId,
          60,
        )
      : await dbListBroadcastTasksByStatuses(teamId, statuses, "all", 60);

  // ★範囲=すべて かつ 部署指定 のときだけ「@mkに関わる全て」に絞る（JS側）
  if (rangeKey === "all" && deptKey && deptKey !== "all") {
    const members = await getUsergroupMembers(teamId, deptKey);
    const memberSet = new Set((members || []).filter(Boolean));

    // personal: 担当者 or 依頼者 が部署メンバーに含まれるもの
    personalTasks = (personalTasks || []).filter((t) => {
      const a = t?.assignee_id;
      const r = t?.requester_user_id;
      return (a && memberSet.has(a)) || (r && memberSet.has(r));
    });

    // broadcast: (対象ユーザーに部署メンバーが含まれる) OR (依頼者が部署メンバー) OR (対象グループが一致)
    //  ※task_targets は既に入っている前提。入ってない古いデータがあっても group_id で拾えるようにしておく。
    broadcastTasks = (broadcastTasks || []).filter((t) => {
      const r = t?.requester_user_id;
      if (r && memberSet.has(r)) return true;

      const gid = t?.broadcast_group_id;
      if (gid && String(gid) === String(deptKey)) return true;

      return false;
    });

    // ※targets を使った「メンバーが対象になってるか」は DBから拾う必要があるので、
    //    ここでは差分最小のため broadcast_group_id / requester_user_id でまず成立させる
    //    （targets版までやるなら、task_targets JOIN を別関数で拾うのが安全）
  }

  const merged = [...personalTasks, ...broadcastTasks].sort(cmp);

  // ★保険：同一IDは必ず1つにする（重複完全排除）
  const seen = new Set();
  tasks = [];
  for (const t of merged) {
    const key = `${t.task_type || "personal"}:${t.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    tasks.push(t);
  }

  // public は参加していなくても表示する / private は表示しない
  // DM（Dxxxx）は基本表示しないが、「範囲=自分あて(to_me)」かつ personal で自分担当のものだけ表示する
  {
    const isToMe = rangeKey === "to_me";

    const uniqChannels = Array.from(
      new Set((tasks || []).map((t) => t.channel_id).filter(Boolean)),
    );
    const okMap = new Map();

for (const ch of uniqChannels) {
  const ok = await canUserSeeChannel({ client, teamId, channelId: ch, userId });
  okMap.set(ch, ok);
}

    tasks = (tasks || []).filter((t) => {
      const ch = String(t.channel_id || "");

      // チャンネル情報が無いものは落とさない
      if (!ch) return true;

      // ✅ DMは例外許可：to_me の personal で、自分が担当のものだけ通す
      if (ch.startsWith("D")) {
        const isPersonal = t.task_type !== "broadcast";
        const isMine = t.assignee_id && t.assignee_id === userId;
        return isToMe && isPersonal && isMine;
      }

      // 通常ルール：publicのみOK（private/その他はNG）
      return okMap.get(ch) === true;
    });
  }

  // 表示：未完了はステータス別に分ける（完了/取り下げはまとめ）
  if (st.scopeKey === "done") {
    // ★完了は直近N時間だけ表示（ここはあなたが2週間=336時間に変更する想定でOK）
    const DONE_VISIBLE_HOURS = 168; // ←あなたの方針でここを336にしてね
    const cutoffMs = Date.now() - DONE_VISIBLE_HOURS * 60 * 60 * 1000;

    const recentDoneTasks = (tasks || []).filter((t) => {
      const ts = t?.completed_at || t?.updated_at || t?.created_at;
      if (!ts) return false;
      const d = new Date(ts);
      if (Number.isNaN(d.getTime())) return false;
      return d.getTime() >= cutoffMs;
    });

    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: `*✅ 完了（直近${DONE_VISIBLE_HOURS}時間）*` },
    });

    if (!recentDoneTasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（直近の完了なし）" }],
      });
    } else {
      for (const t of recentDoneTasks) {
        const isBroadcast = t.task_type === "broadcast";

        // 「未完了に戻せる」権限
        const canReopen = isBroadcast
          ? userId === t.requester_user_id
          : userId === t.requester_user_id || userId === t.assignee_id;

        // 本文（行）
        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text:
              st.viewKey === "all"
                ? taskLineForHome(t, isBroadcast ? "broadcast" : "personal")
                : taskLineForHome(t, st.viewKey),
          },
        });

        // ボタン行：未完了に戻す / 詳細
        const elems = [];

        if (canReopen) {
          elems.push({
            type: "button",
            text: { type: "plain_text", text: "未完了に戻す ↩️" },
            action_id: "reopen_task",
            value: JSON.stringify({ teamId, taskId: t.id }),
            confirm: {
              title: { type: "plain_text", text: "確認" },
              text: { type: "mrkdwn", text: "このタスクを*未完了*に戻します。" },
              confirm: { type: "plain_text", text: "戻す" },
              deny: { type: "plain_text", text: "やめる" },
            },
          });
        }

        elems.push({
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
        });

        blocks.push({ type: "actions", elements: elems });

        blocks.push({ type: "divider" });
      }
    }
  } else {
    // ================================
    // ②：未完了は「期限切れ / 期限内」でグルーピング（JST 기준）
    // ================================
    const today = todayJstYmd(); // 既存関数（JSTのYYYY-MM-DD）を使う :contentReference[oaicite:2]{index=2}

    const dueYmdOf = (t) =>
      slackDateYmd(t?.due_date) ||
      (typeof t?.due_date === "string" ? t.due_date.slice(0, 10) : "");

    const isOverdue = (t) => {
      const due = dueYmdOf(t);
      if (!due) return false; // dueなしは「期限内」扱い（仕様確定後に変えられる）
      return due < today;
    };

    const overdue = tasks.filter((t) => isOverdue(t));

    const todayTasks = tasks.filter((t) => {
      const due = dueYmdOf(t);
      return due && !isOverdue(t) && due === today;
    });

    const laterTasks = tasks.filter((t) => {
      const due = dueYmdOf(t);
      return !isOverdue(t) && (!due || due > today);
    });

    const requesterIconMap = new Map();
    const assigneeIconMap = new Map();

    // requester（全タスク）
    const requesterIds = Array.from(
      new Set((tasks || []).map((t) => t?.requester_user_id).filter(Boolean)),
    );

    // assignee（broadcastは複数対象なので除外）
    const assigneeIds = Array.from(
      new Set(
        (tasks || [])
          .filter((t) => t?.task_type !== "broadcast")
          .map((t) => t?.assignee_id)
          .filter(Boolean),
      ),
    );

    // ★範囲=すべて(all) の時はアイコン取得しない（大量呼び出しで固まりやすいので）
    if (rangeKey !== "all") {
      await Promise.all(
        requesterIds.map(async (uid) => {
          const url = await getUserIconUrl(teamId, uid);
          if (url) requesterIconMap.set(uid, url);
        }),
      );

      await Promise.all(
        assigneeIds.map(async (uid) => {
          const url = await getUserIconUrl(teamId, uid);
          if (url) assigneeIconMap.set(uid, url);
        }),
      );
    }

    const pushTaskList = async (title, list, totalCount = null, opts = null) => {
  // Slack Home view は blocks <= 100 制限がある
  const MAX_BLOCKS = 100;
  const SAFETY = 8; // 見出しや末尾の余裕

  const canAdd = (n) => blocks.length + n <= MAX_BLOCKS - SAFETY;

  const titlePlain = String(title || "")
    .replace(/\*/g, "")
    .trim();

  const count = totalCount ?? list.length;

  // ✅ 見出しは「ボタン1個」の行として表示（疑似：見出しクリック）
  if (opts?.toggleAction) {
    const label = `${titlePlain}（${count}件） ${opts.toggleLabel || ""}`.trim();

    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: label },
          action_id: opts.toggleAction.action_id,
          value: opts.toggleAction.value,
        },
      ],
    });
  } else {
    blocks.push({
      type: "header",
      text: {
        type: "plain_text",
        text: `${titlePlain}（${count}件）`,
      },
    });
  }
  blocks.push({ type: "divider" });

  // ✅ 畳み状態：一覧を出さない（もっと見るとは別）
  if (opts?.folded) {
    if (canAdd(2)) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（閉じています）" }],
      });
      blocks.push({ type: "divider" });
    }
    return;
  }

  if (!list.length) {
    if (canAdd(2)) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（なし）" }],
      });
      blocks.push({ type: "divider" });
    }
    return;
  }

  let shown = 0;

  // ★範囲=すべて のときだけ、詳細を右に出す（下には出さない）
  const showDetailOnRight = rangeKey === "all";
      for (const t of list) {
        const viewKey = t.task_type === "broadcast" ? "broadcast" : "personal";

        // ★ broadcastで「自分が完了済みか？」を判定（範囲=自分あて の時だけ）
        const viewerCompleted =
          rangeKey === "to_me" && t.task_type === "broadcast"
            ? await dbHasUserCompleted(teamId, t.id, userId)
            : false;

        // 詳細ボタン（共通）
        const detailBtn = {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
        };

        // 追加ブロック数を見積もる（※range=all は actions を省けるので軽くなる）
        // 本文(section) + 人(context) + 期限/link(context) + 区切り(context) = 4
        // to_me は完了/詳細 actions が基本つくので +1 (完了済み表示があると +1)
        const needsActions =
          !showDetailOnRight && rangeKey !== "to_me"
            ? true // (詳細を下に出す)
            : rangeKey === "to_me"; // (完了/詳細が必要)

        const needsCompletedHint =
          rangeKey === "to_me" && t.task_type === "broadcast" && viewerCompleted;

        const estimated =
          1 + // section
          1 + // people context
          1 + // meta context
          (needsCompletedHint ? 1 : 0) + // "あなたは完了済み" context
          (needsActions ? 1 : 0) + // actions
          1; // separator context

        if (!canAdd(estimated)) break;

        // ✅ 主：タスク内容（本文）
        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: taskLineForHome(t, viewKey),
          },
          ...(showDetailOnRight ? { accessory: detailBtn } : {}),
        });

        // ✅ 小：アイコン + 依頼者 ⇒ アイコン + 対応者（既存のアイコンMapを利用）
        const requesterId = t?.requester_user_id;
        const assigneeId = t?.assignee_id;

        const requesterIcon = requesterId ? requesterIconMap.get(requesterId) : null;
        const assigneeIcon =
          t?.task_type !== "broadcast" && assigneeId
            ? assigneeIconMap.get(assigneeId)
            : null;

        const assigneeText =
          viewKey === "broadcast"
            ? assigneeDisplay(t)
            : assigneeId
              ? `<@${assigneeId}>`
              : "-";

        const peopleElements = [];
        if (requesterIcon)
          peopleElements.push({
            type: "image",
            image_url: requesterIcon,
            alt_text: "requester",
          });
        if (requesterId)
          peopleElements.push({ type: "mrkdwn", text: `<@${requesterId}>` });
        peopleElements.push({ type: "mrkdwn", text: "⇒" });
        if (assigneeIcon)
          peopleElements.push({
            type: "image",
            image_url: assigneeIcon,
            alt_text: "assignee",
          });
        peopleElements.push({ type: "mrkdwn", text: assigneeText });

        blocks.push({ type: "context", elements: peopleElements });

        // ✅ 小：期限 + 元メッセージへリンク
        const dueText = t?.due_date ? `（${formatDueDateOnly(t.due_date)}）まで` : "";
        const linkText = t?.source_permalink
          ? `🔗 <${t.source_permalink}|元メッセージへ>`
          : "";

        const metaElems = [];
        if (dueText) metaElems.push({ type: "mrkdwn", text: dueText });
        if (linkText) metaElems.push({ type: "mrkdwn", text: linkText });

        blocks.push({
          type: "context",
          elements: metaElems.length ? metaElems : [{ type: "mrkdwn", text: " " }],
        });

        // ✅ Homeの完了ボタンは「範囲=自分あて（to_me）」の時だけ
        if (rangeKey !== "to_me") {
          // rangeKey: requested_by_me / all など
          // ★範囲=all のときは「詳細」は右に出してるので、actions自体を省略する（ブロック節約）
          if (!showDetailOnRight) {
            blocks.push({
              type: "actions",
              elements: [detailBtn],
            });
          }
        } else {
          // rangeKey === "to_me"
          if (t.task_type === "broadcast" && viewerCompleted) {
            // 「完了済み」表示（グレー相当）
            blocks.push({
              type: "context",
              elements: [{ type: "mrkdwn", text: "✅ あなたは完了済み" }],
            });

            // 詳細だけ（※showDetailOnRight の場合は下に出さない）
            if (!showDetailOnRight) {
              blocks.push({
                type: "actions",
                elements: [detailBtn],
              });
            }
          } else {
            // 完了 +（必要なら）詳細（自分あての時だけ）
            const elems = [
              {
                type: "button",
                text: {
                  type: "plain_text",
                  text: t.task_type === "broadcast" ? "自分だけ完了 ✅" : "完了 ✅",
                },
                style: "primary",
                action_id: "complete_task",
                value: JSON.stringify({ teamId, taskId: t.id }),
              },
              detailBtn,
            ];

            blocks.push({ type: "actions", elements: elems });
          }
        }

blocks.push({ type: "divider" });
        shown++;
      }

      if (shown < list.length && canAdd(2)) {
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: `…他 ${list.length - shown}件` }],
        });
        blocks.push({ type: "divider" });
      }
      // NOTE: ループ内ですでに各タスクの後に divider を入れているため、
      // ここでの追加 divider は二重線の原因になるので入れない

    };

    // スマホ優先：期限切れ → 今日 → 明日以降
 // スマホ優先：期限切れ → 今日 → 明日以降
const MORE_LIMIT = 10; // ★範囲=すべて(all) のときだけ「もっと見る」で段階表示

const isAllRange = rangeKey === "all";

const overdueTotal = overdue.length;
const todayTotal = todayTasks.length;

const overdueExpanded = !!st.homeMore?.overdue;
const todayExpanded = !!st.homeMore?.today;

// ✅ 追加：畳み状態（もっと見るとは別）
const overdueFolded = !!st.homeFold?.overdue;
const todayFolded = !!st.homeFold?.today;
const laterFolded = !!st.homeFold?.later;

const overdueVisible =
  isAllRange && !overdueExpanded ? overdue.slice(0, MORE_LIMIT) : overdue;

const todayVisible =
  isAllRange && !todayExpanded
    ? todayTasks.slice(0, MORE_LIMIT)
    : todayTasks;

// ✅ 見出し右の「開く/閉じる」ボタン
const overdueFoldBtn =
  overdueTotal > 0
    ? {
        type: "button",
        text: { type: "plain_text", text: overdueFolded ? "開く" : "閉じる" },
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "overdue" }),
      }
    : null;

const todayFoldBtn =
  todayTotal > 0
    ? {
        type: "button",
        text: { type: "plain_text", text: todayFolded ? "開く" : "閉じる" },
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "today" }),
      }
    : null;

const laterFoldBtn =
  laterTasks.length > 0
    ? {
        type: "button",
        text: { type: "plain_text", text: laterFolded ? "開く" : "閉じる" },
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "later" }),
      }
    : null;

await pushTaskList("*🚨 期限切れ*", overdueVisible, overdueTotal, {
  toggleAction: {
    action_id: "home_toggle_fold",
    value: JSON.stringify({ section: "overdue" }),
  },
  toggleLabel: overdueFolded ? "▽" : "△",
  folded: overdueFolded,
});

// ★既存：もっと見る（期限切れ）
// 畳んでいるときは出さない（押せても意味ない＆混乱するので）
if (!overdueFolded && isAllRange && overdueTotal > MORE_LIMIT) {
  const hidden = Math.max(0, overdueTotal - overdueVisible.length);
  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        text: {
          type: "plain_text",
          text: overdueExpanded ? "閉じる" : `もっと見る（残り${hidden}件）`,
        },
        action_id: "home_toggle_more",
        value: JSON.stringify({ section: "overdue" }),
      },
    ],
  });
  blocks.push({ type: "divider" });
}

await pushTaskList("*🟨 今日*", todayVisible, todayTotal, {
  toggleAction: {
    action_id: "home_toggle_fold",
    value: JSON.stringify({ section: "today" }),
  },
  toggleLabel: todayFolded ? "▽" : "△",
  folded: todayFolded,
});

// ★既存：もっと見る（今日）
if (!todayFolded && isAllRange && todayTotal > MORE_LIMIT) {
  const hidden = Math.max(0, todayTotal - todayVisible.length);
  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        text: {
          type: "plain_text",
          text: todayExpanded ? "閉じる" : `もっと見る（残り${hidden}件）`,
        },
        action_id: "home_toggle_more",
        value: JSON.stringify({ section: "today" }),
      },
    ],
  });
  blocks.push({ type: "divider" });
}

await pushTaskList("*🟩 明日以降*", laterTasks, null, {
  toggleAction: {
    action_id: "home_toggle_fold",
    value: JSON.stringify({ section: "later" }),
  },
  toggleLabel: laterFolded ? "▽" : "△",
  folded: laterFolded,
});
  }

  // ✅ 末尾が見切れてスクロールできない対策：
  // 原因として多いのが「Slack Home の blocks 上限（100個）に到達して末尾が切られる」こと。
  // 折り畳み導入でブロック数が増えると、最後の余白ブロック自体が捨てられて効かなくなる。
  //
  // 対策：
  // - publish 前に blocks を上限内に収める（末尾用の“余白”枠を必ず確保）
  // - 余白は actions を使って高さを確保（スクロール終端の“底上げ”）
  // ✅ 見切れ対策の“余白”を「見える空ボタン」ではなく、ほぼ見えない context で作る
  // - actions(button) は空のUI部品が見えてしまい不快になりがち
  // - context は最小高さを持ちつつ、ゼロ幅スペースなら表示上ほぼ何も出ない
  // - 足りない環境向けに context を複数入れて高さを確保する
  const FOOTER_BLOCKS = [
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }], // ゼロ幅スペース（見えない）
    },
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }],
    },
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }],
    },
  ];

  // Slack blocks 上限は 100。末尾の余白ブロック分を先に確保する
  const SLACK_BLOCK_LIMIT = 100;
  const RESERVE = FOOTER_BLOCKS.length;

  // ✅ blocks が多すぎる場合、末尾が切られて「スクロールできない」原因になるので削る
  if (blocks.length > SLACK_BLOCK_LIMIT - RESERVE) {
    // 末尾は中途半端に切るとUIが崩れやすいので、
    // 「安全に収まるところまで」ガツッと切って注意文を入れる
    const keep = SLACK_BLOCK_LIMIT - RESERVE - 1; // 注意文1個分も確保
    blocks.splice(keep);

    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text:
            "⚠️ 表示件数が多いため一部を省略しました。フィルタ/折り畳みで絞ると下まで見やすくなるよ。",
        },
      ],
    });
  }

  // ✅ 最後に“必ず残る”余白を付与
  blocks.push(...FOOTER_BLOCKS);

  await client.views.publish({
    user_id: userId,
    view: {
      type: "home",
      callback_id: "home",
      blocks,
    },
  });

}

function extractTsFromPermalink(url) {
  // permalink例: https://xxx.slack.com/archives/C12345678/p1707350400000123
  // -> ts: 1707350400.000123
  const s = String(url || "");
  const m = s.match(/\/p(\d{10})(\d{6,})/);
  if (!m) return null;
  return `${m[1]}.${m[2].padStart(6, "0")}`;
}

async function getTeamIdViaAuthTest(client) {
  try {
    const r = await client.auth.test();
    return r?.team_id || null;
  } catch (_) {
    return null;
  }
}

// ================================
// Broadcast: usergroup options (external_multi_select)
// ================================
async function searchUsergroups(query) {
  const res = await app.client.usergroups.list({ include_users: false });
  const groups = (res.usergroups || [])
    .filter((g) => g?.id && g?.handle)
    .map((g) => ({ id: g.id, handle: String(g.handle).replace(/^@/, "") }));

  const q = String(query || "")
    .toLowerCase()
    .trim();
  const filtered = !q
    ? groups
    : groups.filter((g) => g.handle.toLowerCase().includes(q));

  // 上限はSlack推奨に合わせて適当に絞る
  return filtered.slice(0, 100);
}

// ================================
// Usergroup members cache (for Home dept filter by group_id)
// ================================
const USERGROUP_MEMBERS_CACHE_MS = 10 * 60 * 1000;
const usergroupMembersCache = new Map(); // `${teamId}:${groupId}` -> { at, users: string[] }

async function getUsergroupMembers(teamId, groupId) {
  if (!groupId) return [];
  const key = `${teamId}:${groupId}`;
  const cached = usergroupMembersCache.get(key);
  if (cached && Date.now() - cached.at < USERGROUP_MEMBERS_CACHE_MS)
    return cached.users || [];

  try {
    const res = await app.client.usergroups.users.list({ usergroup: groupId });
    const users = res?.users || [];
    usergroupMembersCache.set(key, { at: Date.now(), users });
    return users;
  } catch (e) {
    console.error("usergroups.users.list error:", e?.data || e);
    usergroupMembersCache.set(key, { at: Date.now(), users: [] });
    return [];
  }
}

// ================================
// Channel visibility cache（参加チャンネルのみ表示）
// - public でも「ユーザーが参加していない」なら表示しない
// - private / DM は表示しない（既存方針維持）
// ================================
const CHANNEL_VIS_CACHE_MS = 10 * 60 * 1000;
const channelVisCache = new Map(); // `${teamId}:${channelId}` -> { at, ok }

// user -> joined channels cache（API節約）
const USER_JOINED_CH_CACHE_MS = 10 * 60 * 1000;
const userJoinedChCache = new Map(); // `${teamId}:${userId}` -> { at, set: Set<string> }

async function listUserJoinedChannelsSet(client, teamId, userId) {
  const key = `${teamId}:${userId}`;
  const cached = userJoinedChCache.get(key);
  if (cached && Date.now() - cached.at < USER_JOINED_CH_CACHE_MS) return cached.set;

  const set = new Set();
  let cursor;
  try {
    do {
      const res = await client.users.conversations({
        user: userId,
        types: "public_channel,private_channel",
        limit: 200,
        cursor,
        exclude_archived: true,
      });
      for (const ch of res?.channels || []) {
        if (ch?.id) set.add(ch.id);
      }
      cursor = res?.response_metadata?.next_cursor || null;
    } while (cursor);
  } catch (e) {
    // 失敗時は空（= 表示しない）に倒す
    console.error("users.conversations error:", e?.data || e);
  }

  userJoinedChCache.set(key, { at: Date.now(), set });
  return set;
}

async function canUserSeeChannel({ client, teamId, channelId, userId }) {
  if (!channelId) return true;
  if (!userId) return false; // user前提の判定に寄せる

  // まずIDプレフィックスで高速判定（API節約）
  const id0 = String(channelId)[0];
  if (id0 === "D") return false; // DM
  if (id0 === "G") return false; // private channel（Homeには出さない方針）

  // public channel: 参加している場合のみ表示
  if (id0 === "C") {
    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  }

  // 想定外のID（例：共有チャンネル等）は conversations.info で public を確認しつつ、参加判定
  const key = `${teamId}:${channelId}`;
  const cached = channelVisCache.get(key);
  if (cached && Date.now() - cached.at < CHANNEL_VIS_CACHE_MS) {
    if (!cached.ok) return false;
    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  }

  try {
    const info = await client.conversations.info({ channel: channelId });
    const ch = info?.channel;
    const isPublic = !!ch?.is_channel && !ch?.is_private;
    channelVisCache.set(key, { at: Date.now(), ok: isPublic });
    if (!isPublic) return false;

    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  } catch (_) {
    channelVisCache.set(key, { at: Date.now(), ok: false });
    return false;
  }
}

app.options("home_dept_select", async ({ ack, payload }) => {
  try {
    const q = payload?.value || "";
    const groups = await searchUsergroups(q);

    const options = [
      { text: { type: "plain_text", text: "すべて" }, value: "all" },
      { text: { type: "plain_text", text: "未設定" }, value: "__none__" },
      ...groups.map((g) => ({
        text: { type: "plain_text", text: `@${g.handle}` },
        value: g.id,
      })),
    ];

    await ack({ options });
  } catch (e) {
    console.error("home_dept_select options error:", e?.data || e);
    await ack({
      options: [
        { text: { type: "plain_text", text: "すべて" }, value: "all" },
        { text: { type: "plain_text", text: "未設定" }, value: "__none__" },
      ],
    });
  }
});

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

    const q = String(payload?.value || "")
      .trim()
      .toLowerCase();
    // 初期候補：未入力でも上位5件を返す（担当部署があればその所属から、なければ全員から）
    const allUsers = await listUsersCached(teamId);

    // dept 絞り込み用の許可集合（null=絞り込みなし）
    let allowed = null;
    if (deptKey && deptKey !== "all" && deptKey !== "__none__") {
      const members = await getUsergroupMembers(teamId, deptKey);
      allowed = new Set(members || []);
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
      .sort((a, b) => {
        if (a.id === userId) return -1;
        if (b.id === userId) return 1;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 100)
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
      setHomeState(teamId, userId, {
        viewKey: "all",
        scopeKey: "active",
        personalScopeKey: "to_me",
        assigneeUserId: userId,
        deptKey: "all",
        broadcastScopeKey: "to_me",
      });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("app_home_opened error:", e?.data || e);
  }
});

// ================================
// Custom Step (Workflow Builder): 情シス依頼を自動タスク化
// callback_id: josys_taskify
// ================================
app.function("josys_taskify", async ({ client, inputs, complete, fail, logger }) => {
  try {
    let teamId = inputs?.team_id || inputs?.teamId || null; // 任意
    const requesterUserId = inputs?.requester_user_id || inputs?.requesterUserId || null;
    const typeLabel = String(inputs?.type_label || inputs?.typeLabel || "").trim();
    const content = String(inputs?.content || "").trim();
    const dueRaw = inputs?.due_date || inputs?.dueDate || null;
    const channelId = inputs?.channel_id || inputs?.channelId || null;

// message_ts が取れない環境向け：メッセージのリンクから復元
    const messageLink =
      inputs?.message_link ||
      inputs?.messageLink ||
      inputs?.message_url ||
      inputs?.messageUrl ||
      inputs?.message_permalink ||
      inputs?.messagePermalink ||
      null;

    let msgTs = inputs?.message_ts || inputs?.messageTs || null;
    if (!msgTs && messageLink) {
      msgTs = extractTsFromPermalink(messageLink);
    }

// teamId も inputs に無い環境向け：client から取得
    if (!teamId) {
     teamId = await getTeamIdViaAuthTest(client);
    }

    const corpGroupId = process.env.CORP_SYSTEM_USERGROUP_ID || "";
    const corpHandle = (process.env.CORP_SYSTEM_HANDLE || "corp-system").replace(/^@/, "");

    // ✅ 必須が欠けたら「タスク化しない」
    // ただしワークフロー側の設定ミスに気づけるよう fail にする（静かにスキップしたいなら complete に変更も可）
if (!requesterUserId || !typeLabel || !dueRaw || !channelId || !corpGroupId) {
await complete({ outputs: { task_id: null, skipped: "missing_required" } });
return;

}

// message_ts が無い場合は message_link が必須（どっちも無ければタスク化しない）
if (!msgTs) {
await complete({ outputs: { task_id: null, skipped: "missing_required" } });
return;

}
    // due を YYYY-MM-DD に寄せる（既存ヘルパーを利用）
    const due = slackDateYmd(dueRaw);
    if (!due) {
      await complete({ outputs: { task_id: null, skipped: "missing_required" } });
      return;
    }

    // teamId は body から取れないので、最優先で inputs 経由に寄せる
    // もし入らない環境だったら、ここは後でログ見て調整しよ🫶
if (!teamId) {
  logger?.error?.("missing teamId (inputs/auth.test)");
  await complete({ outputs: { task_id: null, skipped: "missing_required" } });
return;

}


    // 二重作成防止（同一メッセージ起点は1回だけ）
    const existing = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existing?.id) {
      await complete({ outputs: { task_id: existing.id } });
      return;
    }

    // corp-system usergroup を展開して broadcast targets を作る
    const { users: usersFromGroups } = await expandTargetsFromGroups(teamId, [corpGroupId]);
    const targetList = Array.from(usersFromGroups || new Set()).filter(Boolean);

    if (!targetList.length) {
      await complete({ outputs: { task_id: null, skipped: "missing_required" } });
return;

    }

    // permalink（元投稿リンク）
    let permalink = "";
    try {
      const r = await client.chat.getPermalink({ channel: channelId, message_ts: msgTs });
      permalink = r?.permalink || "";
    } catch (_) {}

    // title/desc
    const descParts = [];
    descParts.push(`【種別】${typeLabel}`);
    descParts.push(`【期日】${due}`);
    if (content) descParts.push(`【依頼内容】\n${content}`);
    const description = descParts.join("\n");

    const taskId = randomUUID();
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId,
      message_ts: msgTs,
      source_permalink: permalink || null,
      title: typeLabel, // ✅ タイトルは種別
      description,
      requester_user_id: requesterUserId,     // ✅ 依頼主 = workflow 実行者
      created_by_user_id: requesterUserId,
      assignee_id: null,                      // broadcast 固定
      assignee_label: `@${corpHandle}`,
      status: "in_progress",
      due_date: due,
      requester_dept: requesterDept,
      assignee_dept: null,
      task_type: "broadcast",
      broadcast_group_handle: `@${corpHandle}`,
      broadcast_group_id: corpGroupId,
      total_count: targetList.length,
      completed_count: 0,
      notified_at: null,
    });

    // broadcast: snapshot targets & counts
    await dbInsertTaskTargets(teamId, taskId, targetList);
    const total = await dbCountTargets(teamId, taskId);
    await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
    created.total_count = total;
    created.completed_count = 0;

    // スレッドカード更新（DMは除外）
    try {
      if (!String(channelId || "").startsWith("D")) {
        const blocks = await buildThreadCardBlocks({ teamId, task: created });
        await upsertThreadCard(client, { teamId, channelId, parentTs: msgTs, blocks });
      }
    } catch (e) {
      console.error("workflow step upsertThreadCard error:", e?.data || e);
    }

    // 発行通知（依頼主自身は除外）
    try {
      const toNotify = targetList.filter((u) => u && u !== requesterUserId);
      for (const uid of toNotify) {
        await notifyTaskSimpleDM(uid, created, "📝 タスクが届いたよ");
      }
    } catch (e) {
      console.error("workflow step notify error:", e?.data || e);
    }

    // Home更新（依頼主 + 全対象者）
    try {
      const toRefresh = Array.from(new Set([requesterUserId, ...targetList].filter(Boolean)));
      publishHomeForUsers(client, teamId, toRefresh, 200);
      setTimeout(() => publishHomeForUsers(client, teamId, toRefresh, 200), 200);
    } catch (_) {}

    await complete({ outputs: { task_id: taskId } });
  } catch (error) {
    logger?.error?.(error);
    await complete({ outputs: { task_id: null, skipped: "missing_required" } });
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

    // ✅ 依頼主の初期値は「タスク化した人」
    const actorUserId = shortcut.user?.id || "";


    const msgBlocks = shortcut.message?.blocks || null;

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);

    // ============================
    // ✅ メンション初期値（ユーザー / ユーザーグループ）
    // - 「メンションがある時だけ」初期値に入れる（勝手に自分が入らないように）
    // ============================
    const hasUserMention =
      extractUserIdsFromBlocks(msgBlocks || []).length > 0 ||
      /<@([A-Z0-9]+)(?:\|[^>]+)?>/.test(String(rawText || ""));

    const hasGroupMention =
      extractUserGroupIdsFromBlocks(msgBlocks || []).length > 0 ||
      /<!subteam\^([A-Z0-9]+)(?:\|[^>]+)?>/.test(String(rawText || ""));

    let initialUserIds = [];
    let initialGroupIds = [];
    let initialGroupOptions = [];

    if (hasUserMention || hasGroupMention) {
            // ✅ メンションが無いときの fallback も「タスク化した人」にする
      const targets = inferTargetsFromMessage(rawText, actorUserId, msgBlocks);

      initialUserIds = Array.isArray(targets.userIds) ? targets.userIds.filter(Boolean) : [];
      initialGroupIds = Array.isArray(targets.groupIds) ? targets.groupIds.filter(Boolean) : [];

      // user_select の初期値が多すぎると邪魔なので一応上限（任意）
      initialUserIds = initialUserIds.slice(0, 10);
      initialGroupIds = initialGroupIds.slice(0, 10);

      // groupId -> handle へ（@mk-all みたいな表示用）
      if (initialGroupIds.length) {
        const idToHandle = await getSubteamIdMap(teamId);
        initialGroupOptions = initialGroupIds.map((gid) => {
          const handle = idToHandle.get(gid) || gid;
          const label = `@${String(handle).replace(/^@/, "")}`;
          return {
            text: { type: "plain_text", text: label },
            value: gid,
          };
        });
      }
    }

    await client.views.open({
      trigger_id: shortcut.trigger_id,
      view: {
        type: "modal",
        callback_id: "task_modal",
        // ✅ private_metadata は軽量に（3000文字制限対策）
                private_metadata: JSON.stringify({
          teamId,
          channelId,
          msgTs,
          // ✅ 後方互換のため残してOK（submit側はstate優先）
          requesterUserId: actorUserId,
        }),

        title: { type: "plain_text", text: "タスク作成" },
        submit: { type: "plain_text", text: "決定" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks: [
                    {
            type: "input",
            block_id: "desc",
            label: { type: "plain_text", text: "詳細（元メッセージ全文）" },
            element: {
              type: "plain_text_input",
              action_id: "desc_input",
              multiline: true,
              initial_value: prettyText || "",
            },
          },

          // ✅ 依頼主（選択式）：初期値は「タスク化した人」
          {
            type: "input",
            block_id: "requester",
            label: { type: "plain_text", text: "依頼主" },
            element: {
              type: "users_select",
              action_id: "requester_user_select",
              initial_user: actorUserId,
              placeholder: { type: "plain_text", text: "依頼主を選択" },
            },
          },

          // 対応者（個人：複数OK）
          {
            type: "input",
            optional: true,
            block_id: "assignee_users",
            label: { type: "plain_text", text: "対応者（個人・複数OK）" },
            element: {
              type: "multi_users_select",
              action_id: "assignee_users_select",
              placeholder: { type: "plain_text", text: "ユーザーを選択" },
              ...(initialUserIds.length ? { initial_users: initialUserIds } : {}),
            },
          },

          // 対応者（グループ：@ALL-xxx / @mk-all 等）
          {
            type: "input",
            optional: true,
            block_id: "assignee_groups",
            label: {
              type: "plain_text",
              text: "対応者（グループ：@ALL-xxx / @mk-all など）",
            },
            element: {
              type: "multi_external_select",
              action_id: "assignee_groups_select",
              placeholder: {
                type: "plain_text",
                text: "ユーザーグループを検索",
              },
              min_query_length: 0,
              ...(initialGroupOptions.length
                ? { initial_options: initialGroupOptions }
                : {}),
            },
          },

          {
            type: "input",
            block_id: "due",
            label: { type: "plain_text", text: "期限" },
            element: {
              type: "datepicker",
              action_id: "due_date",
              placeholder: { type: "plain_text", text: "期限" },
              initial_date: todayJstYmd(),
            },
          },

          {
            type: "context",
            elements: [
              {
                type: "mrkdwn",
                text: "💡 対象が1人なら「個人タスク」、2人以上またはグループ指定なら「全社/複数タスク」になります。",
              },
            ],
          },
        ],
      },
    });
  } catch (e) {
    console.error("shortcut error:", e?.data || e);
  }
});


// ================================
// Reaction ✅ -> Task create (via ephemeral button)

// reaction prompt: memory dedupe (no DB)

// ================================

// ✅ のリアクション名（Slack内部名）
const TASK_REACTION_NAME = "task";

// blocks から user_id を拾う（rich_text の user mention を拾う）
function extractUserIdsFromBlocks(blocks) {
  const out = [];
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (typeof node !== "object") return;

    if (node.type === "user" && node.user_id) {
      if (!out.includes(node.user_id)) out.push(node.user_id);
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(blocks);
  return out;
}

// blocks から user_id を拾う（既存）
function extractUserIdsFromBlocks(blocks) {
  const out = [];
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (typeof node !== "object") return;

    if (node.type === "user" && node.user_id) {
      if (!out.includes(node.user_id)) out.push(node.user_id);
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(blocks);
  return out;
}

// ★追加：blocks から usergroup_id を拾う（rich_text の usergroup）
function extractUserGroupIdsFromBlocks(blocks) {
  const out = [];
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (typeof node !== "object") return;

    // Slackのrich_textで usergroup は type: "usergroup" が来ることがある
    if (node.type === "usergroup" && node.usergroup_id) {
      if (!out.includes(node.usergroup_id)) out.push(node.usergroup_id);
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(blocks);
  return out;
}

// ★追加：text から usergroup token を拾う（<!subteam^ID|@handle> / <!subteam^ID>）
function extractUserGroupIdsFromText(rawText) {
  const text = String(rawText || "");
  const out = [];
  const re = /<!subteam\^([A-Z0-9]+)(?:\|[^>]+)?>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const gid = m[1];
    if (gid && !out.includes(gid)) out.push(gid);
  }
  return out;
}

function inferTargetsFromMessage(rawText, fallbackUserId, blocks = null) {
  const users = [];
  const groups = [];

  // ① blocks 優先（textにIDが出ない投稿を救う）
  for (const u of extractUserIdsFromBlocks(blocks)) users.push(u);
  for (const g of extractUserGroupIdsFromBlocks(blocks)) groups.push(g);

  // ② text の <@Uxxx> と <!subteam^...> を拾う（保険）
  {
    const text = String(rawText || "");
    const ure = /<@([A-Z0-9]+)(?:\|[^>]+)?>/g;
    let m;
    while ((m = ure.exec(text)) !== null) {
      const uid = m[1];
      if (uid && !users.includes(uid)) users.push(uid);
    }
  }
  for (const g of extractUserGroupIdsFromText(rawText)) {
    if (!groups.includes(g)) groups.push(g);
  }

  // ③ 何も無ければ fallback（リアクションした人）
  if (!users.length) users.push(fallbackUserId);

  return { userIds: users.filter(Boolean), groupIds: groups.filter(Boolean) };
}

function buildReactionPromptBlocks({
  previewText,
  assigneeId,
  dueYmd,
  payloadCreate,
  payloadEdit,
}) {
  const safePreview = noMention((previewText || "").trim()) || "（本文なし）";
  const short =
    safePreview.length > 300 ? safePreview.slice(0, 300) + "…" : safePreview;

  return [
    { type: "header", text: { type: "plain_text", text: "✅ タスク化の確認" } },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*内容*\n>${short.replace(/\n/g, "\n>")}` },
    },
    {
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: `👤 *対応者*：<@${assigneeId}>　　📅 *期限*：${dueYmd}（今日）`,
        },
      ],
    },
    { type: "divider" },
    {
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "タスク化" },
          style: "primary",
          action_id: "reaction_task_confirm_create",
          value: payloadCreate,
        },
        {
          type: "button",
          text: { type: "plain_text", text: "内容編集" },
          action_id: "reaction_task_open_edit_modal",
          value: payloadEdit,
        },
      ],
    },
  ];
}

app.event("reaction_added", async ({ event, client, body }) => {
  try {
    if ((event?.reaction || "") !== TASK_REACTION_NAME) return;

    const teamId = body?.team_id || body?.team?.id || event?.team;
    const channelId = event?.item?.channel;
    const msgTs = event?.item?.ts;
    const actorUserId = event?.user; // リアクションした人
    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    // すでに「確認UI（スレッドカード）」を出していたら何もしない（1メッセージ1回）
    const existingCard = await dbGetThreadCard(teamId, channelId, msgTs);
    if (existingCard?.card_ts) return;

    // すでにタスク化済みなら案内だけ（ここは現行踏襲）
    const existingTask = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTask?.id) {
      await safeEphemeral(
        client,
        channelId,
        actorUserId,
        "✅ それ、もうタスク化済みだよ〜！",
      );
      return;
    }

    // 元メッセージ取得（本文＋発言者）
    // - thread返信でも安定して取れるように、reactions.get(full:true) を優先する
    let rawText = "";
    let requesterUserId = "";
    let mm = null;

    try {
      const rg = await client.reactions.get({
        channel: channelId,
        timestamp: msgTs,
        full: true,
      });
      mm = rg?.message || null;
      rawText = mm?.text || "";
      requesterUserId = mm?.user || "";
    } catch (e) {
      console.error("reaction_added reactions.get error:", e?.data || e);
    }

    // フォールバック（必要なら）
    if (!mm) {
      try {
        const hist = await client.conversations.history({
          channel: channelId,
          latest: msgTs,
          inclusive: true,
          limit: 1,
        });
        mm = (hist.messages || [])[0] || null;
        rawText = mm?.text || "";
        requesterUserId = mm?.user || "";
      } catch (e) {
        console.error(
          "reaction_added conversations.history error:",
          e?.data || e,
        );
      }
    }

    // ✅ 対応者推定（blocks優先 → text → fallback）
const { userIds: initialUsers, groupIds: initialGroupIds } =
  inferTargetsFromMessage(rawText, actorUserId, mm?.blocks || null);

// 代表1名（既存ロジック互換用）
const assigneeId = initialUsers[0] || actorUserId;


    // 期限は今日固定
    const dueYmd = slackDateYmd(new Date());

    // スレッド親（そのスレッドに出す）
    let threadRootTs = msgTs;
    try {
      const rg = await client.reactions.get({
        channel: channelId,
        timestamp: msgTs,
        full: true,
      });
      const m = rg?.message;
      threadRootTs = m?.thread_ts || m?.ts || msgTs;
    } catch (e) {
      console.error("reactions.get error:", e?.data || e);
    }

    // プレビュー用（確認カード表示用）：<@U...> を人間向けに置換してから出す
    let previewText = rawText;

    try {
      // usergroup等も（もし入ってたら）整形
      previewText = await prettifySlackText(previewText, teamId);

      // <@Uxxx> -> @DisplayName（※この段階では通知はまだ飛ばない）
      previewText = await prettifyUserMentions(previewText, teamId);
    } catch (_) {}

    // payload（create は即作成、edit はモーダル）
    const payloadBase = {
      teamId,
      channelId,
      msgTs,
      requesterUserId: requesterUserId || actorUserId,
      assigneeId,
      dueYmd,
      messageText: rawText,
    };

    const payloadCreate = JSON.stringify({ ...payloadBase, mode: "create" });
    const payloadEdit = JSON.stringify({ ...payloadBase, mode: "edit" });

    const blocks = buildReactionPromptBlocks({
      previewText,
      assigneeId,
      dueYmd,
      payloadCreate,
      payloadEdit,
    });

    // ★キーは msgTs（= 1メッセージ1回）、投稿先は threadRootTs
    if (!String(channelId || "").startsWith("D")) {
      await upsertThreadCard(client, {
        teamId,
        channelId,
        parentTs: msgTs,
        threadTs: threadRootTs,
        blocks,
      });
    }
  } catch (e) {
    if (e?.data?.error !== "not_in_channel")
      console.error("reaction_added error:", e?.data || e);
  }
});

app.action("reaction_task_confirm_create", async ({ ack, body, client }) => {
  await ack();

  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const channelId = payload.channelId;
    const msgTs = payload.msgTs;
    const actorUserId = body.user?.id;

    const requesterUserId = payload.requesterUserId || actorUserId;
    const assigneeId = payload.assigneeId || actorUserId;
    const dueYmd = payload.dueYmd || slackDateYmd(new Date());
    const rawText = payload.messageText || "";

    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    // すでにタスク化済みなら何もしない（痕跡は残ってる想定）
    const existing = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existing?.id) return;

    // permalink
    let permalink = "";
    try {
      const r = await client.chat.getPermalink({
        channel: channelId,
        message_ts: msgTs,
      });
      permalink = r?.permalink || "";
    } catch (_) {}

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);
    const title = generateTitleCandidate(prettyText || rawText || "");

    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const assigneeDept = await resolveDeptForUser(teamId, assigneeId);

    const taskId = randomUUID();

    // この導線は「personalタスクを即作成」だけに絞る（リアクション→確定ボタン）
    const taskType = "personal";
    const status = "in_progress"; // 初期は進行中で固定
    const description = prettyText || rawText || "";
    const due = dueYmd; // "YYYY-MM-DD"

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId,
      message_ts: msgTs, // ← parentTs ではなく msgTs
      source_permalink: permalink || null,
      title,
      description,
      requester_user_id: requesterUserId,
      created_by_user_id: actorUserId,
      assignee_id: assigneeId, // ← personalAssigneeId ではなく assigneeId
      assignee_label: null,
      status,
      due_date: due,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
      task_type: taskType,
      broadcast_group_handle: null,
      broadcast_group_id: null,
      total_count: null,
      completed_count: 0,
      notified_at: null,
    });

    // タスク詳細カードに差し替え（スレッドに出せるチャンネルだけ）
    const doneBlocks = await buildThreadCardBlocks({ teamId, task: created });

    // DM（Dxxxx）は thread card を作らない（仕様）
    if (!String(channelId || "").startsWith("D")) {
      await upsertThreadCard(client, {
        teamId,
        channelId,
        parentTs: msgTs, // 一意キー（リアクション対象）
        threadTs: payload.threadTs || msgTs, // 投稿先スレッド親（threadRootTs）
        blocks: doneBlocks,
      });
    }
  } catch (e) {
    console.error("reaction_task_confirm_create error:", e?.data || e);
  }
});

app.action("reaction_task_open_edit_modal", async ({ ack, body, client }) => {
  await ack();

  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const channelId = payload.channelId;
    const msgTs = payload.msgTs;
    const actorUserId = body.user?.id;

    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    // 元メッセージ取得
    const rawText = payload.messageText || "";
    const requesterUserId = payload.requesterUserId || actorUserId;

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);

    // task_modal を開く（初期値入り）
await client.views.open({
  trigger_id: body.trigger_id,
  view: {
    type: "modal",
    callback_id: "task_modal",
    // ✅ private_metadata は軽量に（3000文字制限対策）
            private_metadata: JSON.stringify({
          teamId,
          channelId,
          msgTs,
          // ✅ 後方互換のため残してOK（submit側はstate優先）
          requesterUserId: actorUserId,
        }),

    title: { type: "plain_text", text: "タスク作成" },
    submit: { type: "plain_text", text: "決定" },
    close: { type: "plain_text", text: "キャンセル" },
    blocks: [
                {
            type: "input",
            block_id: "desc",
            label: { type: "plain_text", text: "詳細（元メッセージ全文）" },
            element: {
              type: "plain_text_input",
              action_id: "desc_input",
              multiline: true,
              initial_value: prettyText || "",
            },
          },

          // ✅ 依頼主（選択式）：初期値は「タスク化した人」
          {
            type: "input",
            block_id: "requester",
            label: { type: "plain_text", text: "依頼主" },
            element: {
              type: "users_select",
              action_id: "requester_user_select",
              initial_user: actorUserId,
              placeholder: { type: "plain_text", text: "依頼主を選択" },
            },
          },

          // 対応者（個人：複数OK）
          {
            type: "input",
            optional: true,
            block_id: "assignee_users",
            label: { type: "plain_text", text: "対応者（個人・複数OK）" },
            element: {
              type: "multi_users_select",
              action_id: "assignee_users_select",
              placeholder: { type: "plain_text", text: "ユーザーを選択" },
            },
          },
          {
            type: "input",
            optional: true,
            block_id: "assignee_groups",
            label: {
              type: "plain_text",
              text: "対応者（グループ：@ALL-xxx / @mk-all など）",
            },
            element: {
              type: "multi_external_select",
              action_id: "assignee_groups_select",
              placeholder: {
                type: "plain_text",
                text: "ユーザーグループを検索",
              },
              min_query_length: 0,
            },
          },

          {
            type: "input",
            block_id: "due",
            label: { type: "plain_text", text: "期限" },
            element: {
              type: "datepicker",
              action_id: "due_date",
              placeholder: { type: "plain_text", text: "期限" },
              initial_date: todayJstYmd(),
            },
          },

          {
            type: "context",
            elements: [
              {
                type: "mrkdwn",
                text: "💡 対象が1人なら「個人タスク」、2人以上またはグループ指定なら「全社/複数タスク」になります。",
              },
            ],
          },
        ],
      },
    });
  } catch (e) {
    console.error("reaction_task_open_edit_modal error:", e?.data || e);
  }
});

// ================================
// Global Shortcut: Open Task List (Home-like modal)
// ================================
function myTasksScopeSelectElement(scopeKey) {
  const cur =
    BROADCAST_SCOPES.find((s) => s.key === scopeKey) || BROADCAST_SCOPES[0];
  return {
    type: "static_select",
    action_id: "my_tasks_scope_select",
    initial_option: {
      text: { type: "plain_text", text: cur.label },
      value: cur.key,
    },
    options: BROADCAST_SCOPES.map((s) => ({
      text: { type: "plain_text", text: s.label },
      value: s.key,
    })),
  };
}

function myTasksStatusSelectElement(scopeKey) {
  const cur = HOME_SCOPES.find((s) => s.key === scopeKey) || HOME_SCOPES[0];
  return {
    type: "static_select",
    action_id: "my_tasks_status_select",
    initial_option: {
      text: { type: "plain_text", text: cur.label },
      value: cur.key,
    },
    options: HOME_SCOPES.map((s) => ({
      text: { type: "plain_text", text: s.label },
      value: s.key,
    })),
  };
}

app.shortcut("open_my_tasks", async ({ shortcut, ack, client, body }) => {
  await ack();
  try {
    const teamId =
      shortcut?.team?.id || body?.team_id || body?.team?.id || null;
    const userId = shortcut?.user?.id || body?.user?.id || null;
    if (!teamId || !userId) return;

    // 初期値：to_me / active
    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: "to_me",
      scopeKey: "active",
    });

    await client.views.open({
      trigger_id: shortcut.trigger_id,
      view,
    });
  } catch (e) {
    console.error("open_my_tasks shortcut error:", e?.data || e);
  }
});

// ================================
// Modal submit: create task -> DB -> thread + ephemeral
// ================================
async function expandTargetsFromGroups(teamId, groupIds) {
  if (!groupIds?.length)
    return { users: new Set(), groupHandles: [], groupIdToHandle: new Map() };

  const idToHandle = await getSubteamIdMap(teamId);
  const groupHandles = [];
  const groupIdToHandle = new Map();

  const users = new Set();
  for (const gid of groupIds) {
    try {
      const handle = idToHandle.get(gid) || gid;
      groupIdToHandle.set(gid, handle);
      groupHandles.push(handle);
      const usersRes = await app.client.usergroups.users.list({
        usergroup: gid,
      });
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

    // ✅ 本文は view.state から読む（private_metadata には入れない）
    const description = (view.state.values.desc?.desc_input?.value || "").trim();

    // ✅ 空はブロック（ユーザーに分かるようにモーダル内エラー）
    if (!description) {
      await ack({
        response_action: "errors",
        errors: { desc: "詳細を入力してください" },
      });
      return;
    }

    const title = generateTitleCandidate(description);


    const selectedUsers =
      view.state.values.assignee_users?.assignee_users_select?.selected_users ||
      [];
    const selectedGroupOptions =
      view.state.values.assignee_groups?.assignee_groups_select
        ?.selected_options || [];
    const selectedGroupIds = selectedGroupOptions
      .map((o) => o?.value)
      .filter(Boolean);

    const due = view.state.values.due?.due_date?.selected_date || null;
    //const status = view.state.values.status?.status_select?.selected_option?.value || "open";
    // （B方針）作成時ステータスは in_progress 固定（迷わせない）
        const status = "in_progress";

    // ✅ 依頼主：ユーザーが選べるようにする（初期値は「タスク化した人」）
    const requesterUserId =
      view.state.values.requester?.requester_user_select?.selected_user ||
      actorUserId;


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
    const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(
      teamId,
      selectedGroupIds,
    );

    // targets = selectedUsers + groupUsers
    const targets = new Set();
    for (const u of selectedUsers) targets.add(u);
    for (const u of groupUsers) targets.add(u);
    let targetList = Array.from(targets);

    // ============================
    // ✅ DM特別ルール：personal固定（=自分あて固定）
    // - DMで「自分の作業メモ → タスク化」を成立させる
    // - ここでは "Dxxxx" を DM として扱う（Group DMもDになる運用前提）
    // ============================
    const isDmChannel = String(channelId || "")[0] === "D";

    if (isDmChannel) {
      // DMは「自分あて」固定にしたいので、他人/グループが選ばれてたら弾く（事故防止）
      const hasOthers =
        (selectedUsers || []).some((u) => u && u !== actorUserId) ||
        (selectedGroupIds || []).length > 0;

      if (hasOthers) {
        await ack({
          response_action: "errors",
          errors: {
            assignee_users: "DMでは「自分あての個人タスク」固定だよ🙏（他ユーザー/グループは選べないよ）",
            assignee_groups: "DMでは「自分あての個人タスク」固定だよ🙏",
          },
        });
        return;
      }

      // 選択が空でもOKにして、自分を入れる
      targetList = [actorUserId];
    }

    const isPersonal = targetList.length === 1 && selectedGroupIds.length === 0;
    const taskType = isPersonal ? "personal" : "broadcast";

    // label for display (no mention)
    // - broadcastは「選択された対象（個人/グループ）」だけをラベル化（グループの全員は展開しない）
    // - メンション通知を避けるため、表示は noMention() を通す
    const labelParts = [];
    for (const gidHandle of groupHandles)
      labelParts.push(`@${String(gidHandle).replace(/^@/, "")}`);
    for (const u of selectedUsers) {
      const name = await getUserDisplayName(teamId, u);
      labelParts.push(`@${name}`);
    }
    const assigneeLabelRaw = labelParts.join(" ");


    // dept resolve (A): requester + (personalのみ assignee)
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const personalAssigneeId = isPersonal ? targetList[0] : null;
    const assigneeDept = isPersonal
      ? await resolveDeptForUser(teamId, personalAssigneeId)
      : null;

    let permalink = "";
    if (channelId && parentTs) {
      try {
        const r = await client.chat.getPermalink({
          channel: channelId,
          message_ts: parentTs,
        });
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
      broadcast_group_handle: groupHandles.length
        ? `@${groupHandles[0]}`
        : null,
      broadcast_group_id: selectedGroupIds.length ? selectedGroupIds[0] : null,
      total_count: taskType === "broadcast" ? targetList.length : null,
      completed_count: 0,
      notified_at: null,
    });

    // broadcast: snapshot targets
    if (taskType === "broadcast") {
      await dbInsertTaskTargets(teamId, taskId, targetList);
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;
    }

        // ✅ DMでの作成は「作ったよ」エフェメラルだけは必ず出す（作成DMは出さない）
    try {
      const isDmChannel = String(channelId || "")[0] === "D";
      if (isDmChannel) {
        await safeEphemeral(
          client,
          channelId,
          actorUserId,
          "📝 DMのメモ、タスクにしたよ！Homeの「自分あて」に出るよ☺️",
        );
      }
    } catch (_) {}

    // ① 発行通知（personal / broadcast）
    // - 自分が発行して自分が対象の場合は通知しない（うるささ回避）
    try {
      if (taskType === "personal") {
        const to = personalAssigneeId;
        if (to && to !== actorUserId) {
          await notifyTaskSimpleDM(to, created, "📝 タスクが届いたよ");
        }
      } else if (taskType === "broadcast") {
        const targets = (targetList || []).filter(
          (u) => u && u !== actorUserId,
        );
        for (const uid of targets) {
          await notifyTaskSimpleDM(uid, created, "📝 タスクが届いたよ");
        }
      }
    } catch (e) {
      console.error("create notify error:", e?.data || e);
    }

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
      } catch (e) {
        if (e?.data?.error === "not_in_channel") {
          await safeEphemeral(
            client,
            channelId,
            actorUserId,
            "🥺 このチャンネルにボットが参加してないよ…！ `/invite @アプリ名` してから試してね✨",
          );
        } else {
          console.error("broadcast notify error:", e?.data || e);
        }
      }
    }

    // thread card
    if (created?.channel_id && created?.message_ts) {
      try {
        const blocks = await buildThreadCardBlocks({ teamId, task: created });
        if (!created.channel_id?.startsWith("D")) {
          await upsertThreadCard(client, {
            teamId,
            channelId,
            parentTs,
            blocks,
          });
        }
      } catch (e) {
        if (e?.data?.error === "not_in_channel") {
          await safeEphemeral(
            client,
            channelId,
            actorUserId,
            "🥺 このチャンネルにボットが参加してないよ…！ `/invite @アプリ名` してから試してね✨",
          );
        } else {
          console.error("thread card error:", e?.data || e);
        }
      }
    }

    // Home refresh（スマホ反映対策：関係者＋対象者へ再描画）
    publishHomeForUsers(client, teamId, [
      actorUserId,
      requesterUserId,
      ...targetList,
    ]);
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
    await openDetailModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      taskId,
      viewerUserId: body.user.id,
      origin,
      isFromModal: body.view?.type === "modal",
    });
  } catch (e) {
    console.error("open_detail_modal error:", e?.data || e);
  }
});

app.action("noop", async ({ ack }) => {
  await ack();
});

app.action("my_tasks_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const scopeKey = meta.scopeKey || "active";

    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: selected,
      scopeKey,
    });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view,
    });
  } catch (e) {
    console.error("my_tasks_scope_select error:", e?.data || e);
  }
});

app.action("my_tasks_status_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "active";

    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const rangeKey = meta.rangeKey || "to_me";
    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey,
      scopeKey: selected,
    });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view,
    });
  } catch (e) {
    console.error("my_tasks_status_select error:", e?.data || e);
  }
});

// 一覧を開く（作成完了エフェメラル等から）
app.action("open_task_list_modal", async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const userId = payload.userId || getUserIdFromBody(body);
    if (!teamId || !userId) return;

    const trigger_id = body.trigger_id;
    if (!trigger_id) return;

    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: "to_me",
      scopeKey: "active",
    });
    await client.views.open({ trigger_id, view });
  } catch (e) {
    console.error("open_task_list_modal error:", e?.data || e);
  }
});

// Home: mode change
app.action("home_view_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    // ★表示は固定（保険：過去UIのイベントが飛んでも all に寄せる）
    setHomeState(teamId, userId, { viewKey: "all" });

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

    setHomeState(teamId, userId, { deptKey: selected, assigneeUserId: null });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_dept_select error:", e?.data || e);
  }
});

app.action("home_broadcast_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const scopeKey = body.actions?.[0]?.selected_option?.value;

    if (!teamId || !userId || !scopeKey) return;

    setHomeState(teamId, userId, { broadcastScopeKey: scopeKey });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_broadcast_scope_select error:", e?.data || e);
  }
});

// Home: 「もっと見る / 閉じる」トグル（範囲=すべて(all) の時だけUI上に出す想定）
app.action("home_toggle_more", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    const payload = safeJsonParse(body.actions?.[0]?.value || "");
    const section = payload?.section; // "overdue" | "today"
    if (!teamId || !userId) return;
    if (section !== "overdue" && section !== "today") return;

    const st = getHomeState(teamId, userId);
    const next = {
      homeMore: {
        ...(st.homeMore || { overdue: false, today: false }),
        [section]: !(st.homeMore?.[section]),
      },
    };

    setHomeState(teamId, userId, next);
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_toggle_more error:", e?.data || e);
  }
});

// Home: 「開く / 閉じる」トグル（畳む機能。もっと見るとは別）
app.action("home_toggle_fold", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    const payload = safeJsonParse(body.actions?.[0]?.value || "");
    const section = payload?.section; // "overdue" | "today" | "later"
    if (!teamId || !userId) return;
    if (section !== "overdue" && section !== "today" && section !== "later") return;

    const st = getHomeState(teamId, userId);
    const next = {
      homeFold: {
        ...(st.homeFold || { overdue: false, today: false, later: false }),
        [section]: !(st.homeFold?.[section]),
      },
    };

    setHomeState(teamId, userId, next);
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_toggle_fold error:", e?.data || e);
  }
});

// Home: フィルタをリセット（Phase8-4）
app.action("home_reset_filters", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    setHomeState(teamId, userId, {
      viewKey: "all",
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

// overflow menu (home/list modal): open detail
app.action("task_row_overflow", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const picked = action?.selected_option?.value || "";
    const p = safeJsonParse(picked) || {};
    const teamId = p.teamId || body.team?.id || body.team_id;
    const taskId = p.taskId;
    const origin = p.origin || "home";
    if (!teamId || !taskId) return;

    // 一覧モーダル内なら、同一モーダルを詳細表示へ更新（既存 open_detail_in_list と同等）
    if (origin === "list_modal" && body.view?.id) {
      const listMeta = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = {
        viewType: listMeta.viewType || "assigned",
        userId: listMeta.userId || body.user.id,
        status: listMeta.status || "open",
        deptKey: listMeta.deptKey || "all",
      };

      const task = await dbGetTaskById(teamId, taskId);
      if (!task) return;

      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: nextView,
      });
      return;
    }

    // Home/その他は通常の詳細モーダルを開く
    await openDetailModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      taskId,
      viewerUserId: body.user.id,
      origin: "home",
      isFromModal: false,
    });
  } catch (e) {
    console.error("task_row_overflow error:", e?.data || e);
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
        await safeEphemeral(
          client,
          task.channel_id || body.user.id,
          userId,
          "🥺 このタスクの対象者じゃないみたい…！",
        );
        return;
      }

      await dbUpsertCompletion(teamId, taskId, userId);

            const total = task.total_count || (await dbCountTargets(teamId, taskId));
      const doneCount = await dbCountCompletions(teamId, taskId);

      // ✅ 全員完了：確認待ち工程は廃止して、そのまま完了(done)へ
      if (doneCount >= total && total > 0) {
        const fresh = await dbGetTaskById(teamId, taskId);
        if (fresh && !["done", "cancelled"].includes(fresh.status)) {
          await dbUpdateStatus(teamId, taskId, "done");
        }

        // ✅ 依頼者へ通知（1回だけ）
        // - notified_at は「全員完了通知の送信済みフラグ」として流用する
        if (fresh && !fresh.notified_at) {
          await dbQuery(
            `UPDATE tasks SET notified_at=now() WHERE team_id=$1 AND id=$2 AND notified_at IS NULL`,
            [teamId, taskId],
          );

          // ✅ タイトル + 詳細ボタンのみの静かな通知に統一
          await notifyTaskSimpleDM(
            fresh.requester_user_id,
            { ...fresh, status: "done" },
            "🎉 全員が完了したよ",
          );

          // ✅ Home再描画：依頼者/対象者へ即反映（スマホ遅延対策で2回）
          try {
            const targets = await dbListTargetUserIds(teamId, taskId);
            const toRefresh = Array.from(
              new Set(
                [fresh.requester_user_id, ...(targets || [])].filter(Boolean),
              ),
            );
            publishHomeForUsers(client, teamId, toRefresh, 200);
            setTimeout(() => {
              publishHomeForUsers(client, teamId, toRefresh, 200);
            }, 200);
          } catch (_) {}
        }
      }

      // スレッドカード更新（進捗表示更新）
      if (task.channel_id && task.message_ts) {
        const refreshed = await dbGetTaskById(teamId, taskId);
        if (refreshed) {
          const blocks = await buildThreadCardBlocks({
            teamId,
            task: refreshed,
          });
          if (!refreshed.channel_id?.startsWith("D")) {
            await upsertThreadCard(client, {
              teamId,
              channelId: refreshed.channel_id,
              parentTs: refreshed.message_ts,
              blocks,
            });
          }
        }
      }

// modal refresh（一覧モーダル廃止：常に detail_modal を更新）
if (body.view?.id) {
  const refreshed = await dbGetTaskById(teamId, taskId);
  if (refreshed) {
    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({
        teamId,
        task: refreshed,
        viewerUserId: body.user.id,
        origin: "home",
      }),
    });
  }
}
      // Home refresh（スマホ反映対策：関係者へまとめて再描画）
      publishHomeForUsers(client, teamId, [userId, task.requester_user_id]);
      return;
    }

    // personal
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    // ★通知：完了（personal）…タイトル＋詳細ボタンだけ
    try {
      const toNotify = Array.from(
        new Set(
          [updated.requester_user_id, updated.assignee_id].filter(Boolean),
        ),
      );
      for (const uid of toNotify) {
        await notifyTaskSimpleDM(uid, updated, "✅ 完了になったよ");
      }
    } catch (_) {}

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || {
        viewType: "assigned",
        userId: body.user.id,
        status: "open",
        deptKey: "all",
      };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({
          teamId,
          task: updated,
          returnState,
          viewerUserId: body.user.id,
        }),
      });
      return;
    }

    if (updated.channel_id && updated.message_ts) {
      // スレッドカードは完了ボタンが無いので、表示だけ更新
      const doneBlocks = [
        {
          type: "header",
          text: { type: "plain_text", text: "✅ 完了しました" },
        },
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: `*${noMention(updated.title)}*\nタスクを完了にしました✨`,
          },
        },
      ];
      if (!updated.channel_id?.startsWith("D")) {
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          blocks: doneBlocks,
        });
      }
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      const refreshed = await dbGetTaskById(teamId, taskId);
      if (refreshed) {
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildDetailModalView({
            teamId,
            task: refreshed,
            viewerUserId: body.user.id,
          }),
        });
      }
    }

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      const relatedIds = Array.from(
        new Set(
          [body.user.id, task.requester_user_id, task.assignee_id].filter(
            Boolean,
          ),
        ),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, relatedIds, 200);
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

    // ★⑤（変更）：broadcast は誰でも完了にできる（運用優先）
    if (task.task_type !== "broadcast") return;

    // すでに完了/取り下げなら何もしない
    if (task.status === "done" || task.status === "cancelled") {
      await safeEphemeral(
        client,
        task.channel_id || body.user.id,
        body.user.id,
        "もう完了（または取り下げ）になってるよ！",
      );
      return;
    }

    // waitingでなくても強制的にdoneへ
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    // ★通知：完了（broadcast）…タイトル＋詳細ボタンだけ
    try {
      const targets = await dbListTargetUserIds(teamId, taskId);
      const toNotify = Array.from(
        new Set(
          [updated.requester_user_id, ...(targets || [])].filter(Boolean),
        ),
      );
      for (const uid of toNotify) {
        await notifyTaskSimpleDM(uid, updated, "✅ 完了になったよ");
      }
    } catch (_) {}

    // ★Home再描画：依頼者/対象者にも反映
    try {
      const targets = await dbListTargetUserIds(teamId, taskId);
      const toRefresh = Array.from(
        new Set(
          [updated.requester_user_id, ...(targets || [])].filter(Boolean),
        ),
      );
      publishHomeForUsers(client, teamId, toRefresh, 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, toRefresh, 200);
      }, 200);
    } catch (_) {}

    // thread card update
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      if (!updated.channel_id?.startsWith("D")) {
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          blocks,
        });
      }
    }

// modal refresh（一覧モーダル廃止：常に detail_modal を更新）
if (body.view?.id) {
  const refreshed = await dbGetTaskById(teamId, taskId);
  if (refreshed) {
    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({
        teamId,
        task: refreshed,
        viewerUserId: body.user.id,
        origin: "home",
      }),
    });
  }
}
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
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: `✅ *確認完了しました*\n「*${noMention(updated.title)}*」を完了にしました。`,
              },
            },
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
      await safeEphemeral(
        client,
        task.channel_id || body.user.id,
        body.user.id,
        "🥺 取り下げできるのは依頼者だけだよ…！",
      );
      return;
    }

    const cancelled = await dbCancelTask(teamId, taskId, body.user.id);
    if (!cancelled) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || {
        viewType: "assigned",
        userId: body.user.id,
        status: "open",
        deptKey: "all",
      };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({
          teamId,
          task: cancelled,
          returnState,
          viewerUserId: body.user.id,
        }),
      });
      return;
    }

    if (cancelled.channel_id && cancelled.message_ts) {
      const blocks = [
        {
          type: "header",
          text: { type: "plain_text", text: "🚫 取り下げました" },
        },
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: `*${noMention(cancelled.title)}*\n依頼者により取り下げられました。`,
          },
        },
      ];
      if (!cancelled.channel_id?.startsWith("D")) {
        await upsertThreadCard(client, {
          teamId,
          channelId: cancelled.channel_id,
          parentTs: cancelled.message_ts,
          blocks,
        });
      }
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildDetailModalView({
          teamId,
          task: cancelled,
          viewerUserId: body.user.id,
        }),
      });
    }

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      const relatedIds = Array.from(
        new Set(
          [
            body.user.id,
            cancelled.requester_user_id,
            cancelled.assignee_id,
          ].filter(Boolean),
        ),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, relatedIds, 200);
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
      await safeEphemeral(
        client,
        task.channel_id || body.user.id,
        body.user.id,
        "🥺 複数タスクのステータスは自動で進むよ（全員完了→確認待ち→依頼者の確認完了）",
      );
      return;
    }

    // personal：ウォッチャー等はステータス変更不可（依頼者 or 対応者のみ）
    const actor = body.user.id;
    if (task.requester_user_id !== actor && task.assignee_id !== actor) {
      await safeEphemeral(
        client,
        task.channel_id || body.user.id,
        actor,
        "🥺 ステータス変更できるのは依頼者か対応者だけだよ…！",
      );
      return;
    }

    const updated = await dbUpdateStatus(teamId, taskId, nextStatus);
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || {
        viewType: "assigned",
        userId: body.user.id,
        status: "open",
        deptKey: "all",
      };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({
          teamId,
          task: updated,
          returnState,
          viewerUserId: body.user.id,
        }),
      });
      return;
    }

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({
        teamId,
        task: updated,
        viewerUserId: body.user.id,
      }),
    });

    // スレッドカード：表示更新
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      if (!updated.channel_id?.startsWith("D")) {
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          blocks,
        });
      }
    }

    // ★通知：確認待ち/完了（personalのみ）
    try {
      if (nextStatus === "waiting") {
        if (updated.requester_user_id) {
          await postDM(
            updated.requester_user_id,
            `⏳ 確認待ちになったよ\n・タイトル：${noMention(updated.title)}\n・期限：${formatDueDateOnly(updated.due_date)}\n・ステータス：${statusLabel(updated.status)}`,
          );
        }
      } else if (nextStatus === "done") {
        const toNotify = Array.from(
          new Set(
            [updated.requester_user_id, updated.assignee_id].filter(Boolean),
          ),
        );
        for (const uid of toNotify) {
          await postDM(
            uid,
            `✅ 完了になったよ\n・タイトル：${noMention(updated.title)}\n・期限：${formatDueDateOnly(updated.due_date)}\n・ステータス：${statusLabel(updated.status)}`,
          );
        }
      }
    } catch (_) {}

    // Phase8-1: Homeリアルタイム再描画（操作した本人のみ / モバイル反映遅延対策）
    try {
      const relatedIds = Array.from(
        new Set(
          [body.user.id, updated.requester_user_id, updated.assignee_id].filter(
            Boolean,
          ),
        ),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(() => {
        publishHomeForUsers(client, teamId, relatedIds, 200);
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
      [teamId, taskId],
    );
    const completionsRes = await dbQuery(
      `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2 ORDER BY user_id`,
      [teamId, taskId],
    );

    const targets = (targetsRes.rows || [])
      .map((r) => r.user_id)
      .filter(Boolean);
    const doneSet = new Set(
      (completionsRes.rows || []).map((r) => r.user_id).filter(Boolean),
    );

    const done = targets.filter((u) => doneSet.has(u));
    const todo = targets.filter((u) => !doneSet.has(u));

    const total = targets.length;
    const doneCount = done.length;

    const listText = (arr, emptyText) => {
      if (!arr.length) return emptyText;
      const MAX = 50;
      const head = arr
        .slice(0, MAX)
        .map((u) => `• <@${u}>`)
        .join("\n");
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
          type: "header",
          text: { type: "plain_text", text: "📊 完了/未完了一覧" },
        },
        {
          type: "section",
          text: { type: "mrkdwn", text: `*${noMention(task.title)}*` },
        },
        { type: "divider" },
        {
          type: "section",
          text: { type: "mrkdwn", text: `進捗：*${doneCount} / ${total}*` },
        },
        { type: "divider" },

        {
          type: "section",
          text: { type: "mrkdwn", text: `✅ *完了済み（${done.length}）*` },
        },
        {
          type: "section",
          text: { type: "mrkdwn", text: listText(done, "（まだいません）") },
        },
        { type: "divider" },

        {
          type: "section",
          text: { type: "mrkdwn", text: `⏳ *未完了（${todo.length}）*` },
        },
        {
          type: "section",
          text: { type: "mrkdwn", text: listText(todo, "（全員完了！🎉）") },
        },
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

  // 期限表示：JST基準で「今日」を優先。DB/pgの型差（Date/文字列）にも耐える。
  const today = todayJstYmd();
  const dueYmd =
    slackDateYmd(task.due_date) ||
    (typeof task.due_date === "string" ? task.due_date.slice(0, 10) : "");

  const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
  const hasLink = !!task?.source_permalink;

  await app.client.chat.postMessage({
    channel,
    text: `⏰ 今日が期限です（${roleLabel}）: ${noMention(task.title)}`,
    blocks: [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `⏰ *今日が期限です*（${roleLabel}）\n*${noMention(task.title)}*`,
        },
      },
      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "詳細を開く" },
            action_id: "open_detail_modal",
            value: payload,
          },
          ...(hasLink
            ? [
                {
                  type: "button",
                  text: { type: "plain_text", text: "元メッセージへ" },
                  url: task.source_permalink,
                },
              ]
            : []),
        ],
      },
    ],
  });
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
      await dbQuery(
        `UPDATE tasks SET notified_at = now() WHERE team_id=$1 AND id=$2`,
        [t.team_id, t.id],
      );
    } catch (e) {
      console.error("notify error:", e?.data || e);
    }
  }

  console.log(`[notify] done. today=${today} count=${tasks.length}`);
}

cron.schedule(
  "0 9 * * *",
  () => {
    runDueNotifyJob().catch((e) =>
      console.error("runDueNotifyJob error:", e?.data || e),
    );
  },
  { timezone: "Asia/Tokyo" },
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

    // ✅ 修正ポイント：broadcast は「だれでも」編集OK
    const canEditTask = isBroadcast
      ? true
      : viewerUserId === task.requester_user_id || viewerUserId === task.assignee_id;

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

  const nextAssignee =
    view.state.values.assignee?.assignee_user?.selected_user || null;
  const nextDue = view.state.values.due?.due_date?.selected_date || null;
  const nextContent = (
    view.state.values.content?.content_text?.value || ""
  ).trim();

  if (!nextContent) {
    await ack({
      response_action: "errors",
      errors: { content: "タスク内容を入力してください" },
    });
    return;
  }

  // ① まず軽い画面へ差し替え（hash_conflict回避）
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "edit_task_modal_saving",
      title: { type: "plain_text", text: "保存中." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "更新しています。" } },
      ],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";

    // ✅ 修正ポイント：broadcast は「だれでも」保存OK
    const canEditTask = isBroadcast
      ? true
      : actorUserId === before.requester_user_id || actorUserId === before.assignee_id;

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
      if (!updated.channel_id || !updated.message_ts) return;

      try {
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          threadTs: updated.message_ts,
          blocks: cardBlocks,
        });
      } catch (e) {
        console.error("upsertThreadCard error:", e?.data || e);
      }
    }

    // Home を更新（対象者/依頼者に広く）
    try {
      const users = [
        updated.requester_user_id,
        updated.assignee_id,
        actorUserId,
      ].filter(Boolean);
      await publishHomeForUsers(client, teamId, users, 250);
    } catch (_) {}

    // ② 現在のモーダルは「保存しました✅」最小UI
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_task_modal_done",
          title: { type: "plain_text", text: "保存しました✅" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: { type: "mrkdwn", text: "タスク内容を更新しました。" },
            },
          ],
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
          blocks: [
            {
              type: "section",
              text: { type: "mrkdwn", text: "もう一度お試しください。" },
            },
          ],
        },
      });
    } catch (_) {}
  }
});

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
  await dbQuery(q, [
    randomUUID(),
    teamId,
    taskId,
    userId,
    String(comment || "").trim(),
  ]);
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
          block_id: "mention",
          optional: true,
          label: { type: "plain_text", text: "メンション（任意・複数可）" },
          element: {
            type: "multi_users_select",
            action_id: "users",
            placeholder: { type: "plain_text", text: "メンションする人を選択" },
          },
        },

        {
          type: "input",
          block_id: "comment",
          label: { type: "plain_text", text: "コメント内容" },
          element: {
            type: "plain_text_input",
            action_id: "body",
            multiline: true,
          },
        },
      ],
    },
  });
});

app.view("comment_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};

  const base = view.state.values.comment?.body?.value?.trim() || "";
  const mentionUserIds = view.state.values.mention?.users?.selected_users || [];

  // <@U1> <@U2> 形式で先頭に付与
  const mentionPrefix = mentionUserIds.map((u) => `<@${u}>`).join(" ");
  const comment = `${mentionPrefix}${mentionPrefix ? " " : ""}${base}`;

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
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "💾 保存中…" } },
      ],
    },
  });

  try {
    // ② 重い処理は ack 後にやる
    await dbInsertTaskComment(meta.teamId, meta.taskId, body.user.id, comment);

    const task = await dbGetTaskById(meta.teamId, meta.taskId);
    if (!task) return;

    // ②-b コメント通知（bot DM）
    // - メンションがあればメンション先へ
    // - メンションが無ければ personal は (依頼者/対応者) へ（自分は除外）
    // - broadcast は依頼者へ（自分は除外）
    try {
      const actor = body.user.id;

      const recipients = new Set();

      // メンション先（複数）
      for (const uid of mentionUserIds || []) {
        if (uid && uid !== actor) recipients.add(uid);
      }

      // メンションが無い場合のフォールバック
      if (recipients.size === 0) {
        const requester = task.requester_user_id;
        const assignee = task.assignee_id;

        if (requester && requester !== actor) recipients.add(requester);

        if (task.task_type !== "broadcast" && assignee && assignee !== actor) {
          recipients.add(assignee);
        }
      }

// DM本文：どのタスクか分かるように blocks +「詳細を開く」ボタンを付ける
const title = task.title || "（タスク）";
const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
// ★追加：コメント内の <@UXXXX> を @表示名 に変換
const prettyComment = await prettifyUserMentions(comment || "", task.team_id);

for (const uid of recipients) {
  try {
    const dm = await app.client.conversations.open({ users: uid });
    const channel = dm.channel?.id;
    if (!channel) continue;

    const blocks = [
      {
        type: "section",
        text: { type: "mrkdwn", text: "💬 *タスクにコメントがありました*" },
      },
      {
        type: "section",
        text: { type: "mrkdwn", text: `*${noMention(title)}*` },
      },
{
  type: "section",
  text: {
    type: "mrkdwn",
    text: `*コメント*\n>${noMention(String(prettyComment).slice(0, 800))}`,
  },
}
    ];

    // 元メッセージリンク（あれば）
    if (task.source_permalink) {
      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `🔗 <${task.source_permalink}|元メッセージへ>`,
        },
      });
    }

    // 詳細を開くボタン
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "詳細を開く" },
          action_id: "open_detail_modal",
          value: payload,
        },
      ],
    });

    await app.client.chat.postMessage({
      channel,
      text: `💬 コメント: ${noMention(title)}`,
      blocks,
    });
  } catch (_) {}
}

    } catch (e) {
      console.error("comment DM notify error:", e?.data || e);
    }

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
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: "✅ 投稿しました！「閉じる」で詳細画面に戻れます。",
            },
          },
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
          blocks: [
            {
              type: "section",
              text: { type: "mrkdwn", text: "🥺 保存に失敗しました…" },
            },
          ],
        },
      });
    } catch (_) {}
  }
});

app.action("open_edit_due_modal", async ({ ack, body, action, client }) => {
  await ack();

  const p = safeJsonParse(action?.value || "{}") || {};
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};

  const teamId = p.teamId || meta.teamId || getTeamIdFromBody(body);
  const taskId = p.taskId || meta.taskId;
  const viewerUserId = getUserIdFromBody(body);
  const origin = p.origin || meta.origin || "home";

  if (!teamId || !taskId || !viewerUserId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    // ✅ 仕様：broadcast は「だれでも」期限変更OK
    // personal は依頼者/対応者のみ（必要なら personal も true にしてOK）
    const isBroadcast = task.task_type === "broadcast";
    const canChangeDue = isBroadcast
      ? true
      : viewerUserId === task.requester_user_id || viewerUserId === task.assignee_id;

    if (!canChangeDue) return;

    const initDue = slackDateYmd(task.due_date);

    // ★親（詳細モーダル）の view_id を保持しておく（後で再描画する）
    const parentViewId = body.view?.id || null;

    const blocks = [
      {
        type: "section",
        text: { type: "mrkdwn", text: `*${noMention(task.title || "（タスク）")}*` },
      },
      {
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
      },
      {
        type: "context",
        elements: [
          {
            type: "mrkdwn",
            text: "※ 期限だけ変更します（本文は変更しません）",
          },
        ],
      },
    ];

    const view = {
      type: "modal",
      callback_id: "edit_due_modal",
      private_metadata: JSON.stringify({
        teamId,
        taskId,
        origin,
        parentViewId,
      }),
      title: { type: "plain_text", text: "期限変更" },
      submit: { type: "plain_text", text: "保存" },
      close: { type: "plain_text", text: "閉じる" },
      blocks,
    };

    // 詳細モーダル上のボタンからなので push が自然（trigger_id必須）
    await client.views.push({ trigger_id: body.trigger_id, view });
  } catch (e) {
    console.error("open_edit_due_modal error:", e?.data || e);
  }
});


app.view("edit_due_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const taskId = meta.taskId;
  const origin = meta.origin || "home";
  const actorUserId = getUserIdFromBody(body);

  const nextDue = view.state.values.due?.due_date?.selected_date || null;

  // ① まず軽い画面へ差し替え（hash_conflict回避）
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "edit_due_modal_saving",
      title: { type: "plain_text", text: "保存中." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [{ type: "section", text: { type: "mrkdwn", text: "更新しています。" } }],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";

    // ✅ 仕様：broadcast は「だれでも」期限変更OK
    const canChangeDue = isBroadcast
      ? true
      : actorUserId === before.requester_user_id || actorUserId === before.assignee_id;

    if (!canChangeDue) return;

    // ✅ due_date だけ更新（本文は触らない）
    const updated = await dbUpdateTaskContent(teamId, taskId, {
      due_date: nextDue,
      description: null,
      assignee_id: null,
      assignee_dept: null,
    });
    if (!updated) return;

    // スレッドカード更新（証跡）
    if (updated.channel_id && updated.message_ts) {
      try {
        const cardBlocks = await buildThreadCardBlocks({ teamId, task: updated });
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          threadTs: updated.message_ts,
          blocks: cardBlocks,
        });
      } catch (e) {
        console.error("upsertThreadCard error:", e?.data || e);
      }
    }

    // Home 再描画（広めに）
    try {
      const users = [updated.requester_user_id, updated.assignee_id, actorUserId].filter(Boolean);
      await publishHomeForUsers(client, teamId, users, 250);
    } catch (_) {}

    // ===== ★ここが本題：背面の詳細モーダルを最新で再描画 =====
    // Slack が提供する previous_view_id が取れるならそれが最優先
    const prevViewId = body.view?.previous_view_id || null;

    // 念のため open 側で持たせた parentViewId もフォールバックに使う
    const parentViewId = meta.parentViewId || null;

    const targetDetailViewId = prevViewId || parentViewId;

    if (targetDetailViewId) {
      try {
        const detailView = await buildDetailModalView({
          teamId,
          task: updated,
          viewerUserId: actorUserId,
          origin,
        });

        await client.views.update({
          view_id: targetDetailViewId,
          view: detailView,
        });
      } catch (e) {
        console.error("refresh detail modal error:", e?.data || e);
      }
    }
    // ===== ★ここまで =====

    // ② 現在のモーダルは「保存しました✅」最小UI
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_due_modal_done",
          title: { type: "plain_text", text: "保存しました✅" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: `期限を更新しました 🗓️\n*${formatDueDateOnly(nextDue)}*`,
              },
            },
          ],
        },
      });
    } catch (_) {}
  } catch (e) {
    console.error("edit_due_modal error:", e?.data || e);

    // 失敗画面（最小UI）
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_due_modal_error",
          title: { type: "plain_text", text: "保存できませんでした" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            { type: "section", text: { type: "mrkdwn", text: "もう一度お試しください。" } },
          ],
        },
      });
    } catch (_) {}
  }
});

// reopen (detail) - personal: status in_progress / broadcast: requester only（今回は personal 前提）
app.action("reopen_task", async ({ ack, body, action, client }) => {
  await ack();

  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;
  const actor = getUserIdFromBody(body);

  if (!teamId || !taskId || !actor) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    // personal：依頼者 or 対応者のみ
    if (task.task_type !== "broadcast") {
      if (task.requester_user_id !== actor && task.assignee_id !== actor) {
        await safeEphemeral(
          client,
          task.channel_id || body.user.id,
          actor,
          "未完了に戻せるのは依頼者か対応者だけだよ…！",
        );
        return;
      }
    } else {
      // broadcast を戻したいなら、まずは依頼者だけにしておくのが安全
      if (task.requester_user_id !== actor) {
        await safeEphemeral(
          client,
          task.channel_id || body.user.id,
          actor,
          " 複数タスクを未完了に戻せるのは依頼者だけだよ…！",
        );
        return;
      }
    }

    // 未完了へ戻す（open でもいいけど、UI的には in_progress が自然）
    const updated = await dbUpdateStatus(teamId, taskId, "in_progress");
    if (!updated) return;

    // 詳細モーダルを再描画（今開いてるモーダル）
    if (body.view?.id) {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildDetailModalView({
          teamId,
          task: updated,
          viewerUserId: actor,
          origin: "home",
        }),
      });
    }

    // スレッドカード：表示更新
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      if (!updated.channel_id?.startsWith("D")) {
        await upsertThreadCard(client, {
          teamId,
          channelId: updated.channel_id,
          parentTs: updated.message_ts,
          blocks,
        });
      }
    }

    // Home 再描画（関係者）
    try {
      const relatedIds = Array.from(
        new Set([actor, updated.requester_user_id, updated.assignee_id].filter(Boolean)),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(() => publishHomeForUsers(client, teamId, relatedIds, 200), 200);
    } catch (_) {}
  } catch (e) {
    console.error("reopen_task error:", e?.data || e);
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
