require("dotenv").config();
const { App } = require("@slack/bolt");
const { Pool } = require("pg");
const { randomUUID } = require("crypto");
const cron = require("node-cron");

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
  const srcLinesRaw =
    (task.description || "").split("\n").slice(0, 10).join("\n") ||
    "（本文なし）";
  const srcLines = noMention(srcLinesRaw);

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

  // personal：完了ボタン（権限者のみ）
  if (!isBroadcast) {
    if (
      canCompletePersonal &&
      task.status !== "done" &&
      task.status !== "cancelled"
    ) {
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

  if (!__comments.length) {
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: "（コメントなし）" }],
    });
  } else {
    for (const c of __comments) {
      const name = await getUserDisplayName(teamId, c.user_id);
      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: `*${name}*\n${c.comment}` },
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

// View種類
const HOME_VIEWS = [
  { key: "personal", label: "個人タスク" },
  { key: "broadcast", label: "全社/複数タスク" },
  // ★追加（③）：すべて
  { key: "all", label: "すべて" },
];

// 状態（表示範囲）
const HOME_SCOPES = [
  { key: "active", label: "未完了" },
  { key: "done", label: "完了" },
  { key: "cancelled", label: "取り下げ" },
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
  };

  // ★表示は常に「すべて」に固定（personal/broadcastの切替を使わない）
  // ★範囲は broadcastScopeKey を共通キーとして使う
  return {
    ...s,
    viewKey: "all",
    broadcastScopeKey: s.broadcastScopeKey || "to_me",
    personalScopeKey: s.broadcastScopeKey || s.personalScopeKey || "to_me",
  };
}

function setHomeState(teamId, userId, next) {
  const k = `${teamId}:${userId}`;

  // ★viewKey は固定、範囲は broadcastScopeKey に統一
  const merged = {
    ...getHomeState(teamId, userId),
    ...next,
    viewKey: "all",
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
  const text = currentDeptText || "部署（@グループ）を検索";
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
  // 一覧の主役は「本文」だけ（人/期限/リンクは context で小さく出す）
  const rawDesc = String(task.description || "")
    .replace(/\r\n/g, "\n")
    .trim();
  let preview = rawDesc;

  preview = preview.replace(/\n{3,}/g, "\n\n");

  const MAX_PREVIEW_CHARS = 200;
  if (preview.length > MAX_PREVIEW_CHARS)
    preview = preview.slice(0, MAX_PREVIEW_CHARS) + "…";

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

  // 範囲（共通）
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*範囲*" },
    accessory: broadcastScopeSelectElement(st.broadcastScopeKey || "to_me"),
  });

  // 範囲＝すべて のときだけ、部署フィルタを出す
  if ((st.broadcastScopeKey || "to_me") === "all") {
    const deptValue = st.deptKey || "all";
    let deptText =
      deptValue === "all"
        ? "すべて"
        : deptValue === "__none__"
          ? "未設定"
          : null;
    if (!deptText && deptValue) {
      const idToHandle = await getSubteamIdMap(teamId);
      const h = idToHandle.get(deptValue);
      deptText = h ? `@${h}` : "部署（@グループ）を検索";
    }

    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: "*部署*" },
      accessory: deptSelectElement(deptValue, deptText),
    });
  }

  // 状態（未完了/完了）
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*状態*" },
    accessory: homeScopeSelectElement(st.scopeKey),
  });

  blocks.push({ type: "divider" });

  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        action_id: "home_create_task",
        text: { type: "plain_text", text: "タスク作成" },
        value: JSON.stringify({ teamId, userId }),
      },
    ],
  });

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

  // public は参加していなくても表示する / private・DM は表示しない
  {
    const uniqChannels = Array.from(
      new Set((tasks || []).map((t) => t.channel_id).filter(Boolean)),
    );
    const okMap = new Map();

    for (const ch of uniqChannels) {
      const ok = await canUserSeeChannel({ client, teamId, channelId: ch });
      okMap.set(ch, ok);
    }

    tasks = (tasks || []).filter((t) => {
      if (!t.channel_id) return true;
      return okMap.get(t.channel_id) === true;
    });
  }

  // 表示：未完了はステータス別に分ける（完了/取り下げはまとめ）
  if (st.scopeKey === "done") {
    // ★追加：完了は「直近24時間」だけ表示する（履歴はDBに残す）
    const DONE_VISIBLE_HOURS = 24;
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
      text: {
        type: "mrkdwn",
        text: `*✅ 完了（直近${DONE_VISIBLE_HOURS}時間）*`,
      },
    });

    if (!recentDoneTasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（直近24時間の完了なし）" }],
      });
    } else {
      for (const t of recentDoneTasks) {
        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text:
              st.viewKey === "all"
                ? taskLineForHome(
                    t,
                    t.task_type === "broadcast" ? "broadcast" : "personal",
                  )
                : taskLineForHome(t, st.viewKey),
          },

          accessory: {
            type: "overflow",
            action_id: "task_row_overflow",
            options: [
              {
                text: { type: "plain_text", text: "詳細" },
                value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
              },
            ],
          },
        });
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: " " }],
        });
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: "━━━━━━━━━━━━━━━━━━━━━━━━━" }],
        });
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

    const pushTaskList = async (title, list) => {
      // Slack Home view は blocks <= 100 制限がある
      const MAX_BLOCKS = 100;
      const SAFETY = 8; // 見出しや末尾の余裕

      const canAdd = (n) => blocks.length + n <= MAX_BLOCKS - SAFETY;

      const titlePlain = String(title || "")
        .replace(/\*/g, "")
        .trim();
      blocks.push({
        type: "header",
        text: { type: "plain_text", text: `${titlePlain}（${list.length}件）` },
      });
      blocks.push({ type: "divider" });

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

      for (const t of list) {
        // 1タスクあたり最低5ブロック（本文 + 人 + 期限/link + actions + 区切り）
        if (!canAdd(5)) break;

        const viewKey = t.task_type === "broadcast" ? "broadcast" : "personal";

        // ★ broadcastで「自分が完了済みか？」を判定（範囲=自分あて の時だけ）
        const viewerCompleted =
          rangeKey === "to_me" && t.task_type === "broadcast"
            ? await dbHasUserCompleted(teamId, t.id, userId)
            : false;

        // ✅ 主：タスク内容（本文）
        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: taskLineForHome(t, viewKey),
          },
        });

        // ✅ 小：アイコン + 依頼者 ⇒ アイコン + 対応者（既存のアイコンMapを利用）
        const requesterId = t?.requester_user_id;
        const assigneeId = t?.assignee_id;

        const requesterIcon = requesterId
          ? requesterIconMap.get(requesterId)
          : null;
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
        const dueText = t?.due_date
          ? `（${formatDueDateOnly(t.due_date)}）まで`
          : "";
        const linkText = t?.source_permalink
          ? `🔗 <${t.source_permalink}|元メッセージへ>`
          : "";

        const metaElems = [];
        if (dueText) metaElems.push({ type: "mrkdwn", text: dueText });
        if (linkText) metaElems.push({ type: "mrkdwn", text: linkText });

        blocks.push({
          type: "context",
          elements: metaElems.length
            ? metaElems
            : [{ type: "mrkdwn", text: " " }],
        });

        // ✅ Homeの完了ボタンは「範囲=自分あて（to_me）」の時だけ
        if (rangeKey !== "to_me") {
          blocks.push({
            type: "actions",
            elements: [
              {
                type: "button",
                text: { type: "plain_text", text: "詳細" },
                action_id: "open_detail_modal",
                value: JSON.stringify({ teamId, taskId: t.id }),
              },
            ],
          });
        } else {
          // rangeKey === "to_me"
          if (t.task_type === "broadcast" && viewerCompleted) {
            // 「完了済み」表示（グレー相当）
            blocks.push({
              type: "context",
              elements: [{ type: "mrkdwn", text: "✅ あなたは完了済み" }],
            });

            // 詳細だけ
            blocks.push({
              type: "actions",
              elements: [
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                },
              ],
            });
          } else {
            // 完了 + 詳細（自分あての時だけ）
            blocks.push({
              type: "actions",
              elements: [
                {
                  type: "button",
                  text: {
                    type: "plain_text",
                    text:
                      t.task_type === "broadcast"
                        ? "自分だけ完了 ✅"
                        : "完了 ✅",
                  },
                  style: "primary",
                  action_id: "complete_task",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                  confirm: {
                    title: { type: "plain_text", text: "確認" },
                    text: {
                      type: "mrkdwn",
                      text: "このタスクを*完了*にしますか？",
                    },
                    confirm: { type: "plain_text", text: "完了にする" },
                    deny: { type: "plain_text", text: "やめる" },
                  },
                },
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                },
              ],
            });
          }
        }
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: "━━━━━━━━━━━━━━━━━━━━━━━━━" }],
        });

        shown++;
      }

      const remaining = Math.max(0, list.length - shown);
      if (remaining > 0 && canAdd(1)) {
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: `（他 ${remaining} 件）` }],
        });
      }

      if (canAdd(1)) {
        blocks.push({ type: "divider" });
      }
    };

    // スマホ優先：期限切れ → 今日 → 明日以降
    await pushTaskList("*🚨 期限切れ*", overdue);
    await pushTaskList("*🟨 今日*", todayTasks);
    await pushTaskList("*🟩 明日以降*", laterTasks);
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
// Channel visibility cache（publicは表示OK / private・DMは表示NG）
// ================================
const CHANNEL_VIS_CACHE_MS = 10 * 60 * 1000;
const channelVisCache = new Map(); // `${teamId}:${channelId}` -> { at, ok }

async function canUserSeeChannel({ client, teamId, channelId }) {
  if (!channelId) return true;

  // まずIDプレフィックスで高速判定（API節約）
  const id0 = String(channelId)[0];
  if (id0 === "C") return true; // public channel
  if (id0 === "G") return false; // private channel
  if (id0 === "D") return false; // DM

  // 想定外のID（例：共有チャンネル等）は conversations.info で確定
  const key = `${teamId}:${channelId}`;
  const cached = channelVisCache.get(key);
  if (cached && Date.now() - cached.at < CHANNEL_VIS_CACHE_MS)
    return !!cached.ok;

  try {
    const info = await client.conversations.info({ channel: channelId });
    const ch = info?.channel;
    const isPublic = !!ch?.is_channel && !ch?.is_private;
    channelVisCache.set(key, { at: Date.now(), ok: isPublic });
    return isPublic;
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
              initial_date: slackDateYmd(new Date()),
            },
          },
          //  { type: "input", block_id: "status", label: { type: "plain_text", text: "ステータス" }, element: statusSelectElement("open") },

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
const TASK_REACTION_NAME = "tasks";

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

// メッセージ本文から「個人メンション」だけ拾う（ユーザーグループ/ here/channel は除外）
function inferAssigneeFromMessageText(rawText, fallbackUserId, blocks = null) {
  // ① blocks の user_id を最優先（textにIDが出ない投稿を救う）
  const fromBlocks = extractUserIdsFromBlocks(blocks);
  if (fromBlocks.length) return fromBlocks[0];

  // ② text の <@Uxxx> を拾う（保険）
  const text = String(rawText || "");
  const userIds = [];
  const re = /<@([A-Z0-9]+)(?:\|[^>]+)?>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const uid = m[1];
    if (!uid) continue;
    if (!userIds.includes(uid)) userIds.push(uid);
  }
  return userIds[0] || fallbackUserId;
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
    const assigneeId = inferAssigneeFromMessageText(
      rawText,
      actorUserId,
      mm?.blocks || null,
    );

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
              initial_date: slackDateYmd(new Date()),
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

async function buildTaskListModalView({
  teamId,
  userId,
  rangeKey = "to_me",
  scopeKey = "active",
}) {
  const statuses =
    scopeKey === "done"
      ? DONE_STATUSES
      : scopeKey === "cancelled"
        ? CANCELLED_STATUSES
        : ACTIVE_STATUSES;

  // ★一覧は personal + broadcast を混在（Home思想）
  const personalScope =
    rangeKey === "to_me" || rangeKey === "requested_by_me" ? rangeKey : "all";
  const personalTasks = await dbListPersonalTasksByStatusesWithScope(
    teamId,
    statuses,
    personalScope,
    userId,
    60,
  );

  const broadcastTasks =
    rangeKey === "to_me" || rangeKey === "requested_by_me"
      ? await dbListBroadcastTasksByStatusesWithScope(
          teamId,
          statuses,
          rangeKey,
          userId,
          60,
        )
      : await dbListBroadcastTasksByStatuses(teamId, statuses, "all", 60);

  // ★保険：同一IDは必ず1つにする（重複完全排除）
  const seen = new Set();
  const tasks = [];
  for (const t of [...personalTasks, ...broadcastTasks]) {
    const key = `${t.task_type || "personal"}:${t.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    tasks.push(t);
  }

  const blocks = [];

  // filters（範囲＋状態だけ）
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*範囲*" },
    accessory: myTasksScopeSelectElement(rangeKey),
  });

  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*状態*" },
    accessory: myTasksStatusSelectElement(scopeKey),
  });

  blocks.push({ type: "divider" });

  // list（Homeの表示ロジックを踏襲）

  // list (Homeの表示ロジックを踏襲)
  if (scopeKey === "done") {
    // ★追加：完了は「直近24時間」だけ表示する（履歴はDBに残す）
    const DONE_VISIBLE_HOURS = 24;
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
      text: {
        type: "mrkdwn",
        text: `*✅ 完了（直近${DONE_VISIBLE_HOURS}時間）*`,
      },
    });

    if (!recentDoneTasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（直近24時間の完了なし）" }],
      });
    } else {
      for (const t of recentDoneTasks) {
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: taskLineForHome(t, "personal") },
          accessory: {
            type: "overflow",
            action_id: "task_row_overflow",
            options: [
              {
                text: { type: "plain_text", text: "詳細" },
                value: JSON.stringify({
                  teamId,
                  taskId: t.id,
                  origin: "list_modal",
                }),
              },
            ],
          },
        });
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: "────────────────────────" }],
        });
      }
    }
  } else if (scopeKey === "cancelled") {
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: "*🟥 取り下げ*" },
    });
    if (!tasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（取り下げなし）" }],
      });
    } else {
      for (const t of tasks) {
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: taskLineForHome(t, "personal") },
          accessory: {
            type: "button",
            text: { type: "plain_text", text: "詳細" },
            action_id: "open_detail_modal",
            value: JSON.stringify({
              teamId,
              taskId: t.id,
              origin: "task_list_modal",
            }),
          },
        });
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: "────────────────────────" }],
        });
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
    const pushTaskList = async (title, list) => {
      // Slack Home view は blocks <= 100 制限がある
      const MAX_BLOCKS = 100;
      const SAFETY = 8; // 見出しや末尾の余裕

      const canAdd = (n) => blocks.length + n <= MAX_BLOCKS - SAFETY;

      const titlePlain = String(title || "")
        .replace(/\*/g, "")
        .trim();
      blocks.push({
        type: "header",
        text: { type: "plain_text", text: `${titlePlain}（${list.length}件）` },
      });
      blocks.push({ type: "divider" });

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

      for (const t of list) {
        // 1タスクあたり最低5ブロック（本文 + 人 + 期限/link + actions + 区切り）
        if (!canAdd(5)) break;

        const viewKey = t.task_type === "broadcast" ? "broadcast" : "personal";

        // ★ broadcastで「自分が完了済みか？」を判定（範囲=自分あて の時だけ）
        const viewerCompleted =
          rangeKey === "to_me" && t.task_type === "broadcast"
            ? await dbHasUserCompleted(teamId, t.id, userId)
            : false;

        // ✅ 主：タスク内容（本文）
        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: taskLineForHome(t, viewKey),
          },
        });

        // ✅ 小：アイコン + 依頼者 ⇒ アイコン + 対応者（既存のアイコンMapを利用）
        const requesterId = t?.requester_user_id;
        const assigneeId = t?.assignee_id;

        const requesterIcon = requesterId
          ? requesterIconMap.get(requesterId)
          : null;
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
        const dueText = t?.due_date
          ? `（${formatDueDateOnly(t.due_date)}）まで`
          : "";
        const linkText = t?.source_permalink
          ? `🔗 <${t.source_permalink}|元メッセージへ>`
          : "";

        const metaElems = [];
        if (dueText) metaElems.push({ type: "mrkdwn", text: dueText });
        if (linkText) metaElems.push({ type: "mrkdwn", text: linkText });

        blocks.push({
          type: "context",
          elements: metaElems.length
            ? metaElems
            : [{ type: "mrkdwn", text: " " }],
        });

        // ✅ Homeの完了ボタンは「範囲=自分あて（to_me）」の時だけ
        if (rangeKey !== "to_me") {
          blocks.push({
            type: "actions",
            elements: [
              {
                type: "button",
                text: { type: "plain_text", text: "詳細" },
                action_id: "open_detail_modal",
                value: JSON.stringify({ teamId, taskId: t.id }),
              },
            ],
          });
        } else {
          // rangeKey === "to_me"
          if (t.task_type === "broadcast" && viewerCompleted) {
            // 「完了済み」表示（グレー相当）
            blocks.push({
              type: "context",
              elements: [{ type: "mrkdwn", text: "✅ あなたは完了済み" }],
            });

            // 詳細だけ
            blocks.push({
              type: "actions",
              elements: [
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                },
              ],
            });
          } else {
            // 完了 + 詳細（自分あての時だけ）
            blocks.push({
              type: "actions",
              elements: [
                {
                  type: "button",
                  text: {
                    type: "plain_text",
                    text:
                      t.task_type === "broadcast"
                        ? "自分だけ完了 ✅"
                        : "完了 ✅",
                  },
                  style: "primary",
                  action_id: "complete_task",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                  confirm: {
                    title: { type: "plain_text", text: "確認" },
                    text: {
                      type: "mrkdwn",
                      text: "このタスクを*完了*にしますか？",
                    },
                    confirm: { type: "plain_text", text: "完了にする" },
                    deny: { type: "plain_text", text: "やめる" },
                  },
                },
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                },
              ],
            });
          }
        }

        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: "━━━━━━━━━━━━━━━━━━━━━━━━━" }],
        });

        shown++;
      }
      const remaining = Math.max(0, list.length - shown);
      if (remaining > 0 && canAdd(1)) {
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: `（他 ${remaining} 件）` }],
        });
      }

      if (canAdd(1)) {
        blocks.push({ type: "divider" });
      }
    };

    // スマホ優先：期限切れ → 今日 → 明日以降
    await pushTaskList("*🚨 期限切れ*", overdue);
    await pushTaskList("*🟨 今日*", todayTasks);
    await pushTaskList("*🟩 明日以降*", laterTasks);
  }

  const meta = { teamId, userId, rangeKey, scopeKey };

  return {
    type: "modal",
    callback_id: "task_list_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "タスク一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
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

    const description =
      view.state.values.desc?.desc_input?.value?.trim() ||
      meta.messageTextPretty ||
      meta.messageText ||
      "";

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
    const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(
      teamId,
      selectedGroupIds,
    );

    // targets = selectedUsers + groupUsers
    const targets = new Set();
    for (const u of selectedUsers) targets.add(u);
    for (const u of groupUsers) targets.add(u);

    const targetList = Array.from(targets);

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

    // ① 発行通知（personal / broadcast）
    // - 自分が発行して自分が対象の場合は通知しない（うるささ回避）
    try {
      if (taskType === "personal") {
        const to = personalAssigneeId;
        if (to && to !== actorUserId) {
          await notifyTaskSimpleDM(to, created, "📝 タスクが届いたよ");
        }
      } else if (taskType === "broadcast") {
        // 対象者へ通知（必要ならここで数が多い場合は抑止もできる）
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

// Home: broadcast 範囲 change（Phase8-3）
app.action("home_broadcast_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    // ★範囲は共通キーとして使う（表示は常に all）
    // ★範囲が all 以外なら dept は意味が薄いので初期化しておく
    setHomeState(teamId, userId, {
      viewKey: "all",
      broadcastScopeKey: selected,
      personalScopeKey: selected,
      ...(selected === "all" ? {} : { deptKey: "all" }),
    });

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

      const nextView = await buildListDetailView({
        teamId,
        task,
        returnState,
        viewerUserId: body.user.id,
      });

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

// Home: タスク作成（メッセージなし）
app.action("home_create_task", async ({ ack, body, client }) => {
  await ack();

  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user?.id;
    if (!teamId || !userId) return;

    const today = jstDateOnly(new Date());
    const initDue = slackDateYmd(today);

    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: "modal",
        callback_id: "task_modal",
        private_metadata: JSON.stringify({
          teamId,
          channelId: "",
          msgTs: "",
          requesterUserId: userId,
          messageText: "",
          messageTextPretty: "",
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
              initial_value: "",
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
            },
          },

          {
            type: "input",
            block_id: "due",
            label: { type: "plain_text", text: "期限" },
            element: {
              type: "datepicker",
              action_id: "due_date",
              ...(initDue ? { initial_date: initDue } : {}),
              placeholder: { type: "plain_text", text: "日付を選択" },
            },
          },
          //{ type: "input", block_id: "status", label: { type: "plain_text", text: "ステータス" }, element: statusSelectElement("open") },

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
    console.error("home_create_task error:", e?.data || e);
  }
});

// Home: open list modal
app.action(
  "open_list_modal_from_home",
  async ({ ack, body, action, client }) => {
    await ack();
    const p = safeJsonParse(action.value || "{}") || {};
    const teamId = p.teamId || body.team?.id || body.team_id;
    const viewType = p.viewType || "assigned";
    const userId = p.userId || body.user.id;
    const status = p.status || "open";
    const deptKey = p.deptKey || "all";

    await client.views.open({
      trigger_id: body.trigger_id,
      view: await buildListModalView({
        teamId,
        viewType,
        userId,
        status,
        deptKey,
      }),
    });
  },
);

// list modal: status filter
app.action("list_filter_select", async ({ ack, body, action, client }) => {
  await ack();
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = meta.teamId || body.team?.id || body.team_id;
  const viewType = meta.viewType || "assigned";
  const userId = meta.userId || body.user.id;
  const deptKey = meta.deptKey || "all";
  const nextStatus = action?.selected_option?.value || "open";

  const nextView = await buildListModalView({
    teamId,
    viewType,
    userId,
    status: nextStatus,
    deptKey,
  });

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

  const nextView = await buildListModalView({
    teamId,
    viewType,
    userId,
    status,
    deptKey: nextDept,
  });

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

    const nextView = await buildListDetailView({
      teamId,
      task,
      returnState,
      viewerUserId: body.user.id,
    });
    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: nextView,
    });
  } catch (e) {
    console.error("open_detail_in_list error:", e?.data || e);
  }
});

app.action("back_to_list", async ({ ack, body, client }) => {
  await ack();
  try {
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const teamId = meta.teamId || body.team?.id || body.team_id;
    const returnState = meta.returnState || {
      viewType: "assigned",
      userId: body.user.id,
      status: "open",
      deptKey: "all",
    };

    const listView = await buildListModalView({
      teamId,
      viewType: returnState.viewType,
      userId: returnState.userId,
      status: returnState.status,
      deptKey: returnState.deptKey,
    });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: listView,
    });
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

      // 全員完了（= 依頼者の確認待ちへ）
      if (doneCount >= total && total > 0) {
        const fresh = await dbGetTaskById(teamId, taskId);
        if (fresh && !["waiting", "done", "cancelled"].includes(fresh.status)) {
          await dbUpdateStatus(teamId, taskId, "waiting");
        }
        // 依頼者へ通知（1回だけ）
        if (fresh && !fresh.notified_at) {
          await dbQuery(
            `UPDATE tasks SET notified_at=now() WHERE team_id=$1 AND id=$2 AND notified_at IS NULL`,
            [teamId, taskId],
          );
          await postRequesterConfirmDM({
            teamId,
            taskId,
            requesterUserId: fresh.requester_user_id,
            title: fresh.title,
          });
          // ★Home再描画：全員完了→確認待ち（依頼者/対象者にも反映）
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

      // modal refresh
      if (body.view?.id) {
        const refreshed = await dbGetTaskById(teamId, taskId);
        if (refreshed) {
          if (body.view.callback_id === "list_detail_modal") {
            const meta2 =
              safeJsonParse(body.view?.private_metadata || "{}") || {};
            const returnState = meta2.returnState || {
              viewType: "assigned",
              userId,
              status: "open",
              deptKey: "all",
            };
            await client.views.update({
              view_id: body.view.id,
              hash: body.view.hash,
              view: await buildListDetailView({
                teamId,
                task: refreshed,
                returnState,
                viewerUserId: userId,
              }),
            });
          } else {
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

    // refresh open modal if any
    if (body.view?.id) {
      if (body.view.callback_id === "list_detail_modal") {
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
      } else if (body.view.callback_id === "detail_modal") {
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildDetailModalView({
            teamId,
            task: updated,
            viewerUserId: body.user.id,
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

// ================================
// Start
// ================================
(async () => {
  const port = process.env.PORT ? Number(process.env.PORT) : 3000;
  await app.start(port);
  console.log(`⚡️ Slack app is running on port ${port}`);
})();
