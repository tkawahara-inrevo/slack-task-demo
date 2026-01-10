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

// 通知抑止：@mk 等を表示したいが、メンション通知は飛ばしたくない
function noMention(s) {
  if (!s) return "";
  return String(s).replace(/@/g, "＠");
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
// DB: Tasks
// ================================
async function dbCreateTask(task) {
  const q = `
    INSERT INTO tasks (
      id, team_id, channel_id, message_ts, source_permalink,
      title, description,
      requester_user_id, created_by_user_id,
      assignee_id, assignee_label,
      status, due_date,
      notified_at, requester_dept, assignee_dept,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,
      $8,$9,
      $10,$11,
      $12,$13,
      $14,$15,$16,
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
    task.assignee_id,
    task.assignee_label,
    task.status,
    task.due_date,
    task.notified_at ?? null,
    task.requester_dept ?? null,
    task.assignee_dept ?? null,
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
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND assignee_id=$2 AND status=$3
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, [teamId, assigneeId, status, limit]);
  return res.rows;
}

async function dbListTasksForRequester(teamId, requesterUserId, status, limit = 10) {
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND requester_user_id=$2 AND status=$3
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, [teamId, requesterUserId, status, limit]);
  return res.rows;
}

// dept filter
async function dbListTasksForAssigneeWithDept(teamId, assigneeId, status, deptKey, limit = 20) {
  if (!deptKey || deptKey === "all") {
    return await dbListTasksForAssignee(teamId, assigneeId, status, limit);
  }
  if (deptKey === "__none__") {
    const q = `
      SELECT * FROM tasks
      WHERE team_id=$1 AND assignee_id=$2 AND status=$3 AND assignee_dept IS NULL
      ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
      LIMIT $4;
    `;
    const res = await dbQuery(q, [teamId, assigneeId, status, limit]);
    return res.rows;
  }
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND assignee_id=$2 AND status=$3 AND assignee_dept=$4
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $5;
  `;
  const res = await dbQuery(q, [teamId, assigneeId, status, deptKey, limit]);
  return res.rows;
}

async function dbListTasksForRequesterWithDept(teamId, requesterUserId, status, deptKey, limit = 20) {
  if (!deptKey || deptKey === "all") {
    return await dbListTasksForRequester(teamId, requesterUserId, status, limit);
  }
  if (deptKey === "__none__") {
    const q = `
      SELECT * FROM tasks
      WHERE team_id=$1 AND requester_user_id=$2 AND status=$3 AND requester_dept IS NULL
      ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
      LIMIT $4;
    `;
    const res = await dbQuery(q, [teamId, requesterUserId, status, limit]);
    return res.rows;
  }
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND requester_user_id=$2 AND status=$3 AND requester_dept=$4
    ORDER BY (due_date IS NULL) ASC, due_date ASC, created_at DESC
    LIMIT $5;
  `;
  const res = await dbQuery(q, [teamId, requesterUserId, status, deptKey, limit]);
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
  return `<@${task.assignee_id}>`;
}

async function safeEphemeral(client, channelId, userId, text) {
  try {
    await client.chat.postEphemeral({ channel: channelId, user: userId, text });
  } catch (_) {}
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

  const payload = JSON.stringify({
    teamId,
    taskId: task.id,
    channelId: task.channel_id || "",
    parentTs: task.message_ts || "",
  });

  return [
    { type: "header", text: { type: "plain_text", text: "⏱ タスク" } },
    { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
    { type: "divider" },

    // ★要望②：ラベル変更＋分離表示
    { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
    { type: "section", text: { type: "mrkdwn", text: `*依頼者部署*：${deptLabel(task.requester_dept)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*対応者部署*：${deptLabel(task.assignee_dept)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: `*元メッセージ*\n${src}` } },
    { type: "divider" },

    {
      type: "actions",
      elements: [
        { type: "button", text: { type: "plain_text", text: "詳細を開く" }, action_id: "open_detail_modal", value: payload },
      ],
    },
    { type: "context", elements: [{ type: "mrkdwn", text: "✅ 完了は「詳細」画面から行います（誤操作防止）" }] },
  ];
}

// ================================
// Detail Modal（views.open）
// ================================
async function buildDetailModalView({ teamId, task }) {
  const srcLinesRaw = (task.description || "").split("\n").slice(0, 10).join("\n") || "（本文なし）";
  const srcLines = noMention(srcLinesRaw);

  const base = {
    teamId,
    taskId: task.id,
    channelId: task.channel_id || "",
    parentTs: task.message_ts || "",
  };

  const canCancel = task.status !== "done" && task.status !== "cancelled";

  return {
    type: "modal",
    callback_id: "detail_modal",
    private_metadata: JSON.stringify({ teamId, taskId: task.id }),
    title: { type: "plain_text", text: "タスク" },
    close: { type: "plain_text", text: "閉じる" },
    blocks: [
      { type: "header", text: { type: "plain_text", text: "📘 タスク" } },
      { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
      { type: "section", text: { type: "mrkdwn", text: `*依頼者部署*：${deptLabel(task.requester_dept)}` } },

      { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },

      { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*対応者部署*：${deptLabel(task.assignee_dept)}` } },

      { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

      { type: "divider" },
      { type: "section", text: { type: "mrkdwn", text: "*ステータス変更*" }, accessory: statusSelectElement(task.status === "cancelled" ? "open" : task.status) },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*元メッセージ（全文）*\n\`\`\`\n${srcLines}\n\`\`\`` } },

      { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "完了 ✅" }, style: "primary", action_id: "complete_task", value: JSON.stringify(base) }] },

      ...(canCancel
        ? [
            {
              type: "actions",
              elements: [{ type: "button", text: { type: "plain_text", text: "取り下げ（依頼者のみ）" }, style: "danger", action_id: "cancel_task", value: JSON.stringify(base) }],
            },
          ]
        : []),
    ],
  };
}

async function openDetailModal(client, { trigger_id, teamId, taskId }) {
  const task = await dbGetTaskById(teamId, taskId);
  if (!task) return;

  await client.views.open({
    trigger_id,
    view: await buildDetailModalView({ teamId, task }),
  });
}

// ================================
// Home: mode + dept filter (① Homeで部署フィルタ)
// ================================
const HOME_MODES = [
  { key: "assigned_active", label: "担当タスク（未完了）", viewType: "assigned", tab: "active" },
  { key: "assigned_done", label: "担当タスク（完了）", viewType: "assigned", tab: "done" },
  { key: "requested_active", label: "依頼したタスク（未完了）", viewType: "requested", tab: "active" },
  { key: "requested_done", label: "依頼したタスク（完了）", viewType: "requested", tab: "done" },
];

function getHomeMode(key) {
  return HOME_MODES.find((m) => m.key === key) || HOME_MODES[0];
}

function homeModeSelectElement(activeKey) {
  const cur = getHomeMode(activeKey);
  return {
    type: "static_select",
    action_id: "home_mode_select",
    initial_option: { text: { type: "plain_text", text: cur.label }, value: cur.key },
    options: HOME_MODES.map((m) => ({ text: { type: "plain_text", text: m.label }, value: m.key })),
  };
}

// Homeの状態を保持（ユーザーごと）
const homeState = new Map(); // `${teamId}:${userId}` -> { modeKey, deptKey }

function getHomeState(teamId, userId) {
  const k = `${teamId}:${userId}`;
  const s = homeState.get(k) || { modeKey: "assigned_active", deptKey: "all" };
  return s;
}
function setHomeState(teamId, userId, next) {
  const k = `${teamId}:${userId}`;
  homeState.set(k, { ...getHomeState(teamId, userId), ...next });
}

async function fetchListTasks({ teamId, viewType, userId, status, limit, deptKey }) {
  if (viewType === "requested") {
    return await dbListTasksForRequesterWithDept(teamId, userId, status, deptKey, limit);
  }
  return await dbListTasksForAssigneeWithDept(teamId, userId, status, deptKey, limit);
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

async function publishHome({ client, teamId, userId }) {
  const { modeKey, deptKey } = getHomeState(teamId, userId);
  const mode = getHomeMode(modeKey);
  const isDone = mode.tab === "done";
  const listStartStatus = isDone ? "done" : "open";

  const deptKeys = await listDeptKeys(teamId);

const blocks = [
  // 1行目：担当（固定ラベル） + モード選択（4択）
  {
    type: "section",
    text: { type: "mrkdwn", text: "*担当*" }, // ← 固定
    accessory: homeModeSelectElement(mode.key), // ← ここだけで切替
  },

  // 2行目：部署（固定ラベル） + 部署フィルタ
  {
    type: "section",
    text: { type: "mrkdwn", text: "*部署*" }, // ← 固定
    accessory: deptSelectElement(deptKey || "all", deptKeys),
  },

  // 3行目：一覧ボタン
  {
    type: "actions",
    elements: [
      {
        type: "button",
        text: { type: "plain_text", text: "一覧" },
        action_id: "open_list_modal_from_home",
        value: JSON.stringify({
          teamId,
          viewType: mode.viewType,
          userId,
          status: listStartStatus,
          deptKey: deptKey || "all",
        }),
      },
    ],
  },

  { type: "divider" },
];


  const listFn = async (status, limit) => fetchListTasks({ teamId, viewType: mode.viewType, userId, status, limit, deptKey: deptKey || "all" });

  const cardLine = (t) =>
    mode.viewType === "requested"
      ? `*${noMention(t.title)}*\n期限：${formatDueDateOnly(t.due_date)} / 対応者：${assigneeDisplay(t)}`
      : `*${noMention(t.title)}*\n期限：${formatDueDateOnly(t.due_date)} / 依頼者：<@${t.requester_user_id}>`;

  if (!isDone) {
    const openTasks = await listFn("open", 10);
    const inProgress = await listFn("in_progress", 10);
    const waiting = await listFn("waiting", 10);

    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟦 未着手*" } });
    blocks.push(...(openTasks.length ? openTasks.map(t => ({
      type: "section",
      text: { type: "mrkdwn", text: cardLine(t) },
      accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
    })) : [{ type: "context", elements: [{ type: "mrkdwn", text: "（未着手なし）" }] }]));
    blocks.push({ type: "divider" });

    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟨 対応中*" } });
    blocks.push(...(inProgress.length ? inProgress.map(t => ({
      type: "section",
      text: { type: "mrkdwn", text: cardLine(t) },
      accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
    })) : [{ type: "context", elements: [{ type: "mrkdwn", text: "（対応中なし）" }] }]));
    blocks.push({ type: "divider" });

    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟧 確認待ち*" } });
    blocks.push(...(waiting.length ? waiting.map(t => ({
      type: "section",
      text: { type: "mrkdwn", text: cardLine(t) },
      accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
    })) : [{ type: "context", elements: [{ type: "mrkdwn", text: "（確認待ちなし）" }] }]));
  } else {
    const doneTasks = await listFn("done", 30);
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*✅ 完了済み*" } });
    blocks.push(...(doneTasks.length ? doneTasks.map(t => ({
      type: "section",
      text: { type: "mrkdwn", text: cardLine(t) },
      accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
    })) : [{ type: "context", elements: [{ type: "mrkdwn", text: "（完了済みなし）" }] }]));
  }

  await client.views.publish({
    user_id: userId,
    view: { type: "home", blocks },
  });
}

// ================================
// List Modal（status + dept filter）
// ================================
function viewTypeLabel(viewType) {
  return viewType === "requested" ? "依頼したタスク" : "担当タスク";
}

async function buildListModalView({ teamId, viewType, userId, status, deptKey }) {
  const tasks = await fetchListTasks({ teamId, viewType, userId, status, limit: 20, deptKey: deptKey || "all" });
  const deptKeys = await listDeptKeys(teamId);

  const deptOptions = [
    { text: { type: "plain_text", text: "すべて" }, value: "all" },
    { text: { type: "plain_text", text: "未設定" }, value: "__none__" },
    ...deptKeys.map((k) => ({ text: { type: "plain_text", text: `@${k}` }, value: k })),
  ];
  const deptText =
    (deptKey || "all") === "all"
      ? "すべて"
      : (deptKey || "all") === "__none__"
        ? "未設定"
        : `@${deptKey}`;

  const blocks = [
    { type: "header", text: { type: "plain_text", text: `📋 ${viewTypeLabel(viewType)}（${statusLabel(status)}）` } },
    { type: "context", elements: [{ type: "mrkdwn", text: "フィルタで切替できます。詳細から完了/ステータス/取り下げを操作できます。" }] },
    { type: "divider" },

    {
      type: "section",
      text: { type: "mrkdwn", text: "*表示フィルタ*" },
      accessory: {
        type: "static_select",
        action_id: "list_filter_select",
        initial_option: { text: { type: "plain_text", text: statusLabel(status) }, value: status },
        options: [
          { text: { type: "plain_text", text: "未着手" }, value: "open" },
          { text: { type: "plain_text", text: "対応中" }, value: "in_progress" },
          { text: { type: "plain_text", text: "確認待ち" }, value: "waiting" },
          { text: { type: "plain_text", text: "完了" }, value: "done" },
          { text: { type: "plain_text", text: "取り下げ" }, value: "cancelled" },
        ],
      },
    },
    {
      type: "section",
      text: { type: "mrkdwn", text: "*部署フィルタ*" },
      accessory: {
        type: "static_select",
        action_id: "dept_filter_select",
        initial_option: { text: { type: "plain_text", text: deptText }, value: deptKey || "all" },
        options: deptOptions,
      },
    },
    { type: "divider" },
  ];

  if (!tasks.length) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "（該当タスクなし）" } });
  } else {
    for (const t of tasks) {
      const deptLine =
        viewType === "requested"
          ? `対応者部署：${deptLabel(t.assignee_dept)}`
          : `依頼者部署：${deptLabel(t.requester_dept)}`;

      const metaLine =
        viewType === "requested"
          ? `対応者：${assigneeDisplay(t)}　｜　期限：${formatDueDateOnly(t.due_date)}　｜　ステータス：${statusLabel(t.status)}\n${deptLine}`
          : `依頼者：<@${t.requester_user_id}>　｜　期限：${formatDueDateOnly(t.due_date)}　｜　ステータス：${statusLabel(t.status)}\n${deptLine}`;

      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: `*${noMention(t.title)}*\n${metaLine}` },
        accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_in_list", value: JSON.stringify({ teamId, taskId: t.id }) },
      });
      blocks.push({ type: "divider" });
    }
  }

  return {
    type: "modal",
    callback_id: "list_modal",
    private_metadata: JSON.stringify({ teamId, viewType, userId, status, deptKey: deptKey || "all" }),
    title: { type: "plain_text", text: "一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

// List modal -> detail (same modal)
async function buildListDetailView({ teamId, task, returnState }) {
  const srcLinesRaw = (task.description || "").split("\n").slice(0, 10).join("\n") || "（本文なし）";
  const srcLines = noMention(srcLinesRaw);
  const canCancel = task.status !== "done" && task.status !== "cancelled";

  const meta = { mode: "list_detail", teamId, taskId: task.id, returnState };

  const base = { teamId, taskId: task.id, channelId: task.channel_id || "", parentTs: task.message_ts || "" };

  const backLabel = returnState?.viewType === "requested" ? "依頼一覧へ戻る" : "担当一覧へ戻る";

  return {
    type: "modal",
    callback_id: "list_detail_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks: [
      { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: `← ${backLabel}` }, action_id: "back_to_list", value: JSON.stringify({ teamId }) }] },
      { type: "header", text: { type: "plain_text", text: "📘 タスク詳細" } },
      { type: "section", text: { type: "mrkdwn", text: `*${noMention(task.title)}*` } },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
      { type: "section", text: { type: "mrkdwn", text: `*依頼者部署*：${deptLabel(task.requester_dept)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*対応者部署*：${deptLabel(task.assignee_dept)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

      { type: "divider" },
      { type: "section", text: { type: "mrkdwn", text: "*ステータス変更*" }, accessory: statusSelectElement(task.status === "cancelled" ? "open" : task.status) },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*元メッセージ（全文）*\n\`\`\`\n${srcLines}\n\`\`\`` } },

      { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "完了 ✅" }, style: "primary", action_id: "complete_task", value: JSON.stringify(base) }] },
      ...(canCancel ? [{ type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "取り下げ（依頼者のみ）" }, style: "danger", action_id: "cancel_task", value: JSON.stringify(base) }] }] : []),
    ],
  };
}

// ================================
// Events
// ================================
app.event("app_home_opened", async ({ event, client, body }) => {
  try {
    const teamId = body.team_id || body.team?.id || event.team;
    const userId = event.user;
    setHomeState(teamId, userId, { modeKey: "assigned_active", deptKey: "all" });
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

    const prettyText = await prettifySlackText(rawText, teamId);
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
          { type: "input", block_id: "assignee_user", label: { type: "plain_text", text: "対応者" }, element: { type: "users_select", action_id: "assignee_user_select", placeholder: { type: "plain_text", text: "ユーザーを選択" } } },
          { type: "input", block_id: "due", label: { type: "plain_text", text: "期限" }, element: { type: "datepicker", action_id: "due_date", placeholder: { type: "plain_text", text: "日付を選択" } } },
          { type: "input", block_id: "status", label: { type: "plain_text", text: "ステータス" }, element: statusSelectElement("open") },
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
app.view("task_modal", async ({ ack, body, view, client }) => {
  await ack();

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

    const assigneeUserId = view.state.values.assignee_user?.assignee_user_select?.selected_user;
    if (!assigneeUserId) return;

    const due = view.state.values.due?.due_date?.selected_date || null;
    const status = view.state.values.status?.status_select?.selected_option?.value || "open";
    const requesterUserId = meta.requesterUserId || actorUserId;

    // dept resolve (A)
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const assigneeDept = await resolveDeptForUser(teamId, assigneeUserId);

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
      assignee_id: assigneeUserId,
      assignee_label: null,
      status,
      due_date: due,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
    });

    // Create feedback (no auto detail modal)
    try {
      const payload = JSON.stringify({
        teamId,
        taskId,
        channelId: channelId || "",
        parentTs: parentTs || "",
      });

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

    // Best-effort home refresh (作成者/対応者のHomeに反映しやすくする)
    try { await publishHome({ client, teamId, userId: requesterUserId }); } catch (_) {}
    try { await publishHome({ client, teamId, userId: assigneeUserId }); } catch (_) {}

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
  if (!teamId || !taskId) return;

  try {
    await openDetailModal(client, { trigger_id: body.trigger_id, teamId, taskId });
  } catch (e) {
    console.error("open_detail_modal error:", e?.data || e);
  }
});

// Home: mode change
app.action("home_mode_select", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user.id;
    const modeKey = action?.selected_option?.value || "assigned_active";
    setHomeState(teamId, userId, { modeKey });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_mode_select error:", e?.data || e);
  }
});

// Home: dept change
app.action("home_dept_select", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user.id;
    const deptKey = action?.selected_option?.value || "all";
    setHomeState(teamId, userId, { deptKey });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_dept_select error:", e?.data || e);
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

    const nextView = await buildListDetailView({ teamId, task, returnState });
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

// complete (detail only)
app.action("complete_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;
  const channelId = p.channelId;
  const parentTs = p.parentTs;

  if (!teamId || !taskId) return;

  try {
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState }),
      });
      return;
    }

    if (channelId && parentTs) {
      // スレッドカードは完了ボタンが無いので、表示だけ更新
      const doneBlocks = [
        { type: "header", text: { type: "plain_text", text: "✅ 完了しました" } },
        { type: "section", text: { type: "mrkdwn", text: `*${noMention(updated.title)}*\nタスクを完了にしました✨` } },
      ];
      await upsertThreadCard(client, { teamId, channelId, parentTs, blocks: doneBlocks });
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      const refreshed = await dbGetTaskById(teamId, taskId);
      if (refreshed) {
        await client.views.update({
          view_id: body.view.id,
          hash: body.view.hash,
          view: await buildDetailModalView({ teamId, task: refreshed }),
        });
      }
    }
  } catch (e) {
    console.error("complete_task error:", e?.data || e);
  }
});

app.action("cancel_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId;
  const channelId = p.channelId;
  const parentTs = p.parentTs;

  if (!teamId || !taskId) return;

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    if (task.requester_user_id !== body.user.id) {
      await safeEphemeral(client, channelId || body.user.id, body.user.id, "🥺 取り下げできるのは依頼者だけだよ…！");
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
        view: await buildListDetailView({ teamId, task: cancelled, returnState }),
      });
      return;
    }

    if (channelId && parentTs) {
      const blocks = [
        { type: "header", text: { type: "plain_text", text: "🚫 取り下げました" } },
        { type: "section", text: { type: "mrkdwn", text: `*${noMention(cancelled.title)}*\n依頼者により取り下げられました。` } },
      ];
      await upsertThreadCard(client, { teamId, channelId, parentTs, blocks });
    }

    if (body.view?.id && body.view.callback_id === "detail_modal") {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildDetailModalView({ teamId, task: cancelled }),
      });
    }
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

    const updated = await dbUpdateStatus(teamId, taskId, nextStatus);
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open", deptKey: "all" };
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState }),
      });
      return;
    }

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({ teamId, task: updated }),
    });

    // スレッドカードは完了ボタンなしで、表示だけ更新する
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      await upsertThreadCard(client, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
    }
  } catch (e) {
    console.error("status_select error:", e?.data || e);
  }
});

// ================================
// Due notify (09:00 JST)
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
// Start
// ================================
(async () => {
  const port = process.env.PORT ? Number(process.env.PORT) : 3000;
  await app.start(port);
  console.log(`⚡️ Slack app is running on port ${port}`);
})();
