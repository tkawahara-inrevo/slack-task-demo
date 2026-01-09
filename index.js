require("dotenv").config();
const { App } = require("@slack/bolt");
const { Pool } = require("pg");
const { randomUUID } = require("crypto");

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

function generateTitleCandidate(text, maxLen = 22) {
  if (!text) return "（タスク）";
  let s = String(text);

  s = s.replace(/\r\n/g, "\n");
  s = s.replace(/https?:\/\/\S+/g, "");
  s = s.replace(/<@[A-Z0-9]+>/g, "");
  s = s.replace(/<#[A-Z0-9]+\|[^>]+>/g, "");
  s = s.replace(/:[a-z0-9_+-]+:/gi, "");
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

// 期限を YYYY/MM/DD のみにする（強化版）
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

async function dbQuery(text, params) {
  const client = await pool.connect();
  try {
    return await client.query(text, params);
  } finally {
    client.release();
  }
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
      assignee_type, assignee_id, assignee_label,
      status, due_date,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,
      $8,$9,
      $10,$11,$12,
      $13,$14,
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
    task.assignee_type,
    task.assignee_id,
    task.assignee_label,
    task.status,
    task.due_date,
  ];
  const res = await dbQuery(q, params);
  return res.rows[0];
}

async function dbGetTaskById(teamId, taskId) {
  const q = `SELECT * FROM tasks WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0] || null;
}

// 担当（assignee）で一覧
async function dbListTasksForAssignee(teamId, assigneeType, assigneeId, status, limit = 10) {
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND assignee_type=$2 AND assignee_id=$3 AND status=$4
    ORDER BY
      (due_date IS NULL) ASC,
      due_date ASC,
      created_at DESC
    LIMIT $5;
  `;
  const res = await dbQuery(q, [teamId, assigneeType, assigneeId, status, limit]);
  return res.rows;
}

// 依頼（requester）で一覧
async function dbListTasksForRequester(teamId, requesterUserId, status, limit = 10) {
  const q = `
    SELECT * FROM tasks
    WHERE team_id=$1 AND requester_user_id=$2 AND status=$3
    ORDER BY
      (due_date IS NULL) ASC,
      due_date ASC,
      created_at DESC
    LIMIT $4;
  `;
  const res = await dbQuery(q, [teamId, requesterUserId, status, limit]);
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
// UI Builders
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

// ✅ スレッド：詳細 → 完了（一覧は無し）
async function buildThreadCardBlocks({ teamId, task }) {
  const src = task.source_permalink
    ? `<${task.source_permalink}|元メッセージを開く>`
    : `> ${(task.description || "").slice(0, 140)}`;

  const payload = JSON.stringify({
    teamId,
    taskId: task.id,
    channelId: task.channel_id || "",
    parentTs: task.message_ts || "",
  });

  return [
    { type: "header", text: { type: "plain_text", text: "⏱ タスク" } },
    { type: "section", text: { type: "mrkdwn", text: `*${task.title}*` } },
    { type: "divider" },

    { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
    { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
    { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: `*元メッセージ*\n${src}` } },
    { type: "divider" },

    {
      type: "actions",
      elements: [
        { type: "button", text: { type: "plain_text", text: "詳細を開く" }, action_id: "open_detail_modal", value: payload },
        { type: "button", text: { type: "plain_text", text: "完了 ✅" }, style: "primary", action_id: "complete_task", value: payload },
      ],
    },
  ];
}

// ================================
// Detail Modal（Home/スレッド/エフェメラルから開く用：views.open）
// ================================
async function buildDetailModalView({ teamId, task }) {
  const srcLines = (task.description || "").split("\n").slice(0, 10).join("\n") || "（本文なし）";

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
      { type: "section", text: { type: "mrkdwn", text: `*${task.title}*` } },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
      { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

      { type: "divider" },
      {
        type: "section",
        text: { type: "mrkdwn", text: "*ステータス変更*" },
        accessory: statusSelectElement(task.status === "cancelled" ? "open" : task.status),
      },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*元メッセージ（全文）*\n\`\`\`\n${srcLines}\n\`\`\`` } },

      {
        type: "actions",
        elements: [
          { type: "button", text: { type: "plain_text", text: "完了 ✅" }, style: "primary", action_id: "complete_task", value: JSON.stringify(base) },
        ],
      },

      ...(canCancel
        ? [
            {
              type: "actions",
              elements: [
                { type: "button", text: { type: "plain_text", text: "取り下げ（依頼者のみ）" }, style: "danger", action_id: "cancel_task", value: JSON.stringify(base) },
              ],
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
// List Modal（担当/依頼 + プルダウンで status フィルタ）
// ================================
async function fetchListTasks({ teamId, viewType, userId, status, limit }) {
  if (viewType === "requested") {
    return await dbListTasksForRequester(teamId, userId, status, limit);
  }
  // default: assigned
  return await dbListTasksForAssignee(teamId, "user", userId, status, limit);
}

function viewTypeLabel(viewType) {
  return viewType === "requested" ? "依頼したタスク" : "担当タスク";
}

async function buildListModalView({ teamId, viewType, userId, status }) {
  const tasks = await fetchListTasks({ teamId, viewType, userId, status, limit: 20 });

  const blocks = [
    { type: "header", text: { type: "plain_text", text: `📋 ${viewTypeLabel(viewType)}（${statusLabel(status)}）` } },
    { type: "context", elements: [{ type: "mrkdwn", text: "フィルタで切替できます。詳細から完了/ステータス/取り下げを操作できます。" }] },
    { type: "divider" },
  ];

  blocks.push({
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
  });

  blocks.push({ type: "divider" });

  if (!tasks.length) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "（該当タスクなし）" } });
  } else {
    for (const t of tasks) {
      const metaLine =
        viewType === "requested"
          ? `対応者：${assigneeDisplay(t)}　｜　期限：${formatDueDateOnly(t.due_date)}　｜　ステータス：${statusLabel(t.status)}`
          : `依頼者：<@${t.requester_user_id}>　｜　期限：${formatDueDateOnly(t.due_date)}　｜　ステータス：${statusLabel(t.status)}`;

      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: `*${t.title}*\n${metaLine}` },
        accessory: {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_in_list",
          value: JSON.stringify({ teamId, taskId: t.id }),
        },
      });
      blocks.push({ type: "divider" });
    }
  }

  return {
    type: "modal",
    callback_id: "list_modal",
    private_metadata: JSON.stringify({ teamId, viewType, userId, status }),
    title: { type: "plain_text", text: "一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

// ================================
// List Modal -> Detail (same modal)
// ================================
async function buildListDetailView({ teamId, task, returnState }) {
  const srcLines = (task.description || "").split("\n").slice(0, 10).join("\n") || "（本文なし）";
  const canCancel = task.status !== "done" && task.status !== "cancelled";

  const meta = {
    mode: "list_detail",
    teamId,
    taskId: task.id,
    returnState, // { viewType, userId, status }
  };

  const base = {
    teamId,
    taskId: task.id,
    channelId: task.channel_id || "",
    parentTs: task.message_ts || "",
  };

  // 戻り導線のための表示名
  const backLabel = returnState?.viewType === "requested" ? "依頼一覧へ戻る" : "担当一覧へ戻る";

  return {
    type: "modal",
    callback_id: "list_detail_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks: [
      {
        type: "actions",
        elements: [
          { type: "button", text: { type: "plain_text", text: `← ${backLabel}` }, action_id: "back_to_list", value: JSON.stringify({ teamId }) },
        ],
      },

      { type: "header", text: { type: "plain_text", text: "📘 タスク詳細" } },
      { type: "section", text: { type: "mrkdwn", text: `*${task.title}*` } },
      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` } },
      { type: "section", text: { type: "mrkdwn", text: `*期限*：${formatDueDateOnly(task.due_date)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` } },
      { type: "section", text: { type: "mrkdwn", text: `*ステータス*：${statusLabel(task.status)}` } },

      { type: "divider" },

      {
        type: "section",
        text: { type: "mrkdwn", text: "*ステータス変更*" },
        accessory: statusSelectElement(task.status === "cancelled" ? "open" : task.status),
      },

      { type: "divider" },
      { type: "section", text: { type: "mrkdwn", text: `*元メッセージ（全文）*\n\`\`\`\n${srcLines}\n\`\`\`` } },

      {
        type: "actions",
        elements: [
          { type: "button", text: { type: "plain_text", text: "完了 ✅" }, style: "primary", action_id: "complete_task", value: JSON.stringify(base) },
        ],
      },

      ...(canCancel
        ? [
            {
              type: "actions",
              elements: [
                { type: "button", text: { type: "plain_text", text: "取り下げ（依頼者のみ）" }, style: "danger", action_id: "cancel_task", value: JSON.stringify(base) },
              ],
            },
          ]
        : []),
    ],
  };
}

// ================================
// Home: 4択（担当/依頼 × 未完了/完了）
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

async function publishHome2({ client, teamId, userId, modeKey }) {
  const mode = getHomeMode(modeKey);
  const isDone = mode.tab === "done";
  const listStartStatus = isDone ? "done" : "open";

  // 上段：左=ラベル、右=プルダウン / 次段：一覧ボタン（短く）
  const blocks = [
    { type: "section", text: { type: "mrkdwn", text: `*${mode.label}*` }, accessory: homeModeSelectElement(mode.key) },
    {
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "一覧" },
          action_id: "open_list_modal_from_home",
          value: JSON.stringify({ teamId, viewType: mode.viewType, userId, status: listStartStatus }),
        },
      ],
    },
    { type: "divider" },
  ];

  // タスク取得（担当=assignee、依頼=requester）
  const listFn = async (status, limit) => fetchListTasks({ teamId, viewType: mode.viewType, userId, status, limit });

  if (!isDone) {
    const openTasks = await listFn("open", 10);
    const inProgress = await listFn("in_progress", 10);
    const waiting = await listFn("waiting", 10);

    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟦 未着手*" } });
    blocks.push(
      ...(openTasks.length
        ? openTasks.map((t) => ({
            type: "section",
            text: {
              type: "mrkdwn",
              text:
                mode.viewType === "requested"
                  ? `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 対応者：${assigneeDisplay(t)}`
                  : `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 依頼者：<@${t.requester_user_id}>`,
            },
            accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
          }))
        : [{ type: "context", elements: [{ type: "mrkdwn", text: "（未着手なし）" }] }])
    );

    blocks.push({ type: "divider" });
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟨 対応中*" } });
    blocks.push(
      ...(inProgress.length
        ? inProgress.map((t) => ({
            type: "section",
            text: {
              type: "mrkdwn",
              text:
                mode.viewType === "requested"
                  ? `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 対応者：${assigneeDisplay(t)}`
                  : `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 依頼者：<@${t.requester_user_id}>`,
            },
            accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
          }))
        : [{ type: "context", elements: [{ type: "mrkdwn", text: "（対応中なし）" }] }])
    );

    blocks.push({ type: "divider" });
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*🟧 確認待ち*" } });
    blocks.push(
      ...(waiting.length
        ? waiting.map((t) => ({
            type: "section",
            text: {
              type: "mrkdwn",
              text:
                mode.viewType === "requested"
                  ? `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 対応者：${assigneeDisplay(t)}`
                  : `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 依頼者：<@${t.requester_user_id}>`,
            },
            accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
          }))
        : [{ type: "context", elements: [{ type: "mrkdwn", text: "（確認待ちなし）" }] }])
    );
  } else {
    const doneTasks = await listFn("done", 30);

    blocks.push({ type: "section", text: { type: "mrkdwn", text: "*✅ 完了済み*" } });
    blocks.push(
      ...(doneTasks.length
        ? doneTasks.map((t) => ({
            type: "section",
            text: {
              type: "mrkdwn",
              text:
                mode.viewType === "requested"
                  ? `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 対応者：${assigneeDisplay(t)}`
                  : `*${t.title}*\n期限：${formatDueDateOnly(t.due_date)} / 依頼者：<@${t.requester_user_id}>`,
            },
            accessory: { type: "button", text: { type: "plain_text", text: "詳細" }, action_id: "open_detail_modal", value: JSON.stringify({ teamId, taskId: t.id }) },
          }))
        : [{ type: "context", elements: [{ type: "mrkdwn", text: "（完了済みなし）" }] }])
    );
  }

  await client.views.publish({
    user_id: userId,
    view: { type: "home", blocks },
  });
}

// ================================
// Home Tab Event
// ================================
app.event("app_home_opened", async ({ event, client, body }) => {
  try {
    const teamId = body.team_id || body.team?.id || event.team;
    const userId = event.user;

    // 初期：担当未完了
    await publishHome2({ client, teamId, userId, modeKey: "assigned_active" });
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
    const messageText = shortcut.message?.text || "";
    const requesterUserId = shortcut.message?.user || "";
    const actorUserId = shortcut.user?.id;

    const titleCandidate = generateTitleCandidate(messageText);

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
          messageText,
          actorUserId,
        }),
        title: { type: "plain_text", text: "タスク作成" },
        submit: { type: "plain_text", text: "決定" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks: [
          { type: "input", block_id: "title", label: { type: "plain_text", text: "タイトル（自動候補）" }, element: { type: "plain_text_input", action_id: "title_input", initial_value: titleCandidate } },
          { type: "input", block_id: "desc", label: { type: "plain_text", text: "詳細（元メッセージ全文）" }, element: { type: "plain_text_input", action_id: "desc_input", multiline: true, initial_value: messageText || "" } },
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
    const description = view.state.values.desc?.desc_input?.value?.trim() || meta.messageText || "";

    const assigneeUserId = view.state.values.assignee_user?.assignee_user_select?.selected_user;
    if (!assigneeUserId) return;

    const due = view.state.values.due?.due_date?.selected_date || null;
    const status = view.state.values.status?.status_select?.selected_option?.value || "open";
    const requesterUserId = meta.requesterUserId || actorUserId;

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
      assignee_type: "user",
      assignee_id: assigneeUserId,
      assignee_label: null,
      status,
      due_date: due,
    });

    // ✅ 作成後：詳細モーダルは出さない。エフェメラル導線だけ。
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
          {
            type: "actions",
            elements: [{ type: "button", text: { type: "plain_text", text: "詳細を開く" }, action_id: "open_detail_modal", value: payload }],
          },
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
  } catch (e) {
    console.error("view submit error:", e?.data || e);
  }
});

// ================================
// Actions
// ================================

// Home/スレッド/エフェメラルの詳細（views.open）
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

// Homeの「一覧」ボタン（モーダルを開く：担当/依頼を反映）
app.action("open_list_modal_from_home", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const teamId = p.teamId || body.team?.id || body.team_id;
  const viewType = p.viewType || "assigned";
  const userId = p.userId || body.user.id;
  const status = p.status || "open";

  await client.views.open({
    trigger_id: body.trigger_id,
    view: await buildListModalView({ teamId, viewType, userId, status }),
  });
});

// Home 4択切替
app.action("home_mode_select", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user.id;
    const modeKey = action?.selected_option?.value || "assigned_active";
    await publishHome2({ client, teamId, userId, modeKey });
  } catch (e) {
    console.error("home_mode_select error:", e?.data || e);
  }
});

// 一覧モーダルのフィルタ切替（プルダウン）
app.action("list_filter_select", async ({ ack, body, action, client }) => {
  await ack();

  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = meta.teamId || body.team?.id || body.team_id;
  const viewType = meta.viewType || "assigned";
  const userId = meta.userId || body.user.id;

  const nextStatus = action?.selected_option?.value || "open";

  const nextView = await buildListModalView({
    teamId,
    viewType,
    userId,
    status: nextStatus,
  });

  await client.views.update({
    view_id: body.view.id,
    hash: body.view.hash,
    view: nextView,
  });
});

// 一覧モーダル内の「詳細」：同じ枠で詳細に差し替え
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
    };

    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    const nextView = await buildListDetailView({ teamId, task, returnState });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: nextView,
    });
  } catch (e) {
    console.error("open_detail_in_list error:", e?.data || e);
  }
});

// 詳細（一覧内）→ 一覧へ戻る
app.action("back_to_list", async ({ ack, body, client }) => {
  await ack();

  try {
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const teamId = meta.teamId || body.team?.id || body.team_id;
    const returnState = meta.returnState || { viewType: "assigned", userId: body.user.id, status: "open" };

    const listView = await buildListModalView({
      teamId,
      viewType: returnState.viewType,
      userId: returnState.userId,
      status: returnState.status,
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

// 完了
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

    // 一覧内詳細なら、その場で更新
    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open" };

      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState }),
      });
      return;
    }

    // スレッドカード更新（可能なら）
    if (channelId && parentTs) {
      const doneBlocks = [
        { type: "header", text: { type: "plain_text", text: "✅ 完了しました" } },
        { type: "section", text: { type: "mrkdwn", text: `*${updated.title}*\nタスクを完了にしました✨` } },
      ];
      await upsertThreadCard(client, { teamId, channelId, parentTs, blocks: doneBlocks });
    }

    // 通常の詳細モーダルなら更新
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

// 取り下げ（依頼者のみ）
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
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open" };

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
        { type: "section", text: { type: "mrkdwn", text: `*${cancelled.title}*\n依頼者により取り下げられました。` } },
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

// ステータス変更
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
      const returnState = meta2.returnState || { viewType: "assigned", userId: body.user.id, status: "open" };

      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: await buildListDetailView({ teamId, task: updated, returnState }),
      });
      return;
    }

    // detail_modal（views.openで開いたやつ）を更新
    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view: await buildDetailModalView({ teamId, task: updated }),
    });

    // スレッドカード更新
    if (updated.channel_id && updated.message_ts) {
      const blocks = await buildThreadCardBlocks({ teamId, task: updated });
      await upsertThreadCard(client, { teamId, channelId: updated.channel_id, parentTs: updated.message_ts, blocks });
    }
  } catch (e) {
    console.error("status_select error:", e?.data || e);
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
