require("dotenv").config();
const { App } = require("@slack/bolt");

// ================================
// Slack Bolt App
// ================================
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
});

// ================================
// Demo in-memory store
// ================================
const tasksByAssignee = new Map(); // assigneeUserId => tasks[]
const threadCardByKey = new Map(); // `${channelId}:${parentTs}` => { cardTs }

function seedTasks(userId) {
  if (tasksByAssignee.has(userId)) return;
  tasksByAssignee.set(userId, [
    {
      id: "t1",
      title: "企画書作成",
      requesterLabel: "山田",
      due: "2026-04-30",
      status: "open",
      sourceText: "来週までに企画書まとめてもらえますか？",
      sourcePermalink: "",
      channelId: "",
      parentTs: "",
    },
    {
      id: "t2",
      title: "仕様確認",
      requesterLabel: "佐藤",
      due: "2026-04-25",
      status: "open",
      sourceText: "この仕様でOKか確認お願い！",
      sourcePermalink: "",
      channelId: "",
      parentTs: "",
    },
    {
      id: "t3",
      title: "打合せ資料作成",
      requesterLabel: "鈴木",
      due: "2026-04-20",
      status: "open",
      sourceText: "次回MTG用の資料作成できる？",
      sourcePermalink: "",
      channelId: "",
      parentTs: "",
    },
  ]);
}

function formatDue(due) {
  if (!due) return "未設定";
  return due.replaceAll("-", "/");
}

function getUserTasks(userId) {
  seedTasks(userId);
  return tasksByAssignee.get(userId) || [];
}

function getOpenTasksSorted(userId) {
  return getUserTasks(userId)
    .filter((t) => t.status === "open")
    .sort((a, b) => (a.due || "").localeCompare(b.due || ""));
}

function upsertTask(assigneeUserId, task) {
  const tasks = getUserTasks(assigneeUserId);
  const idx = tasks.findIndex((t) => t.id === task.id);
  if (idx >= 0) tasks[idx] = task;
  else tasks.unshift(task);
  tasksByAssignee.set(assigneeUserId, tasks);
}

function findTask(assigneeUserId, taskId) {
  return getUserTasks(assigneeUserId).find((t) => t.id === taskId);
}

function markDone(assigneeUserId, taskId) {
  const t = findTask(assigneeUserId, taskId);
  if (t) t.status = "done";
}

function safeJsonParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

// ================================
// Title candidate generator (NEW)
//  - 詳細(元メッセージ全文)から短いタイトル候補を作る
// ================================
function generateTitleCandidate(text, maxLen = 22) {
  if (!text) return "（タスク）";

  let s = String(text);

  // normalize
  s = s.replace(/\r\n/g, "\n");
  s = s.replace(/https?:\/\/\S+/g, ""); // URL除去
  s = s.replace(/<@[A-Z0-9]+>/g, ""); // メンション除去
  s = s.replace(/<#[A-Z0-9]+\|[^>]+>/g, ""); // チャンネル参照除去
  s = s.replace(/:[a-z0-9_+-]+:/gi, ""); // :emoji: 風
  s = s.replace(/[【】\[\]（）()]/g, " ");
  s = s.replace(/\s+/g, " ").trim();

  // common prefixes
  s = s.replace(
    /^(すみません|恐縮ですが|お疲れ様です|取り急ぎ|ごめん|失礼|お願い|至急|急ぎ)\s*/g,
    ""
  );

  // first sentence
  const cut = s.split(/[\n。！？!?]/)[0].trim();
  let title = cut || s;

  // soften endings
  title = title
    .replace(/(お願いします|ください|してもらえますか|して下さい|お願いします。?)$/g, "")
    .trim();

  if (!title) title = "（タスク）";
  if (title.length > maxLen) title = title.slice(0, maxLen) + "…";
  return title;
}

async function publishHome(client, userId) {
  await client.views.publish({
    user_id: userId,
    view: { type: "home", blocks: buildHomeBlocks(userId) },
  });
}

// ================================
// Home tab UI
// ================================
function buildHomeBlocks(userId) {
  const openTasks = getOpenTasksSorted(userId);

  const blocks = [
    { type: "header", text: { type: "plain_text", text: "📝 自分のタスク" } },
    {
      type: "context",
      elements: [
        { type: "mrkdwn", text: "メッセージ右クリック → *アプリ* → *タスク化*（デモ）" },
      ],
    },
    { type: "divider" },
  ];

  if (openTasks.length === 0) {
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: "🎉 未完了タスクはありません！" },
    });
    return blocks;
  }

  for (const task of openTasks) {
    const src = task.sourcePermalink
      ? `<${task.sourcePermalink}|元メッセージ>`
      : `_${(task.sourceText || "").slice(0, 60)}_`;

    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text:
          `*${task.title}*\n` +
          `依頼者：${task.requesterLabel || "未設定"}\n` +
          `期限：${formatDue(task.due)}\n` +
          `元メッセージ：${src}`,
      },
    });

    // Homeから「いつでも」詳細モーダルを開ける
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({
            assigneeUserId: userId,
            taskId: task.id,
          }),
        },
        {
          type: "button",
          text: { type: "plain_text", text: "完了 ✅" },
          style: "primary",
          action_id: "complete_task",
          value: JSON.stringify({
            assigneeUserId: userId,
            taskId: task.id,
            // Homeからはスレッドに戻れないので channelId/parentTs は任意
            channelId: task.channelId || "",
            parentTs: task.parentTs || "",
          }),
        },
      ],
    });

    blocks.push({ type: "divider" });
  }

  return blocks;
}

// ================================
// Thread card (right pane) UI
// ================================
function threadKey(channelId, parentTs) {
  return `${channelId}:${parentTs}`;
}

function miniOpenList(assigneeUserId, excludeTaskId) {
  const others = getOpenTasksSorted(assigneeUserId).filter((t) => t.id !== excludeTaskId);
  const top = others.slice(0, 3);
  if (top.length === 0) return "他の未完了タスクはありません✨";
  return top
    .map((t, i) => `${i + 1}. *${t.title}*（期限：${formatDue(t.due)}）`)
    .join("\n");
}

function buildThreadCardBlocks({ assigneeUserId, task, channelId, parentTs }) {
  const payloadBase = {
    assigneeUserId,
    taskId: task.id,
    channelId,
    parentTs,
  };

  const src = task.sourcePermalink
    ? `<${task.sourcePermalink}|元メッセージを開く>`
    : `> ${(task.sourceText || "").slice(0, 140)}`;

  return [
    { type: "header", text: { type: "plain_text", text: "🧭 タスク（右側スレッド）" } },
    { type: "section", text: { type: "mrkdwn", text: `*${task.title}*` } },
    {
      type: "section",
      fields: [
        { type: "mrkdwn", text: `*期限*\n${formatDue(task.due)}` },
        { type: "mrkdwn", text: `*対応者*\n<@${assigneeUserId}>` },
        { type: "mrkdwn", text: `*依頼者*\n${task.requesterLabel || "未設定"}` },
      ],
    },
    { type: "section", text: { type: "mrkdwn", text: `*元メッセージ*\n${src}` } },
    { type: "divider" },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*最近の未完了（上位3件）*\n${miniOpenList(assigneeUserId, task.id)}` },
    },
    {
      type: "actions",
      elements: [
        // ✅ いつでも詳細モーダル
        {
          type: "button",
          text: { type: "plain_text", text: "詳細を開く" },
          action_id: "open_detail_modal",
          value: JSON.stringify(payloadBase),
        },
        // ✅ 右側で完了
        {
          type: "button",
          text: { type: "plain_text", text: "完了 ✅" },
          style: "primary",
          action_id: "complete_task",
          value: JSON.stringify(payloadBase),
        },
        // ✅ 一覧表示（スレッドカードを一覧に差し替え）
        {
          type: "button",
          text: { type: "plain_text", text: "一覧を開く" },
          action_id: "thread_show_list",
          value: JSON.stringify({
            ...payloadBase,
            mode: "list",
          }),
        },
      ],
    },
    {
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "📌 一覧は App Home にも常に表示されます（右は「今の1件」）",
        },
      ],
    },
  ];
}

function keepsListBlocks({ assigneeUserId, channelId, parentTs }) {
  const open = getOpenTasksSorted(assigneeUserId).slice(0, 10);

  const blocks = [
    { type: "header", text: { type: "plain_text", text: "📋 未完了タスク一覧（右側スレッド）" } },
    { type: "context", elements: [{ type: "mrkdwn", text: "フォーカスしたいタスクを選んでね✨" }] },
    { type: "divider" },
  ];

  if (open.length === 0) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "🎉 未完了タスクはありません！" } });
  } else {
    for (const t of open) {
      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${t.title}*\n期限：${formatDue(t.due)} / 依頼者：${t.requesterLabel || "未設定"}`,
        },
        accessory: {
          type: "button",
          text: { type: "plain_text", text: "フォーカス" },
          action_id: "thread_focus_task",
          value: JSON.stringify({
            assigneeUserId,
            taskId: t.id,
            channelId,
            parentTs,
          }),
        },
      });
      blocks.push({ type: "divider" });
    }
  }

  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        text: { type: "plain_text", text: "Home更新" },
        action_id: "refresh_home",
        value: JSON.stringify({ assigneeUserId }),
      },
    ],
  });

  return blocks;
}

async function upsertThreadCard(client, { channelId, parentTs, blocks }) {
  const key = threadKey(channelId, parentTs);
  const existing = threadCardByKey.get(key);

  if (existing?.cardTs) {
    await client.chat.update({
      channel: channelId,
      ts: existing.cardTs,
      text: "タスク表示（更新）",
      blocks,
    });
    return existing.cardTs;
  }

  const res = await client.chat.postMessage({
    channel: channelId,
    thread_ts: parentTs,
    text: "タスク表示",
    blocks,
  });

  const cardTs = res?.ts;
  if (cardTs) threadCardByKey.set(key, { cardTs });
  return cardTs;
}

// ================================
// Detail Modal UI (スクショ風)
// ================================
function buildDetailModalView({ assigneeUserId, task }) {
  const srcLines =
    task.sourceText
      ? task.sourceText.split("\n").slice(0, 6).join("\n")
      : "（本文なし）";

  const recent = getOpenTasksSorted(assigneeUserId)
    .filter((t) => t.id !== task.id)
    .slice(0, 3)
    .map((t, i) => `${i + 1}. *${t.title}*（期限：${formatDue(t.due)}）`)
    .join("\n");

  const recentText = recent || "他の未完了タスクはありません✨";

  return {
    type: "modal",
    callback_id: "detail_modal",
    private_metadata: JSON.stringify({ assigneeUserId, taskId: task.id }),
    title: { type: "plain_text", text: "タスク" },
    close: { type: "plain_text", text: "閉じる" },
    blocks: [
      { type: "header", text: { type: "plain_text", text: "📘 タスク" } },

      { type: "section", text: { type: "mrkdwn", text: `*${task.title}*` } },

      {
        type: "section",
        fields: [
          { type: "mrkdwn", text: `*期限*\n${formatDue(task.due)}` },
          { type: "mrkdwn", text: `*対応者*\n<@${assigneeUserId}>` },
          { type: "mrkdwn", text: `*依頼者*\n${task.requesterLabel || "未設定"}` },
        ],
      },

      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*元メッセージ*\n\`\`\`\n${srcLines}\n\`\`\`` } },

      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "完了 ✅" },
            style: "primary",
            action_id: "complete_task",
            value: JSON.stringify({
              assigneeUserId,
              taskId: task.id,
              channelId: task.channelId || "",
              parentTs: task.parentTs || "",
              from: "modal",
            }),
          },
          {
            type: "button",
            text: { type: "plain_text", text: "一覧（Home）更新" },
            action_id: "refresh_home",
            value: JSON.stringify({ assigneeUserId }),
          },
        ],
      },

      { type: "divider" },

      { type: "section", text: { type: "mrkdwn", text: `*最近の未完了（上位3件）*\n${recentText}` } },

      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: { type: "plain_text", text: "一覧を開く" },
            action_id: "open_list_modal",
            value: JSON.stringify({ assigneeUserId }),
          },
        ],
      },

      {
        type: "context",
        elements: [{ type: "mrkdwn", text: "📌 一覧は App Home にも常に表示されます（右は「今の1件」）" }],
      },
    ],
  };
}

function buildListModalView({ assigneeUserId }) {
  const open = getOpenTasksSorted(assigneeUserId).slice(0, 10);

  const blocks = [
    { type: "header", text: { type: "plain_text", text: "📋 未完了タスク一覧" } },
    { type: "context", elements: [{ type: "mrkdwn", text: "タップで詳細を開けるよ✨" }] },
    { type: "divider" },
  ];

  if (open.length === 0) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "🎉 未完了タスクはありません！" } });
  } else {
    for (const t of open) {
      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${t.title}*\n期限：${formatDue(t.due)} / 依頼者：${t.requesterLabel || "未設定"}`,
        },
        accessory: {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ assigneeUserId, taskId: t.id }),
        },
      });
      blocks.push({ type: "divider" });
    }
  }

  return {
    type: "modal",
    callback_id: "list_modal",
    private_metadata: JSON.stringify({ assigneeUserId }),
    title: { type: "plain_text", text: "一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

async function openDetailModal(client, { trigger_id, assigneeUserId, taskId }) {
  const task = findTask(assigneeUserId, taskId);
  if (!task) return;

  await client.views.open({
    trigger_id,
    view: buildDetailModalView({ assigneeUserId, task }),
  });
}

async function safeEphemeral(client, { channelId, userId, text }) {
  try {
    await client.chat.postEphemeral({ channel: channelId, user: userId, text });
  } catch {
    // ignore
  }
}

// ================================
// Events
// ================================
app.event("app_home_opened", async ({ event, client }) => {
  try {
    await publishHome(client, event.user);
  } catch (e) {
    console.error("app_home_opened error:", e);
  }
});

// ================================
// Shortcut: create_task_from_message
// ================================
app.shortcut("create_task_from_message", async ({ shortcut, ack, client }) => {
  await ack();

  try {
    const messageText = shortcut.message?.text || "";
    const requesterUserId = shortcut.message?.user || "";
    const requesterLabel = requesterUserId ? `<@${requesterUserId}>` : "未設定";
    const channelId = shortcut.channel?.id || "";
    const msgTs = shortcut.message?.ts || "";

    const titleCandidate = generateTitleCandidate(messageText);

    await client.views.open({
      trigger_id: shortcut.trigger_id,
      view: {
        type: "modal",
        callback_id: "task_modal",
        private_metadata: JSON.stringify({ messageText, requesterLabel, channelId, msgTs }),
        title: { type: "plain_text", text: "タスク作成" },
        submit: { type: "plain_text", text: "決定" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks: [
          {
            type: "input",
            block_id: "title",
            label: { type: "plain_text", text: "タイトル（自動候補）" },
            element: {
              type: "plain_text_input",
              action_id: "title_input",
              initial_value: titleCandidate,
            },
          },
          {
            type: "input",
            block_id: "desc",
            label: { type: "plain_text", text: "詳細（元メッセージ全文）" },
            element: {
              type: "plain_text_input",
              action_id: "desc_input",
              multiline: true,
              initial_value: messageText || "",
            },
          },
          {
            type: "input",
            block_id: "assignee",
            label: { type: "plain_text", text: "対応者" },
            element: {
              type: "users_select",
              action_id: "assignee_user",
              placeholder: { type: "plain_text", text: "対応者を選択" },
            },
          },
          {
            type: "input",
            block_id: "due",
            label: { type: "plain_text", text: "期限" },
            element: {
              type: "datepicker",
              action_id: "due_date",
              placeholder: { type: "plain_text", text: "日付を選択" },
            },
          },
        ],
      },
    });
  } catch (e) {
    console.error("shortcut error:", e);
  }
});

// ================================
// Modal submit: create task -> thread card + open detail modal
// ================================
app.view("task_modal", async ({ ack, body, view, client }) => {
  await ack();

  try {
    const meta = safeJsonParse(view.private_metadata || "{}") || {};
    const actorUserId = body.user.id;

    const title =
      view.state.values.title?.title_input?.value?.trim() || "（無題タスク）";

    // NEW: 詳細（全文）を保存
    const description =
      view.state.values.desc?.desc_input?.value?.trim() || meta.messageText || "";

    const assigneeUserId =
      view.state.values.assignee?.assignee_user?.selected_user;
    const due = view.state.values.due?.due_date?.selected_date || "";

    if (!assigneeUserId) return;

    // permalink (nice-to-have)
    let permalink = "";
    if (meta.channelId && meta.msgTs) {
      try {
        const r = await client.chat.getPermalink({
          channel: meta.channelId,
          message_ts: meta.msgTs,
        });
        permalink = r?.permalink || "";
      } catch {}
    }

    const taskId = `t_${Date.now()}`;
    const task = {
      id: taskId,
      title,
      requesterLabel: meta.requesterLabel || `<@${actorUserId}>`,
      due,
      status: "open",
      // NEW: sourceText = 詳細（全文）
      sourceText: description,
      sourcePermalink: permalink,
      channelId: meta.channelId || "",
      parentTs: meta.msgTs || "",
    };

    // store
    upsertTask(assigneeUserId, task);

    // home更新（対応者）
    await publishHome(client, assigneeUserId);

    // ✅ 1) スレッドにタスクカード（右側）
    if (task.channelId && task.parentTs) {
      const blocks = buildThreadCardBlocks({
        assigneeUserId,
        task,
        channelId: task.channelId,
        parentTs: task.parentTs,
      });

      try {
        await upsertThreadCard(client, {
          channelId: task.channelId,
          parentTs: task.parentTs,
          blocks,
        });
      } catch (e) {
        if (e?.data?.error === "not_in_channel") {
          await safeEphemeral(client, {
            channelId: task.channelId,
            userId: actorUserId,
            text:
              "🥺 このチャンネルにボットが参加してないから、右側スレッドに表示できないよ…！\n" +
              "このチャンネルで `/invite @アプリ名` してからもう一回やってみてね✨",
          });
        } else {
          console.error("thread card error:", e?.data || e);
        }
      }
    }

    // ✅ 2) すぐスクショ風の詳細モーダルを出す（真ん中）
    await openDetailModal(client, {
      trigger_id: body.trigger_id,
      assigneeUserId,
      taskId,
    });
  } catch (e) {
    console.error("view submit error:", e);
  }
});

// ================================
// Actions: open detail modal (いつでも)
// ================================
app.action("open_detail_modal", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;
  const taskId = p.taskId;

  if (!taskId) return;

  try {
    await openDetailModal(client, {
      trigger_id: body.trigger_id,
      assigneeUserId,
      taskId,
    });
  } catch (e) {
    console.error("open_detail_modal error:", e?.data || e);
  }
});

// ================================
// Actions: open list modal (from detail modal)
// ================================
app.action("open_list_modal", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;

  try {
    await client.views.open({
      trigger_id: body.trigger_id,
      view: buildListModalView({ assigneeUserId }),
    });
  } catch (e) {
    console.error("open_list_modal error:", e?.data || e);
  }
});

// ================================
// Actions: refresh home
// ================================
app.action("refresh_home", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;

  try {
    await publishHome(client, assigneeUserId);
  } catch (e) {
    console.error("refresh_home error:", e?.data || e);
  }
});

// ================================
// Actions: complete task (from thread/home/modal)
// ================================
app.action("complete_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;
  const taskId = p.taskId;

  if (!taskId) return;

  try {
    markDone(assigneeUserId, taskId);
    await publishHome(client, assigneeUserId);

    // スレッドカードを“完了しました”表示に差し替え（元メッセージの右側に残る）
    const channelId = p.channelId;
    const parentTs = p.parentTs;
    if (channelId && parentTs) {
      const doneBlocks = [
        { type: "header", text: { type: "plain_text", text: "✅ 完了しました" } },
        { type: "section", text: { type: "mrkdwn", text: `タスクID：\`${taskId}\` を完了にしました` } },
        {
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "一覧を開く" },
              action_id: "thread_show_list",
              value: JSON.stringify({ assigneeUserId, channelId, parentTs, mode: "list" }),
            },
            {
              type: "button",
              text: { type: "plain_text", text: "Home更新" },
              action_id: "refresh_home",
              value: JSON.stringify({ assigneeUserId }),
            },
          ],
        },
        { type: "context", elements: [{ type: "mrkdwn", text: "📌 一覧は App Home に反映されています" }] },
      ];

      await upsertThreadCard(client, { channelId, parentTs, blocks: doneBlocks });
    }

    // モーダル上で押された場合：表示を更新
    if (body.view?.id) {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: {
          type: "modal",
          title: { type: "plain_text", text: "タスク" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            { type: "header", text: { type: "plain_text", text: "✅ 完了しました" } },
            { type: "section", text: { type: "mrkdwn", text: `タスクID：\`${taskId}\` を完了にしました` } },
            { type: "divider" },
            {
              type: "actions",
              elements: [
                {
                  type: "button",
                  text: { type: "plain_text", text: "一覧を開く" },
                  action_id: "open_list_modal",
                  value: JSON.stringify({ assigneeUserId }),
                },
                {
                  type: "button",
                  text: { type: "plain_text", text: "Home更新" },
                  action_id: "refresh_home",
                  value: JSON.stringify({ assigneeUserId }),
                },
              ],
            },
          ],
        },
      });
    }
  } catch (e) {
    console.error("complete_task error:", e?.data || e);
  }
});

// ================================
// Actions: thread show list / focus from list
// ================================
app.action("thread_show_list", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;
  const channelId = p.channelId;
  const parentTs = p.parentTs;

  if (!channelId || !parentTs) return;

  try {
    const blocks = keepsListBlocks({ assigneeUserId, channelId, parentTs });
    await upsertThreadCard(client, { channelId, parentTs, blocks });
  } catch (e) {
    console.error("thread_show_list error:", e?.data || e);
  }
});

app.action("thread_focus_task", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action.value || "{}") || {};
  const assigneeUserId = p.assigneeUserId || body.user.id;
  const taskId = p.taskId;
  const channelId = p.channelId;
  const parentTs = p.parentTs;

  if (!taskId || !channelId || !parentTs) return;

  try {
    const task = findTask(assigneeUserId, taskId);
    if (!task) return;

    const blocks = buildThreadCardBlocks({ assigneeUserId, task, channelId, parentTs });
    await upsertThreadCard(client, { channelId, parentTs, blocks });
  } catch (e) {
    console.error("thread_focus_task error:", e?.data || e);
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
