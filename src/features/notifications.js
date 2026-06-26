function registerNotificationJobs(deps) {
  const {
    app,
    cron,
    cutAfterSlash,
    canUserReceiveDm = async () => true,
    getUserDueSchedule = async () => "morning_only",
    dbGetNotificationThread,
    dbQuery,
    dbUpsertNotificationThread,
    ensureBotInChannel,
    formatDueDateOnly,
    getSubteamIdMap,
    getUserDisplayName,
    getUsergroupMembers,
    groupBy,
    isOverdueChannelNotificationEnabled = async () => true,
    isJpBusinessDayYmd = () => true,
    noMention,
    normalizeHandle,
    pickNoticeText,
    shortenAssigneeLabel,
    shortenOneLine,
    todayJstYmd,
  } = deps;

async function notifyUserDM(userId, task, roleLabel, kind = "due") {
  if (!userId) return;
  if (!(await canUserReceiveDm(task?.team_id, userId, kind))) return;

  const dm = await app.client.conversations.open({ users: userId });
  const channel = dm.channel?.id;
  if (!channel) return;

  const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
  const hasLink = !!task?.source_permalink;

  await app.client.chat.postMessage({
    channel,
    text: `今日が期限です（${roleLabel}）: ${noMention(task.title)}`,
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

// 朝の集約リマインド: 1ユーザーに「期限切れ + 当日期限」を1通のDMで配信（営業日のみ）
function buildTaskListItem(task, label) {
  const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
  const due = formatDueDateOnly(task.due_date);
  const titleLine = `*${noMention(task.title).slice(0, 100)}*  _(期限: ${due} / ${label})_`;
  const elements = [
    {
      type: "button",
      text: { type: "plain_text", text: "詳細を開く" },
      action_id: "open_detail_modal",
      value: payload,
    },
  ];
  if (task.source_permalink) {
    elements.push({
      type: "button",
      text: { type: "plain_text", text: "元メッセージ" },
      url: task.source_permalink,
    });
  }
  return [
    { type: "section", text: { type: "mrkdwn", text: `• ${titleLine}` } },
    { type: "actions", elements },
  ];
}

async function sendMorningReminderDm(userId, overdueItems, todayItems) {
  if (!overdueItems.length && !todayItems.length) return;
  if (!userId) return;
  // 最初のタスクの team_id を使って DM 可否判定（簡略）
  const sampleTask = (overdueItems[0] || todayItems[0])?.task;
  if (sampleTask && !(await canUserReceiveDm(sampleTask.team_id, userId, "due"))) return;

  const dm = await app.client.conversations.open({ users: userId });
  const channel = dm.channel?.id;
  if (!channel) return;

  const blocks = [];
  blocks.push({
    type: "header",
    text: { type: "plain_text", text: "⏰ 本日のタスクリマインド" },
  });
  blocks.push({
    type: "context",
    elements: [{
      type: "mrkdwn",
      text: `期限切れ *${overdueItems.length}* 件　/　今日が期限 *${todayItems.length}* 件`,
    }],
  });

  if (overdueItems.length) {
    blocks.push({ type: "divider" });
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: `🚨 *期限切れタスク (${overdueItems.length}件)*` },
    });
    for (const { task, role } of overdueItems) {
      const label = role === "requester" ? "依頼者" : "対応者";
      blocks.push(...buildTaskListItem(task, label));
    }
  }

  if (todayItems.length) {
    blocks.push({ type: "divider" });
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: `🗓️ *今日が期限のタスク (${todayItems.length}件)*` },
    });
    for (const { task, role } of todayItems) {
      const label = role === "requester" ? "依頼者" : "対応者";
      blocks.push(...buildTaskListItem(task, label));
    }
  }

  // Slack chat.postMessage の blocks 上限は 50。超えたら末尾は省略表示に
  const SLACK_BLOCK_LIMIT = 50;
  if (blocks.length > SLACK_BLOCK_LIMIT) {
    blocks.length = SLACK_BLOCK_LIMIT - 1;
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: "_…件数が多いため一部省略。ホームタブで全件確認してください_" }],
    });
  }

  await app.client.chat.postMessage({
    channel,
    text: `タスクリマインド: 期限切れ ${overdueItems.length}件 / 今日 ${todayItems.length}件`,
    blocks,
  });
}

async function runDueNotifyJob() {
  const today = todayJstYmd();
  // 営業日のみ配信
  if (!isJpBusinessDayYmd(today)) {
    console.log(`[notify] skipped (not business day): ${today}`);
    return;
  }

  // 期限切れ + 当日期限のタスクを取得（個人タスクのみ）
  const q = `
    SELECT *
    FROM tasks
    WHERE due_date IS NOT NULL
      AND due_date <= $1
      AND status NOT IN ('done','cancelled')
      AND (task_type IS NULL OR task_type='personal')
    ORDER BY due_date ASC, created_at ASC
    LIMIT 2000;
  `;
  const tasks = (await dbQuery(q, [today])).rows;

  // ユーザー別にバケツへ振り分け（対応者 + 依頼者の両方を別エントリで通知）
  const byUser = new Map(); // user_id → { overdue: [], today: [] }
  const pushItem = (userId, role, task, bucket) => {
    if (!userId) return;
    if (!byUser.has(userId)) byUser.set(userId, { overdue: [], today: [] });
    byUser.get(userId)[bucket].push({ task, role });
  };
  // pg ドライバが date 型を Date オブジェクトに変換するので、ISO日付文字列に正規化
  const toYmd = (v) => v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10);
  for (const t of tasks) {
    const due = toYmd(t.due_date);
    const bucket = due === today ? "today" : "overdue";
    pushItem(t.assignee_id, "assignee", t, bucket);
    // 依頼者と対応者が同じ場合は重複させない
    if (t.requester_user_id && t.requester_user_id !== t.assignee_id) {
      pushItem(t.requester_user_id, "requester", t, bucket);
    }
  }

  // ユーザーごとに集約DM配信
  let sent = 0;
  for (const [userId, items] of byUser) {
    try {
      await sendMorningReminderDm(userId, items.overdue, items.today);
      sent++;
    } catch (e) {
      console.error("[notify] aggregated dm error:", e?.data || e);
    }
  }

  console.log(`[notify] done. today=${today} tasks=${tasks.length} users_notified=${sent}`);
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

async function runAfternoonDueNotifyJob() {
  const today = todayJstYmd();

  const q = `
    SELECT *
    FROM tasks
    WHERE due_date = $1
      AND status NOT IN ('done','cancelled')
      AND (task_type IS NULL OR task_type='personal')
    ORDER BY created_at ASC
    LIMIT 500;
  `;
  const tasks = (await dbQuery(q, [today])).rows;

  const BATCH_SIZE = 5;
  for (let i = 0; i < tasks.length; i += BATCH_SIZE) {
    const batch = tasks.slice(i, i + BATCH_SIZE);
    await Promise.allSettled(
      batch.map(async (t) => {
        try {
          const users = [
            { id: t.requester_user_id, role: "依頼者", kind: "due_requester" },
            { id: t.assignee_id, role: "対応者", kind: "due" },
          ];
          for (const u of users) {
            if (!u.id) continue;
            const schedule = await getUserDueSchedule(t.team_id, u.id);
            if (schedule === "morning_and_afternoon") {
              await notifyUserDM(u.id, t, `${u.role}・午後リマインド`, u.kind);
            }
          }
        } catch (e) {
          console.error("afternoon notify error:", e?.data || e);
        }
      }),
    );
  }

  console.log(`[afternoon-notify] done. today=${today} count=${tasks.length}`);
}

cron.schedule(
  "0 16 * * *",
  () => {
    runAfternoonDueNotifyJob().catch((e) =>
      console.error("runAfternoonDueNotifyJob error:", e?.data || e),
    );
  },
  { timezone: "Asia/Tokyo" },
);
if (process.env.RUN_NOTIFY_NOW === "true") {
  runDueNotifyJob().catch(console.error);
}

// ================================
// Daily overdue notify to channel (e.g. @mk)
// - posts to a fixed channel
// - mentions a usergroup handle (default: mk)
// - lists overdue tasks that are strictly "assigned to @mk" (broadcast_group_id = @mk)
//   plus personal tasks assigned to @mk members
// ================================
const MK_OVERDUE_NOTIFY_CHANNEL_ID =
  process.env.MK_OVERDUE_NOTIFY_CHANNEL_ID || "C087A0B6597";

const MK_OVERDUE_NOTIFY_USERGROUP_HANDLE =
  (process.env.MK_OVERDUE_NOTIFY_USERGROUP_HANDLE || "mk")
    .trim()
    .replace(/^@/, "");
const MK_OVERDUE_THREAD_KIND = "mk_overdue_notify";


async function getUsergroupIdByHandle(teamId, handle) {
  const h = normalizeHandle(handle);
  if (!h) return null;

  const idToHandle = await getSubteamIdMap(teamId); // id -> handle
  for (const [id, hh] of idToHandle.entries()) {
    if (normalizeHandle(hh) === h) return id;
  }
  return null;
}

// 通知は「@mk だけ」鳴らしたいので、個人メンションは抑止した表示にする
async function displayTargetsForNotice(teamId, task) {
  if (!task) return "-";

  if (!task.task_type || task.task_type === "personal") {
    const name = await getUserDisplayName(teamId, task.assignee_id);
    return noMention(`@${cutAfterSlash(name)}`);
  }

  // broadcast はラベルに @ が入っているので、通知は抑止表示にする
  const raw = shortenAssigneeLabel(task.assignee_label || "（複数対象）");
  return noMention(raw);
}

// タイトルが空なら本文（description）を使う。ただし「メンションだらけ」を除去して意味が残るようにする

async function ensureOverdueNotifyThreadRoot(teamId, channelId) {
  const existing = await dbGetNotificationThread(
    teamId,
    channelId,
    MK_OVERDUE_THREAD_KIND,
  );

  if (existing?.parent_ts) {
    try {
      const res = await app.client.conversations.history({
        channel: channelId,
        latest: existing.parent_ts,
        oldest: existing.parent_ts,
        inclusive: true,
        limit: 1,
      });
      const msg = res?.messages?.[0];
      if (msg?.ts === existing.parent_ts) return existing.parent_ts;
    } catch (_) {}
  }

  const posted = await app.client.chat.postMessage({
    channel: channelId,
    text: "【タスク期限切れ通知スレッド】",
    blocks: [
      {
        type: "header",
        text: { type: "plain_text", text: "タスク期限切れ通知スレッド" },
      },
      {
        type: "context",
        elements: [
          {
            type: "mrkdwn",
            text: "毎日の期限切れ通知はこのスレッドに投稿されます。",
          },
        ],
      },
    ],
  });

  const parentTs = posted?.ts || null;
  if (parentTs) {
    await dbUpsertNotificationThread(
      teamId,
      channelId,
      MK_OVERDUE_THREAD_KIND,
      parentTs,
    );
  }
  return parentTs;
}

async function runMkOverdueNotifyJob() {
  const today = todayJstYmd(); // YYYY-MM-DD (JST)
  const channelId = MK_OVERDUE_NOTIFY_CHANNEL_ID;

  if (!isJpBusinessDayYmd(today)) {
    console.log(`[mk_overdue_notify] skip: non-business-day. today=${today}`);
    return;
  }

  // tasks テーブルに出てくる team_id を対象に回す（単一WSなら実質1件）
  const teamsRes = await dbQuery(
    `SELECT DISTINCT team_id FROM tasks WHERE team_id IS NOT NULL LIMIT 50;`,
    [],
  );
  const teamIds = (teamsRes.rows || []).map((r) => r.team_id).filter(Boolean);

  for (const teamId of teamIds) {
    try {
      if (!(await isOverdueChannelNotificationEnabled(teamId))) {
        continue;
      }

      const groupId = await getUsergroupIdByHandle(
        teamId,
        MK_OVERDUE_NOTIFY_USERGROUP_HANDLE,
      );
      if (!groupId) {
        console.log(
          `[mk_overdue_notify] skip: usergroup not found. team=${teamId} handle=@${MK_OVERDUE_NOTIFY_USERGROUP_HANDLE}`,
        );
        continue;
      }

      // bot join if needed
      await ensureBotInChannel({ client: app.client, channelId });

      const members = await getUsergroupMembers(teamId, groupId);
      const allowed = Array.from(new Set((members || []).filter(Boolean)));

      if (!allowed.length) {
        console.log(
          `[mk_overdue_notify] skip: empty usergroup. team=${teamId} group=${groupId}`,
        );
        continue;
      }

      // 対象を絞る（重要）：
      // - personal: assignee が mkメンバー
      // - broadcast: 「@mk グループに割り当てられた」ものだけ (= broadcast_group_id = groupId)
      //
      // ※ 全社グループや他グループのbroadcastに mkメンバーが “含まれてしまう” ケースを除外するため
const q = `
  SELECT t.*
  FROM tasks t
  WHERE t.team_id = $1
    AND t.due_date IS NOT NULL
    AND t.due_date < $2
    AND (
      (
        (t.task_type IS NULL OR t.task_type = 'personal')
        AND t.status NOT IN ('done', 'cancelled')
        AND t.assignee_id = ANY($3)
      )
      OR
      (
        t.task_type = 'broadcast'
        AND t.status NOT IN ('done', 'cancelled')
        AND EXISTS (
          SELECT 1
          FROM task_targets tt_mk
          WHERE tt_mk.team_id = t.team_id
            AND tt_mk.task_id::text = t.id
            AND tt_mk.user_id = ANY($3)
        )
        AND EXISTS (
          SELECT 1
          FROM task_targets tt_open
          WHERE tt_open.team_id = t.team_id
            AND tt_open.task_id::text = t.id
            AND tt_open.user_id = ANY($3)
            AND NOT EXISTS (
              SELECT 1
              FROM task_completions tc
              WHERE tc.team_id = t.team_id
                AND tc.task_id::text = t.id
                AND tc.user_id = tt_open.user_id
            )
        )
      )
    )
  ORDER BY t.due_date ASC, t.created_at ASC
  LIMIT 200;
`;

      const allTasks = (await dbQuery(q, [teamId, today, allowed])).rows || [];

      if (!allTasks.length) {
        console.log(
          `[mk_overdue_notify] no overdue. team=${teamId} today=${today}`,
        );
        continue;
      }

      const mention = `<!subteam^${groupId}|@${MK_OVERDUE_NOTIFY_USERGROUP_HANDLE}>`;
      const parentTs = await ensureOverdueNotifyThreadRoot(teamId, channelId);

      // 読みやすさ + ブロック制限対策：最大20件
      const MAX_SHOW = 20;
      const showTasks = allTasks.slice(0, MAX_SHOW);
      const rest = Math.max(0, allTasks.length - showTasks.length);

      // 期限日ごとにまとめて見やすくする
      const byDue = groupBy(showTasks, (t) => formatDueDateOnly(t.due_date));
      const dueKeys = Array.from(byDue.keys()).sort((a, b) => a.localeCompare(b));

      const blocks = [];

      // ヘッダ（Home文言は入れない）
      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text:
            `${mention} *期限切れタスク*\n` +
            `*${allTasks.length}件*あります 🥺⚠️`,
        },
      });

      blocks.push({ type: "divider" });

      for (const due of dueKeys) {
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: `🔴 *期限: ${due}*` },
        });

        const items = byDue.get(due) || [];
        for (const t of items) {
          const targets = await displayTargetsForNotice(teamId, t);
          const title = shortenOneLine(noMention(pickNoticeText(t)), 90);

          const payload = JSON.stringify({
            teamId,
            taskId: t.id,
            origin: "mk_overdue_notify",
          });

          blocks.push({
            type: "section",
            text: {
              type: "mrkdwn",
              text: `• *${title}*  _(${targets})_`,
            },
            accessory: {
              type: "button",
              text: { type: "plain_text", text: "詳細を開く" },
              action_id: "open_detail_modal",
              value: payload,
            },
          });

          if (t.source_permalink && t.message_ts) {
            blocks.push({
              type: "context",
              elements: [
                {
                  type: "mrkdwn",
                  text: `🔗 <${t.source_permalink}|元メッセージへ>`,
                },
              ],
            });
          } else if (!t.message_ts) {
            blocks.push({
              type: "context",
              elements: [
                {
                  type: "mrkdwn",
                  text:
                    "Home \u306e\u30bf\u30b9\u30af\u4f5c\u6210\u304b\u3089\u4f5c\u6210\u3055\u308c\u305f\u30bf\u30b9\u30af\u3067\u3059\u3002",
                },
              ],
            });
          }
        }

        blocks.push({ type: "divider" });
      }

      if (rest) {
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: `…他 *${rest}件*` }],
        });
      }

      await app.client.chat.postMessage({
        channel: channelId,
        ...(parentTs ? { thread_ts: parentTs } : {}),
        text: `${mention} 期限切れタスクが ${allTasks.length}件あります`,
        blocks,
      });

      console.log(
        `[mk_overdue_notify] posted. team=${teamId} today=${today} count=${allTasks.length}`,
      );
    } catch (e) {
      console.error("runMkOverdueNotifyJob error:", e?.data || e);
    }
  }
}

cron.schedule(
  "0 11 * * *",
  () => {
    runMkOverdueNotifyJob().catch((e) =>
      console.error("runMkOverdueNotifyJob error:", e?.data || e),
    );
  },
  { timezone: "Asia/Tokyo" },
);

}

module.exports = {
  registerNotificationJobs,
};
