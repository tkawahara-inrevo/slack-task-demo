function registerNotificationJobs(deps) {
  const {
    app,
    cron,
    cutAfterSlash,
    canUserReceiveDm = async () => true,
    dbQuery,
    ensureBotInChannel,
    formatDueDateOnly,
    getSubteamIdMap,
    getUserDisplayName,
    getUsergroupMembers,
    groupBy,
    isOverdueChannelNotificationEnabled = async () => true,
    noMention,
    normalizeHandle,
    pickNoticeText,
    shortenAssigneeLabel,
    shortenOneLine,
    todayJstYmd,
  } = deps;

async function notifyUserDM(userId, task, roleLabel) {
  if (!userId) return;
  if (!(await canUserReceiveDm(task?.team_id, userId, "due"))) return;

  const dm = await app.client.conversations.open({ users: userId });
  const channel = dm.channel?.id;
  if (!channel) return;

  // 期限表示：JST基準で「今日」を優先。DB/pgの型差（Date/文字列）にも耐える。
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

async function runMkOverdueNotifyJob() {
  const today = todayJstYmd(); // YYYY-MM-DD (JST)
  const channelId = MK_OVERDUE_NOTIFY_CHANNEL_ID;

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
  SELECT DISTINCT t.*
  FROM tasks t
  LEFT JOIN task_targets tt
    ON tt.team_id = t.team_id
   AND tt.task_id::text = t.id
  WHERE t.team_id = $1
    AND t.status NOT IN ('done', 'cancelled')
    AND t.due_date IS NOT NULL
    AND t.due_date < $2
    AND (
      (
        (t.task_type IS NULL OR t.task_type = 'personal')
        AND t.assignee_id = ANY($3)
      )
      OR
      (
        t.task_type = 'broadcast'
        AND tt.user_id = ANY($3)
        AND NOT EXISTS (
          SELECT 1 FROM task_completions tc
          WHERE tc.team_id = t.team_id
            AND tc.task_id::text = t.id
            AND tc.user_id = tt.user_id
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

          if (t.source_permalink) {
            blocks.push({
              type: "context",
              elements: [
                {
                  type: "mrkdwn",
                  text: `🔗 <${t.source_permalink}|元メッセージへ>`,
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

if (process.env.RUN_MK_OVERDUE_NOTIFY_NOW === "true") {
  runMkOverdueNotifyJob().catch(console.error);
}
}

module.exports = {
  registerNotificationJobs,
};
