function registerTaskUiFeature(deps) {
  const {
    app,
    __cacheGet,
    __cacheKey,
    __cachePut,
    buildDetailModalView,
    buildAssigneeLabelRaw,
    buildTaskListModalView,
    buildThreadCardBlocks,
    dbCountCompletions,
    dbCountTargets,
    dbCreateTask,
    dbGetTaskById,
    dbInsertTaskTargets,
    dbIsUserTarget,
    dbInsertTaskComment,
    dbListTargetUserIds,
    dbQuery,
    dbReplaceTaskTargets,
    dbUpdateBroadcastCounts,
    dbUpdateTaskContent,
    dbUpdateStatus,
    dbUpdateTaskEditableFields,
    dbUpsertCompletion,
    ensureBotInChannel,
    fetchMessageTextByTs,
    formatDueDateOnly,
    getSubteamIdMap,
    getTeamIdFromBody,
    getUserIdFromBody,
    canUserReceiveDm = async () => true,
    noMention,
    notifyTaskSimpleDM,
    openDetailModal,
    parseActionMeta,
    prettifySlackText,
    prettifyUserMentions,
    publishHomeBurst = publishHomeForUsers,
    publishHomeForUsers,
    randomUUID,
    resolveDeptForUser,
    safeEphemeral,
    safeJsonParse,
    slackDateYmd,
    statusLabel,
    uniqIds,
    upsertThreadCard,
  } = deps;

async function notifyCreateResultOnSource({
  client,
  channelId,
  parentTs,
  actorUserId,
  text,
}) {
  if (!text) return;

  if (channelId && parentTs && !String(channelId).startsWith("D")) {
    try {
      await safeEphemeral(client, channelId, actorUserId, text);
      return;
    } catch (_) {}
  }

  if (actorUserId) {
    try {
      const dm = await app.client.conversations.open({ users: actorUserId });
      const dmChannel = dm.channel?.id;
      if (!dmChannel) return;
      await app.client.chat.postMessage({
        channel: dmChannel,
        text,
      });
    } catch (_) {}
  }
}

async function postDM(userId, text) {
  if (!userId || !text) return;
  try {
    const dm = await app.client.conversations.open({ users: userId });
    const channel = dm.channel?.id;
    if (!channel) return;
    await app.client.chat.postMessage({ channel, text });
  } catch (_) {}
}

async function buildListDetailView({
  teamId,
  task,
  viewerUserId,
  returnState,
}) {
  return buildDetailModalView({
    teamId,
    task,
    viewerUserId,
    origin: "list_modal",
    returnState,
  });
}

async function buildBroadcastInitialOptions(teamId, task) {
  if (!teamId || !task?.id) {
    return { initialUserIds: [], initialGroupOptions: [] };
  }

  const initialUserIds = [];
  const initialGroupOptions = [];

  if (task.broadcast_group_id) {
    const idToHandle = await getSubteamIdMap(teamId);
    const handle = idToHandle.get(task.broadcast_group_id) || task.broadcast_group_id;
    initialGroupOptions.push({
      text: {
        type: "plain_text",
        text: `@${String(handle).replace(/^@/, "")}`,
      },
      value: task.broadcast_group_id,
    });
  } else {
    const targetIds = await dbListTargetUserIds(teamId, task.id);
    initialUserIds.push(...(targetIds || []));
  }

  return { initialUserIds, initialGroupOptions };
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

app.view("task_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};
  const actorUserId = body.user.id;

  const teamId = meta.teamId || body.team?.id || body.team_id;
  const channelId = meta.channelId || "";
  const parentTs = meta.msgTs || "";
  const sourceMode = meta.sourceMode || "message";

  const isDmChannel = String(channelId || "")[0] === "D";
  const isStandalone = sourceMode === "standalone";

  try {
    if (!teamId || !actorUserId) {
      await ack();
      return;
    }

    // 元メッセージ由来だけ参加確認
    if (!isStandalone && !isDmChannel && channelId) {
      const joinRes = await ensureBotInChannel({ client, channelId });

      if (!joinRes.ok) {
        let errText = "このチャンネルに参加できなかったよ…！";
        if (joinRes.isPrivate) {
          errText =
            "このプライベートチャンネルにはボットが未参加だよ。`/invite @Task Demo` のあとでもう一度試してね。";
        }

        await ack({
          response_action: "errors",
          errors: { title: errText },
        });
        return;
      }

      if (joinRes.joined) {
        await safeEphemeral(
          client,
          channelId,
          actorUserId,
          "🤖 このチャンネルに参加したよ！このままタスク作るね◎",
        );
      }
    }

    let description = "";

    if (isStandalone) {
      description = (
        view.state.values.content?.content_text?.value || ""
      ).trim();

      if (!description) {
        await ack({
          response_action: "errors",
          errors: { content: "タスク内容を入力してください" },
        });
        return;
      }
    } else {
      const cacheKey = meta.cacheKey || __cacheKey(teamId, channelId, parentTs);
      description = __cacheGet(cacheKey);

      if (!description) {
        const raw = await fetchMessageTextByTs(client, channelId, parentTs);
        let pretty = await prettifySlackText(raw, teamId);
        pretty = await prettifyUserMentions(pretty, teamId);
        description = String(pretty || raw || "").trim();
        if (description) __cachePut(cacheKey, description);
      }

      if (!description) {
        await ack({
          response_action: "errors",
          errors: {
            title: "元メッセージ取得に失敗したよ（もう一度お試しください）",
          },
        });
        return;
      }
    }

    const inputTitle = (
      view.state.values.title?.title_input?.value || ""
    ).trim();
    const title = inputTitle ? inputTitle : description;

    const selectedUsers =
      view.state.values.assignee_users?.assignee_users_select?.selected_users ||
      [];
    const selectedGroupOptions =
      view.state.values.assignee_groups?.assignee_groups_select?.selected_options ||
      [];
    const selectedGroupIds = selectedGroupOptions
      .map((o) => o?.value)
      .filter(Boolean);

    if (!selectedUsers.length && !selectedGroupIds.length) {
      await ack({
        response_action: "errors",
        errors: {
          assignee_users: "対応者（個人 or グループ）を1つ以上選んでください",
          assignee_groups: "対応者（個人 or グループ）を1つ以上選んでください",
        },
      });
      return;
    }

    const due = view.state.values.due?.due_date?.selected_date || null;
    const requesterUserId =
      view.state.values.requester?.requester_user_select?.selected_user ||
      actorUserId;

    await ack();

    const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(
      teamId,
      selectedGroupIds,
    );

    const targetList = uniqIds([
      ...selectedUsers,
      ...Array.from(groupUsers || []),
    ]);

    const isPersonal = targetList.length === 1 && selectedGroupIds.length === 0;
    const taskType = isPersonal ? "personal" : "broadcast";
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const personalAssigneeId = isPersonal ? targetList[0] : null;
    const assigneeDept = isPersonal
      ? await resolveDeptForUser(teamId, personalAssigneeId)
      : null;
    const assigneeLabelRaw = await buildAssigneeLabelRaw(
      teamId,
      selectedUsers,
      groupHandles,
    );

    let permalink = "";
    if (!isStandalone && channelId && parentTs) {
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
      status: "in_progress",
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

    if (taskType === "broadcast") {
      await dbInsertTaskTargets(teamId, taskId, targetList);
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;
    }

    try {
      if (taskType === "personal") {
        const to = personalAssigneeId;
        if (to && to !== actorUserId) {
          await notifyTaskSimpleDM(to, created, "📝 タスクが届いたよ");
        }
      } else {
        for (const uid of targetList.filter((u) => u && u !== actorUserId)) {
          await notifyTaskSimpleDM(uid, created, "📝 タスクが届いたよ");
        }
      }
    } catch (e) {
      console.error("create notify error:", e?.data || e);
    }

    if (created?.channel_id && created?.message_ts && !created.channel_id.startsWith("D")) {
      try {
        const blocks = await buildThreadCardBlocks({ teamId, task: created });
        await upsertThreadCard(client, {
          teamId,
          channelId: created.channel_id,
          parentTs: created.message_ts,
          blocks,
        });
      } catch (e) {
        console.error("thread card error:", e?.data || e);
      }
    }
try {
  await notifyCreateResultOnSource({
    client,
    channelId,
    parentTs,
    actorUserId,
    text: isStandalone
      ? "📝 タスクを作成したよ！作成者にはこのDM、対応者にはタスク通知を送ったよ。"
      : isDmChannel
        ? "📝 このDMメッセージからタスクを作成したよ！"
        : "📝 タスクを作成したよ！",
  });
} catch (e) {
  console.error("create result notify failed:", e?.data || e);
}

    publishHomeForUsers(client, teamId, [
      actorUserId,
      requesterUserId,
      ...targetList,
    ]);
  } catch (e) {
    console.error("view submit error:", e?.data || e);
try {
  await notifyCreateResultOnSource({
    client,
    channelId,
    parentTs,
    actorUserId,
    text: `⚠️ タスク作成に失敗したよ: ${e?.message || "unknown error"}`,
  });
} catch (notifyErr) {
  console.error("create result notify failed:", notifyErr?.data || notifyErr);
}
  }
});

// async function runOverdueThreadReminderJob() {
//   const today = todayJstYmd();

//   const q = `
//     SELECT *
//     FROM tasks
//     WHERE due_date < $1
//       AND status NOT IN ('done','cancelled')
//       AND channel_id IS NOT NULL
//       AND message_ts IS NOT NULL
//     ORDER BY due_date ASC, created_at ASC
//     LIMIT 500;
//   `;
//   const tasks = (await dbQuery(q, [today])).rows || [];

//   for (const t of tasks) {
//     try {
//       let notifyUserIds = [];

//       if (t.task_type === "broadcast") {
//         const targets = await dbListTargetUserIds(t.team_id, t.id);
//         const completionsRes = await dbQuery(
//           `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2`,
//           [t.team_id, t.id],
//         );
//         const doneSet = new Set(
//           (completionsRes.rows || []).map((r) => r.user_id).filter(Boolean),
//         );
//         notifyUserIds = targets.filter((u) => u && !doneSet.has(u));
//       } else {
//         notifyUserIds = t.assignee_id ? [t.assignee_id] : [];
//       }

//       notifyUserIds = uniqIds(notifyUserIds);
//       if (!notifyUserIds.length) continue;

//       const titleText = noMention(t.title || "（無題）");
//       const dueText = formatDueDateOnly(t.due_date);

//       // ✅ DM起点タスクはスレッド返信せず、対応者へアプリDM
//       if (String(t.channel_id || "").startsWith("D")) {
//         for (const uid of notifyUserIds) {
//           try {
//             await notifyTaskSimpleDM(uid, t, `⏰ 期限切れだよ（期限: ${dueText}）`);
//           } catch (dmErr) {
//             console.error(
//               "runOverdueThreadReminderJob dm fallback error:",
//               dmErr?.data || dmErr,
//             );
//           }
//         }
//         continue;
//       }

//       const mentionText = notifyUserIds.map((u) => `<@${u}>`).join(" ");

//       await postMessageInThread(
//         app.client,
//         t.channel_id,
//         t.message_ts,
//         `⏰ 期限切れリマインド\n${mentionText}\n*${titleText}*\n期限: ${dueText}`,
//       );
//     } catch (e) {
//       console.error("runOverdueThreadReminderJob error:", e?.data || e);
//     }
//   }

//   console.log(
//     `[overdue_thread_reminder] done. today=${today} count=${tasks.length}`,
//   );
// }

//if (process.env.ENABLE_OVERDUE_REMINDER === "true") {
//  cron.schedule(
//    "0 10 * * *",
//    () => {
//      runOverdueThreadReminderJob().catch((e) =>
//        console.error("runOverdueThreadReminderJob error:", e?.data || e),
//      );
//    },
//    { timezone: "Asia/Tokyo" },
//  );
//}

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
      const task = await dbGetTaskById(teamId, taskId);
      if (!task) return;
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || {
        viewType: "assigned",
        userId: body.user.id,
        status: "in_progress",
        deptKey: "all",
      };
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

async function handleCompleteTask({ client, body, teamId, taskId }) {
  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    if (task.task_type === "broadcast") {
      const userId = getUserIdFromBody(body);

      const isTarget = await dbIsUserTarget(teamId, taskId, userId);
      if (!isTarget) return;

      await dbUpsertCompletion(teamId, taskId, userId);

      const total = task.total_count || (await dbCountTargets(teamId, taskId));
      const doneCount = await dbCountCompletions(teamId, taskId);

      if (doneCount >= total && total > 0) {
        const fresh = await dbGetTaskById(teamId, taskId);
        if (fresh && !["done", "cancelled"].includes(fresh.status)) {
          await dbUpdateStatus(teamId, taskId, "done");
        }

        if (fresh && !fresh.notified_at) {
          await dbQuery(
            `UPDATE tasks SET notified_at=now() WHERE team_id=$1 AND id=$2 AND notified_at IS NULL`,
            [teamId, taskId],
          );

          await notifyTaskSimpleDM(
            fresh.requester_user_id,
            { ...fresh, status: "done" },
            "🎉 全員が完了したよ",
          );

          try {
            const targets = await dbListTargetUserIds(teamId, taskId);
            const toRefresh = Array.from(
              new Set(
                [fresh.requester_user_id, ...(targets || [])].filter(Boolean),
              ),
            );
            publishHomeBurst(client, teamId, toRefresh, 200);
          } catch (_) {}
        }
      }

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
      // ✅ Home view には views.update しない（固まり/反映中対策）
      // ✅ detail_modal のときだけ更新する
      if (body?.view?.id && body.view.callback_id === "detail_modal") {
        const refreshed = await dbGetTaskById(teamId, taskId);
        if (refreshed) {
          await client.views.update({
            view_id: body.view.id,
            hash: body.view.hash,
            view: await buildDetailModalView({
              teamId,
              task: refreshed,
              viewerUserId: userId,
              origin: "home",
            }),
          });
        }
      }

      publishHomeForUsers(client, teamId, [userId, task.requester_user_id]);
      return;
    }

    // personal
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

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

    if (updated.channel_id && updated.message_ts) {
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

    try {
      const relatedIds = Array.from(
        new Set(
          [body.user.id, task.requester_user_id, task.assignee_id].filter(
            Boolean,
          ),
        ),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(
        () => publishHomeForUsers(client, teamId, relatedIds, 200),
        200,
      );
    } catch (_) {}
  } catch (e) {
    console.error("complete_task error:", e?.data || e);
  }
}

app.action("complete_task", async ({ ack, body, action, client }) => {
  await ack();

  const { teamId, taskId } = parseActionMeta(body, action);
  if (!teamId || !taskId) return;

  // 既存の「反映中…」表示があるならここは残したままでOK

  await handleCompleteTask({ client, body, teamId, taskId });
});

// broadcast: requester confirms after all targets completed (waiting -> done)
app.action("confirm_broadcast_done", async ({ ack, body, action, client }) => {
  await ack();

  const { teamId, taskId } = parseActionMeta(body, action);
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

    // ★通知：完了（broadcast）は「依頼者だけ」
    try {
      if (updated.requester_user_id) {
        await notifyTaskSimpleDM(
          updated.requester_user_id,
          updated,
          "✅ 完了になったよ",
        );
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
      publishHomeBurst(client, teamId, toRefresh, 200);
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
      publishHomeBurst(client, teamId, [body.user.id], 200);
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

    // ✅ broadcast は手動変更しない（権限/導線的にも出さない前提なので黙ってreturn）
    if (task.task_type === "broadcast") return;

    // ✅ personal：依頼者 or 対応者のみ（権限なしは黙ってreturn）
    const actor = body.user.id;
    if (actor !== task.requester_user_id && actor !== task.assignee_id) return;

    const updated = await dbUpdateStatus(teamId, taskId, nextStatus);
    if (!updated) return;

    if (body.view?.callback_id === "list_detail_modal") {
      const meta2 = safeJsonParse(body.view?.private_metadata || "{}") || {};
      const returnState = meta2.returnState || {
        viewType: "assigned",
        userId: body.user.id,
        status: "in_progress",
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
    // ★通知：完了（personalのみ）
    try {
      if (nextStatus === "done") {
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
// Edit Task modal
// ================================
app.action("open_edit_task_modal", async ({ ack, body, action, client }) => {
  await ack();

  const meta = safeJsonParse(action.value || "{}") || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const taskId = meta.taskId;
  const viewerUserId = getUserIdFromBody(body);

  if (!teamId || !taskId || !viewerUserId) return;

  const loadingView = {
    type: "modal",
    callback_id: "edit_task_modal_loading",
    title: { type: "plain_text", text: "タスク編集" },
    close: { type: "plain_text", text: "キャンセル" },
    blocks: [
      { type: "section", text: { type: "mrkdwn", text: "読み込み中…⏳" } },
    ],
  };

  let openedViewId = null;

  try {
    const pushed = await client.views.push({
      trigger_id: body.trigger_id,
      view: loadingView,
    });
    openedViewId = pushed?.view?.id || null;
  } catch (e) {
    console.error("open_edit_task_modal push error:", e?.data || e);
    return;
  }

  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task || !openedViewId) return;

    const isBroadcast = task.task_type === "broadcast";
    const canEditTask = isBroadcast
      ? true
      : viewerUserId === task.requester_user_id ||
        viewerUserId === task.assignee_id;

    if (!canEditTask) return;

    const initDue = slackDateYmd(task.due_date);
    const blocks = [];

    if (isBroadcast) {
      const { initialUserIds, initialGroupOptions } =
        await buildBroadcastInitialOptions(teamId, task);

      blocks.push({
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
      });

      blocks.push({
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
      });
    } else {
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

    const editView = {
      type: "modal",
      callback_id: "edit_task_modal",
      private_metadata: JSON.stringify({ teamId, taskId }),
      title: { type: "plain_text", text: "タスク編集" },
      submit: { type: "plain_text", text: "保存" },
      close: { type: "plain_text", text: "キャンセル" },
      blocks,
    };

    await client.views.update({
      view_id: openedViewId,
      view: editView,
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
    const canEditTask = isBroadcast
      ? true
      : actorUserId === before.requester_user_id ||
        actorUserId === before.assignee_id;

    if (!canEditTask) return;

    let updated = null;
    let usersToRefresh = [before.requester_user_id, actorUserId];
    let usersToNotify = [];

       if (isBroadcast) {
      const beforeTargets = uniqIds(await dbListTargetUserIds(teamId, taskId));

      const selection = await getBroadcastEditSelectionFromView(
        teamId,
        before,
        view.state.values,
      );

      const {
        changed,
        selectedUsers,
        selectedGroupIds,
        groupHandles,
        nextTargets,
      } = selection;

      if (!nextTargets.length) {
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
                  text: {
                    type: "mrkdwn",
                    text: "対応者（個人 or グループ）を1つ以上選んでください。",
                  },
                },
              ],
            },
          });
        } catch (_) {}
        return;
      }

      if (changed) {
        await dbReplaceTaskTargets(teamId, taskId, nextTargets);
      }

      const completedCount = changed
        ? await dbCountCompletions(teamId, taskId)
        : before.completed_count ?? 0;

      const assigneeLabelRaw = await buildAssigneeLabelRaw(
        teamId,
        selectedUsers,
        groupHandles,
      );

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        assignee_id: null,
        assignee_label: assigneeLabelRaw || null,
        assignee_dept: null,
        due_date: nextDue,
        description: nextContent,
        broadcast_group_handle: groupHandles.length
          ? `@${groupHandles[0]}`
          : null,
        broadcast_group_id: selectedGroupIds.length
          ? selectedGroupIds[0]
          : null,
        total_count: nextTargets.length,
        completed_count: completedCount,
      });

      usersToNotify = changed
        ? nextTargets.filter((u) => !beforeTargets.includes(u))
        : [];

      usersToRefresh = uniqIds([
        ...usersToRefresh,
        ...beforeTargets,
        ...nextTargets,
      ]);
    } else {
      const nextAssignee =
        view.state.values.assignee?.assignee_user?.selected_user || null;
      if (!nextAssignee) return;

      let patchAssigneeDept = null;
      try {
        patchAssigneeDept = await resolveDeptForUser(teamId, nextAssignee);
      } catch (_) {}

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        assignee_id: nextAssignee,
        assignee_label: null,
        assignee_dept: patchAssigneeDept,
        due_date: nextDue,
        description: nextContent,
        broadcast_group_handle: null,
        broadcast_group_id: null,
        total_count: null,
        completed_count: 0,
      });

      if (nextAssignee && nextAssignee !== before.assignee_id) {
        usersToNotify = [nextAssignee];
      }

      usersToRefresh = uniqIds([
        ...usersToRefresh,
        before.assignee_id,
        nextAssignee,
      ]);
    }

    if (!updated) return;

    try {
      for (const uid of usersToNotify.filter((u) => u && u !== actorUserId)) {
        await notifyTaskSimpleDM(uid, updated, "👋 あなたが対応者に追加されたよ");
      }
    } catch (e) {
      console.error("assignee notify error:", e?.data || e);
    }

    if (updated.channel_id && updated.message_ts && !updated.channel_id.startsWith("D")) {
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

    try {
      await publishHomeForUsers(client, teamId, usersToRefresh, 250);
    } catch (_) {}

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
      const prettyComment = await prettifyUserMentions(
        comment || "",
        task.team_id,
      );

      for (const uid of recipients) {
        try {
          if (!(await canUserReceiveDm(task.team_id, uid, "comment"))) {
            continue;
          }

          const dm = await app.client.conversations.open({ users: uid });
          const channel = dm.channel?.id;
          if (!channel) continue;

          const blocks = [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "💬 *タスクにコメントがありました*",
              },
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
            },
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
      : viewerUserId === task.requester_user_id ||
        viewerUserId === task.assignee_id;

    if (!canChangeDue) return;

    const initDue = slackDateYmd(task.due_date);

    // ★親（詳細モーダル）の view_id を保持しておく（後で再描画する）
    const parentViewId = body.view?.id || null;

    const blocks = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${noMention(task.title || "（タスク）")}*`,
        },
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
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "更新しています。" } },
      ],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";

    // ✅ 仕様：broadcast は「だれでも」期限変更OK
    const canChangeDue = isBroadcast
      ? true
      : actorUserId === before.requester_user_id ||
        actorUserId === before.assignee_id;

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
        const cardBlocks = await buildThreadCardBlocks({
          teamId,
          task: updated,
        });
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
      const users = [
        updated.requester_user_id,
        updated.assignee_id,
        actorUserId,
      ].filter(Boolean);
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

    // ✅ 権限なしは黙ってreturn（UIでも出さない前提）
    const ok =
      task.task_type === "broadcast"
        ? task.requester_user_id === actor
        : task.requester_user_id === actor || task.assignee_id === actor;
    if (!ok) return;

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
        new Set(
          [actor, updated.requester_user_id, updated.assignee_id].filter(
            Boolean,
          ),
        ),
      );
      publishHomeForUsers(client, teamId, relatedIds, 200);
      setTimeout(
        () => publishHomeForUsers(client, teamId, relatedIds, 200),
        200,
      );
    } catch (_) {}
  } catch (e) {
    console.error("reopen_task error:", e?.data || e);
  }
});


// ================================
// Start
// ================================

  return {
    expandTargetsFromGroups,
  };
}

module.exports = {
  registerTaskUiFeature,
};
