function registerTaskUiFeature(deps) {
  const {
    app,
    __cacheGet,
    __cacheKey,
    __cachePut,
    buildDetailModalView,
    buildAssigneeLabelRaw,
    buildTaskListModalView,
    buildThreadCardBlocks: _buildThreadCardBlocks,
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
    cutAfterSlash,
    getSubteamIdMap,
    getTeamIdFromBody,
    getUserDisplayName,
    getUserIdFromBody,
    getUsergroupMembers,
    canUserReceiveDm = async () => true,
    noMention,
    notifyTaskSimpleDM,
    normalizeHandle,
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
    upsertThreadCard: _upsertThreadCard,
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

async function getBroadcastEditSelectionFromView(teamId, task, values) {
  const selectedUsers =
    values?.assignee_users?.assignee_users_select?.selected_users || [];
  const selectedGroupOptions =
    values?.assignee_groups?.assignee_groups_select?.selected_options || [];
  const selectedGroupIds = selectedGroupOptions
    .map((option) => option?.value)
    .filter(Boolean);

  const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(
    teamId,
    selectedGroupIds,
  );

  const nextTargets = uniqIds([
    ...selectedUsers,
    ...Array.from(groupUsers || []),
  ]);

  const prevGroupIds = task?.broadcast_group_id ? [task.broadcast_group_id] : [];
  const prevTargets = uniqIds(await dbListTargetUserIds(teamId, task.id));
  const changed =
    JSON.stringify([...selectedUsers].sort()) !==
      JSON.stringify([...(task?.assignee_id ? [task.assignee_id] : [])].sort()) ||
    JSON.stringify([...selectedGroupIds].sort()) !==
      JSON.stringify([...prevGroupIds].sort()) ||
    JSON.stringify([...nextTargets].sort()) !==
      JSON.stringify([...prevTargets].sort());

  return {
    changed,
    selectedUsers,
    selectedGroupIds,
    groupHandles,
    nextTargets,
  };
}

async function buildCommentMentionCatalog(teamId, task) {
  const userAliasToId = new Map();
  const groupAliasToId = new Map();
  const hintHandles = [];
  const userOptions = [];
  const groupOptions = [];
  const userOptionIds = new Set();
  const groupOptionIds = new Set();

  const addHint = (handle) => {
    const normalized = normalizeHandle(handle);
    if (!normalized) return;
    const at = `@${normalized}`;
    if (!hintHandles.includes(at)) hintHandles.push(at);
  };

  const addUserAlias = (alias, userId) => {
    const normalized = normalizeHandle(alias);
    if (!normalized || !userId) return;
    if (!userAliasToId.has(normalized)) userAliasToId.set(normalized, userId);
    addHint(normalized);
  };

  const addGroupAlias = (alias, groupId) => {
    const normalized = normalizeHandle(alias);
    if (!normalized || !groupId) return;
    if (!groupAliasToId.has(normalized)) groupAliasToId.set(normalized, groupId);
    addHint(normalized);
  };

  const addUserOption = (userId, label) => {
    if (!userId || userOptionIds.has(userId)) return;
    userOptionIds.add(userId);
    userOptions.push({
      text: {
        type: "plain_text",
        text: String(label || userId).slice(0, 75),
      },
      value: userId,
    });
  };

  const addGroupOption = (groupId, handle) => {
    if (!groupId || groupOptionIds.has(groupId)) return;
    groupOptionIds.add(groupId);
    groupOptions.push({
      text: {
        type: "plain_text",
        text: `@${String(handle || groupId).replace(/^@/, "")}`.slice(0, 75),
      },
      value: groupId,
    });
  };

  const userIds = new Set();
  if (task?.requester_user_id) userIds.add(task.requester_user_id);
  if (task?.assignee_id) userIds.add(task.assignee_id);

  if (task?.task_type === "broadcast" && task?.id) {
    for (const uid of await dbListTargetUserIds(teamId, task.id)) {
      if (uid) userIds.add(uid);
    }
  }

  // ユーザー名取得とグループ取得を並列実行
  const [, idToHandle] = await Promise.all([
    Promise.all(Array.from(userIds).map(async (userId) => {
      addUserAlias(userId, userId);
      try {
        const displayName = await getUserDisplayName(teamId, userId);
        const shortName = cutAfterSlash(displayName);
        addUserAlias(displayName, userId);
        addUserAlias(shortName, userId);
        addUserOption(userId, shortName || displayName || userId);
      } catch (_) {}
    })),
    getSubteamIdMap(teamId),
  ]);
  for (const [groupId, handle] of idToHandle.entries()) {
    addGroupAlias(handle, groupId);
    addGroupOption(groupId, handle);
  }

  return {
    userAliasToId,
    groupAliasToId,
    userOptions,
    groupOptions,
    hintText: "",
  };
}

function replaceCommentPseudoMentions(text, catalog) {
  const source = String(text || "");
  if (!source) {
    return { text: "", mentionedUserIds: [], mentionedGroupIds: [] };
  }

  const mentionedUserIds = new Set();
  const mentionedGroupIds = new Set();

  const resolvedText = source.replace(
    /(^|[\s(])@([A-Za-z0-9._\-\/]+)/g,
    (match, prefix, handle) => {
      const normalized = normalizeHandle(handle);
      if (!normalized) return match;

      const groupId = catalog?.groupAliasToId?.get(normalized);
      if (groupId) {
        mentionedGroupIds.add(groupId);
        return prefix + "<!subteam^" + groupId + "|@" + normalized + ">";
      }

      const userId = catalog?.userAliasToId?.get(normalized);
      if (userId) {
        mentionedUserIds.add(userId);
        return prefix + "<@" + userId + ">";
      }

      return match;
    },
  );

  return {
    text: resolvedText,
    mentionedUserIds: Array.from(mentionedUserIds),
    mentionedGroupIds: Array.from(mentionedGroupIds),
  };
}

async function listIncompleteTaskTargetUserIds(teamId, taskId) {
  if (!teamId || !taskId) return [];

  const [targetsRes, completionsRes] = await Promise.all([
    dbQuery(
      `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2`,
      [teamId, taskId],
    ),
    dbQuery(
      `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2`,
      [teamId, taskId],
    ),
  ]);

  const doneSet = new Set(
    (completionsRes.rows || []).map((row) => row.user_id).filter(Boolean),
  );

  return (targetsRes.rows || [])
    .map((row) => row.user_id)
    .filter((userId) => userId && !doneSet.has(userId));
}

app.shortcut("open_my_tasks", async ({ shortcut, ack, client, body }) => {
  await ack();
  try {
    const teamId =
      shortcut?.team?.id || body?.team_id || body?.team?.id || null;
    const userId = shortcut?.user?.id || body?.user?.id || null;
    if (!teamId || !userId) return;

    // 蛻晄悄蛟､・嗾o_me / active
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
  const cacheKey = meta.cacheKey || __cacheKey(teamId, channelId, parentTs);
  const cachedDescription = __cacheGet(cacheKey);

  try {
    if (!teamId || !actorUserId) {
      await ack();
      return;
    }

    console.info("task_modal submit start", {
      teamId,
      actorUserId,
      sourceMode,
      channelId,
      parentTs,
    });

    let isDmLikeSource = isDmChannel;
    if (!isStandalone && channelId && !isDmLikeSource) {
      try {
        const info = await client.conversations.info({ channel: channelId });
        const ch = info?.channel || {};
        isDmLikeSource = !!ch.is_im || !!ch.is_mpim;
      } catch (_) {}
    }

    // ショートカット時点で本文を受け取れているなら、そのままタスク化を進める。
    // 参加確認は「元メッセージを追加で取りに行く必要がある通常チャンネル」のときだけ行う。
    if (!isStandalone && !isDmLikeSource && channelId && !cachedDescription) {
      const joinRes = await ensureBotInChannel({ client, channelId });

      if (!joinRes.ok) {
        let errText = "このチャンネルに参加できませんでした。";
        if (joinRes.isPrivate) {
          errText =
            "このプライベートチャンネルには bot が未参加です。`/invite @Task Demo` をお試しください。";
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
          "このチャンネルに参加しました。このままタスクを作成できます。",
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
      description = cachedDescription;

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
            title: "メッセージ取得に失敗しました。もう一度お試しください。",
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
          assignee_users: "担当者またはグループを1つ以上選択してください",
          assignee_groups: "担当者またはグループを1つ以上選択してください",
        },
      });
      return;
    }

    const due = view.state.values.due?.due_date?.selected_date || null;
    const requesterUserId =
      view.state.values.requester?.requester_user_select?.selected_user ||
      actorUserId;

    await ack({
      response_action: "update",
      view: {
        type: "modal",
        callback_id: "task_modal_saving",
        title: { type: "plain_text", text: "タスク作成" },
        close: { type: "plain_text", text: "閉じる" },
        blocks: [
          {
            type: "section",
            text: { type: "mrkdwn", text: "タスクを作成しています..." },
          },
        ],
      },
    });

    const { users: groupUsers, groupHandles } = await expandTargetsFromGroups(
      teamId,
      selectedGroupIds,
    );

    const targetList = uniqIds([
      ...selectedUsers,
      ...Array.from(groupUsers || []),
    ]);

    console.info("task_modal resolved targets", {
      teamId,
      actorUserId,
      selectedUsersCount: selectedUsers.length,
      selectedGroupIdsCount: selectedGroupIds.length,
      targetCount: targetList.length,
      requesterUserId,
      due,
    });

    const isPersonal = targetList.length === 1 && selectedGroupIds.length === 0;
    const taskType = isPersonal ? "personal" : "broadcast";
    const personalAssigneeId = isPersonal ? targetList[0] : null;
    const [requesterDept, assigneeDept, assigneeLabelRaw] = await Promise.all([
      resolveDeptForUser(teamId, requesterUserId),
      isPersonal ? resolveDeptForUser(teamId, personalAssigneeId) : null,
      buildAssigneeLabelRaw(teamId, selectedUsers, groupHandles),
    ]);

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
          await notifyTaskSimpleDM(to, created, "タスクが割り当てられました");
        }
      } else {
        for (const uid of targetList.filter((u) => u && u !== actorUserId)) {
          await notifyTaskSimpleDM(uid, created, "タスクが割り当てられました");
        }
      }
    } catch (e) {
      console.error("create notify error:", e?.data || e);
    }

    // 依頼者に「タスク発行しました」DM
    try {
      if (actorUserId) {
        const dm = await client.conversations.open({ users: actorUserId });
        const ch = dm.channel?.id;
        if (ch) {
          const payload = JSON.stringify({ teamId, taskId: created.id });
          await client.chat.postMessage({
            channel: ch,
            text: `✅ タスクを発行しました: ${noMention(created.title)}`,
            blocks: [
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text: `✅ *タスクを発行しました*\n*${noMention(created.title)}*`,
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
                ],
              },
            ],
          });
        }
      }
    } catch (e) {
      console.error("requester DM error:", e?.data || e);
    }
try {
  await notifyCreateResultOnSource({
    client,
    channelId,
    parentTs,
    actorUserId,
    text: isStandalone
      ? "タスクを作成しました。結果は DM に送ります。"
      : isDmLikeSource
        ? "DM からタスクを作成しました。"
        : "タスクを作成しました。",
  });
} catch (e) {
  console.error("create result notify failed:", e?.data || e);
}

    try {
      await publishHomeBurst(client, teamId, [
        actorUserId,
        requesterUserId,
        ...targetList,
      ]);
    } catch (e) {
      console.error("publish home after create failed:", e?.data || e);
    }

    console.info("task_modal submit success", {
      teamId,
      actorUserId,
      taskId,
      taskType,
      targetCount: targetList.length,
    });

    try {
      await client.views.update({
        view_id: view.id,
        view: {
          type: "modal",
          callback_id: "task_modal_done",
          title: { type: "plain_text", text: "タスク作成" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "タスクを作成しました。必要なら Home や詳細画面で続けて確認できます。",
              },
            },
          ],
        },
      });
    } catch (e) {
      console.error("task_modal success view update failed:", e?.data || e);
    }
  } catch (e) {
    console.error("view submit error:", e?.data || e);
    console.error("task_modal submit failed", {
      teamId,
      actorUserId,
      sourceMode,
      channelId,
      parentTs,
      error: e?.message || String(e),
    });
try {
  await notifyCreateResultOnSource({
    client,
    channelId,
    parentTs,
    actorUserId,
    text: "タスク作成に失敗しました: " + (e?.message || "unknown error"),
  });
} catch (notifyErr) {
  console.error("create result notify failed:", notifyErr?.data || notifyErr);
}
    try {
      await client.views.update({
        view_id: view.id,
        view: {
          type: "modal",
          callback_id: "task_modal_error",
          title: { type: "plain_text", text: "タスク作成" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: `タスク作成に失敗しました。\n${e?.message || "unknown error"}`,
              },
            },
          ],
        },
      });
    } catch (_) {}
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

//       const titleText = noMention(t.title || "・育┌鬘鯉ｼ・);
//       const dueText = formatDueDateOnly(t.due_date);

//       // 笨・DM襍ｷ轤ｹ繧ｿ繧ｹ繧ｯ縺ｯ繧ｹ繝ｬ繝・ラ霑比ｿ｡縺帙★縲∝ｯｾ蠢懆・∈繧｢繝励ΜDM
//       if (String(t.channel_id || "").startsWith("D")) {
//         for (const uid of notifyUserIds) {
//           try {
//             await notifyTaskSimpleDM(uid, t, `竢ｰ 譛滄剞蛻・ｌ縺繧茨ｼ域悄髯・ ${dueText}・荏);
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
//         `竢ｰ 譛滄剞蛻・ｌ繝ｪ繝槭う繝ｳ繝噂n${mentionText}\n*${titleText}*\n譛滄剞: ${dueText}`,
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
  console.log("[open_detail_modal] received", { teamId, taskId, origin, viewType: body.view?.type });
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
    console.error("[open_detail_modal] error:", e?.data || e);
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

    // 荳隕ｧ繝｢繝ｼ繝繝ｫ蜀・↑繧峨∝酔荳繝｢繝ｼ繝繝ｫ繧定ｩｳ邏ｰ陦ｨ遉ｺ縺ｸ譖ｴ譁ｰ・域里蟄・open_detail_in_list 縺ｨ蜷檎ｭ会ｼ・
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

    // Home/縺昴・莉悶・騾壼ｸｸ縺ｮ隧ｳ邏ｰ繝｢繝ｼ繝繝ｫ繧帝幕縺・
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
            "全員が完了しました",
          );

          try {
            const targets = await dbListTargetUserIds(teamId, taskId);
            const toRefresh = Array.from(
              new Set(
                [fresh.requester_user_id, ...(targets || [])].filter(Boolean),
              ),
            );
            await publishHomeBurst(client, teamId, toRefresh, 200);
          } catch (_) {}
        }
      }

      // スレッドカード廃止（DM通知に一本化）
      // 笨・Home view 縺ｫ縺ｯ views.update 縺励↑縺・ｼ亥崋縺ｾ繧・蜿肴丐荳ｭ蟇ｾ遲厄ｼ・
      // 笨・detail_modal 縺ｮ縺ｨ縺阪□縺第峩譁ｰ縺吶ｋ
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

      await publishHomeBurst(client, teamId, [userId, task.requester_user_id], 200);
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
        await notifyTaskSimpleDM(uid, updated, "タスクが完了しました");
      }
    } catch (_) {}

    if (updated.channel_id && updated.message_ts) {
      const _doneBlocks = [
        {
          type: "header",
          text: { type: "plain_text", text: "タスクが完了しました" },
        },
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: "*" + noMention(updated.title) + "*\nタスクを完了しました。",
          },
        },
      ];
      // スレッドカード廃止（DM通知に一本化）
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
      await publishHomeBurst(client, teamId, relatedIds, 200);
    } catch (_) {}
  } catch (e) {
    console.error("complete_task error:", e?.data || e);
  }
}

app.action("complete_task", async ({ ack, body, action, client }) => {
  await ack();

  const { teamId, taskId } = parseActionMeta(body, action);
  if (!teamId || !taskId) return;

  // 譌｢蟄倥・縲悟渚譏荳ｭ窶ｦ縲崎｡ｨ遉ｺ縺後≠繧九↑繧峨％縺薙・谿九＠縺溘∪縺ｾ縺ｧOK

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

    // 笘・側・亥､画峩・会ｼ喘roadcast 縺ｯ隱ｰ縺ｧ繧ょｮ御ｺ・↓縺ｧ縺阪ｋ・磯°逕ｨ蜆ｪ蜈茨ｼ・
    if (task.task_type !== "broadcast") return;

    // 縺吶〒縺ｫ螳御ｺ・蜿悶ｊ荳九￡縺ｪ繧我ｽ輔ｂ縺励↑縺・
    if (task.status === "done" || task.status === "cancelled") {
      await safeEphemeral(
        client,
        task.channel_id || body.user.id,
        body.user.id,
        "このタスクはすでに完了または取消済みです。",
      );
      return;
    }

    // waiting縺ｧ縺ｪ縺上※繧ょｼｷ蛻ｶ逧・↓done縺ｸ
    const updated = await dbUpdateStatus(teamId, taskId, "done");
    if (!updated) return;

    // 笘・夂衍・壼ｮ御ｺ・ｼ・roadcast・峨・縲御ｾ晞ｼ閠・□縺代・
    try {
      if (updated.requester_user_id) {
        await notifyTaskSimpleDM(
          updated.requester_user_id,
          updated,
          "タスクが完了しました",
        );
      }
    } catch (_) {}

    // 笘・ome蜀肴緒逕ｻ・壻ｾ晞ｼ閠・蟇ｾ雎｡閠・↓繧ょ渚譏
    try {
      const targets = await dbListTargetUserIds(teamId, taskId);
      const toRefresh = Array.from(
        new Set(
          [updated.requester_user_id, ...(targets || [])].filter(Boolean),
        ),
      );
      await publishHomeBurst(client, teamId, toRefresh, 200);
    } catch (_) {}

    // スレッドカード廃止（DM通知に一本化）

    // modal refresh・井ｸ隕ｧ繝｢繝ｼ繝繝ｫ蟒・ｭ｢・壼ｸｸ縺ｫ detail_modal 繧呈峩譁ｰ・・
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
      await publishHomeBurst(client, teamId, [body.user.id], 200);
    } catch (_) {}

    // best effort: update original DM message if action came from DM
    if (body.channel?.id && body.message?.ts) {
      try {
        await client.chat.update({
          channel: body.channel.id,
          ts: body.message.ts,
          text: "進捗を更新しました",
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "進捗を更新しました。\n・" + noMention(updated.title) + " の状態を変更しました。",
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

    // 笨・broadcast 縺ｯ謇句虚螟画峩縺励↑縺・ｼ域ｨｩ髯・蟆守ｷ夂噪縺ｫ繧ょ・縺輔↑縺・燕謠舌↑縺ｮ縺ｧ鮟吶▲縺ｦreturn・・
    if (task.task_type === "broadcast") return;

    // 笨・personal・壻ｾ晞ｼ閠・or 蟇ｾ蠢懆・・縺ｿ・域ｨｩ髯舌↑縺励・鮟吶▲縺ｦreturn・・
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
    } else {
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

    // 繧ｹ繝ｬ繝・ラ繧ｫ繝ｼ繝会ｼ夊｡ｨ遉ｺ譖ｴ譁ｰ
    // スレッドカード廃止（DM通知に一本化）
    // 笘・夂衍・壼ｮ御ｺ・ｼ・ersonal縺ｮ縺ｿ・・
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
            "進捗を更新しました\n・タイトル: " + noMention(updated.title) + "\n・期限: " + formatDueDateOnly(updated.due_date) + "\n・ステータス: " + statusLabel(updated.status),
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
      await publishHomeBurst(client, teamId, relatedIds, 200);
    } catch (_) {}
  } catch (e) {
    console.error("status_select error:", e?.data || e);
  }
});


// ================================
// 進捗一覧モーダルのビルダー（ページング対応）
// ================================
async function buildProgressView({ teamId, taskId, page = 0 }) {
  const PAGE_SIZE = 20;

  const [targetsRes, completionsRes] = await Promise.all([
    dbQuery(
      `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2 ORDER BY user_id`,
      [teamId, taskId],
    ),
    dbQuery(
      `SELECT user_id FROM task_completions WHERE team_id=$1 AND task_id=$2 ORDER BY user_id`,
      [teamId, taskId],
    ),
  ]);

  const targets = (targetsRes.rows || []).map((r) => r.user_id).filter(Boolean);
  const doneSet = new Set((completionsRes.rows || []).map((r) => r.user_id).filter(Boolean));

  const done = targets.filter((u) => doneSet.has(u));
  const todo = targets.filter((u) => !doneSet.has(u));

  const DONE_MAX = 50;
  const doneHead = done.slice(0, DONE_MAX).map((u) => `・ <@${u}>`).join("\n");
  const doneMore = done.length > DONE_MAX ? `\n...ほか ${done.length - DONE_MAX} 人` : "";
  const doneListText = done.length ? doneHead + doneMore : "まだありません";

  const start = page * PAGE_SIZE;
  const pageTodo = todo.slice(start, start + PAGE_SIZE);
  const hasPrev = page > 0;
  const hasNext = start + PAGE_SIZE < todo.length;

  const todoBlocks = pageTodo.map((u) => ({
    type: "section",
    text: { type: "mrkdwn", text: `<@${u}>` },
    accessory: {
      type: "button",
      text: { type: "plain_text", text: "対象から外す" },
      action_id: "remove_target_user",
      value: JSON.stringify({ teamId, taskId, userId: u }),
      confirm: {
        title: { type: "plain_text", text: "確認" },
        text: { type: "mrkdwn", text: `<@${u}> をこのタスクの対象から外しますか？` },
        confirm: { type: "plain_text", text: "外す" },
        deny: { type: "plain_text", text: "キャンセル" },
      },
    },
  }));

  const blocks = [
    { type: "header", text: { type: "plain_text", text: "完了 / 未完了" } },
    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: `*完了* ${done.length}件` } },
    { type: "section", text: { type: "mrkdwn", text: doneListText } },
    { type: "divider" },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*未完了* ${todo.length}件` },
      ...(todo.length > 0
        ? {
            accessory: {
              type: "button",
              text: { type: "plain_text", text: "リマインドする" },
              action_id: "remind_incomplete_users",
              value: JSON.stringify({ teamId, taskId }),
              style: "primary",
            },
          }
        : {}),
    },
    ...(todoBlocks.length
      ? todoBlocks
      : [{ type: "section", text: { type: "mrkdwn", text: "まだありません" } }]),
  ];

  if (hasPrev || hasNext) {
    const navElems = [];
    if (hasPrev) {
      navElems.push({
        type: "button",
        text: { type: "plain_text", text: `← 前${PAGE_SIZE}件` },
        action_id: "progress_modal_prev",
        value: JSON.stringify({ teamId, taskId, page: page - 1 }),
      });
    }
    if (hasNext) {
      navElems.push({
        type: "button",
        text: { type: "plain_text", text: `次${PAGE_SIZE}件 →` },
        action_id: "progress_modal_next",
        value: JSON.stringify({ teamId, taskId, page: page + 1 }),
      });
    }
    blocks.push({ type: "divider" });
    blocks.push({ type: "actions", elements: navElems });
    const showing = `未完了 ${start + 1}〜${Math.min(start + PAGE_SIZE, todo.length)} / ${todo.length}人`;
    blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: showing }] });
  }

  return {
    type: "modal",
    callback_id: "progress_modal",
    private_metadata: JSON.stringify({ teamId, taskId, origin: "progress", page }),
    title: { type: "plain_text", text: "進捗一覧" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

app.action("open_progress_modal", async ({ ack, body, action, client }) => {
  await ack();
  const p = safeJsonParse(action?.value || "{}") || {};
  const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
  const teamId = p.teamId || meta.teamId || body.team?.id || body.team_id;
  const taskId = p.taskId || meta.taskId;
  if (!teamId || !taskId) return;
  try {
    const task = await dbGetTaskById(teamId, taskId);
    if (!task || task.task_type !== "broadcast") return;
    const view = await buildProgressView({ teamId, taskId, page: 0 });
    if (body.view?.id) {
      await client.views.push({ trigger_id: body.trigger_id, view });
    } else {
      await client.views.open({ trigger_id: body.trigger_id, view });
    }
  } catch (e) {
    console.error("open_progress_modal error:", e?.data || e);
  }
});

async function handleProgressModalPage({ ack, body, client }) {
  await ack();
  try {
    const p = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const { teamId, taskId, page = 0 } = p;
    if (!teamId || !taskId) return;
    const view = await buildProgressView({ teamId, taskId, page });
    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view });
  } catch (e) {
    console.error("progress_modal_page error:", e?.data || e);
  }
}
app.action("progress_modal_prev", handleProgressModalPage);
app.action("progress_modal_next", handleProgressModalPage);

// ================================
// Remove target user from broadcast task
// ================================
app.action("remove_target_user", async ({ ack, body, action, client }) => {
  await ack();

  try {
    const p = safeJsonParse(action?.value || "{}") || {};
    const { teamId, taskId, userId: targetUserId } = p;
    if (!teamId || !taskId || !targetUserId) return;

    const task = await dbGetTaskById(teamId, taskId);
    if (!task || task.task_type !== "broadcast") return;

    await dbQuery(
      `DELETE FROM task_targets WHERE team_id=$1 AND task_id=$2 AND user_id=$3`,
      [teamId, taskId, targetUserId],
    );

    const countRes = await dbQuery(
      `SELECT COUNT(*) as cnt FROM task_targets WHERE team_id=$1 AND task_id=$2`,
      [teamId, taskId],
    );
    const newTotal = parseInt(countRes.rows[0]?.cnt || "0", 10);
    await dbQuery(
      `UPDATE tasks SET total_count=$3, updated_at=now() WHERE team_id=$1 AND id=$2`,
      [teamId, taskId, newTotal],
    );

    const doneCount = await dbCountCompletions(teamId, taskId);
    if (newTotal > 0 && doneCount >= newTotal && !["done", "cancelled"].includes(task.status)) {
      await dbUpdateStatus(teamId, taskId, "done");
    }

    // 現在のページを維持してモーダルを再描画
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const currentPage = meta.page || 0;
    const view = await buildProgressView({ teamId, taskId, page: currentPage });
    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view });

    await publishHomeBurst(client, teamId, [body.user.id, task.requester_user_id].filter(Boolean), 200);
  } catch (e) {
    console.error("remove_target_user error:", e?.data || e);
  }
});

// ================================
// Edit Task modal
// ================================
// ================================
// リマインド: 未完了ユーザーにDM送信
// ================================
app.action("remind_incomplete_users", async ({ ack, body, action, client }) => {
  await ack();

  try {
    const p = safeJsonParse(action?.value || "{}") || {};
    const { teamId, taskId } = p;
    if (!teamId || !taskId) return;

    const task = await dbGetTaskById(teamId, taskId);
    if (!task) return;

    // 未完了ユーザーを取得
    const targetUserIds = await dbListTargetUserIds(teamId, taskId);
    const incompleteUsers = [];
    for (const uid of targetUserIds) {
      const done = await dbQuery(
        `SELECT 1 FROM task_completions WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1`,
        [teamId, taskId, uid],
      );
      if (done.rows.length === 0) {
        incompleteUsers.push(uid);
      }
    }

    if (incompleteUsers.length === 0) return;

    // 未完了ユーザーにDM送信
    let sentCount = 0;
    const BATCH = 5;
    for (let i = 0; i < incompleteUsers.length; i += BATCH) {
      const batch = incompleteUsers.slice(i, i + BATCH);
      await Promise.allSettled(
        batch.map(async (uid) => {
          try {
            const dm = await client.conversations.open({ users: uid });
            const ch = dm.channel?.id;
            if (!ch) return;

            const payload = JSON.stringify({ teamId, taskId });
            await client.chat.postMessage({
              channel: ch,
              text: `🔔 リマインド: ${noMention(task.title)} がまだ未完了です`,
              blocks: [
                {
                  type: "section",
                  text: {
                    type: "mrkdwn",
                    text: `🔔 *リマインド*\n*${noMention(task.title)}* がまだ未完了です。対応をお願いします。`,
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
                  ],
                },
              ],
            });
            sentCount++;
          } catch (e) {
            console.error("remind DM error:", e?.data || e);
          }
        }),
      );
    }

    // 送信完了のフィードバック（エフェメラルメッセージ的にモーダルを更新）
    const senderUserId = body?.user?.id;
    if (senderUserId) {
      try {
        const dm = await client.conversations.open({ users: senderUserId });
        if (dm.channel?.id) {
          await client.chat.postMessage({
            channel: dm.channel.id,
            text: `✅ 未完了の ${sentCount} 名にリマインドを送信しました（${noMention(task.title)}）`,
          });
        }
      } catch (_) {}
    }
  } catch (e) {
    console.error("remind_incomplete_users error:", e?.data || e);
  }
});

app.view("detail_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const taskId = meta.taskId;
  const actorUserId = getUserIdFromBody(body);

  const requesterUserId =
    view.state.values.requester?.requester_user_select?.selected_user || null;
  const nextDue = view.state.values.due?.due_date?.selected_date || null;

  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "detail_modal_saving",
      title: { type: "plain_text", text: "保存中..." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text: "保存しています..." },
        },
      ],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";
    const canEditTask =
      before.status !== "cancelled" &&
      (isBroadcast ||
        actorUserId === before.requester_user_id ||
        actorUserId === before.assignee_id);
    if (!canEditTask) return;

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

    if (!requesterUserId || !nextTargets.length) {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "detail_modal_error",
          title: { type: "plain_text", text: "保存できませんでした" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: !requesterUserId
                  ? "依頼者を選択してください。"
                  : "対応者またはグループを1つ以上選択してください。",
              },
            },
          ],
        },
      });
      return;
    }

    const nextTaskType =
      nextTargets.length === 1 && selectedGroupIds.length === 0
        ? "personal"
        : "broadcast";

    const beforeTargets =
      before.task_type === "broadcast"
        ? uniqIds(await dbListTargetUserIds(teamId, taskId))
        : uniqIds([before.assignee_id].filter(Boolean));

    let updated = null;
    if (nextTaskType === "broadcast") {
      if (changed || before.task_type !== "broadcast") {
        await dbReplaceTaskTargets(teamId, taskId, nextTargets);
      }

      const completedCount =
        changed || before.task_type !== "broadcast"
          ? await dbCountCompletions(teamId, taskId)
          : before.completed_count ?? 0;

      const assigneeLabelRaw = await buildAssigneeLabelRaw(
        teamId,
        selectedUsers,
        groupHandles,
      );

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        task_type: "broadcast",
        assignee_id: null,
        assignee_label: assigneeLabelRaw || null,
        assignee_dept: null,
        due_date: nextDue,
        description: before.description,
        broadcast_group_handle: groupHandles.length
          ? `@${groupHandles[0]}`
          : null,
        broadcast_group_id: selectedGroupIds.length
          ? selectedGroupIds[0]
          : null,
        total_count: nextTargets.length,
        completed_count: completedCount,
      });
    } else {
      const nextAssignee = nextTargets[0];
      let patchAssigneeDept = null;
      try {
        patchAssigneeDept = await resolveDeptForUser(teamId, nextAssignee);
      } catch (_) {}

      if (before.task_type === "broadcast") {
        await dbReplaceTaskTargets(teamId, taskId, []);
      }

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        task_type: "personal",
        assignee_id: nextAssignee,
        assignee_label: null,
        assignee_dept: patchAssigneeDept,
        due_date: nextDue,
        description: before.description,
        broadcast_group_handle: null,
        broadcast_group_id: null,
        total_count: null,
        completed_count: 0,
      });
    }

    if (!updated) return;

    let requesterDept = null;
    try {
      requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    } catch (_) {}

    const requesterUpdatedRes = await dbQuery(
      `UPDATE tasks
         SET requester_user_id=$3,
             requester_dept=$4,
             updated_at=now()
       WHERE team_id=$1 AND id=$2
       RETURNING *`,
      [teamId, taskId, requesterUserId, requesterDept],
    );
    updated = requesterUpdatedRes.rows[0] || updated;

    const usersToNotify = nextTargets.filter((u) => !beforeTargets.includes(u));
    const usersToRefresh = uniqIds([
      actorUserId,
      before.requester_user_id,
      requesterUserId,
      ...beforeTargets,
      ...nextTargets,
    ]);

    try {
      for (const uid of usersToNotify.filter((u) => u && u !== actorUserId)) {
        await notifyTaskSimpleDM(uid, updated, "担当者が更新されました");
      }
    } catch (e) {
      console.error("detail_modal assignee notify error:", e?.data || e);
    }

    try {
      await publishHomeBurst(client, teamId, usersToRefresh, 250);
    } catch (_) {}

    await client.views.update({
      view_id: body.view.id,
      view: await buildDetailModalView({
        teamId,
        task: updated,
        viewerUserId: actorUserId,
        origin: meta.origin || "home",
      }),
    });
  } catch (e) {
    console.error("detail_modal submit error:", e?.data || e);
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "detail_modal_error",
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
      { type: "section", text: { type: "mrkdwn", text: "読み込み中..." } },
    ],
  };

  let openedViewId = null;

  try {
    // 既存モーダルがあれば push、なければ open
    const hasExistingModal = !!body.view?.id;
    const pushed = hasExistingModal
      ? await client.views.push({ trigger_id: body.trigger_id, view: loadingView })
      : await client.views.open({ trigger_id: body.trigger_id, view: loadingView });
    openedViewId = pushed?.view?.id || null;
  } catch (e) {
    console.error("open_edit_task_modal push/open error:", e?.data || e);
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
    const { initialUserIds, initialGroupOptions } = isBroadcast
      ? await buildBroadcastInitialOptions(teamId, task)
      : {
          initialUserIds: task.assignee_id ? [task.assignee_id] : [],
          initialGroupOptions: [],
        };

    blocks.push({
      type: "input",
      optional: true,
      block_id: "assignee_users",
      label: { type: "plain_text", text: "担当者（複数可）" },
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
        text: "担当グループ（例: @mk）",
      },
      element: {
        type: "multi_external_select",
        action_id: "assignee_groups_select",
        placeholder: {
          type: "plain_text",
          text: "グループを検索",
        },
        min_query_length: 0,
        ...(initialGroupOptions.length
          ? { initial_options: initialGroupOptions }
          : {}),
      },
    });

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
      title: { type: "plain_text", text: "\u4fdd\u5b58\u4e2d..." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "保存しています..." } },
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

    const beforeTargets =
      before.task_type === "broadcast"
        ? uniqIds(await dbListTargetUserIds(teamId, taskId))
        : uniqIds([before.assignee_id].filter(Boolean));

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
            title: { type: "plain_text", text: "編集できませんでした" },
            close: { type: "plain_text", text: "閉じる" },
            blocks: [
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text: "担当者またはグループを1つ以上選択してください。",
                },
              },
            ],
          },
        });
      } catch (_) {}
      return;
    }

    const nextTaskType =
      nextTargets.length === 1 && selectedGroupIds.length === 0
        ? "personal"
        : "broadcast";

    if (nextTaskType === "broadcast") {
      if (changed || before.task_type !== "broadcast") {
        await dbReplaceTaskTargets(teamId, taskId, nextTargets);
      }

      const completedCount =
        changed || before.task_type !== "broadcast"
          ? await dbCountCompletions(teamId, taskId)
          : before.completed_count ?? 0;

      const assigneeLabelRaw = await buildAssigneeLabelRaw(
        teamId,
        selectedUsers,
        groupHandles,
      );

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        task_type: "broadcast",
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
    } else {
      const nextAssignee = nextTargets[0];
      let patchAssigneeDept = null;
      try {
        patchAssigneeDept = await resolveDeptForUser(teamId, nextAssignee);
      } catch (_) {}

      if (before.task_type === "broadcast") {
        await dbReplaceTaskTargets(teamId, taskId, []);
      }

      updated = await dbUpdateTaskEditableFields(teamId, taskId, {
        task_type: "personal",
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
    }

    usersToNotify = nextTargets.filter((u) => !beforeTargets.includes(u));
    usersToRefresh = uniqIds([
      ...usersToRefresh,
      ...beforeTargets,
      ...nextTargets,
    ]);

    if (!updated) return;

    try {
      for (const uid of usersToNotify.filter((u) => u && u !== actorUserId)) {
        await notifyTaskSimpleDM(uid, updated, "担当者が更新されました");
      }
    } catch (e) {
      console.error("assignee notify error:", e?.data || e);
    }

    // スレッドカード廃止（DM通知に一本化）

    try {
      await publishHomeBurst(client, teamId, usersToRefresh, 250);
    } catch (_) {}

    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_task_modal_done",
          title: { type: "plain_text", text: "保存しました" },
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
          title: { type: "plain_text", text: "編集できませんでした" },
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
  const task = await dbGetTaskById(meta.teamId, meta.taskId);
  const mentionCatalog = task
    ? await buildCommentMentionCatalog(meta.teamId, task)
    : { hintText: "" };

  // 隕ｪ・郁ｩｳ邏ｰ繝｢繝ｼ繝繝ｫ・峨ｒ譖ｴ譁ｰ縺吶ｋ縺溘ａ縺ｫ菫晄戟・磯哩縺倥◆譎ゅ↓蜿､縺・Δ繝ｼ繝繝ｫ縺ｸ謌ｻ繧峨↑縺・ｈ縺・↓縺吶ｋ・・
  meta.parent_view_id = body.view?.id || null;
  meta.parent_view_type = body.view?.type || null;

  // 隧ｳ邏ｰ繝｢繝ｼ繝繝ｫ荳翫°繧峨・ push 縺梧ｭ｣隗｣・医Δ繝ｼ繝繝ｫ莠碁㍾ open 縺ｯ荳榊庄・・
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
        ...(mentionCatalog.hintText
          ? [
              {
                type: "context",
                elements: [
                  {
                    type: "mrkdwn",
                    text: mentionCatalog.hintText,
                  },
                ],
              },
            ]
          : []),
        {
          type: "input",
          block_id: "mention_users",
          optional: true,
          label: { type: "plain_text", text: "メンション先ユーザー（任意）" },
          element: {
            type: "multi_static_select",
            action_id: "users",
            placeholder: { type: "plain_text", text: "候補ユーザーを選択" },
            options: mentionCatalog.userOptions,
          },
        },
        ...(mentionCatalog.groupOptions?.length
          ? [
              {
                type: "input",
                block_id: "mention_groups",
                optional: true,
                label: { type: "plain_text", text: "メンション先グループ（任意）" },
                element: {
                  type: "multi_static_select",
                  action_id: "groups",
                  placeholder: { type: "plain_text", text: "候補グループを選択" },
                  options: mentionCatalog.groupOptions,
                },
              },
            ]
          : []),

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
        ...(task?.task_type === "broadcast"
          ? [
              {
                type: "input",
                block_id: "notify_scope",
                optional: true,
                label: { type: "plain_text", text: "通知先" },
                element: {
                  type: "checkboxes",
                  action_id: "notify_scope_check",
                  options: [
                    {
                      text: {
                        type: "plain_text",
                        text: "未完了者のみに通知",
                      },
                      value: "incomplete_only",
                    },
                  ],
                },
              },
            ]
          : []),
      ],
    },
  });
});

app.view("comment_modal", async ({ ack, body, view, client }) => {
  const meta = safeJsonParse(view.private_metadata || "{}") || {};

  const base = view.state.values.comment?.body?.value?.trim() || "";
  const task = await dbGetTaskById(meta.teamId, meta.taskId);
  const mentionCatalog = task
    ? await buildCommentMentionCatalog(meta.teamId, task)
    : null;
  const mentionUserIds =
    view.state.values.mention_users?.users?.selected_options
      ?.map((option) => option?.value)
      .filter(Boolean) || [];
  const mentionGroupIds =
    view.state.values.mention_groups?.groups?.selected_options
      ?.map((option) => option?.value)
      .filter(Boolean) || [];
  const resolvedComment = replaceCommentPseudoMentions(base, mentionCatalog);
  const notifyIncompleteOnly = Boolean(
    view.state.values.notify_scope?.notify_scope_check?.selected_options?.some(
      (option) => option?.value === "incomplete_only",
    ),
  );

  const explicitGroupMentions = mentionGroupIds.map(
    (groupId) => `<!subteam^${groupId}>`,
  );
  const mentionPrefix = [...mentionUserIds.map((u) => `<@${u}>`), ...explicitGroupMentions].join(" ");
  const comment = `${mentionPrefix}${mentionPrefix ? " " : ""}${resolvedComment.text}`.trim();

  if (!comment) {
    await ack({
      response_action: "errors",
      errors: { comment: "コメントを入力してください" },
    });
    return;
  }

  // 竭 縺ｾ縺・遘剃ｻ･蜀・↓霆ｽ縺・判髱｢縺ｸ蟾ｮ縺玲崛縺茨ｼ育｢ｺ螳溘↓UI繧定誠縺ｨ縺輔↑縺・ｼ・
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "comment_modal_saving",
      title: { type: "plain_text", text: "コメント" },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "投稿しています..." } },
      ],
    },
  });

  try {
    // 竭｡ 驥阪＞蜃ｦ逅・・ ack 蠕後↓繧・ｋ
    await dbInsertTaskComment(meta.teamId, meta.taskId, body.user.id, comment);

    if (!task) return;

    try {
      if (
        task.channel_id &&
        task.message_ts &&
        !String(task.channel_id).startsWith("D")
      ) {
        await app.client.chat.postMessage({
          channel: task.channel_id,
          thread_ts: task.message_ts,
          text: comment,
          mrkdwn: true,
          unfurl_links: false,
          unfurl_media: false,
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: `*<@${body.user.id}> がコメントしました*\n${comment}`,
              },
            },
          ],
        });
      }
    } catch (e) {
      console.error("comment thread post error:", e?.data || e);
    }

    // 竭｡-b 繧ｳ繝｡繝ｳ繝磯夂衍・・ot DM・・
    // - 繝｡繝ｳ繧ｷ繝ｧ繝ｳ縺後≠繧後・繝｡繝ｳ繧ｷ繝ｧ繝ｳ蜈医∈
    // - 繝｡繝ｳ繧ｷ繝ｧ繝ｳ縺檎┌縺代ｌ縺ｰ personal 縺ｯ (萓晞ｼ閠・蟇ｾ蠢懆・ 縺ｸ・郁・蛻・・髯､螟厄ｼ・
    // - broadcast 縺ｯ萓晞ｼ閠・∈・郁・蛻・・髯､螟厄ｼ・
    try {
      const actor = body.user.id;

      const recipients = new Set();

      // 繝｡繝ｳ繧ｷ繝ｧ繝ｳ蜈茨ｼ郁､・焚・・
      for (const uid of mentionUserIds || []) {
        if (uid && uid !== actor) recipients.add(uid);
      }
      for (const gid of mentionGroupIds || []) {
        try {
          const groupUsers = await getUsergroupMembers(task.team_id, gid);
          for (const uid of groupUsers || []) {
            if (uid && uid !== actor) recipients.add(uid);
          }
        } catch (_) {}
      }
      for (const uid of resolvedComment.mentionedUserIds || []) {
        if (uid && uid !== actor) recipients.add(uid);
      }
      for (const gid of resolvedComment.mentionedGroupIds || []) {
        try {
          const groupUsers = await getUsergroupMembers(task.team_id, gid);
          for (const uid of groupUsers || []) {
            if (uid && uid !== actor) recipients.add(uid);
          }
        } catch (_) {}

      }
      if (task.task_type === "broadcast" && notifyIncompleteOnly) {
        const incompleteSet = new Set(
          (await listIncompleteTaskTargetUserIds(task.team_id, task.id)).filter(
            (uid) => uid && uid !== actor,
          ),
        );

        if (recipients.size === 0) {
          for (const uid of incompleteSet) recipients.add(uid);
        } else {
          for (const uid of Array.from(recipients)) {
            if (!incompleteSet.has(uid)) recipients.delete(uid);
          }
        }
      }

      // 繝｡繝ｳ繧ｷ繝ｧ繝ｳ縺檎┌縺・ｴ蜷医・繝輔か繝ｼ繝ｫ繝舌ャ繧ｯ
      if (recipients.size === 0) {
        const requester = task.requester_user_id;
        const assignee = task.assignee_id;

        if (
          !(task.task_type === "broadcast" && notifyIncompleteOnly) &&
          requester &&
          requester !== actor
        ) {
          recipients.add(requester);
        }

        if (task.task_type !== "broadcast" && assignee && assignee !== actor) {
          recipients.add(assignee);
        }
      }

      // DM譛ｬ譁・ｼ壹←縺ｮ繧ｿ繧ｹ繧ｯ縺句・縺九ｋ繧医≧縺ｫ blocks +縲瑚ｩｳ邏ｰ繧帝幕縺上阪・繧ｿ繝ｳ繧剃ｻ倥￠繧・
      const title = task.title || "タスク";
      const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
      // 笘・ｿｽ蜉・壹さ繝｡繝ｳ繝亥・縺ｮ <@UXXXX> 繧・@陦ｨ遉ｺ蜷・縺ｫ螟画鋤
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
            text: "*タスクにコメントがありました*",
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
                text: "*コメント*\n>" + noMention(String(prettyComment).slice(0, 800)),
              },
            },
          ];

          // 蜈・Γ繝・そ繝ｼ繧ｸ繝ｪ繝ｳ繧ｯ・医≠繧後・・・
          if (task.source_permalink && task.message_ts) {
            blocks.push({
              type: "section",
              text: {
                type: "mrkdwn",
                text: "<" + task.source_permalink + "|元メッセージへ>",
              },
            });
          } else if (!task.message_ts) {
            blocks.push({
              type: "context",
              elements: [
                {
                  type: "mrkdwn",
                  text:
                    "\u3053\u306e\u30bf\u30b9\u30af\u306f Home \u306e\u30bf\u30b9\u30af\u4f5c\u6210\u304b\u3089\u4f5c\u6210\u3055\u308c\u307e\u3057\u305f\u3002",
                },
              ],
            });
          }

          // 隧ｳ邏ｰ繧帝幕縺上・繧ｿ繝ｳ
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
            text: "コメント通知: " + noMention(title),
            blocks,
          });
        } catch (_) {}
      }
    } catch (e) {
      console.error("comment DM notify error:", e?.data || e);
    }

    // 竭｢ 隕ｪ・郁ｩｳ邏ｰ繝｢繝ｼ繝繝ｫ・峨ｒ譖ｴ譁ｰ縺励※縲√さ繝｡繝ｳ繝医Δ繝ｼ繝繝ｫ縺ｯ縲梧兜遞ｿ螳御ｺ・崎｡ｨ遉ｺ縺ｫ縺吶ｋ
    // 縺薙≧縺吶ｋ縺ｨ縲・哩縺倥◆譎ゅ↓蜿､縺・ｩｳ邏ｰ繝｢繝ｼ繝繝ｫ縺悟・縺ｦ縺上ｋ蝠城｡後ｒ髦ｲ縺偵ｋ
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

    // 繧ｳ繝｡繝ｳ繝医Δ繝ｼ繝繝ｫ蛛ｴ縺ｯ螳御ｺ・Γ繝・そ繝ｼ繧ｸ・郁・蜍輔〒隧ｳ邏ｰ縺ｫ謌ｻ縺輔↑縺・ｼ・
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
              text: "投稿しました。必要なら詳細画面から続けて確認できます。",
            },
          },
        ],
      },
    });
  } catch (e) {
    console.error("comment_modal post-save error:", e?.data || e);
    // 螟ｱ謨苓｡ｨ遉ｺ縺縺第峩譁ｰ・井ｻｻ諢擾ｼ・
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
              text: { type: "mrkdwn", text: "投稿に失敗しました。もう一度お試しください。" },
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

    // 笨・莉墓ｧ假ｼ喘roadcast 縺ｯ縲後□繧後〒繧ゅ肴悄髯仙､画峩OK
    // personal 縺ｯ萓晞ｼ閠・蟇ｾ蠢懆・・縺ｿ・亥ｿ・ｦ√↑繧・personal 繧・true 縺ｫ縺励※OK・・
    const isBroadcast = task.task_type === "broadcast";
    const canChangeDue = isBroadcast
      ? true
      : viewerUserId === task.requester_user_id ||
        viewerUserId === task.assignee_id;

    if (!canChangeDue) return;

    const initDue = slackDateYmd(task.due_date);

    // 笘・ｦｪ・郁ｩｳ邏ｰ繝｢繝ｼ繝繝ｫ・峨・ view_id 繧剃ｿ晄戟縺励※縺翫￥・亥ｾ後〒蜀肴緒逕ｻ縺吶ｋ・・
    const parentViewId = body.view?.id || null;

    const blocks = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*" + noMention(task.title || "タスク") + "*",
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
            text: "期限を空にすると未設定になります。",
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

    // 隧ｳ邏ｰ繝｢繝ｼ繝繝ｫ荳翫・繝懊ち繝ｳ縺九ｉ縺ｪ縺ｮ縺ｧ push 縺瑚・辟ｶ・・rigger_id蠢・茨ｼ・
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

  // 竭 縺ｾ縺夊ｻｽ縺・判髱｢縺ｸ蟾ｮ縺玲崛縺茨ｼ・ash_conflict蝗樣∩・・
  await ack({
    response_action: "update",
    view: {
      type: "modal",
      callback_id: "edit_due_modal_saving",
      title: { type: "plain_text", text: "\u4fdd\u5b58\u4e2d..." },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        { type: "section", text: { type: "mrkdwn", text: "保存しています..." } },
      ],
    },
  });

  try {
    const before = await dbGetTaskById(teamId, taskId);
    if (!before) return;

    const isBroadcast = before.task_type === "broadcast";

    // 笨・莉墓ｧ假ｼ喘roadcast 縺ｯ縲後□繧後〒繧ゅ肴悄髯仙､画峩OK
    const canChangeDue = isBroadcast
      ? true
      : actorUserId === before.requester_user_id ||
        actorUserId === before.assignee_id;

    if (!canChangeDue) return;

    // 笨・due_date 縺縺第峩譁ｰ・域悽譁・・隗ｦ繧峨↑縺・ｼ・
    const updated = await dbUpdateTaskContent(teamId, taskId, {
      due_date: nextDue,
      description: null,
      assignee_id: null,
      assignee_dept: null,
    });
    if (!updated) return;

    // 繧ｹ繝ｬ繝・ラ繧ｫ繝ｼ繝画峩譁ｰ・郁ｨｼ霍｡・・
    // スレッドカード廃止（DM通知に一本化）

    // Home 蜀肴緒逕ｻ・亥ｺ・ａ縺ｫ・・
    try {
      const users = [
        updated.requester_user_id,
        updated.assignee_id,
        actorUserId,
      ].filter(Boolean);
      await publishHomeBurst(client, teamId, users, 250);
    } catch (_) {}

    // =====笘・％縺薙′譛ｬ鬘鯉ｼ夊レ髱｢縺ｮ隧ｳ邏ｰ繝｢繝ｼ繝繝ｫ繧呈怙譁ｰ縺ｧ蜀肴緒逕ｻ =====
    // Slack 縺梧署萓帙☆繧・previous_view_id 縺悟叙繧後ｋ縺ｪ繧峨◎繧後′譛蜆ｪ蜈・
    const prevViewId = body.view?.previous_view_id || null;

    // 蠢ｵ縺ｮ縺溘ａ open 蛛ｴ縺ｧ謖√◆縺帙◆ parentViewId 繧ゅヵ繧ｩ繝ｼ繝ｫ繝舌ャ繧ｯ縺ｫ菴ｿ縺・
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
    // ===== 笘・％縺薙∪縺ｧ =====

    // 竭｡ 迴ｾ蝨ｨ縺ｮ繝｢繝ｼ繝繝ｫ縺ｯ縲御ｿ晏ｭ倥＠縺ｾ縺励◆笨・肴怙蟆酋I
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_due_modal_done",
          title: { type: "plain_text", text: "保存しました" },
          close: { type: "plain_text", text: "閉じる" },
          blocks: [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "期限を更新しました\n*" + formatDueDateOnly(nextDue) + "*",
              },
            },
          ],
        },
      });
    } catch (_) {}
  } catch (e) {
    console.error("edit_due_modal error:", e?.data || e);

    // 螟ｱ謨礼判髱｢・域怙蟆酋I・・
    try {
      await client.views.update({
        view_id: body.view.id,
        view: {
          type: "modal",
          callback_id: "edit_due_modal_error",
          title: { type: "plain_text", text: "更新できませんでした" },
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

    // 笨・讓ｩ髯舌↑縺励・鮟吶▲縺ｦreturn・・I縺ｧ繧ょ・縺輔↑縺・燕謠撰ｼ・
    const ok =
      task.task_type === "broadcast"
        ? task.requester_user_id === actor
        : task.requester_user_id === actor || task.assignee_id === actor;
    if (!ok) return;

    // 譛ｪ螳御ｺ・∈謌ｻ縺呻ｼ・pen 縺ｧ繧ゅ＞縺・￠縺ｩ縲ゞI逧・↓縺ｯ in_progress 縺瑚・辟ｶ・・
    const updated = await dbUpdateStatus(teamId, taskId, "in_progress");
    if (!updated) return;

    // 隧ｳ邏ｰ繝｢繝ｼ繝繝ｫ繧貞・謠冗判・井ｻ企幕縺・※繧九Δ繝ｼ繝繝ｫ・・
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

    // 繧ｹ繝ｬ繝・ラ繧ｫ繝ｼ繝会ｼ夊｡ｨ遉ｺ譖ｴ譁ｰ
    // スレッドカード廃止（DM通知に一本化）

    // Home 蜀肴緒逕ｻ・磯未菫り・ｼ・
    try {
      const relatedIds = Array.from(
        new Set(
          [actor, updated.requester_user_id, updated.assignee_id].filter(
            Boolean,
          ),
        ),
      );
      await publishHomeBurst(client, teamId, relatedIds, 200);
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
