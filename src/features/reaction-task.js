function registerReactionFeature(deps) {
  const {
    __cacheKey,
    __cachePut,
    app,
    buildAssigneeLabelRaw = async (tid, users) => users.map(u => `<@${u}>`).join(" "),
    buildThreadCardBlocks,
    dbCreateTask,
    dbGetTaskBySource,
    dbGetThreadCard,
    dbInsertTaskTargets = async () => {},
    getSubteamIdMap = async () => new Map(),
    getTeamIdFromBody,
    getUsergroupMembers = async () => [],
    isReactionTaskifyEnabled = async () => true,
    noMention,
    openTaskCreateModal,
    prettifySlackText,
    prettifyUserMentions,
    randomUUID,
    resolveDeptForUser,
    safeEphemeral,
    safeJsonParse,
    slackDateYmd,
    publishHomeBurst = () => {},
    uniqIds = (arr) => [...new Set(arr)],
    upsertThreadCard,
    notifyTaskSimpleDM = async () => {},
  } = deps;

  async function isDmLikeConversation(client, channelId) {
    const id = String(channelId || "");
    if (!id) return false;
    if (id.startsWith("D")) return true;
    try {
      const info = await client.conversations.info({ channel: id });
      const ch = info?.channel || {};
      return !!ch.is_im || !!ch.is_mpim;
    } catch (_) {
      return false;
    }
  }

  async function notifyActorResult({
    client,
    actorUserId,
    channelId,
    threadTs = null,
    text,
    blocks = null,
    dmOnly = false,
  }) {
    const isDmLike = await isDmLikeConversation(client, channelId);

    if (!dmOnly && !isDmLike) {
      try {
        const ephemeralArgs = {
          channel: channelId,
          user: actorUserId,
          text,
          ...(blocks ? { blocks } : {}),
        };
        if (threadTs) ephemeralArgs.thread_ts = threadTs;
        await client.chat.postEphemeral(ephemeralArgs);
        return;
      } catch (e) {
        console.error("reaction notify ephemeral error:", e?.data || e);
      }
    }

    if (dmOnly && !isDmLike) return;

    try {
      const dm = await app.client.conversations.open({ users: actorUserId });
      const dmChannel = dm.channel?.id;
      if (!dmChannel) return;
      await app.client.chat.postMessage({
        channel: dmChannel,
        text,
        ...(blocks ? { blocks } : {}),
      });
    } catch (e) {
      console.error("reaction notify dm error:", e?.data || e);
    }
  }

  // メンション情報からタスクを作成するヘルパー
  // userIds/groupIds が複数 or グループあり → broadcast、そうでなければ personal
  async function createTaskFromMentions({
    teamId, channelId, msgTs, rawText, baseText,
    blocks = null,
    requesterUserId, actorUserId, dueYmd, permalink,
  }) {
    const { userIds, groupIds } = inferTargetsFromMessage(rawText, actorUserId, blocks);
    const isMulti = groupIds.length > 0 || userIds.length > 1;

    // テキスト整形
    let prettyText;
    try {
      prettyText = await prettifySlackText(baseText, teamId);
      prettyText = await prettifyUserMentions(prettyText, teamId);
    } catch (_) { prettyText = baseText; }
    const description = String(prettyText || baseText || "").trim();
    const title = description || "（本文なし）";
    const taskId = randomUUID();

    if (!isMulti) {
      // === PERSONAL ===
      const assigneeId = userIds[0] || actorUserId;
      const [requesterDept, assigneeDept] = await Promise.all([
        resolveDeptForUser(teamId, requesterUserId),
        resolveDeptForUser(teamId, assigneeId),
      ]);
      const created = await dbCreateTask({
        id: taskId, team_id: teamId, channel_id: channelId, message_ts: msgTs,
        source_permalink: permalink || null, title, description,
        requester_user_id: requesterUserId, created_by_user_id: actorUserId,
        assignee_id: assigneeId, assignee_label: null,
        status: "in_progress", due_date: dueYmd,
        requester_dept: requesterDept, assignee_dept: assigneeDept,
        task_type: "personal", broadcast_group_handle: null, broadcast_group_id: null,
        total_count: null, completed_count: 0, notified_at: null,
      });
      if (assigneeId && assigneeId !== actorUserId) {
        await notifyTaskSimpleDM(assigneeId, created, "タスクが割り当てられました").catch(() => {});
      }
      return { created, targetList: [assigneeId] };
    }

    // === BROADCAST ===
    const idToHandle = await getSubteamIdMap(teamId).catch(() => new Map());
    const groupHandles = groupIds.map(gid => idToHandle.get(gid) || gid);
    let expanded = [...userIds];
    for (const gid of groupIds) {
      const members = await getUsergroupMembers(teamId, gid).catch(() => []);
      expanded.push(...(members || []));
    }
    const targetList = uniqIds(expanded.filter(Boolean));
    if (!targetList.length) {
      // グループが空だったなどのフォールバック → personal with actor
      const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
      const created = await dbCreateTask({
        id: taskId, team_id: teamId, channel_id: channelId, message_ts: msgTs,
        source_permalink: permalink || null, title, description,
        requester_user_id: requesterUserId, created_by_user_id: actorUserId,
        assignee_id: actorUserId, assignee_label: null,
        status: "in_progress", due_date: dueYmd,
        requester_dept: requesterDept, assignee_dept: requesterDept,
        task_type: "personal", broadcast_group_handle: null, broadcast_group_id: null,
        total_count: null, completed_count: 0, notified_at: null,
      });
      return { created, targetList: [actorUserId] };
    }

    const assigneeLabel = await buildAssigneeLabelRaw(teamId, targetList, groupHandles).catch(() => targetList.map(u => `<@${u}>`).join(" "));
    const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
    const created = await dbCreateTask({
      id: taskId, team_id: teamId, channel_id: channelId, message_ts: msgTs,
      source_permalink: permalink || null, title, description,
      requester_user_id: requesterUserId, created_by_user_id: actorUserId,
      assignee_id: null, assignee_label: assigneeLabel,
      status: "in_progress", due_date: dueYmd,
      requester_dept: requesterDept, assignee_dept: null,
      task_type: "broadcast",
      broadcast_group_handle: groupHandles[0] || null,
      broadcast_group_id: groupIds[0] || null,
      total_count: targetList.length, completed_count: 0, notified_at: null,
    });
    await dbInsertTaskTargets(teamId, taskId, targetList);
    for (const uid of targetList.filter(u => u !== actorUserId)) {
      await notifyTaskSimpleDM(uid, created, "タスクが割り当てられました").catch(() => {});
    }
    return { created, targetList };
  }

const TASK_REACTION_NAME = "task";

// キーワードタスク化：全角・半角両対応
const TASK_KEYWORD_PATTERNS = [
  /＜タスク化＞/,
  /<タスク化>/,
];

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
    if (!(await isReactionTaskifyEnabled(teamId))) return;
    const channelId = event?.item?.channel;
    const msgTs = event?.item?.ts;
    const actorUserId = event?.user; // リアクションした人
    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    console.info("reaction_added start", {
      teamId,
      channelId,
      msgTs,
      actorUserId,
      reaction: event?.reaction,
    });

    // すでに「確認UI（スレッドカード）」を出していたら何もしない（1メッセージ1回）
    const existingCard = await dbGetThreadCard(teamId, channelId, msgTs);
    if (existingCard?.card_ts) return;

    const existingTaskForActor = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTaskForActor?.id) {
      await notifyActorResult({
        client,
        actorUserId,
        channelId,
        threadTs: msgTs,
        text: "タスク化済みです。",
        dmOnly: true,
      });
      return;
    }

    // すでにタスク化済みなら案内だけ（ここは現行踏襲）
    const existingTask = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTask?.id) {
      await safeEphemeral(client, channelId, actorUserId, "✅ タスク化済み");
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

    // 期限は今日固定
    const dueYmd = slackDateYmd(new Date());
    const effectiveRequester = requesterUserId || actorUserId;

    // permalink取得
    const permalink = await client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
      .then(r => r?.permalink || "").catch(() => "");

    // メンション情報に基づいてタスク作成（single → personal, multi/group → broadcast）
    const { created, targetList } = await createTaskFromMentions({
      teamId, channelId, msgTs,
      rawText, baseText: rawText,
      blocks: mm?.blocks || null,
      requesterUserId: effectiveRequester,
      actorUserId, dueYmd, permalink,
    });

    // 元メッセージがスレッド返信か親かを判定
    const isParentMsg = !mm?.thread_ts || mm?.thread_ts === mm?.ts;

    // リアクションした人にエフェメラルで通知
    // 親メッセージの場合はチャンネル内に表示（thread_ts なし）、スレッド返信の場合はスレッド内
    try {
      const payload = JSON.stringify({ teamId, taskId: created.id });
      const ephemeralArgs = {
        channel: channelId,
        user: actorUserId,
        text: `✅ タスク化しました: ${noMention(created.title)}`,
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: `✅ *タスク化しました*\n*${noMention(created.title)}*`,
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
              {
                type: "button",
                text: { type: "plain_text", text: "内容を編集" },
                action_id: "open_edit_task_modal",
                value: payload,
              },
            ],
          },
        ],
      };
      if (!isParentMsg) ephemeralArgs.thread_ts = msgTs;
      await client.chat.postEphemeral(ephemeralArgs);
    } catch (e) {
      console.error("ephemeral notify error:", e?.data || e);
    }

    try {
      const payload = JSON.stringify({ teamId, taskId: created.id });
      await notifyActorResult({
        client,
        actorUserId,
        channelId,
        threadTs: !isParentMsg ? msgTs : null,
        text: `タスク化しました: ${noMention(created.title)}`,
        dmOnly: true,
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: `✅ *タスク化しました*\n*${noMention(created.title)}*`,
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
              {
                type: "button",
                text: { type: "plain_text", text: "内容を編集" },
                action_id: "open_edit_task_modal",
                value: payload,
              },
            ],
          },
        ],
      });
    } catch (e) {
      console.error("reaction dm notify error:", e?.data || e);
    }

    try {
      await publishHomeBurst(
        client,
        teamId,
        [...new Set([effectiveRequester, actorUserId, ...targetList])].filter(Boolean),
        200,
      );
    } catch (_) {}
    console.info("reaction_added success", {
      teamId,
      channelId,
      msgTs,
      actorUserId,
      taskId: created.id,
      targetCount: targetList.length,
    });
  } catch (e) {
    console.error("reaction_added error:", e?.data || e);
  }
});

// ================================
// キーワードタスク化: メッセージに ＜タスク化＞ or <タスク化> が含まれたら即タスク作成
// ================================
app.event("message", async ({ event, client, body }) => {
  try {
    // bot メッセージ・編集・削除は無視
    if (event?.subtype) return;

    const rawText = event?.text || "";
    const matched = TASK_KEYWORD_PATTERNS.some((re) => re.test(rawText));
    if (!matched) return;

    const teamId = body?.team_id || body?.team?.id || event?.team;
    if (!teamId) return;
    if (!(await isReactionTaskifyEnabled(teamId))) return;

    const channelId = event?.channel;
    const msgTs = event?.ts;
    const actorUserId = event?.user;
    if (!channelId || !msgTs || !actorUserId) return;

    // すでにタスク化済みなら無視
    const existingTask = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTask?.id) return;

    // キーワードを除去したテキストをタイトルにする
    let cleanText = rawText;
    for (const re of TASK_KEYWORD_PATTERNS) {
      cleanText = cleanText.replace(re, "");
    }
    cleanText = cleanText.trim();

    const dueYmd = slackDateYmd(new Date());

    // permalink取得
    const permalink = await client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
      .then((r) => r?.permalink || "").catch(() => "");

    // メンション情報に基づいてタスク作成（single → personal, multi/group → broadcast）
    const { created, targetList } = await createTaskFromMentions({
      teamId, channelId, msgTs,
      rawText, baseText: cleanText,
      blocks: event?.blocks || null,
      requesterUserId: actorUserId,
      actorUserId, dueYmd, permalink,
    });

    // 元メッセージがスレッド返信か親かを判定
    const isParentMsg = !event?.thread_ts || event?.thread_ts === msgTs;

    // キーワードタスク化した人にエフェメラル通知
    // 親メッセージの場合はチャンネル内に表示（thread_ts なし）、スレッド返信の場合はスレッド内
    try {
      const payload = JSON.stringify({ teamId, taskId: created.id });
      const ephemeralArgs = {
        channel: channelId,
        user: actorUserId,
        text: `✅ タスク化しました: ${noMention(created.title)}`,
        blocks: [
          {
            type: "section",
            text: {
              type: "mrkdwn",
              text: `✅ *タスク化しました*\n*${noMention(created.title)}*`,
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
              {
                type: "button",
                text: { type: "plain_text", text: "内容を編集" },
                action_id: "open_edit_task_modal",
                value: payload,
              },
            ],
          },
        ],
      };
      if (!isParentMsg) ephemeralArgs.thread_ts = msgTs;
      await client.chat.postEphemeral(ephemeralArgs);
    } catch (e) {
      console.error("ephemeral notify error:", e?.data || e);
    }

    try {
      await publishHomeBurst(
        client,
        teamId,
        [...new Set([actorUserId, ...targetList])].filter(Boolean),
        200,
      );
    } catch (_) {}
  } catch (e) {
    if (e?.data?.error !== "not_in_channel")
      console.error("keyword_taskify error:", e?.data || e);
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

    // permalink取得・テキスト整形・dept解決を並列実行
    const [permalinkResult, prettyTextResult, requesterDept, assigneeDept] =
      await Promise.all([
        client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
          .then(r => r?.permalink || "").catch(() => ""),
        (async () => {
          let t = await prettifySlackText(rawText, teamId);
          t = await prettifyUserMentions(t, teamId);
          return t;
        })(),
        resolveDeptForUser(teamId, requesterUserId),
        resolveDeptForUser(teamId, assigneeId),
      ]);

    const permalink = permalinkResult;
    const prettyText = prettyTextResult;
    const description = String(prettyText || rawText || "").trim();
    const title = description || "（本文なし）";

    const taskId = randomUUID();

    // この導線は「personalタスクを即作成」だけに絞る（リアクション→確定ボタン）
    const taskType = "personal";
    const status = "in_progress"; // 初期は進行中で固定
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

    // 依頼者に「タスク発行しました」DM
    try {
      if (actorUserId) {
        const dm = await client.conversations.open({ users: actorUserId });
        const ch = dm.channel?.id;
        if (ch) {
          const dmPayload = JSON.stringify({ teamId, taskId: created.id });
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
                    value: dmPayload,
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

    // 対応者にDM（依頼者と別の場合）
    try {
      if (assigneeId && assigneeId !== actorUserId) {
        await notifyTaskSimpleDM(assigneeId, created, "タスクが割り当てられました");
      }
    } catch (_) {}
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

    const rawText = payload.messageText || "";

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);

    const cacheKey = __cacheKey(teamId, channelId, msgTs);
    __cachePut(cacheKey, prettyText || "");

    await openTaskCreateModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      channelId,
      msgTs,
      actorUserId,
      includeContentInput: false,
    });
  } catch (e) {
    console.error("reaction_task_open_edit_modal error:", e?.data || e);
  }
});

  return {
    extractUserGroupIdsFromBlocks,
    extractUserIdsFromBlocks,
    inferTargetsFromMessage,
  };
}

module.exports = {
  registerReactionFeature,
};
