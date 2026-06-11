function registerHomeFeature(deps) {
  const {
    app,
    assigneeDisplay,
    buildTaskListModalView,
    canUserSeeChannel,
    dbHasUserCompleted,
    dbIsUserTarget = async () => false,
    dbGetUserCompletedTaskIds = async () => new Set(),
    dbListBroadcastTasksByStatuses,
    dbListBroadcastTasksByStatusesWithScope,
    dbListPersonalTasksByStatusesWithScope,
    formatDueDateOnly,
    getTeamIdFromBody,
    getUserDisplayName,
    getUserIconUrl,
    getUserIdFromBody,
    getUsergroupMembers,
    resolveHomeDefaults = async () => ({
      displayMode: "standard",
      rangeKey: "to_me",
      scopeKey: "active",
    }),
    handleCompleteTask,
    noMention,
    openDetailModal,
    openTaskCreateModal,
    safeJsonParse,
    searchUsergroups,
    slackDateYmd,
    toAtShortName,
    todayJstYmd,
    openUserSettingsModal,
    dbQuery = async () => ({ rows: [] }),
    dbListDashTeamMembers = async () => [],
    getUserSettings = async () => ({}),
    dbCreatePersonalFilter = async () => {},
    dbListPersonalFilters = async () => [],
    dbUpdatePersonalFilter = async () => {},
    dbDeletePersonalFilter = async () => {},
    dbSetPersonalFilterMembers = async () => {},
    dbGetPersonalFilterMemberIds = async () => [],
    dbGetDashTeamSubtree = async () => [],
    dbGetDashboardRole = async () => "member",
    randomUUID = () => require("crypto").randomUUID(),
  } = deps;

  async function isBroadcastAssignedToUser(task, teamId, userId) {
    if (!task || task.task_type !== "broadcast" || !userId) return false;
    return !!(await dbIsUserTarget(teamId, task.id, userId));
  }

async function publishHomeForUsers(client, teamId, userIds, intervalMs = 200) {
  const uniq = Array.from(new Set((userIds || []).filter(Boolean)));
  // 1人目は即座に await で確実に反映
  if (uniq.length > 0) {
    await publishHome({ client, teamId, userId: uniq[0] }).catch(() => {});
  }
  // 2人目以降は間隔を空けて
  for (let i = 1; i < uniq.length; i++) {
    const u = uniq[i];
    setTimeout(() => {
      publishHome({ client, teamId, userId: u }).catch(() => {});
    }, i * intervalMs);
  }
}

const homeState = new Map();        // key → state
const homeStateLastUsed = new Map(); // key → timestamp
const hydratedHomeState = new Set();

// 24時間アクセスのないエントリを60分ごとに削除（メモリリーク防止）
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [k, ts] of homeStateLastUsed) {
    if (ts < cutoff) {
      homeState.delete(k);
      homeStateLastUsed.delete(k);
      hydratedHomeState.delete(k);
    }
  }
}, 60 * 60 * 1000).unref();

// 未完了（= 進行中）
const ACTIVE_STATUSES = ["in_progress"];
const DONE_STATUSES = ["done"];

function getHomeState(teamId, userId) {
  const k = `${teamId}:${userId}`;
  homeStateLastUsed.set(k, Date.now());
  const s = homeState.get(k) || {
    viewKey: "all",
    scopeKey: "active",
    personalScopeKey: "to_me",
    assigneeUserId: userId,
    deptKey: "all",
    broadcastScopeKey: "to_me",
    displayMode: "standard", // "standard" | "compact"
    homeMore: { overdue: false, today: false },
    homeFold: { overdue: false, today: false, later: false },
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
  homeStateLastUsed.set(k, Date.now());
  hydratedHomeState.add(k);
}

async function ensureHomeStateLoaded(teamId, userId) {
  const k = `${teamId}:${userId}`;
  if (hydratedHomeState.has(k)) return;

  const defaults = await resolveHomeDefaults(teamId, userId);
  setHomeState(teamId, userId, {
    displayMode: defaults.displayMode,
    broadcastScopeKey: defaults.rangeKey,
    personalScopeKey: defaults.rangeKey,
    scopeKey: defaults.scopeKey,
  });
}

function stripMentions(text) {
  let s = text
    // HTMLエンティティ
    .replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&lt;/g, "").replace(/&gt;/g, "")
    // SlackリンクURL: <https://...|表示テキスト> → 表示テキストを残す
    .replace(/<https?:\/\/[^|>]*\|([^>]+)>/g, "$1")
    // ラベルなしURL: <https://...> → 除去
    .replace(/<https?:\/\/[^>]+>/g, "")
    // Slackメンション・チャンネル
    .replace(/<@[^>]+>/g, " ")
    .replace(/<!subteam\^[^>]+>/g, " ")
    .replace(/<!channel>/g, " ")
    .replace(/<!here>/g, " ")
    .replace(/<!everyone>/g, " ")
    // 絵文字コード（日本語含む）: :woman-bowing: :女性土下座:
    .replace(/:[^\s:]{1,40}:/g, "")
    // @ハンドル（英数字）
    .replace(/[@＠][\w][\w.-]*/g, " ")
    // @日本語名（+姓）(+/英名）— 例: @土井 燎/Kagari Doi
    .replace(/[@＠][^\x00-\x7F]+(?:\s+[^\x00-\x7F]+)*(?:\s*\/\s*[A-Za-z\s.]+)?/g, " ")
    // 残りのURL
    .replace(/https?:\/\/\S+/g, "");

  // CC:/FYI: プレフィックスと @メンション を先頭から繰り返し除去
  for (let i = 0; i < 6; i++) {
    const before = s;
    s = s
      .replace(/^\s*(CC|FYI|Cc|fyi|cc)\s*:?\s*/iu, "")
      .replace(/^(\s*@\S+(\s+\S*\/\S+)?(\s+[A-Za-z]\S*)?)+\s*/u, "");
    if (s === before) break;
  }

  // 先頭の記号をクリーンアップ
  s = s.replace(/^[\s:：・>\-\[\]【】♪　]+/, "");

  return s.replace(/\s+/g, " ").trim();
}

function taskLineForHome(task) {
  const rawTitle = String(task.title || "").trim();
  const rawDesc = String(task.description || "").trim();

  // タイトル・本文どちらもメンションを除去して表示
  let preview = stripMentions(rawTitle) || stripMentions(rawDesc) || "（本文なし）";

  // @表記を抑止（通知防止）
  preview = preview.replace(/@/g, "＠");

  // ★ 最大5行（読み取りのための split のみ。文字列自体は置換しない）
  const MAX_LINES = 5;
  const lines = preview.split(/\r?\n/);
  if (lines.length > MAX_LINES) {
    preview = lines.slice(0, MAX_LINES).join("\n") + "\n…";
  }

  // ★ 最大200文字程度
  const MAX_PREVIEW_CHARS = 200;
  if (preview.length > MAX_PREVIEW_CHARS) {
    preview = preview.slice(0, MAX_PREVIEW_CHARS) + "…";
  }

  if (!preview) preview = "（本文なし）";
  return preview;
}

function buildHomeFiltersModalView({ teamId, userId, st, deptText: _deptText, groups = [], personalFilters = [], dashTeams = [], selectedDeptId = null, myDeptIds = new Set(), isAdmin = false }) {
  const rangeKey = st.broadcastScopeKey || "to_me";
  const scopeKey = st.scopeKey || "active";

  // Build dash dept/team options
  const deptRows = dashTeams.filter(t => !t.parent_id);
  const childMap = {};
  for (const t of dashTeams) {
    if (t.parent_id) { if (!childMap[t.parent_id]) childMap[t.parent_id] = []; childMap[t.parent_id].push(t); }
  }
  const visibleDepts = isAdmin
    ? [...deptRows.filter(d => !d.hidden && myDeptIds.has(d.id)), ...deptRows.filter(d => !d.hidden && !myDeptIds.has(d.id))]
    : deptRows.filter(d => !d.hidden && myDeptIds.has(d.id));

  const rangeOptions = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    { text: { type: "plain_text", text: "範囲：自分が発行" }, value: "requested_by_me" },
    ...personalFilters.map((f) => ({ text: { type: "plain_text", text: `★ ${f.name}` }, value: `pf:${f.id}` })),
    ...visibleDepts.map((d) => ({ text: { type: "plain_text", text: `🏢 ${d.name}` }, value: `dash_dept:${d.id}` })),
    ...(isAdmin ? [{ text: { type: "plain_text", text: "範囲：すべて" }, value: "all" }] : []),
  ];

  const stateOptions = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];

  // Determine which dept is "active" for showing team sub-selector
  // rangeKey may be dash_dept:X or dash_team:X; selectedDeptId overrides (from block_actions update)
  let activeDeptId = selectedDeptId;
  if (!activeDeptId) {
    if (rangeKey.startsWith("dash_dept:")) activeDeptId = rangeKey.slice(10);
    else if (rangeKey.startsWith("dash_team:")) {
      const tid = rangeKey.slice(10);
      const t = dashTeams.find(dt => dt.id === tid);
      if (t && t.parent_id) activeDeptId = t.parent_id;
      else if (t) activeDeptId = t.id;
    }
  }

  const activeChildren = activeDeptId ? (childMap[activeDeptId] || []) : [];

  const blocks = [
    {
      type: "input",
      block_id: "range",
      label: { type: "plain_text", text: "範囲" },
      dispatch_action: true,
      element: {
        type: "static_select",
        action_id: "home_filters_range",
        options: rangeOptions,
        initial_option: rangeOptions.find((o) => o.value === rangeKey) || rangeOptions[0],
      },
    },
    {
      type: "input",
      block_id: "state",
      label: { type: "plain_text", text: "状態" },
      element: {
        type: "static_select",
        action_id: "home_filters_state",
        options: stateOptions,
        initial_option: stateOptions.find((o) => o.value === scopeKey) || stateOptions[0],
      },
    },
  ];

  // Show team sub-selector only when a dept is selected and has children
  if (activeChildren.length > 0) {
    const teamOptions = [
      { text: { type: "plain_text", text: "チーム：すべて" }, value: `dash_dept:${activeDeptId}` },
      ...activeChildren.map(c => ({ text: { type: "plain_text", text: c.name }, value: `dash_team:${c.id}` })),
    ];
    // initial: current rangeKey if it matches a child, else "すべて"
    const initialTeam = teamOptions.find(o => o.value === rangeKey) || teamOptions[0];
    blocks.push({
      type: "input",
      block_id: "team_sub",
      optional: true,
      label: { type: "plain_text", text: "チーム" },
      element: {
        type: "static_select",
        action_id: "home_filters_team_sub",
        options: teamOptions,
        initial_option: initialTeam,
      },
    });
  }

  return {
    type: "modal",
    callback_id: "home_filters_modal",
    private_metadata: JSON.stringify({ teamId, userId }),
    title: { type: "plain_text", text: "絞り込み" },
    submit: { type: "plain_text", text: "適用" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

app.action("open_home_filters_modal", async ({ ack, body, client }) => {
  await ack();

  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);

  const [groups, personalFilters, dashTeamsRes, myTeamResModal, dashboardRoleModal] = await Promise.all([
    searchUsergroups(""),
    dbListPersonalFilters(teamId, userId).catch(() => []),
    dbQuery(`SELECT id, name, parent_id, hidden FROM dash_teams WHERE team_id=$1 ORDER BY name ASC`, [teamId]).catch(() => ({ rows: [] })),
    dbQuery(
      `SELECT DISTINCT dt.id, dt.parent_id FROM dash_team_members dtm
       JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
       WHERE dtm.team_id=$1 AND dtm.user_id=$2`,
      [teamId, userId]
    ).catch(() => ({ rows: [] })),
    dbGetDashboardRole(teamId, userId).catch(() => "member"),
  ]);
  const dashTeams = dashTeamsRes.rows || [];
  const isAdminModal = dashboardRoleModal === "admin";
  const myDeptIds = new Set();
  for (const r of (myTeamResModal.rows || [])) {
    if (!r.parent_id) myDeptIds.add(r.id);
    else myDeptIds.add(r.parent_id);
  }

  await client.views.open({
    trigger_id: body.trigger_id,
    view: buildHomeFiltersModalView({ teamId, userId, st, groups, personalFilters, dashTeams, myDeptIds, isAdmin: isAdminModal }),
  });
});

app.action("home_filters_range", async ({ ack, body, client }) => {
  await ack();
  // Only handle if inside the modal (view present)
  if (!body.view) return;
  const view = body.view;
  const meta = safeJsonParse(view.private_metadata) || {};
  const teamId = meta.teamId;
  const userId = meta.userId;
  if (!teamId || !userId) return;

  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);
  const selectedRange = body.actions?.[0]?.selected_option?.value || "to_me";

  // Only need dash teams if a dept was selected
  let dashTeams = [];
  if (selectedRange.startsWith("dash_dept:") || selectedRange.startsWith("dash_team:")) {
    const res = await dbQuery(`SELECT id, name, parent_id, hidden FROM dash_teams WHERE team_id=$1 ORDER BY name ASC`, [teamId]).catch(() => ({ rows: [] }));
    dashTeams = res.rows || [];
  }

  // Determine selectedDeptId from newly chosen range
  let selectedDeptId = null;
  if (selectedRange.startsWith("dash_dept:")) selectedDeptId = selectedRange.slice(10);
  else if (selectedRange.startsWith("dash_team:")) {
    const tid = selectedRange.slice(10);
    const t = dashTeams.find(dt => dt.id === tid);
    if (t && t.parent_id) selectedDeptId = t.parent_id;
    else if (t) selectedDeptId = t.id;
  }

  const [groups, personalFilters] = await Promise.all([
    searchUsergroups(""),
    dbListPersonalFilters(teamId, userId).catch(() => []),
  ]);

  // Temporarily set the broadcastScopeKey so the modal reflects the new selection
  const stOverride = { ...st, broadcastScopeKey: selectedRange };

  await client.views.update({
    view_id: view.id,
    hash: view.hash,
    view: buildHomeFiltersModalView({ teamId, userId, st: stOverride, groups, personalFilters, dashTeams, selectedDeptId }),
  }).catch(() => {});
});

app.view("home_filters_modal", async ({ ack, body, view, client }) => {
  await ack();

  const meta = safeJsonParse(view.private_metadata) || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const userId = meta.userId || getUserIdFromBody(body);
  if (!teamId || !userId) return;

  await ensureHomeStateLoaded(teamId, userId);
  const rangeRaw =
    view?.state?.values?.range?.home_filters_range?.selected_option?.value ||
    "to_me";
  const scope =
    view?.state?.values?.state?.home_filters_state?.selected_option?.value ||
    "active";

  // If a team_sub block is present, its value takes precedence over the dept range
  const teamSubOpt = view?.state?.values?.team_sub?.home_filters_team_sub?.selected_option || null;
  const range = teamSubOpt ? teamSubOpt.value : rangeRaw;

  const deptOpt =
    view?.state?.values?.dept?.home_dept_select?.selected_option || null;
  const dept = deptOpt?.value || "all";

  setHomeState(teamId, userId, {
    broadcastScopeKey: range,
    scopeKey: scope,
    ...(range === "all" ? { deptKey: dept } : {}),
  });

  await publishHome({ client, teamId, userId });
});

async function publishHome({ client, teamId, userId }) {
  // HOME_V2=1 で新ホームUIに切替（V1は無変更で残す）
  if (process.env.HOME_V2 === "1") {
    return publishHomeV2({ client, teamId, userId });
  }
  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);
  const statuses = st.scopeKey === "done" ? DONE_STATUSES : ACTIVE_STATUSES;

  const blocks = [];

  // ✅ フィルタは Home 上のプルダウンで直接変更する（モーダル遷移を挟まない）
  const rangeKey0 = st.broadcastScopeKey || "to_me";
  const stateKey0 = st.scopeKey || "active";
  const deptKey0 = st.deptKey || "all";

  const stateOptions0 = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];

  // パーソナルフィルターと dash_teams は常に取得（範囲選択肢に使う）。部署グループは rangeKey=all のときだけ取得
  const [deptGroups, personalFilters, dashTeamsRes, myTeamRes, dashboardRole] = await Promise.all([
    rangeKey0 === "all" ? searchUsergroups("") : Promise.resolve([]),
    dbListPersonalFilters(teamId, userId).catch(() => []),
    dbQuery(`SELECT id, name, parent_id, hidden FROM dash_teams WHERE team_id=$1 ORDER BY name ASC`, [teamId]).catch(() => ({ rows: [] })),
    dbQuery(
      `SELECT DISTINCT dt.id, dt.parent_id FROM dash_team_members dtm
       JOIN dash_teams dt ON dt.id = dtm.dash_team_id AND dt.team_id = dtm.team_id
       WHERE dtm.team_id=$1 AND dtm.user_id=$2`,
      [teamId, userId]
    ).catch(() => ({ rows: [] })),
    dbGetDashboardRole(teamId, userId).catch(() => "member"),
  ]);
  const isAdmin = dashboardRole === "admin";
  const dashTeams0 = dashTeamsRes.rows || [];
  const deptRows0 = dashTeams0.filter(t => !t.parent_id);

  // 自分が所属する部署IDセット（直接所属 or 子チーム経由）
  const myDeptIds = new Set();
  for (const r of (myTeamRes.rows || [])) {
    if (!r.parent_id) myDeptIds.add(r.id);
    else myDeptIds.add(r.parent_id);
  }

  // 非adminは自分の所属部署のみ、adminは全部署（自分の所属優先）
  const visibleDepts = isAdmin
    ? [...deptRows0.filter(d => !d.hidden && myDeptIds.has(d.id)), ...deptRows0.filter(d => !d.hidden && !myDeptIds.has(d.id))]
    : deptRows0.filter(d => !d.hidden && myDeptIds.has(d.id));

  const rangeOptions0 = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    { text: { type: "plain_text", text: "範囲：自分が発行" }, value: "requested_by_me" },
    ...personalFilters.map((f) => ({ text: { type: "plain_text", text: `★ ${f.name}` }, value: `pf:${f.id}` })),
    ...visibleDepts.map((d) => ({ text: { type: "plain_text", text: `🏢 ${d.name}` }, value: `dash_dept:${d.id}` })),
    ...(isAdmin ? [{ text: { type: "plain_text", text: "範囲：すべて" }, value: "all" }] : []),
  ];
  const deptOptions = [
    { text: { type: "plain_text", text: "部署：すべて" }, value: "all" },
    { text: { type: "plain_text", text: "部署：未設定" }, value: "__none__" },
    ...deptGroups.map((g) => ({ text: { type: "plain_text", text: `@${g.handle}` }, value: g.id })),
  ];

  // dash_team:X は range ドロップダウンにない → 親部署の dash_dept:X を選択状態にする
  let rangeInitialOption0 = rangeOptions0.find((o) => o.value === rangeKey0);
  if (!rangeInitialOption0 && rangeKey0.startsWith("dash_team:")) {
    const tid = rangeKey0.slice(10);
    const t = dashTeams0.find(dt => dt.id === tid);
    const parentId = t?.parent_id || t?.id;
    rangeInitialOption0 = rangeOptions0.find(o => o.value === `dash_dept:${parentId}`);
  }
  rangeInitialOption0 = rangeInitialOption0 || rangeOptions0[0];

  const actionElements = [
    {
      type: "static_select",
      action_id: "home_broadcast_scope_select",
      options: rangeOptions0,
      initial_option: rangeInitialOption0,
    },
  ];

  if (rangeKey0 === "all") {
    actionElements.push({
      type: "static_select",
      action_id: "home_dept_select",
      options: deptOptions,
      initial_option: deptOptions.find((o) => o.value === deptKey0) || deptOptions[0],
    });
  }

  // When dept selected: show child team sub-selector inline
  if (rangeKey0.startsWith("dash_dept:") || rangeKey0.startsWith("dash_team:")) {
    const childMap0 = {};
    for (const t of dashTeams0) {
      if (t.parent_id) { if (!childMap0[t.parent_id]) childMap0[t.parent_id] = []; childMap0[t.parent_id].push(t); }
    }
    let activeDeptId0 = rangeKey0.startsWith("dash_dept:") ? rangeKey0.slice(10) : null;
    if (!activeDeptId0 && rangeKey0.startsWith("dash_team:")) {
      const tid = rangeKey0.slice(10);
      const t = dashTeams0.find(dt => dt.id === tid);
      activeDeptId0 = (t && t.parent_id) ? t.parent_id : (t ? t.id : null);
    }
    const children0 = activeDeptId0 ? (childMap0[activeDeptId0] || []) : [];
    if (children0.length > 0) {
      const teamOptions0 = [
        { text: { type: "plain_text", text: "チーム：すべて" }, value: `dash_dept:${activeDeptId0}` },
        ...children0.map(c => ({ text: { type: "plain_text", text: c.name }, value: `dash_team:${c.id}` })),
      ];
      actionElements.push({
        type: "static_select",
        action_id: "home_team_sub_select",
        options: teamOptions0,
        initial_option: teamOptions0.find(o => o.value === rangeKey0) || teamOptions0[0],
      });
    }
    // State selector for dept mode
    actionElements.push({
      type: "static_select",
      action_id: "home_scope_select",
      options: stateOptions0,
      initial_option: stateOptions0.find((o) => o.value === stateKey0) || stateOptions0[0],
    });
  } else if (rangeKey0 !== "all") {
    actionElements.push({
      type: "static_select",
      action_id: "home_scope_select",
      options: stateOptions0,
      initial_option:
        stateOptions0.find((o) => o.value === stateKey0) || stateOptions0[0],
    });
  }

  actionElements.push({
    type: "button",
    action_id: "open_task_list_modal",
    text: { type: "plain_text", text: "🔍 検索/一覧" },
    value: JSON.stringify({ teamId, userId, rangeKey: rangeKey0, scopeKey: stateKey0 }),
  });

  actionElements.push({
    type: "button",
    action_id: "open_home_task_create_modal",
    text: { type: "plain_text", text: "＋ タスク作成" },
    style: "primary",
    value: JSON.stringify({ teamId, userId }),
  });

  actionElements.push({
    type: "button",
    action_id: "open_user_settings_from_home",
    text: { type: "plain_text", text: "設定" },
    value: JSON.stringify({ teamId, userId }),
  });

  blocks.push({ type: "actions", elements: actionElements });

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
  const listFetchLimit = rangeKey === "requested_by_me" ? 200 : 60;
  // personal / broadcast を並列取得
  const [personalTasksRaw, broadcastTasksRaw] = await Promise.all([
    dbListPersonalTasksByStatusesWithScope(teamId, statuses, personalScope, userId, listFetchLimit),
    rangeKey === "to_me" || rangeKey === "requested_by_me"
      ? dbListBroadcastTasksByStatusesWithScope(teamId, statuses, rangeKey, userId, listFetchLimit)
      : dbListBroadcastTasksByStatuses(teamId, statuses, "all", listFetchLimit),
  ]);

  // ✅ 方針：依頼者=対応者 の personal タスクは「範囲=すべて」では出さない
  // - to_me / requested_by_me では今まで通り見える（自分の整理には必要）
  let personalTasks = rangeKey === "all"
    ? (personalTasksRaw || []).filter((t) => {
        const r = t?.requester_user_id;
        const a = t?.assignee_id;
        return !(r && a && r === a);
      })
    : personalTasksRaw;

  let broadcastTasks = broadcastTasksRaw;

  if (rangeKey === "to_me") {
    const fallbackBroadcastTasks = await dbListBroadcastTasksByStatuses(
      teamId,
      statuses,
      "all",
      Math.max(120, listFetchLimit * 2),
    );
    const existingIds = new Set((broadcastTasks || []).map((t) => String(t.id)));

    const candidates = (fallbackBroadcastTasks || []).filter(
      (t) => !existingIds.has(String(t.id)),
    );
    if (candidates.length > 0) {
      // 完了済みタスクを一括取得（N回 → 1回）
      const candidateIds = candidates.map((t) => t.id);
      const completedSet = await dbGetUserCompletedTaskIds(teamId, candidateIds, userId).catch(() => new Set());
      const assignedIds = new Set(
        (
          await Promise.all(
            candidates.map(async (task) =>
              (await dbIsUserTarget(teamId, task.id, userId)) ? String(task.id) : null,
            ),
          )
        ).filter(Boolean),
      );

      for (const task of candidates) {
        const isAssigned = assignedIds.has(String(task.id));

        if (!isAssigned) continue;

        const alreadyCompleted = completedSet.has(task.id);
        const shouldInclude =
          st.scopeKey === "done"
            ? task.status === "done" || alreadyCompleted
            : !alreadyCompleted;

        if (shouldInclude) {
          broadcastTasks.push(task);
          existingIds.add(String(task.id));
        }
      }
    }
  }

  // ★範囲=すべて かつ 部署指定 のときだけ「@mkに関わる全て」に絞る（JS側）
  if (rangeKey === "all" && deptKey && deptKey !== "all") {
    const members = deptKey.startsWith("pf:")
      ? await dbGetPersonalFilterMemberIds(teamId, deptKey.slice(3))
      : await getUsergroupMembers(teamId, deptKey);
    const memberSet = new Set((members || []).filter(Boolean));

    // personal: 担当者 or 依頼者 が部署メンバーに含まれるもの
    personalTasks = (personalTasks || []).filter((t) => {
      const a = t?.assignee_id;
      const r = t?.requester_user_id;
      return (a && memberSet.has(a)) || (r && memberSet.has(r));
    });

    // broadcast: (対象ユーザーに部署メンバーが含まれる) OR (依頼者が部署メンバー) OR (対象グループが一致)
    broadcastTasks = (broadcastTasks || []).filter((t) => {
      const r = t?.requester_user_id;
      if (r && memberSet.has(r)) return true;

      const gid = t?.broadcast_group_id;
      if (gid && String(gid) === String(deptKey)) return true;

      return false;
    });
  }

  // ★パーソナルフィルター範囲：フィルターメンバーで絞り込む（依頼者=対応者も除外しない）
  let pfMemberSet = null;
  if (rangeKey.startsWith("pf:")) {
    const pfId = rangeKey.slice(3);
    const members = await dbGetPersonalFilterMemberIds(teamId, pfId).catch(() => []);
    pfMemberSet = new Set((members || []).filter(Boolean));

    personalTasks = (personalTasks || []).filter((t) => {
      const a = t?.assignee_id;
      const r = t?.requester_user_id;
      return (a && pfMemberSet.has(a)) || (r && pfMemberSet.has(r));
    });

    // broadcast: 依頼者 OR task_targets に対象メンバーが含まれる
    const bcastIds = (broadcastTasks || []).map(t => t.id);
    let targetTaskIds = new Set();
    if (bcastIds.length > 0 && pfMemberSet.size > 0) {
      const ttRes = await dbQuery(
        `SELECT DISTINCT task_id::text FROM task_targets WHERE team_id=$1 AND task_id::text = ANY($2) AND user_id = ANY($3)`,
        [teamId, bcastIds, Array.from(pfMemberSet)]
      ).catch(() => ({ rows: [] }));
      targetTaskIds = new Set(ttRes.rows.map(r => String(r.task_id)));
    }

    broadcastTasks = (broadcastTasks || []).filter((t) => {
      if (pfMemberSet.has(t?.requester_user_id)) return true;
      return targetTaskIds.has(String(t.id));
    });
  }

  // ★dash_dept:X / dash_team:X 範囲：チームメンバーで絞り込む
  if (rangeKey.startsWith("dash_dept:") || rangeKey.startsWith("dash_team:")) {
    const rootId = rangeKey.startsWith("dash_dept:") ? rangeKey.slice(10) : rangeKey.slice(10);
    const subtreeIds = await dbGetDashTeamSubtree(teamId, rootId).catch(() => []);
    const memberRes = await dbQuery(
      `SELECT DISTINCT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id = ANY($2)`,
      [teamId, subtreeIds]
    ).catch(() => ({ rows: [] }));
    const memberSet = new Set(memberRes.rows.map(r => r.user_id).filter(Boolean));

    // 対応者（assignee）がチームメンバーのものだけ表示
    personalTasks = (personalTasks || []).filter((t) => memberSet.has(t?.assignee_id));

    const bcastIds = (broadcastTasks || []).map(t => t.id);
    let targetTaskIds = new Set();
    if (bcastIds.length > 0 && memberSet.size > 0) {
      const ttRes = await dbQuery(
        `SELECT DISTINCT task_id::text FROM task_targets WHERE team_id=$1 AND task_id::text = ANY($2) AND user_id = ANY($3)`,
        [teamId, bcastIds, Array.from(memberSet)]
      ).catch(() => ({ rows: [] }));
      targetTaskIds = new Set(ttRes.rows.map(r => String(r.task_id)));
    }

    // broadcast も対応者（task_targets）がチームメンバーのものだけ
    broadcastTasks = (broadcastTasks || []).filter((t) => targetTaskIds.has(String(t.id)));
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
    const requesterVisibleIds = new Set();
    if (rangeKey === "requested_by_me") {
      for (const task of tasks || []) {
        if (!task) continue;
        if (String(task.requester_user_id || "") === String(userId)) {
          requesterVisibleIds.add(String(task.id));
        }
      }
    }

    const uniqChannels = Array.from(
      new Set((tasks || []).map((t) => t.channel_id).filter(Boolean)),
    );
    const okMap = new Map(
      await Promise.all(
        uniqChannels.map(async (ch) => {
          const ok = await canUserSeeChannel({
            client,
            teamId,
            channelId: ch,
            userId,
          });
          return [ch, ok];
        }),
      ),
    );

    tasks = (tasks || []).filter((t) => {
      const ch = String(t.channel_id || "");

      // チャンネル情報が無いものは落とさない
      if (!ch) return true;

      // ✅ DM起点タスクは Home の範囲絞り込み結果をそのまま通す
      if (ch.startsWith("D")) {
        return true;
      }

      if (
        rangeKey === "requested_by_me" &&
        requesterVisibleIds.has(String(t.id))
      ) {
        return true;
      }

      // ✅ 自分あて(to_me)または pf: の personal タスクは、チャンネルを離れていても表示
      if (t.task_type === "personal" || !t.task_type) {
        if (rangeKey === "to_me" && String(t.assignee_id) === String(userId)) return true;
        if (pfMemberSet && pfMemberSet.has(String(t.assignee_id))) return true;
      }

      // 通常ルール：publicのみOK（private/その他はNG）
      return okMap.get(ch) === true;
    });
  }

  // ✅ ダッシュボード連携フィルター（個人設定で指定されたプロジェクトorチームで絞り込み）
  {
    const userSettings = await getUserSettings(teamId, userId);
    const projectFilterId = userSettings.homeProjectFilter || "";
    const teamFilterId = userSettings.homeDashTeamFilter || "";

    if (projectFilterId) {
      // プロジェクトに紐づくタスクIDだけ表示
      const ptRes = await dbQuery(
        `SELECT task_id FROM project_tasks WHERE team_id=$1 AND project_id=$2`,
        [teamId, projectFilterId],
      );
      const projectTaskIds = new Set(ptRes.rows.map((r) => String(r.task_id)));
      tasks = tasks.filter((t) => projectTaskIds.has(String(t.id)));
    } else if (teamFilterId) {
      // チームメンバーのタスクだけ表示
      const members = await dbListDashTeamMembers(teamId, teamFilterId);
      const memberIds = new Set(members.map((m) => m.user_id));
      tasks = tasks.filter((t) =>
        memberIds.has(t.assignee_id) || memberIds.has(t.requester_user_id),
      );
    }
  }

  // 表示：未完了はステータス別に分ける（完了/取り下げはまとめ）
  if (rangeKey === "to_me") {
    const hiddenAssignedTasks = [];
    const visibleKeys = new Set(
      (tasks || []).map((t) => `${t.task_type || "personal"}:${t.id}`),
    );
    const allCandidates = [...personalTasks, ...broadcastTasks].filter((task) => {
      if (!task) return false;
      const key = `${task.task_type || "personal"}:${task.id}`;
      return !visibleKeys.has(key);
    });

    // broadcastタスクの割り当て・完了チェックを並列実行
    const broadcastCandidates = allCandidates.filter((t) => t.task_type === "broadcast");
    const personalCandidates = allCandidates.filter((t) => t.task_type !== "broadcast");

    if (broadcastCandidates.length > 0) {
      const bcChecks = await Promise.all(
        broadcastCandidates.map((task) =>
          Promise.all([
            isBroadcastAssignedToUser(task, teamId, userId),
            dbHasUserCompleted(teamId, task.id, userId),
          ]).then(([isAssigned, hasCompleted]) => ({ task, isAssigned, hasCompleted })),
        ),
      );
      for (const { task, isAssigned, hasCompleted } of bcChecks) {
        if (!isAssigned) continue;
        if (st.scopeKey === "done") {
          if (task.status !== "done" && !hasCompleted) continue;
        } else if (hasCompleted) {
          continue;
        }
        hiddenAssignedTasks.push(task);
      }
    }

    for (const task of personalCandidates) {
      if (String(task.assignee_id || "") === String(userId)) {
        hiddenAssignedTasks.push(task);
      }
    }

    if (hiddenAssignedTasks.length) {
      tasks = [...tasks, ...hiddenAssignedTasks].sort(cmp);
    }
  }

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
      text: {
        type: "mrkdwn",
        text: `*✅ 完了（直近${DONE_VISIBLE_HOURS}時間）*`,
      },
    });

    if (!recentDoneTasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（直近の完了なし）" }],
      });
    } else {
      // Slack Home は100ブロック上限 → footer(3) + 余裕(3) を残す
      const DONE_BLOCK_LIMIT = 94;
      let doneShown = 0;

      for (const t of recentDoneTasks) {
        const isBroadcast = t.task_type === "broadcast";
        const canReopen =
          !isBroadcast &&
          (userId === t.requester_user_id || userId === t.assignee_id);

        // この1タスクで使うブロック数 = section + context + actions + divider
        const estimated = 4;
        if (blocks.length + estimated > DONE_BLOCK_LIMIT) break;

        const rawDesc = String(t.description || "")
          .replace(/\r\n/g, "\n")
          .trim();

        const MAX_LINES = 2;
        const MAX_CHARS = 160;

        let preview = rawDesc || "（本文なし）";
        const lines = preview.split("\n");
        if (lines.length > MAX_LINES)
          preview = lines.slice(0, MAX_LINES).join("\n") + "\n…";
        if (preview.length > MAX_CHARS)
          preview = preview.slice(0, MAX_CHARS) + "…";
        preview = noMention(preview);

        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: `${preview}` },
        });

        blocks.push({
          type: "context",
          elements: [
            {
              type: "mrkdwn",
              text: `依頼者：<@${t.requester_user_id}>　/　対応者：${assigneeDisplay(t)}　/　期限：${formatDueDateOnly(t.due_date)}`,
            },
          ],
        });

        const elems = [];
        if (canReopen) {
          elems.push({
            type: "button",
            text: { type: "plain_text", text: "未完了に戻す ↩️" },
            action_id: "reopen_task",
            value: JSON.stringify({ teamId, taskId: t.id }),
            confirm: {
              title: { type: "plain_text", text: "確認" },
              text: {
                type: "mrkdwn",
                text: "このタスクを*未完了*に戻します。",
              },
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
        doneShown++;
      }

      if (doneShown < recentDoneTasks.length) {
        blocks.push({
          type: "actions",
          elements: [{
            type: "button",
            text: { type: "plain_text", text: `…他 ${recentDoneTasks.length - doneShown}件を一覧で見る` },
            action_id: "open_task_list_modal",
            value: JSON.stringify({ teamId, userId, rangeKey: st.broadcastScopeKey || "to_me", scopeKey: "done" }),
          }],
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
    const shortNameMap = new Map();
    const viewerCompletedMap = new Map();

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
    const displayNameIds = Array.from(
      new Set([...requesterIds, ...assigneeIds].filter(Boolean)),
    );
    const broadcastTaskIdsForViewer = Array.from(
      new Set(
        (tasks || [])
          .filter((t) => rangeKey === "to_me" && t?.task_type === "broadcast")
          .map((t) => t?.id)
          .filter(Boolean),
      ),
    );

    // ★範囲=すべて(all) の時はアイコン取得しない（大量呼び出しで固まりやすいので）
    const preloads = [
      Promise.all(
        displayNameIds.map(async (uid) => {
          const full = await getUserDisplayName(teamId, uid);
          shortNameMap.set(uid, toAtShortName(full));
        }),
      ),
      // broadcastの完了済みタスクを一括取得（N回→1回のDBクエリ）
      dbGetUserCompletedTaskIds(teamId, broadcastTaskIdsForViewer, userId).then(
        (completedSet) => {
          for (const taskId of broadcastTaskIdsForViewer) {
            viewerCompletedMap.set(taskId, completedSet.has(taskId));
          }
        },
      ),
    ];

    if (rangeKey !== "all") {
      preloads.push(
        Promise.all(
          requesterIds.map(async (uid) => {
            const url = await getUserIconUrl(teamId, uid);
            if (url) requesterIconMap.set(uid, url);
          }),
        ),
      );
      preloads.push(
        Promise.all(
          assigneeIds.map(async (uid) => {
            const url = await getUserIconUrl(teamId, uid);
            if (url) assigneeIconMap.set(uid, url);
          }),
        ),
      );
    }

    await Promise.all(preloads);

    const pushTaskList = async (
      title,
      list,
      totalCount = null,
      opts = null,
    ) => {
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
        const label =
          `${titlePlain}（${count}件） ${opts.toggleLabel || ""}`.trim();

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
        return;
      }

      // ✅ 0件のときは何も出さない（UIノイズ削減）
      if (!list.length) {
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
            ? viewerCompletedMap.get(t.id) === true
            : false;

        // 詳細ボタン（共通）
        const detailBtn = {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
        };

        const needsActions =
          !showDetailOnRight && rangeKey !== "to_me"
            ? true // (詳細を下に出す)
            : rangeKey === "to_me"; // (完了/詳細が必要)

        const needsCompletedHint =
          rangeKey === "to_me" &&
          t.task_type === "broadcast" &&
          viewerCompleted;

        const estimated =
          1 + // section
          1 + // people context
          1 + // meta context
          (needsCompletedHint ? 1 : 0) + // "あなたは完了済み" context
          (needsActions ? 1 : 0) + // actions
          1; // separator context

        if (!canAdd(estimated)) break;
        // ✅ 主：タスク内容（本文）
        const compactOptions = [
          // ✅ コンパクト時：完了もここに入れる（to_me かつ 未完了のときだけ）
          ...(rangeKey === "to_me" &&
          !(t.task_type === "broadcast" && viewerCompleted)
            ? [
                {
                  text: {
                    type: "plain_text",
                    text:
                      t.task_type === "broadcast"
                        ? "自分だけ完了 ✅"
                        : "完了 ✅",
                  },
                  value: `c:${t.id}`,
                },
              ]
            : []),

          // ✅ 詳細
          {
            text: { type: "plain_text", text: "詳細を開く" },
            value: `d:${t.id}`,
          },
        ].slice(0, 5);
        const compactOverflow =
          st.displayMode === "compact"
            ? {
                accessory: {
                  type: "overflow",
                  action_id: "home_task_overflow",
                  options: compactOptions,
                },
              }
            : null;

        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: taskLineForHome(t, viewKey),
          },

          // 🖥 標準：従来どおり（範囲=all のときだけ右に詳細）
          ...(st.displayMode !== "compact" && showDetailOnRight
            ? { accessory: detailBtn }
            : {}),

          // 📱 コンパクト：右端に小さい「…」
          ...(compactOverflow || {}),
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

        // 名前取得を軽くするためのローカルキャッシュ
        function getShortAtName(userId) {
          if (!userId) return "-";
          return shortNameMap.get(userId) || "-";
        }

        // broadcast の対応者表示（assignee_label が "@田中/John" みたいな形式でも短縮する）
        const assigneeText =
          viewKey === "broadcast"
            ? assigneeDisplay(t) // ← broadcastはここに統一（shortenAssigneeLabelもここで効く）
            : assigneeId
              ? getShortAtName(assigneeId)
              : "-";

        const requesterText = requesterId
          ? getShortAtName(requesterId)
          : "-";

        const peopleElements = [];
        if (requesterIcon)
          peopleElements.push({
            type: "image",
            image_url: requesterIcon,
            alt_text: "requester",
          });
        peopleElements.push({ type: "mrkdwn", text: requesterText });
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
        const sourceHintText = !t?.message_ts
          ? "Home \u304b\u3089\u4f5c\u6210"
          : "";
        const hasSourceMessage = !!(t?.source_permalink && t?.message_ts);
        const linkText = hasSourceMessage
          ? `🔗 <${t.source_permalink}|元メッセージへ>`
          : "";

        const metaElems = [];
        if (dueText) metaElems.push({ type: "mrkdwn", text: dueText });
        if (linkText) metaElems.push({ type: "mrkdwn", text: linkText });
        if (sourceHintText)
          metaElems.push({ type: "mrkdwn", text: sourceHintText });

        blocks.push({
          type: "context",
          elements: metaElems.length
            ? metaElems
            : [{ type: "mrkdwn", text: " " }],
        });

        // ✅ Homeの完了ボタンは「範囲=自分あて（to_me）」の時だけ
        if (rangeKey !== "to_me") {
          // 📱コンパクトは右端「…」に詳細が入ってるので actions を出さない
          if (st.displayMode !== "compact" && !showDetailOnRight) {
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
            if (st.displayMode !== "compact" && !showDetailOnRight) {
              blocks.push({
                type: "actions",
                elements: [detailBtn],
              });
            }
          } else {
            // 完了 +（必要なら）詳細（自分あての時だけ）
            if (st.displayMode === "standard") {
              // 🖥 標準：今まで通り（PC快適性維持）
              const elems = [
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
                },
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({
                    teamId,
                    taskId: t.id,
                    origin: "home",
                  }),
                },
              ];
              blocks.push({ type: "actions", elements: elems });
            } else {
            }
          }
        }

        blocks.push({ type: "divider" });
        shown++;
      }

      if (shown < list.length && canAdd(2)) {
        blocks.push({
          type: "actions",
          elements: [{
            type: "button",
            text: { type: "plain_text", text: `…他 ${list.length - shown}件を一覧で見る` },
            action_id: "open_task_list_modal",
            value: JSON.stringify({ teamId, userId, rangeKey, scopeKey: st.scopeKey || "active" }),
          }],
        });
        blocks.push({ type: "divider" });
      }
    };

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

    await pushTaskList("*🚨 期限切れ*", overdueVisible, overdueTotal, {
      toggleAction: {
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "overdue" }),
      },
      toggleLabel: overdueFolded ? "▽" : "△",
      folded: overdueFolded,
    });

    if (!overdueFolded && isAllRange && overdueTotal > MORE_LIMIT) {
      const hidden = Math.max(0, overdueTotal - overdueVisible.length);
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: {
              type: "plain_text",
              text: overdueExpanded
                ? "閉じる"
                : `もっと見る（残り${hidden}件）`,
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
  const RESERVE = FOOTER_BLOCKS.length; // 3

  // ✅ blocks が多すぎる場合、末尾が切られて「スクロールできない」原因になるので削る
  if (blocks.length > SLACK_BLOCK_LIMIT - RESERVE) {
    // splice(keep) → keep 個残す。その後 context(1) + actions(1) + footer(3) = 5 を追加 → keep + 5 <= 100
    // なので keep <= 95
    const keep = SLACK_BLOCK_LIMIT - RESERVE - 2; // = 95
    blocks.splice(keep);

    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "⚠️ 表示件数が多いため一部を省略しました。フィルタ/折り畳みで絞ると下まで見やすくなるよ。",
        },
      ],
    });
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          action_id: "open_task_list_modal",
          text: { type: "plain_text", text: "一覧を開く" },
          value: JSON.stringify({
            teamId,
            userId,
            rangeKey,
            scopeKey: st.scopeKey || "active",
          }),
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

// ============================================================================
// publishHomeV2: 期限グルーピング + 色分け + クイック完了の新ホーム
// HOME_V2=1 で有効化。データ取得は最小限（rangeKey: to_me/requested_by_me/all、
// 部署フィルタ・パーソナルフィルタは詳細フィルタモーダルからのみ）。
// ============================================================================
async function publishHomeV2({ client, teamId, userId }) {
  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);
  const statuses = st.scopeKey === "done" ? DONE_STATUSES : ACTIVE_STATUSES;
  const rangeKey = st.broadcastScopeKey || "to_me";
  const scopeKey = st.scopeKey || "active";
  const isDoneView = scopeKey === "done";

  const personalScope = (rangeKey === "to_me" || rangeKey === "requested_by_me") ? rangeKey : "all";
  const fetchLimit = 200;

  const [personalRaw, broadcastRaw] = await Promise.all([
    dbListPersonalTasksByStatusesWithScope(teamId, statuses, personalScope, userId, fetchLimit),
    (rangeKey === "to_me" || rangeKey === "requested_by_me")
      ? dbListBroadcastTasksByStatusesWithScope(teamId, statuses, rangeKey, userId, fetchLimit)
      : dbListBroadcastTasksByStatuses(teamId, statuses, "all", fetchLimit),
  ]);

  // to_me で broadcast を補完取得（V1と同様、assigned だが完了済みでないものを拾う）
  let broadcastTasks = broadcastRaw || [];
  if (rangeKey === "to_me") {
    const fallback = await dbListBroadcastTasksByStatuses(teamId, statuses, "all", Math.max(120, fetchLimit * 2));
    const existing = new Set(broadcastTasks.map(t => String(t.id)));
    const candidates = (fallback || []).filter(t => !existing.has(String(t.id)));
    if (candidates.length) {
      const ids = candidates.map(t => t.id);
      const completedSet = await dbGetUserCompletedTaskIds(teamId, ids, userId).catch(() => new Set());
      const assignedIds = new Set((await Promise.all(
        candidates.map(async t => (await dbIsUserTarget(teamId, t.id, userId)) ? String(t.id) : null)
      )).filter(Boolean));
      for (const task of candidates) {
        if (!assignedIds.has(String(task.id))) continue;
        const alreadyCompleted = completedSet.has(task.id);
        const shouldInclude = isDoneView ? (task.status === "done" || alreadyCompleted) : !alreadyCompleted;
        if (shouldInclude) broadcastTasks.push(task);
      }
    }
  }

  // 重複排除 + ソート
  const seen = new Set();
  const tasks = [];
  const all = [...(personalRaw || []), ...broadcastTasks];
  all.sort((a, b) => {
    const at = a?.due_date ? new Date(a.due_date).getTime() : null;
    const bt = b?.due_date ? new Date(b.due_date).getTime() : null;
    if (at === null && bt !== null) return 1;
    if (at !== null && bt === null) return -1;
    if (at !== null && bt !== null && at !== bt) return at - bt;
    const ac = a?.created_at ? new Date(a.created_at).getTime() : 0;
    const bc = b?.created_at ? new Date(b.created_at).getTime() : 0;
    return bc - ac;
  });
  for (const t of all) {
    const k = `${t.task_type || "personal"}:${t.id}`;
    if (seen.has(k)) continue;
    seen.add(k);
    tasks.push(t);
  }

  // JST 日付ユーティリティ
  const todayYmd = todayJstYmd();
  const addDaysYmd = (n) => {
    const d = new Date();
    d.setDate(d.getDate() + n);
    const j = new Date(d.getTime() + (d.getTimezoneOffset() + 9 * 60) * 60000);
    return j.toISOString().slice(0, 10);
  };
  const weekEndYmd = addDaysYmd(7);

  const dueYmdOf = (t) => slackDateYmd(t?.due_date) || (typeof t?.due_date === "string" ? t.due_date.slice(0, 10) : "");

  // グルーピング: 期限切れ / 今日中 / 明日以降 / 期限なし
  const groups = {
    overdue: [],
    today: [],
    later: [],
    noDue: [],
    done: [],
  };
  for (const t of tasks) {
    if (isDoneView) { groups.done.push(t); continue; }
    const ymd = dueYmdOf(t);
    if (!ymd) groups.noDue.push(t);
    else if (ymd < todayYmd) groups.overdue.push(t);
    else if (ymd === todayYmd) groups.today.push(t);
    else groups.later.push(t);
  }

  const blocks = [];

  // ── ヘッダー & サマリ ─────────────────────────
  blocks.push({
    type: "header",
    text: { type: "plain_text", text: isDoneView ? "🐶 完了タスク" : "🐶 タスクホーム" },
  });

  if (!isDoneView) {
    const overdueN = groups.overdue.length;
    const todayN = groups.today.length;
    const laterN = groups.later.length;
    const totalN = overdueN + todayN + laterN + groups.noDue.length;
    blocks.push({
      type: "context",
      elements: [{
        type: "mrkdwn",
        text: `📊 未完了 *${totalN}* 件　/　🔴 期限切れ *${overdueN}*　🟡 今日中 *${todayN}*　🟢 明日以降 *${laterN}*`,
      }],
    });
  } else {
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: `📊 完了 *${groups.done.length}* 件（最近）` }],
    });
  }

  // ── フィルタ行 ─────────────────────────────
  const stateOptions = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];
  const rangeOptions = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    { text: { type: "plain_text", text: "範囲：自分が発行" }, value: "requested_by_me" },
    { text: { type: "plain_text", text: "範囲：すべて" }, value: "all" },
  ];

  blocks.push({
    type: "actions",
    elements: [
      {
        type: "static_select",
        action_id: "home_broadcast_scope_select",
        options: rangeOptions,
        initial_option: rangeOptions.find(o => o.value === rangeKey) || rangeOptions[0],
      },
      {
        type: "static_select",
        action_id: "home_scope_select",
        options: stateOptions,
        initial_option: stateOptions.find(o => o.value === scopeKey) || stateOptions[0],
      },
      {
        type: "button",
        action_id: "open_task_list_modal",
        text: { type: "plain_text", text: "🔍 検索/一覧" },
        value: JSON.stringify({ teamId, userId, rangeKey, scopeKey }),
      },
      {
        type: "button",
        action_id: "open_home_task_create_modal",
        text: { type: "plain_text", text: "＋ タスク作成" },
        style: "primary",
        value: JSON.stringify({ teamId, userId }),
      },
      {
        type: "button",
        action_id: "open_user_settings_from_home",
        text: { type: "plain_text", text: "⚙️ 設定" },
        value: JSON.stringify({ teamId, userId }),
      },
    ],
  });
  blocks.push({ type: "divider" });

  // ── タスクレンダリング ──────────────────────
  const SLACK_BLOCK_LIMIT = 100;
  const RESERVE = 4; // 末尾フッター余裕

  const renderTaskRow = (t, _dueIcon, dueLabel) => {
    const payload = JSON.stringify({ teamId, taskId: t.id, origin: "home" });
    // taskLineForHome でメンション・絵文字コード等を除去、最大2行で表示
    const cleaned = taskLineForHome(t);
    const cleanedLines = cleaned.split(/\r?\n/).filter(Boolean);
    let titleText = "";
    if (cleanedLines.length === 0) {
      titleText = "（本文なし）";
    } else if (cleanedLines.length === 1) {
      // 1行が長ければ約60字でやさしく折り返し
      const line = cleanedLines[0];
      if (line.length > 70) {
        titleText = line.slice(0, 60) + "\n" + line.slice(60, 120) + (line.length > 120 ? "…" : "");
      } else {
        titleText = line;
      }
    } else {
      titleText = cleanedLines.slice(0, 2).join("\n");
      if (titleText.length > 140) titleText = titleText.slice(0, 140) + "…";
    }

    const isDone = t.status === "done";
    const broadcastMark = t.task_type === "broadcast" ? "（一斉）" : "";

    // section: タイトル（太字なし）+ メタ情報を改行で結合
    const sectionBlock = {
      type: "section",
      text: {
        type: "mrkdwn",
        text: `${titleText}\n_<@${t.requester_user_id}> → ${assigneeDisplay(t)}${broadcastMark}　·　📅 ${dueLabel}_`,
      },
    };

    // actions: 完了 + 詳細（下に並ぶ）
    const actionElements = [];
    if (!isDone) {
      actionElements.push({
        type: "button",
        style: "primary",
        text: { type: "plain_text", text: "✅ 完了" },
        action_id: "complete_task",
        value: payload,
        confirm: {
          title: { type: "plain_text", text: "完了にしますか？" },
          text: { type: "mrkdwn", text: `*${titleText.slice(0, 80)}*` },
          confirm: { type: "plain_text", text: "完了する" },
          deny: { type: "plain_text", text: "キャンセル" },
        },
      });
    }
    actionElements.push({
      type: "button",
      text: { type: "plain_text", text: "📄 詳細" },
      action_id: "open_detail_modal",
      value: payload,
    });

    return [sectionBlock, { type: "actions", elements: actionElements }];
  };

  const sectionConfig = isDoneView
    ? [{ key: "done", icon: "✅", title: "完了済み" }]
    : [
        { key: "overdue", icon: "🔴", title: "期限切れ" },
        { key: "today",   icon: "🟡", title: "今日中" },
        { key: "later",   icon: "🟢", title: "明日以降" },
        { key: "noDue",   icon: "⚪", title: "期限なし" },
      ];

  const SECTION_LIMIT = 8; // 各セクション最大表示
  const TASK_BLOCKS = 3; // section + actions + divider

  let sectionIdx = 0;
  for (const sec of sectionConfig) {
    const list = groups[sec.key];
    if (!list.length) continue;

    // セクション境目を強調: 上に余白 → header → 件数 context
    if (sectionIdx > 0) {
      blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: "　" }] });
    }
    sectionIdx++;
    blocks.push({
      type: "header",
      text: { type: "plain_text", text: `${sec.icon}  ${sec.title}  ${sec.icon}`, emoji: true },
    });
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: `_${list.length}件_` }],
    });
    blocks.push({ type: "divider" });

    const willRender = Math.min(list.length, SECTION_LIMIT);
    // 余白計算
    const remainingBudget = SLACK_BLOCK_LIMIT - RESERVE - blocks.length;
    const maxByBudget = Math.max(0, Math.floor(remainingBudget / TASK_BLOCKS));
    const shown = Math.min(willRender, maxByBudget);

    for (let i = 0; i < shown; i++) {
      const t = list[i];
      const ymd = dueYmdOf(t);
      let dueIcon = sec.icon, dueLabel = "期限なし";
      if (ymd) dueLabel = formatDueDateOnly(t.due_date) || ymd;
      if (sec.key === "today") dueLabel += "（今日）";
      if (sec.key === "overdue") dueLabel += "（期限切れ）";
      if (sec.key === "done") { dueIcon = "✅"; dueLabel = formatDueDateOnly(t.due_date) || "—"; }
      blocks.push(...renderTaskRow(t, dueIcon, dueLabel));
      if (i < shown - 1) blocks.push({ type: "divider" });
    }

    if (list.length > shown) {
      blocks.push({
        type: "actions",
        elements: [{
          type: "button",
          text: { type: "plain_text", text: `…他 ${list.length - shown} 件を一覧で見る` },
          action_id: "open_task_list_modal",
          value: JSON.stringify({ teamId, userId, rangeKey, scopeKey }),
        }],
      });
    }
  }

  if (!blocks.some(b => b.type === "section" && b.text?.text?.includes("*"))) {
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: isDoneView ? "_完了タスクなし_" : "🎉 _未完了タスクなし。お疲れさまです！_" },
    });
  }

  // フッター余白
  blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: " " }] });

  await client.views.publish({
    user_id: userId,
    view: { type: "home", callback_id: "home", blocks },
  });
}

app.options("assignee_groups_select", async ({ ack, payload }) => {
  try {
    const q = payload?.value || "";
    const groups = await searchUsergroups(q);
    console.info("[options] assignee_groups_select", { q, count: groups.length, handles: groups.map(g => g.handle) });
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
      const members = deptKey.startsWith("pf:")
        ? await dbGetPersonalFilterMemberIds(teamId, deptKey.slice(3))
        : await getUsergroupMembers(teamId, deptKey);
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

app.action("my_tasks_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const scopeKey = meta.scopeKey || "active";
    const searchQuery = meta.searchQuery || "";

    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: selected,
      scopeKey,
      searchQuery,
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
    const searchQuery = meta.searchQuery || "";

    // 📱スマホ対策：まずローディング表示（ここが体感に効く！）
    const loadingView = {
      type: "modal",
      callback_id: "task_list_modal",
      title: { type: "plain_text", text: "タスク一覧" },
      close: { type: "plain_text", text: "閉じる" },
      private_metadata: body.view?.private_metadata || "{}",
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text: "読み込み中…⏳" },
        },
      ],
    };

    try {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: loadingView,
      });
    } catch (_) {}

    // 本描画（DB/整形が重くても、ユーザーは「反応した」って分かる）
    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey,
      scopeKey: selected,
      searchQuery,
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
      rangeKey: payload.rangeKey || "to_me",
      scopeKey: payload.scopeKey || "active",
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

app.action("home_team_sub_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const scopeKey = body.actions?.[0]?.selected_option?.value;
    if (!teamId || !userId || !scopeKey) return;
    setHomeState(teamId, userId, { broadcastScopeKey: scopeKey });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_team_sub_select error:", e?.data || e);
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
        [section]: !st.homeMore?.[section],
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
    if (section !== "overdue" && section !== "today" && section !== "later")
      return;

    const st = getHomeState(teamId, userId);
    const next = {
      homeFold: {
        ...(st.homeFold || { overdue: false, today: false, later: false }),
        [section]: !st.homeFold?.[section],
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

app.action("toggle_home_display_mode", async ({ ack, body, client }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const st = getHomeState(teamId, userId);
  const nextMode = st.displayMode === "compact" ? "standard" : "compact";
  setHomeState(teamId, userId, { displayMode: nextMode });

  await publishHome({ client, teamId, userId });
});

app.action("home_task_overflow", async ({ ack, body, client, action }) => {
  await ack();

  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const v = action?.selected_option?.value || "";
    if (!teamId || !userId || !v) return;

    // value: "c:<taskId>" | "d:<taskId>"
    const [kind, taskId] = v.split(":");
    if (!taskId) return;

    if (kind === "d") {
      await openDetailModal(client, {
        trigger_id: body.trigger_id,
        teamId,
        taskId,
        viewerUserId: userId,
        origin: "home",
        isFromModal: false,
      });
      return;
    }

    if (kind === "c") {
      // ✅ body をそのまま渡す（handleCompleteTask 側が body を期待している）
      await handleCompleteTask({ client, body, teamId, taskId });
      return;
    }
  } catch (e) {
    console.error("home_task_overflow error:", e?.data || e);
  }
});

// ================================
// Personal filter create
// ================================
function buildPersonalFilterEditorView({
  mode = "create",
  teamId,
  userId,
  filterId = null,
  filterName = "",
  memberIds = [],
}) {
  const isEdit = mode === "edit";
  return {
    type: "modal",
    callback_id: isEdit
      ? "personal_filter_edit_modal"
      : "personal_filter_create_modal",
    private_metadata: JSON.stringify({ teamId, userId, filterId }),
    title: {
      type: "plain_text",
      text: isEdit ? "フィルター編集" : "フィルター作成",
    },
    submit: {
      type: "plain_text",
      text: isEdit ? "保存" : "作成",
    },
    close: { type: "plain_text", text: "キャンセル" },
    blocks: [
      {
        type: "input",
        block_id: "filter_name",
        label: { type: "plain_text", text: "フィルター名" },
        element: {
          type: "plain_text_input",
          action_id: "value",
          placeholder: {
            type: "plain_text",
            text: "分かりやすい名前を入力",
          },
          ...(filterName ? { initial_value: filterName } : {}),
        },
      },
      {
        type: "input",
        block_id: "filter_members",
        label: { type: "plain_text", text: "メンバー" },
        element: {
          type: "multi_users_select",
          action_id: "value",
          placeholder: {
            type: "plain_text",
            text: "メンバーを選択",
          },
          ...(memberIds.length ? { initial_users: memberIds } : {}),
        },
      },
    ],
  };
}

app.action("open_personal_filter_modal", async ({ ack, body, client }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const isFromModal = body.view?.type === "modal";
  try {
    const openFn = isFromModal ? client.views.push : client.views.open;
    await openFn.call(client.views, {
      trigger_id: body.trigger_id,
      view: buildPersonalFilterEditorView({ mode: "create", teamId, userId }),
    });
  } catch (e) {
    console.error("open_personal_filter_modal error:", e?.data || e);
  }
});

app.view("personal_filter_create_modal", async ({ ack, body, view, client }) => {
  await ack();
  const meta = safeJsonParse(view.private_metadata) || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const userId = meta.userId || getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const name = view.state.values.filter_name?.value?.value?.trim();
  const memberIds = view.state.values.filter_members?.value?.selected_users || [];
  if (!name) return;

  try {
    const id = randomUUID();
    await dbCreatePersonalFilter(teamId, userId, id, name);
    await dbSetPersonalFilterMembers(teamId, id, memberIds);

    // 作成したフィルタを即座にアクティブにして Home を更新
    const filters = await dbListPersonalFilters(teamId, userId).catch(() => []);
    const previousViewId =
      body.view?.previous_view_id || body.view?.root_view_id || null;

    if (previousViewId) {
      await client.views.update({
        view_id: previousViewId,
        view: buildPersonalFilterManageView(teamId, userId, filters),
      });
    }
  } catch (e) {
    console.error("personal_filter_create_modal error:", e?.data || e);
  }
});

// ================================
// パーソナルフィルター管理（一覧・削除・名前変更）
// ================================

function buildPersonalFilterManageView(teamId, userId, filters) {
  const blocks = [
    {
      type: "section",
      text: { type: "mrkdwn", text: filters.length === 0 ? "まだフィルターがありません。" : `*${filters.length}件* のフィルターがあります。` },
      accessory: {
        type: "button",
        action_id: "open_personal_filter_modal",
        text: { type: "plain_text", text: "＋ 新規作成" },
        value: JSON.stringify({ teamId, userId }),
      },
    },
  ];

  for (const f of filters) {
    const meta = JSON.stringify({ teamId, userId, filterId: f.id, filterName: f.name });
    blocks.push({ type: "divider" });
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: `★ *${f.name}*` },
      accessory: {
        type: "overflow",
        action_id: "personal_filter_overflow",
        options: [
          { text: { type: "plain_text", text: "編集" }, value: `edit:${meta}` },
          { text: { type: "plain_text", text: "🗑️ 削除" }, value: `delete:${meta}` },
        ],
      },
    });
  }

  return {
    type: "modal",
    callback_id: "personal_filter_manage_modal",
    private_metadata: JSON.stringify({ teamId, userId }),
    title: { type: "plain_text", text: "フィルター管理" },
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

app.action("open_personal_filter_manage_modal", async ({ ack, body, client }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const isFromModal = body.view?.type === "modal";
  try {
    const filters = await dbListPersonalFilters(teamId, userId);
    const openFn = isFromModal ? client.views.push : client.views.open;
    await openFn.call(client.views, {
      trigger_id: body.trigger_id,
      view: buildPersonalFilterManageView(teamId, userId, filters),
    });
  } catch (e) {
    console.error("open_personal_filter_manage_modal error:", e?.data || e);
  }
});

app.action("personal_filter_overflow", async ({ ack, body, client, action }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const raw = action.selected_option?.value || "";
  const colonIdx = raw.indexOf(":");
  const op = raw.slice(0, colonIdx);
  const meta = safeJsonParse(raw.slice(colonIdx + 1)) || {};

  try {
    if (op === "delete") {
      await dbDeletePersonalFilter(teamId, userId, meta.filterId);
      // アクティブなフィルターが削除されたらリセット
      const st = getHomeState(teamId, userId);
      if (st.deptKey === `pf:${meta.filterId}`) {
        setHomeState(teamId, userId, { deptKey: "all" });
        await publishHome({ client, teamId, userId });
      }
      // 管理モーダルを更新
      const filters = await dbListPersonalFilters(teamId, userId);
      await client.views.update({
        view_id: body.view.id,
        view: buildPersonalFilterManageView(teamId, userId, filters),
      });
    } else if (op === "edit") {
      const memberIds = await dbGetPersonalFilterMemberIds(teamId, meta.filterId);
      await client.views.push({
        trigger_id: body.trigger_id,
        view: buildPersonalFilterEditorView({
          mode: "edit",
          teamId,
          userId,
          filterId: meta.filterId,
          filterName: meta.filterName || "",
          memberIds,
        }),
      });
    }
  } catch (e) {
    console.error("personal_filter_overflow error:", e?.data || e);
  }
});

app.view("personal_filter_edit_modal", async ({ ack, body, view, client }) => {
  await ack();
  const meta = safeJsonParse(view.private_metadata) || {};
  const { teamId, userId, filterId } = meta;
  if (!teamId || !userId || !filterId) return;

  const name = view.state.values.filter_name?.value?.value?.trim();
  const memberIds = view.state.values.filter_members?.value?.selected_users || [];
  if (!name) return;

  try {
    await dbUpdatePersonalFilter(teamId, userId, filterId, name);
    await dbSetPersonalFilterMembers(teamId, filterId, memberIds);
    const filters = await dbListPersonalFilters(teamId, userId);
    const previousViewId = body.view?.root_view_id;
    if (previousViewId) {
      await client.views.update({
        view_id: previousViewId,
        view: buildPersonalFilterManageView(teamId, userId, filters),
      });
    }

    const st = getHomeState(teamId, userId);
    if (st.deptKey === `pf:${filterId}`) {
      await publishHome({ client, teamId, userId });
    }
  } catch (e) {
    console.error("personal_filter_edit_modal error:", e?.data || e);
  }
});

app.action("open_home_task_create_modal", async ({ ack, body, client }) => {
  await ack();

  try {
    const teamId = getTeamIdFromBody(body);
    const actorUserId = getUserIdFromBody(body);
    if (!teamId || !actorUserId) return;

    await openTaskCreateModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      channelId: "",
      msgTs: "",
      actorUserId,
      includeContentInput: true,
      initialContent: "",
    });
  } catch (e) {
    console.error("open_home_task_create_modal error:", e?.data || e);
  }
});

app.action("open_user_settings_from_home", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const p = safeJsonParse(action?.value || "{}") || {};
    const teamId = p.teamId || getTeamIdFromBody(body);
    const userId = p.userId || getUserIdFromBody(body);
    if (!teamId || !userId) return;
    await openUserSettingsModal({ client, triggerId: body.trigger_id, teamId, userId });
  } catch (e) {
    console.error("open_user_settings_from_home error:", e?.data || e);
  }
});

// タスク一覧モーダル：ページネーション（前へ / 次へ）
async function handleTaskListModalPage({ ack, body, client }) {
  await ack();
  try {
    const p = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = p.teamId || getTeamIdFromBody(body);
    const userId = p.userId || getUserIdFromBody(body);
    const { rangeKey = "to_me", scopeKey = "active", page = 0, searchQuery = "" } = p;
    if (!teamId || !userId) return;
    const view = await buildTaskListModalView({ teamId, userId, rangeKey, scopeKey, page, searchQuery });
    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view });
  } catch (e) {
    console.error("task_list_modal_page error:", e?.data || e);
  }
}
app.action("task_list_modal_prev", handleTaskListModalPage);
app.action("task_list_modal_next", handleTaskListModalPage);

// 検索 input は値変更を発火しない（dispatch_action 無し）
app.action("my_tasks_search_input", async ({ ack }) => { await ack(); });

// モーダルの submit（🔍 検索）→ state から検索ワード読んで再描画
app.view("task_list_modal", async ({ ack, body, view }) => {
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const meta = safeJsonParse(view?.private_metadata || "{}") || {};
    const query = view?.state?.values?.search_block?.my_tasks_search_input?.value || "";
    const newView = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: meta.rangeKey || "to_me",
      scopeKey: meta.scopeKey || "active",
      page: 0,
      searchQuery: query,
    });
    await ack({ response_action: "update", view: newView });
  } catch (e) {
    console.error("task_list_modal submit error:", e?.data || e);
    await ack();
  }
});

// 検索クリア
app.action("my_tasks_search_clear", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: meta.rangeKey || "to_me",
      scopeKey: meta.scopeKey || "active",
      page: 0,
      searchQuery: "",
    });
    await client.views.update({ view_id: body.view.id, hash: body.view.hash, view });
  } catch (e) {
    console.error("my_tasks_search_clear error:", e?.data || e);
  }
});

  return {
    getHomeState,
    publishHome,
    publishHomeForUsers,
    setHomeState,
  };
}

module.exports = {
  registerHomeFeature,
};
