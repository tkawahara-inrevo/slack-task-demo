require("dotenv").config();
const { randomUUID } = require("crypto");
const cron = require("node-cron");
const path = require("path");
const cookieParser = require("cookie-parser");
const { app, receiver } = require("./src/slack/app");
const { registerAdminFeature } = require("./src/features/admin");
const { registerHomeFeature } = require("./src/features/home");
const { registerSettingsFeature } = require("./src/features/settings");
const { registerNotificationJobs } = require("./src/features/notifications");
const { registerReactionFeature } = require("./src/features/reaction-task");
const { registerTaskUiFeature } = require("./src/features/task-ui");
const { generateToken, registerDashboardApi } = require("./src/features/dashboard-api");
const { stampAttendance } = require("./src/features/ieyasu");
const { registerPochiAiSlack } = require("./src/features/pochi-ai-slack");
const { registerSourceDoneListener, syncTaskDoneReaction } = require("./src/features/task-source-reaction");
const { INQUIRY_CHANNEL_ID, handleInquiryMessage } = require("./src/features/crm-inquiry");
const {
  __cacheGet,
  __cacheKey,
  __cachePut,
  assigneeDisplay,
  cutAfterSlash,
  extractChannelIdFromPermalink,
  extractTsFromPermalink,
  fetchMessageTextByTs,
  formatDueDateOnly,
  getTeamIdFromBody,
  getUserIdFromBody,
  groupBy,
  isJpBusinessDayYmd,
  jstYmdFromSlackTs,
  looksLikeSlackChannelId,
  looksLikeSlackTeamId,
  looksLikeSlackUserId,
  nextJpBusinessDayFromYmd,
  noMention,
  normalizeHandle,
  parseActionMeta,
  pickNoticeText,
  safeJsonParse,
  shortenAssigneeLabel,
  shortenOneLine,
  slackDateYmd,
  statusLabel,
  toAtShortName,
  todayJstYmd,
  uniqIds,
} = require("./src/utils/common");
const {
  dbEnsureSettingsSchema,
  dbGetNotificationThread,
  dbCountCompletions,
  dbCountTargets,
  dbCreateTask,
  dbGetTaskById,
  dbGetTaskBySource,
  dbGetTeamSettings,
  dbGetThreadCard,
  dbGetUserDept,
  dbGetUserSettings,
  dbHasUserCompleted,
  dbGetUserCompletedTaskIds,
  dbInsertTaskComment,
  dbInsertTaskTargets,
  dbIsUserTarget,
  dbListBroadcastTasksByStatuses,
  dbListBroadcastTasksByStatusesWithScope,
  dbListPersonalTasksByStatusesWithScope,
  dbListTargetUserIds,
  dbListTaskComments,
  dbQuery,
  dbTransaction,
  dbReplaceTaskTargets,
  dbUpdateBroadcastCounts,
  dbUpdateStatus,
  dbUpdateTaskContent,
  dbUpdateTaskEditableFields,
  dbUpsertCompletion,
  dbUpsertNotificationThread,
  dbUpsertTeamSettings,
  dbUpsertThreadCard,
  dbUpsertUserDept,
  dbUpsertUserSettings,
  // Personal filters
  dbCreatePersonalFilter,
  dbListPersonalFilters,
  dbUpdatePersonalFilter,
  dbDeletePersonalFilter,
  dbSetPersonalFilterMembers,
  dbGetPersonalFilterMemberIds,
  // Dashboard
  dbGetDashboardRole,
  dbSetDashboardRole,
  dbListDashboardAdmins,
  dbCreateDashTeam,
  dbListDashTeams,
  dbGetDashTeam,
  dbDeleteDashTeam,
  dbUpdateDashTeam,
  dbAddDashTeamMember,
  dbRemoveDashTeamMember,
  dbListDashTeamMembers,
  dbListDashTeamMembersWithProfile,
  dbUpdateDashTeamMemberRole,
  dbGetUserSlackTitle,
  dbUserHasAdminTeamRole,
  dbGetUserDashTeamRoles,
  dbGetDashTeamSubtree,
  dbUpdateDashTeamFull,
  dbGetUserDashTeams,
  dbListDashboardVisibleUsers,
  dbReplaceDashboardVisibleUsers,
  dbListDashboardVisibleTeams,
  dbReplaceDashboardVisibleTeams,
  dbUpsertDashboardUserDirectoryMember,
  dbSetUserDirectoryActive,
  dbListDashboardUserDirectory,
  dbGetDashboardDirectoryMember,
  dbListWorkloadItems,
  dbGetWorkloadItem,
  dbCreateWorkloadItem,
  dbUpdateWorkloadItem,
  dbDeleteWorkloadItem,
  dbListWorkloadCells,
  dbSetWorkloadCells,
  dbCopyWorkloadMonth,
  dbListWorkloadCategories,
  dbUpsertWorkloadCategory,
  dbUpdateWorkloadCategory,
  dbDeleteWorkloadCategory,
  dbCreateProject,
  dbListProjects,
  dbGetProject,
  dbDeleteProject,
  dbUpdateProject,
  dbAddProjectTask,
  dbRemoveProjectTask,
  dbListProjectTasks,
  // Integrations
  dbCreateIntegration,
  dbListIntegrations,
  dbGetIntegration,
  dbUpdateIntegration,
  dbDeleteIntegration,
  dbListFieldMappings,
  dbSetFieldMappings,
  dbCreateSyncLog,
  dbUpdateSyncLog,
  dbListSyncLogs,
  // CRM
  dbCreateClient,
  dbListClients,
  dbGetClient,
  dbUpdateClient,
  dbDeleteClient,
  dbCreateClientContact,
  dbListClientContacts,
  dbUpdateClientContact,
  dbDeleteClientContact,
  dbCreateDeal,
  dbListDeals,
  dbGetDeal,
  dbUpdateDeal,
  dbDeleteDeal,
  dbAddDealMember,
  dbRemoveDealMember,
  dbListDealMembers,
  dbIsDealMember,
  dbCreateDealActivity,
  dbListDealActivities,
  dbDeleteDealActivity,
  dbCreateDealPayment,
  dbListDealPayments,
  dbUpdateDealPayment,
  dbDeleteDealPayment,
  dbAddDealTask,
  dbRemoveDealTask,
  dbListDealTasks,
  dbCreateDeliverable,
  dbListDeliverables,
  dbUpdateDeliverable,
  dbDeleteDeliverable,
  dbCreateDealPosition,
  dbListDealPositions,
  dbUpdateDealPosition,
  dbDeleteDealPosition,
  dbCreateDealMediaPlan,
  dbListDealMediaPlans,
  dbUpdateDealMediaPlan,
  dbDeleteDealMediaPlan,
  dbCreateCalcDef,
  dbListCalcDefs,
  dbUpdateCalcDef,
  dbDeleteCalcDef,
  dbPipelineSummary,
} = require("./src/db");

let resolveHomeDefaultsForHome = async () => ({
  displayMode: "standard",
  rangeKey: "to_me",
  scopeKey: "active",
});
let getUserSettingsForHome = async () => ({});

const {
  publishHome,
  publishHomeForUsers,
  setHomeState,
} = registerHomeFeature({
  app,
  assigneeDisplay,
  buildTaskListModalView: (...args) => buildTaskListModalView(...args),
  canUserSeeChannel: (...args) => canUserSeeChannel(...args),
  dbHasUserCompleted,
  dbIsUserTarget,
  dbGetUserCompletedTaskIds,
  dbListBroadcastTasksByStatuses,
  dbListBroadcastTasksByStatusesWithScope,
  dbListPersonalTasksByStatusesWithScope,
  formatDueDateOnly,
  getSubteamIdMap: (...args) => getSubteamIdMap(...args),
  getTeamIdFromBody,
  getUserDisplayName: (...args) => getUserDisplayName(...args),
  getUserIconUrl: (...args) => getUserIconUrl(...args),
  getUserIdFromBody,
  getUsergroupMembers: (...args) => getUsergroupMembers(...args),
  handleCompleteTask: (...args) => handleCompleteTask(...args),
  noMention,
  openDetailModal: (...args) => openDetailModal(...args),
  openTaskCreateModal: (...args) => openTaskCreateModal(...args),
  openUserSettingsModal: (...args) => openUserSettingsModal(...args),
  resolveHomeDefaults: (...args) => resolveHomeDefaultsForHome(...args),
  safeJsonParse,
  searchUsergroups,
  slackDateYmd,
  toAtShortName,
  todayJstYmd,
  dbQuery,
  dbListDashTeamMembers,
  getUserSettings: (...args) => getUserSettingsForHome(...args),
  dbCreatePersonalFilter,
  dbListPersonalFilters,
  dbUpdatePersonalFilter,
  dbDeletePersonalFilter,
  dbSetPersonalFilterMembers,
  dbGetPersonalFilterMemberIds,
  dbGetDashTeamSubtree,
  dbGetDashboardRole,
  randomUUID,
});

const settingsFeature = registerSettingsFeature({
  app,
  dbGetTeamSettings,
  dbGetUserSettings,
  dbUpsertTeamSettings,
  dbUpsertUserSettings,
  dbListProjects,
  dbListDashTeams,
  dbQuery,
  getTeamIdFromBody,
  getUserIdFromBody,
  publishHome,
  safeJsonParse,
  setHomeState,
});
const {
  canManageTeamSettings,
  getTeamSettings,
  isOverdueChannelNotificationEnabled,
  isReactionTaskifyEnabled,
  isUserDmEnabled,
  getUserDueSchedule,
  openTeamSettingsModal,
  openUserSettingsModal,
  resolveHomeDefaults,
  getUserSettings: getUserSettingsFromSettings,
} = settingsFeature;
resolveHomeDefaultsForHome = resolveHomeDefaults;
getUserSettingsForHome = getUserSettingsFromSettings;

// Pochi AI 機能（要約リアクション + /pochi-ask）
registerPochiAiSlack({
  app,
  getUserDisplayName,
  getTeamIdFromBody,
});

registerSourceDoneListener({
  app,
  dbGetTaskBySource,
  dbUpdateStatus,
  notifyTaskSimpleDM,
});

registerAdminFeature({
  app,
  canManageTeamSettings,
  dbHasUserCompleted,
  dbQuery,
  formatDueDateOnly,
  getTeamIdFromBody,
  getTeamSettings,
  getUserDisplayName,
  getUserIdFromBody,
  openDetailModal: (...args) => openDetailModal(...args),
  openTeamSettingsModal,
  safeJsonParse,
  statusLabel,
});

registerNotificationJobs({
  app,
  canUserReceiveDm: isUserDmEnabled,
  dbGetNotificationThread,
  cron,
  cutAfterSlash,
  dbQuery,
  dbUpsertNotificationThread,
  ensureBotInChannel,
  formatDueDateOnly,
  getSubteamIdMap,
  getUserDisplayName,
  getUsergroupMembers,
  groupBy,
  isJpBusinessDayYmd,
  isOverdueChannelNotificationEnabled,
  noMention,
  normalizeHandle,
  pickNoticeText,
  shortenAssigneeLabel,
  shortenOneLine,
  todayJstYmd,
  getUserDueSchedule,
});

const {
  extractUserGroupIdsFromBlocks,
  extractUserIdsFromBlocks,
  inferTargetsFromMessage,
} = registerReactionFeature({
  __cacheKey,
  __cachePut,
  app,
  buildAssigneeLabelRaw: (...args) => buildAssigneeLabelRaw(...args),
  buildThreadCardBlocks: (...args) => buildThreadCardBlocks(...args),
  dbCreateTask,
  dbGetTaskBySource,
  dbGetThreadCard,
  dbInsertTaskTargets,
  dbQuery,
  getSubteamIdMap: (...args) => getSubteamIdMap(...args),
  getTeamIdFromBody,
  getUsergroupMembers: (...args) => getUsergroupMembers(...args),
  isReactionTaskifyEnabled,
  noMention,
  openTaskCreateModal: (...args) => openTaskCreateModal(...args),
  prettifySlackText: (...args) => prettifySlackText(...args),
  prettifyUserMentions: (...args) => prettifyUserMentions(...args),
  publishHomeBurst,
  randomUUID,
  resolveDeptForUser: (...args) => resolveDeptForUser(...args),
  safeEphemeral: (...args) => safeEphemeral(...args),
  safeJsonParse,
  slackDateYmd,
  uniqIds,
  upsertThreadCard: (...args) => upsertThreadCard(...args),
  notifyTaskSimpleDM: (...args) => notifyTaskSimpleDM(...args),
});

const { expandTargetsFromGroups } = registerTaskUiFeature({
  app,
  __cacheGet,
  __cacheKey,
  __cachePut,
  buildAssigneeLabelRaw: (...args) => buildAssigneeLabelRaw(...args),
  buildDetailModalView: (...args) => buildDetailModalView(...args),
  buildTaskListModalView: (...args) => buildTaskListModalView(...args),
  buildThreadCardBlocks: (...args) => buildThreadCardBlocks(...args),
  canUserReceiveDm: isUserDmEnabled,
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
  ensureBotInChannel: (...args) => ensureBotInChannel(...args),
  fetchMessageTextByTs,
  formatDueDateOnly,
  getSubteamIdMap: (...args) => getSubteamIdMap(...args),
  getTeamIdFromBody,
  getUserDisplayName: (...args) => getUserDisplayName(...args),
  getUserIdFromBody,
  getUsergroupMembers: (...args) => getUsergroupMembers(...args),
  cutAfterSlash,
  noMention,
  notifyTaskSimpleDM: (...args) => notifyTaskSimpleDM(...args),
  normalizeHandle,
  openDetailModal: (...args) => openDetailModal(...args),
  parseActionMeta,
  prettifySlackText: (...args) => prettifySlackText(...args),
  prettifyUserMentions: (...args) => prettifyUserMentions(...args),
  publishHomeForUsers,
  randomUUID,
  resolveDeptForUser: (...args) => resolveDeptForUser(...args),
  safeEphemeral: (...args) => safeEphemeral(...args),
  safeJsonParse,
  slackDateYmd,
  statusLabel,
  uniqIds,
  upsertThreadCard: (...args) => upsertThreadCard(...args),
  publishHomeBurst,
});


async function publishHomeBurst(client, teamId, userIds, intervalMs = 200) {
  // 1回目は await で確実に反映してからリトライ
  await publishHomeForUsers(client, teamId, userIds, intervalMs);
  setTimeout(
    () => publishHomeForUsers(client, teamId, userIds, intervalMs),
    1500,
  );
  setTimeout(
    () => publishHomeForUsers(client, teamId, userIds, intervalMs),
    4000,
  );
}

const BACKOFFICE_TASK_THREAD_CHANNEL =
  process.env.BACKOFFICE_TASK_THREAD_CHANNEL || "C0AP0PEL5ME";

// Home蜀肴緒逕ｻ繧貞ｰ代＠縺壹▽謚輔￡繧具ｼ医せ繝槭・縺ｮ蜿肴丐驕・ｻｶ蟇ｾ遲厄ｼ・



// ================================
// <!subteam^ID> 繧・@handle 縺ｫ逶ｴ縺吶◆繧√・蜃ｦ逅・ｼ・く繝｣繝・す繝･
// ================================
const subteamCache = new Map(); // teamId -> { at, idToHandle: Map }
const SUBTEAM_CACHE_MS = 60 * 60 * 1000;
const subteamCacheInflight = new Map();

async function getSubteamIdMap(teamId) {
  const now = Date.now();
  const cached = subteamCache.get(teamId);
  if (cached && now - cached.at < SUBTEAM_CACHE_MS) return cached.idToHandle;

  if (subteamCacheInflight.has(teamId)) {
    return subteamCacheInflight.get(teamId);
  }

  const loadPromise = (async () => {
    const res = await app.client.usergroups.list({ include_users: false });
    const map = new Map();
    for (const g of res.usergroups || []) {
      if (g?.id && g?.handle) map.set(g.id, String(g.handle).replace(/^@/, ""));
    }
    subteamCache.set(teamId, { at: Date.now(), idToHandle: map });
    return map;
  })();

  subteamCacheInflight.set(teamId, loadPromise);
  try {
    return await loadPromise;
  } finally {
    subteamCacheInflight.delete(teamId);
  }
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
// 繝｡繝ｳ繧ｷ繝ｧ繝ｳ蝗樣∩逕ｨ縺ｮ螟画鋤蜃ｦ逅・
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
  await Promise.all(
    ids.map(async (uid) => {
      const name = await getUserDisplayName(teamId, uid);
      idToName[uid] = name && String(name).trim() ? String(name).trim() : uid;
    }),
  );

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
const userIconInflight = new Map();

async function getUserIconUrl(teamId, userId) {
  if (!teamId || !userId) return null;

  const key = `${teamId}:${userId}`;
  const cached = userIconCache.get(key);
  if (cached && Date.now() - cached.at < USER_ICON_CACHE_MS) return cached.url;

  if (userIconInflight.has(key)) {
    return userIconInflight.get(key);
  }

  const loadPromise = (async () => {
    const res = await app.client.users.info({ user: userId });
    const u = res?.user;
    const url =
      u?.profile?.image_24 ||
      u?.profile?.image_32 ||
      u?.profile?.image_48 ||
      null;

    userIconCache.set(key, { at: Date.now(), url });
    return url;
  })().catch(() => null);

  userIconInflight.set(key, loadPromise);
  try {
    return await loadPromise;
  } finally {
    userIconInflight.delete(key);
  }
}

// ================================
// User display name cache (for assignee labels)
// ================================
const userNameCache = new Map(); // `${teamId}:${userId}` -> { at, name }
const USER_CACHE_MS = 60 * 60 * 1000;
const userNameInflight = new Map();

async function getUserDisplayName(teamId, userId) {
  const key = `${teamId}:${userId}`;
  const cached = userNameCache.get(key);
  if (cached && Date.now() - cached.at < USER_CACHE_MS) return cached.name;

  if (userNameInflight.has(key)) {
    return userNameInflight.get(key);
  }

  const loadPromise = (async () => {
    const res = await app.client.users.info({ user: userId });
    const u = res?.user;
    const name =
      (u?.profile?.display_name && u.profile.display_name.trim()) ||
      (u?.real_name && u.real_name.trim()) ||
      (u?.name && String(u.name).trim()) ||
      userId;
    userNameCache.set(key, { at: Date.now(), name });
    return name;
  })().catch(() => userId);

  userNameInflight.set(key, loadPromise);
  try {
    return await loadPromise;
  } finally {
    userNameInflight.delete(key);
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
const deptGroupInflight = new Map();

function deptKeyFromAllHandle(handle) {
  const h = String(handle || "").replace(/^@/, "");
  return h.endsWith("-all") ? h.slice(0, -4) : h;
}

async function fetchDeptGroups(teamId) {
  const now = Date.now();
  const cached = deptGroupCache.get(teamId);
  if (cached && now - cached.at < DEPT_CACHE_TTL_MS) return cached;

  if (deptGroupInflight.has(teamId)) {
    return deptGroupInflight.get(teamId);
  }

  const loadPromise = (async () => {
    const res = await app.client.usergroups.list({ include_users: false });
    const groups = (res.usergroups || [])
      .filter((g) => g?.id && g?.handle)
      .map((g) => ({ id: g.id, handle: String(g.handle).replace(/^@/, "") }));

  // 驛ｨ鄂ｲ莉｣陦ｨ繧呈ｱｺ繧√ｋ・・・会ｼ咼EPT_ALL_HANDLES 縺後≠繧後・縺昴ｌ縺縺代√↑縺代ｌ縺ｰ "*-all" 繧貞・驛ｨ
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

  // 蜆ｪ蜈磯・ｽ搾ｼ郁､・焚謇螻槭・縺ｨ縺阪・豎ｺ螳夲ｼ・
  const deptKeys = Array.from(membersByDeptKey.keys());
  let orderedKeys = deptKeys.slice().sort((a, b) => a.localeCompare(b));
  if (DEPT_PRIORITY.length) {
    const set = new Set(deptKeys);
    orderedKeys = [];
    for (const k of DEPT_PRIORITY) if (set.has(k)) orderedKeys.push(k);
    for (const k of deptKeys.sort((a, b) => a.localeCompare(b)))
      if (!orderedKeys.includes(k)) orderedKeys.push(k);
  }

  // insertion order 繧・priority 鬆・↓謨ｴ縺医ｋ
  const rebuilt = new Map();
  for (const k of orderedKeys) rebuilt.set(k, membersByDeptKey.get(k));
  const finalMembers = new Map();
  for (const [k, v] of rebuilt.entries()) if (v) finalMembers.set(k, v);

    const next = {
      at: Date.now(),
      deptKeys: orderedKeys,
      membersByDeptKey: finalMembers,
    };
    deptGroupCache.set(teamId, next);
    return next;
  })();

  deptGroupInflight.set(teamId, loadPromise);
  try {
    return await loadPromise;
  } finally {
    deptGroupInflight.delete(teamId);
  }
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

// ================================
// DB: Tasks (+broadcast)
// ================================
// UI pieces
// ================================

async function safeEphemeral(client, channelId, userId, text) {
  try {
    await client.chat.postEphemeral({ channel: channelId, user: userId, text });
  } catch (_) {}
}

/**
 * 繝懊ャ繝医ｒ蜿ょ刈縺輔○繧九◆繧√・髢｢謨ｰ
 */
async function ensureBotInChannel({ client, channelId }) {
  const id = String(channelId || "");
  if (!id)
    return { ok: false, isPrivate: false, joined: false, reason: "no_channel" };

  if (id.startsWith("D")) {
    return { ok: true, isPrivate: false, joined: false, reason: "dm" };
  }

  try {
    const info = await client.conversations.info({ channel: id });
    const ch = info?.channel || {};
    const isPrivate = !!ch.is_private;
    const isMember = !!ch.is_member;

    if (isMember)
      return { ok: true, isPrivate, joined: false, reason: "already_member" };

    if (isPrivate) {
      return {
        ok: false,
        isPrivate: true,
        joined: false,
        reason: "private_not_member",
      };
    }

    try {
      await client.conversations.join({ channel: id });
      return { ok: true, isPrivate: false, joined: true, reason: "joined" };
    } catch (e) {
      return {
        ok: false,
        isPrivate: false,
        joined: false,
        reason: "join_failed",
        error: e,
      };
    }
  } catch (e) {
    return {
      ok: false,
      isPrivate: false,
      joined: false,
      reason: "info_failed",
      error: e,
    };
  }
}


async function notifyTaskSimpleDM(
  userId,
  task,
  headerText = "??????????????",
  kind = null,
) {
  if (!userId || !task?.team_id || !task?.id) return;

  const resolvedKind =
    kind ||
    (String(headerText || "").includes("??")
      ? "due"
      : String(headerText || "").includes("????")
        ? "comment"
        : String(headerText || "").includes("??")
          ? "completion"
          : "generic");

  if (!(await isUserDmEnabled(task.team_id, userId, resolvedKind))) return;

  try {
    const dm = await app.client.conversations.open({ users: userId });
    const channel = dm.channel?.id;
    if (!channel) return;

    const payload = JSON.stringify({ teamId: task.team_id, taskId: task.id });
    const hasSourceLink = !!task?.source_permalink;

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
            ...(hasSourceLink
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
  } catch (_) {}
}


// async function postMessageInThread(client, channelId, parentTs, text, blocks = null) {
//   if (!client || !channelId || !parentTs || !text) return;
//   await client.chat.postMessage({
//     channel: channelId,
//     thread_ts: parentTs,
//     text,
//     ...(blocks ? { blocks } : {}),
//   });
// }


function buildTaskCreateModalBlocks({
  actorUserId,
  initialUserIds = [],
  initialGroupOptions = [],
  includeContentInput = false,
  initialContent = "",
}) {
  const blocks = [
    {
      type: "input",
      optional: true,
      block_id: "title",
      label: { type: "plain_text", text: "タイトル（任意）" },
      element: {
        type: "plain_text_input",
        action_id: "title_input",
        multiline: false,
        placeholder: {
          type: "plain_text",
          text: "空欄なら本文がタイトルになります",
        },
      },
    },
    {
      type: "input",
      block_id: "requester",
      label: { type: "plain_text", text: "依頼者" },
      element: {
        type: "users_select",
        action_id: "requester_user_select",
        initial_user: actorUserId,
        placeholder: { type: "plain_text", text: "依頼者を選択" },
      },
    },
    {
      type: "input",
      optional: true,
      block_id: "assignee_users",
      label: { type: "plain_text", text: "担当者（複数可）（任意）" },
      element: {
        type: "multi_users_select",
        action_id: "assignee_users_select",
        placeholder: { type: "plain_text", text: "ユーザーを選択" },
        ...(initialUserIds.length ? { initial_users: initialUserIds } : {}),
      },
    },
    {
      type: "input",
      optional: true,
      block_id: "assignee_groups",
      label: {
        type: "plain_text",
        text: "担当グループ（例: @mk）（任意）",
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
    },
  ];

  if (includeContentInput) {
    blocks.push({
      type: "input",
      block_id: "content",
      label: { type: "plain_text", text: "タスク内容" },
      element: {
        type: "plain_text_input",
        action_id: "content_text",
        multiline: true,
        initial_value: initialContent || "",
        placeholder: {
          type: "plain_text",
          text: "タスク内容を入力",
        },
      },
    });
  }

  blocks.push({
    type: "input",
    block_id: "due",
    label: { type: "plain_text", text: "期限" },
    element: {
      type: "datepicker",
      action_id: "due_date",
      placeholder: { type: "plain_text", text: "期限" },
      initial_date: todayJstYmd(),
    },
  });

  if (!includeContentInput) {
    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "メッセージ起点の作成では、元メッセージ本文がタスク内容になります。",
        },
      ],
    });
  }

  return blocks;
}

async function openTaskCreateModal(
  client,
  {
    trigger_id,
    teamId,
    channelId = "",
    msgTs = "",
    actorUserId,
    initialUserIds = [],
    initialGroupOptions = [],
    includeContentInput = false,
    initialContent = "",
  },
) {
  await client.views.open({
    trigger_id,
    view: {
      type: "modal",
      callback_id: "task_modal",
      private_metadata: JSON.stringify({
        teamId,
        channelId,
        msgTs,
        requesterUserId: actorUserId,
        sourceMode: includeContentInput ? "standalone" : "message",
      }),
      title: { type: "plain_text", text: "タスク作成" },
      submit: { type: "plain_text", text: "作成" },
      close: { type: "plain_text", text: "キャンセル" },
      blocks: buildTaskCreateModalBlocks({
        actorUserId,
        initialUserIds,
        initialGroupOptions,
        includeContentInput,
        initialContent,
      }),
    },
  });
}



async function buildAssigneeLabelRaw(teamId, selectedUsers, groupHandles) {
  const labelParts = [];
  for (const gidHandle of groupHandles || []) {
    labelParts.push(`@${String(gidHandle).replace(/^@/, "")}`);
  }
  for (const u of selectedUsers || []) {
    const name = await getUserDisplayName(teamId, u);
    labelParts.push(`@${name}`);
  }
  return labelParts.join(" ");
}
// ================================
// Thread Card (upsert)
// ================================
async function upsertThreadCard(
  client,
  { teamId, channelId, parentTs, threadTs = null, blocks },
) {
  // parentTs 縺ｯ縲後き繝ｼ繝峨・荳諢上く繝ｼ縲搾ｼ・ 1繝｡繝・そ繝ｼ繧ｸ1蝗槭・蛻､螳壹↓菴ｿ縺・ｼ・
  const existing = await dbGetThreadCard(teamId, channelId, parentTs);
  if (existing?.card_ts) {
    try {
      await client.chat.update({
        channel: channelId,
        ts: existing.card_ts,
        text: "?????????",
        blocks,
      });
      return existing.card_ts;
    } catch (e) {
      if (e?.data?.error !== "not_found") throw e;
    }
  }

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

async function buildThreadCardBlocks({ teamId, task }) {
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
  ];
  return [
    ...common,
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
  ];
}

async function buildDetailModalView({
  teamId,
  task,
  viewerUserId,
  origin = "home",
}) {
  const raw = String(task.description || "").replace(/\r\n/g, "\n");

  const MAX_LINES = 10;
  let srcLinesRaw =
    raw.split("\n").slice(0, MAX_LINES).join("\n") || "（本文なし）";

  let srcLines = noMention(srcLinesRaw).replace(/```/g, "'''");

  const MAX_DETAIL_CHARS = 2600;
  if (srcLines.length > MAX_DETAIL_CHARS) {
    srcLines = srcLines.slice(0, MAX_DETAIL_CHARS) + "\n...";
  }

  const isBroadcast = task.task_type === "broadcast";
  const broadcastAssigneeFallback = async () => {
    if (!isBroadcast || !viewerUserId) return false;
    if (String(task.assignee_label || "").includes(`<@${viewerUserId}>`)) {
      return true;
    }
    if (task.broadcast_group_id) {
      try {
        const members = await getUsergroupMembers(teamId, task.broadcast_group_id);
        return (members || []).includes(viewerUserId);
      } catch (_) {}
    }
    return false;
  };

  const canCompletePersonal =
    !isBroadcast &&
    (viewerUserId === task.requester_user_id ||
      viewerUserId === task.assignee_id);

  const canEdit =
    task.status !== "cancelled" &&
    (isBroadcast ||
      viewerUserId === task.requester_user_id ||
      viewerUserId === task.assignee_id);

  const meta = { teamId, taskId: task.id, origin };
  let detailInitialUserIds = isBroadcast
    ? uniqIds(await dbListTargetUserIds(teamId, task.id))
    : uniqIds([task.assignee_id].filter(Boolean));
  let detailInitialGroupOptions = [];
  if (task.broadcast_group_id) {
    try {
      const subteams = await getSubteamIdMap(teamId);
      const handle = subteams.get(task.broadcast_group_id);
      if (handle) {
        detailInitialGroupOptions = [
          {
            text: {
              type: "plain_text",
              text: "@" + String(handle).replace(/^@/, ""),
            },
            value: task.broadcast_group_id,
          },
        ];
      }
      try {
        const groupMembers = await getUsergroupMembers(teamId, task.broadcast_group_id);
        const memberSet = new Set(groupMembers || []);
        detailInitialUserIds = detailInitialUserIds.filter((userId) => !memberSet.has(userId));
      } catch (_) {}
    } catch (_) {}
  }

  const blocks = [];

  if (canEdit) {
    blocks.push(
      {
        type: "input",
        block_id: "requester",
        label: { type: "plain_text", text: "依頼者" },
        element: {
          type: "users_select",
          action_id: "requester_user_select",
          placeholder: { type: "plain_text", text: "依頼者を選択" },
          initial_user: task.requester_user_id,
        },
      },
      {
        type: "input",
        optional: true,
        block_id: "assignee_users",
        label: { type: "plain_text", text: "対応者（複数可）" },
        element: {
          type: "multi_users_select",
          action_id: "assignee_users_select",
          placeholder: { type: "plain_text", text: "ユーザーを選択" },
          ...(detailInitialUserIds.length
            ? { initial_users: detailInitialUserIds }
            : {}),
        },
      },
      {
        type: "input",
        optional: true,
        block_id: "assignee_groups",
        label: { type: "plain_text", text: "対応者グループ（例: @mk）" },
        element: {
          type: "multi_external_select",
          action_id: "assignee_groups_select",
          placeholder: { type: "plain_text", text: "グループを検索" },
          min_query_length: 0,
          ...(detailInitialGroupOptions.length
            ? { initial_options: detailInitialGroupOptions }
            : {}),
        },
      },
      {
        type: "input",
        optional: true,
        block_id: "due",
        label: { type: "plain_text", text: "期日" },
        element: {
          type: "datepicker",
          action_id: "due_date",
          ...(slackDateYmd(task.due_date)
            ? { initial_date: slackDateYmd(task.due_date) }
            : {}),
          placeholder: { type: "plain_text", text: "日付を選択" },
        },
      },
      { type: "divider" },
    );
  } else {
    blocks.push(
      {
        type: "section",
        text: { type: "mrkdwn", text: `*依頼者*：<@${task.requester_user_id}>` },
      },
      {
        type: "section",
        text: { type: "mrkdwn", text: `*対応者*：${assigneeDisplay(task)}` },
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*期限*：${formatDueDateOnly(task.due_date)}`,
        },
      },
      { type: "divider" },
    );
  }

  if (!isBroadcast) {
    if (canCompletePersonal && task.status !== "cancelled") {
      if (task.status === "done") {
        blocks.push({
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "未完了に戻す" },
              action_id: "reopen_task",
              value: JSON.stringify({ teamId, taskId: task.id }),
              confirm: {
                title: { type: "plain_text", text: "確認" },
                text: {
                  type: "mrkdwn",
                  text: "このタスクを未完了に戻します。",
                },
                confirm: { type: "plain_text", text: "戻す" },
                deny: { type: "plain_text", text: "キャンセル" },
              },
            },
          ],
        });
        blocks.push({ type: "divider" });
      } else {
        blocks.push({
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "完了" },
              style: "primary",
              action_id: "complete_task",
              value: JSON.stringify({ teamId, taskId: task.id }),
            },
          ],
        });
        blocks.push({ type: "divider" });
      }
    }
  }
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: `*タスク内容*\n\`\`\`\n${srcLines}\n\`\`\`` },
  });

  const hasSourceMessage = !!(task?.source_permalink && task?.message_ts);

  if (hasSourceMessage) {
    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text: `🔗 <${task.source_permalink}|元メッセージへ>`,
      },
    });
  } else if (!task?.message_ts) {
    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "\u3053\u306e\u30bf\u30b9\u30af\u306f Home \u306e\u30bf\u30b9\u30af\u4f5c\u6210\u304b\u3089\u4f5c\u6210\u3055\u308c\u307e\u3057\u305f\u3002",
        },
      ],
    });
  }

  let __comments = [];
  try {
    __comments = await dbListTaskComments(teamId, task.id, 10);
  } catch (e) {
    console.error("load comments error", e);
  }

  blocks.push({ type: "divider" });
  blocks.push({
    type: "section",
    text: { type: "mrkdwn", text: "*📝 コメント*" },
  });

  function formatCommentCreatedAtJst(createdAt) {
    if (!createdAt) return null;

    const d = createdAt instanceof Date ? createdAt : new Date(createdAt);
    if (Number.isNaN(d.getTime())) return null;

    try {
      const parts = new Intl.DateTimeFormat("ja-JP", {
        timeZone: "Asia/Tokyo",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).formatToParts(d);

      const get = (type) => parts.find((p) => p.type === type)?.value || "";
      return `${get("year")}/${get("month")}/${get("day")} ${get("hour")}:${get("minute")}`;
    } catch (_) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${y}/${m}/${dd} ${hh}:${mm}`;
    }
  }

  if (!__comments.length) {
    blocks.push({
      type: "context",
      elements: [{ type: "mrkdwn", text: "コメントはまだありません。" }],
    });
  } else {
    for (const c of __comments) {
      const name = await getUserDisplayName(teamId, c.user_id);
      const at = formatCommentCreatedAtJst(c.created_at);

      const header = at ? `*${name}*  _(${at})_` : `*${name}*`;
      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: `${header}\n${c.comment}` },
      });
    }
  }

  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        action_id: "open_comment_modal",
        text: { type: "plain_text", text: "コメントを追加" },
        value: JSON.stringify({ teamId, taskId: task.id }),
      },
    ],
  });

  blocks.push({ type: "divider" });
  if (isBroadcast) {
    const isTarget =
      (await dbIsUserTarget(teamId, task.id, viewerUserId)) ||
      (await broadcastAssigneeFallback());
    const already = await dbHasUserCompleted(teamId, task.id, viewerUserId);

    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "未完了者を確認" },
          action_id: "open_progress_modal",
          value: JSON.stringify({ teamId, taskId: task.id }),
        },
      ],
    });

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
            text: { type: "plain_text", text: "自分を完了" },
            style: "primary",
            action_id: "complete_task",
            value: JSON.stringify({ teamId, taskId: task.id }),
          },
        ],
      });
    }
    if (task.status !== "done" && task.status !== "cancelled" && task.broadcast_group_id) {
      const elems = [
        {
          type: "button",
          text: { type: "plain_text", text: "全員を完了にする" },
          style: "primary",
          action_id: "confirm_broadcast_done",
          value: JSON.stringify({ teamId, taskId: task.id }),
          confirm: {
            title: { type: "plain_text", text: "確認" },
            text: {
              type: "mrkdwn",
              text: "まだ未完了の人がいても、このタスクを完了にします。",
            },
            confirm: { type: "plain_text", text: "完了にする" },
            deny: { type: "plain_text", text: "やめる" },
          },
        },
      ];

      blocks.push({ type: "actions", elements: elems });
    }
  }
  return {
    type: "modal",
    callback_id: "detail_modal",
    private_metadata: JSON.stringify(meta),
    title: { type: "plain_text", text: "タスク" },
    ...(canEdit ? { submit: { type: "plain_text", text: "保存" } } : {}),
    close: { type: "plain_text", text: "閉じる" },
    blocks,
  };
}

// ================================
// タスク一覧モーダル（ページング付き）
// ================================
async function buildTaskListModalView({ teamId, userId, rangeKey = "to_me", scopeKey = "active", page = 0 }) {
  const PAGE_SIZE = 18;
  const statuses = scopeKey === "done" ? ["done"] : ["in_progress"];
  const personalScope = (rangeKey === "to_me" || rangeKey === "requested_by_me") ? rangeKey : "all";
  const fetchLimit = 300;

  // rangeOptions 構築に必要なデータを並列取得
  const [personalTasksRaw, broadcastTasksRaw, personalFilters, dashTeamsRes, myTeamRes, dashboardRole] = await Promise.all([
    dbListPersonalTasksByStatusesWithScope(teamId, statuses, personalScope, userId, fetchLimit),
    (rangeKey === "to_me" || rangeKey === "requested_by_me")
      ? dbListBroadcastTasksByStatusesWithScope(teamId, statuses, rangeKey, userId, fetchLimit)
      : dbListBroadcastTasksByStatuses(teamId, statuses, "all", fetchLimit),
    dbListPersonalFilters(teamId, userId).catch(() => []),
    dbQuery(`SELECT id, name, parent_id FROM dash_teams WHERE team_id=$1 ORDER BY name ASC`, [teamId]).catch(() => ({ rows: [] })),
    dbQuery(
      `SELECT DISTINCT dt.id, dt.parent_id FROM dash_team_members dtm
       JOIN dash_teams dt ON dt.id=dtm.dash_team_id AND dt.team_id=dtm.team_id
       WHERE dtm.team_id=$1 AND dtm.user_id=$2`,
      [teamId, userId]
    ).catch(() => ({ rows: [] })),
    dbGetDashboardRole(teamId, userId).catch(() => "member"),
  ]);

  const isAdmin = dashboardRole === "admin";
  const dashTeams = dashTeamsRes.rows || [];
  const deptRows = dashTeams.filter(t => !t.parent_id);
  const myDeptIds = new Set();
  for (const r of (myTeamRes.rows || [])) {
    if (!r.parent_id) myDeptIds.add(r.id);
    else myDeptIds.add(r.parent_id);
  }
  const visibleDepts = isAdmin
    ? [...deptRows.filter(d => myDeptIds.has(d.id)), ...deptRows.filter(d => !myDeptIds.has(d.id))]
    : deptRows.filter(d => myDeptIds.has(d.id));

  // pf: / dash_dept: / dash_team: のフィルタ処理
  let personalTasks = personalTasksRaw || [];
  let broadcastTasks = broadcastTasksRaw || [];

  if (rangeKey.startsWith("pf:")) {
    const pfId = rangeKey.slice(3);
    const members = await dbGetPersonalFilterMemberIds(teamId, pfId).catch(() => []);
    const pfSet = new Set(members.filter(Boolean));
    personalTasks = personalTasks.filter(t => pfSet.has(t?.assignee_id) || pfSet.has(t?.requester_user_id));
    const bcastIds = broadcastTasks.map(t => t.id);
    let targetIds = new Set();
    if (bcastIds.length > 0 && pfSet.size > 0) {
      const ttRes = await dbQuery(
        `SELECT DISTINCT task_id::text FROM task_targets WHERE team_id=$1 AND task_id::text = ANY($2) AND user_id = ANY($3)`,
        [teamId, bcastIds, Array.from(pfSet)]
      ).catch(() => ({ rows: [] }));
      targetIds = new Set(ttRes.rows.map(r => String(r.task_id)));
    }
    broadcastTasks = broadcastTasks.filter(t => pfSet.has(t?.requester_user_id) || targetIds.has(String(t.id)));
  } else if (rangeKey.startsWith("dash_dept:") || rangeKey.startsWith("dash_team:")) {
    const rootId = rangeKey.startsWith("dash_dept:") ? rangeKey.slice(10) : rangeKey.slice(10);
    const subtreeIds = await dbGetDashTeamSubtree(teamId, rootId).catch(() => []);
    const memberRes = await dbQuery(
      `SELECT DISTINCT user_id FROM dash_team_members WHERE team_id=$1 AND dash_team_id = ANY($2)`,
      [teamId, subtreeIds]
    ).catch(() => ({ rows: [] }));
    const memberSet = new Set(memberRes.rows.map(r => r.user_id).filter(Boolean));
    personalTasks = personalTasks.filter(t => memberSet.has(t?.assignee_id));
    const bcastIds = broadcastTasks.map(t => t.id);
    let targetIds = new Set();
    if (bcastIds.length > 0 && memberSet.size > 0) {
      const ttRes = await dbQuery(
        `SELECT DISTINCT task_id::text FROM task_targets WHERE team_id=$1 AND task_id::text = ANY($2) AND user_id = ANY($3)`,
        [teamId, bcastIds, Array.from(memberSet)]
      ).catch(() => ({ rows: [] }));
      targetIds = new Set(ttRes.rows.map(r => String(r.task_id)));
    }
    broadcastTasks = broadcastTasks.filter(t => targetIds.has(String(t.id)));
  }

  const toTime = (d) => {
    if (!d) return null;
    const dt = d instanceof Date ? d : new Date(d);
    return Number.isNaN(dt.getTime()) ? null : dt.getTime();
  };
  const cmp = (a, b) => {
    const at = toTime(a.due_date), bt = toTime(b.due_date);
    if (at === null && bt !== null) return 1;
    if (at !== null && bt === null) return -1;
    if (at !== null && bt !== null && at !== bt) return at - bt;
    const ac = toTime(a.created_at), bc = toTime(b.created_at);
    if (ac !== null && bc !== null && ac !== bc) return bc - ac;
    return String(b.id || "").localeCompare(String(a.id || ""));
  };

  const seen = new Set();
  const tasks = [];
  for (const t of [...personalTasks, ...broadcastTasks].sort(cmp)) {
    const key = `${t.task_type || "personal"}:${t.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    tasks.push(t);
  }

  const total = tasks.length;
  const start = page * PAGE_SIZE;
  const pageTasks = tasks.slice(start, start + PAGE_SIZE);

  // rangeOptions: Home と同じ順・同じ権限ルール
  let rangeInitialOption;
  const rangeOptions = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    { text: { type: "plain_text", text: "範囲：自分が発行" }, value: "requested_by_me" },
    ...personalFilters.map(f => ({ text: { type: "plain_text", text: `★ ${f.name}` }, value: `pf:${f.id}` })),
    ...visibleDepts.map(d => ({ text: { type: "plain_text", text: `🏢 ${d.name}` }, value: `dash_dept:${d.id}` })),
    ...(isAdmin ? [{ text: { type: "plain_text", text: "範囲：すべて" }, value: "all" }] : []),
  ];
  rangeInitialOption = rangeOptions.find(o => o.value === rangeKey);
  if (!rangeInitialOption && rangeKey.startsWith("dash_team:")) {
    const tid = rangeKey.slice(10);
    const t = dashTeams.find(dt => dt.id === tid);
    const parentId = t?.parent_id || t?.id;
    rangeInitialOption = rangeOptions.find(o => o.value === `dash_dept:${parentId}`);
  }
  rangeInitialOption = rangeInitialOption || rangeOptions[0];

  const statusOptions = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];

  const meta = JSON.stringify({ teamId, userId, rangeKey, scopeKey, page });
  const blocks = [];

  blocks.push({
    type: "actions",
    elements: [
      {
        type: "static_select",
        action_id: "my_tasks_scope_select",
        options: rangeOptions,
        initial_option: rangeInitialOption,
      },
      {
        type: "static_select",
        action_id: "my_tasks_status_select",
        options: statusOptions,
        initial_option: statusOptions.find((o) => o.value === scopeKey) || statusOptions[0],
      },
    ],
  });
  blocks.push({ type: "divider" });

  if (!pageTasks.length) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "（該当するタスクなし）" } });
  }

  for (const t of pageTasks) {
    const payload = JSON.stringify({ teamId, taskId: t.id, origin: "list_modal" });
    const rawTitle = String(t.title || t.description || "（本文なし）");
    const titleText = noMention(rawTitle).slice(0, 150);
    blocks.push({
      type: "section",
      text: { type: "mrkdwn", text: titleText },
      accessory: {
        type: "button",
        text: { type: "plain_text", text: "詳細" },
        action_id: "open_detail_modal",
        value: payload,
      },
    });
    blocks.push({
      type: "context",
      elements: [{
        type: "mrkdwn",
        text: `<@${t.requester_user_id}> → ${assigneeDisplay(t)}  /  ${formatDueDateOnly(t.due_date) || "期限なし"}`,
      }],
    });
  }

  const hasPrev = page > 0;
  const hasNext = start + PAGE_SIZE < total;
  if (hasPrev || hasNext) {
    const navButtons = [];
    if (hasPrev) {
      navButtons.push({
        type: "button",
        text: { type: "plain_text", text: `← 前${PAGE_SIZE}件` },
        action_id: "task_list_modal_prev",
        value: JSON.stringify({ teamId, userId, rangeKey, scopeKey, page: page - 1 }),
      });
    }
    if (hasNext) {
      navButtons.push({
        type: "button",
        text: { type: "plain_text", text: `次${PAGE_SIZE}件 →` },
        action_id: "task_list_modal_next",
        value: JSON.stringify({ teamId, userId, rangeKey, scopeKey, page: page + 1 }),
      });
    }
    blocks.push({ type: "divider" });
    blocks.push({ type: "actions", elements: navButtons });
  }

  if (total > 0) {
    const showing = `${start + 1}〜${Math.min(start + PAGE_SIZE, total)} / ${total}件`;
    blocks.push({ type: "context", elements: [{ type: "mrkdwn", text: showing }] });
  }

  return {
    type: "modal",
    callback_id: "task_list_modal",
    private_metadata: meta,
    title: { type: "plain_text", text: "タスク一覧" },
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
  // DB・表示名解決を先に行い、trigger_id を1回だけ views.open/push に使う
  // (loading→update の2ステップはモバイル/ブラウザで描画されないことがあるため)
  const task = await dbGetTaskById(teamId, taskId);
  if (!task) return;

  const view = await buildDetailModalView({
    teamId,
    task,
    viewerUserId,
    origin,
  });

  try {
    if (isFromModal) {
      await client.views.push({ trigger_id, view });
    } else {
      await client.views.open({ trigger_id, view });
    }
    console.log("[detail_modal] opened", { teamId, taskId });
  } catch (e) {
    console.error("[detail_modal] open failed", e?.data || e);
    throw e;
  }
}

// ================================
// Home: filters
// ================================
// Home縺ｮ迥ｶ諷九ｒ菫晄戟・医Θ繝ｼ繧ｶ繝ｼ縺斐→・・

async function getTeamIdViaAuthTest(client) {
  try {
    const r = await client.auth.test();
    return r?.team_id || null;
  } catch (_) {
    return null;
  }
}

function addJpBusinessDaysYmd(baseYmd, businessDays) {
  const total = Math.max(0, Number(businessDays || 0));
  let cur = baseYmd;
  for (let i = 0; i < total; i++) {
    cur = nextJpBusinessDayFromYmd(cur);
    if (!cur) return null;
  }
  return cur;
}

async function getTaskBySourceAndBroadcastKey(
  teamId,
  channelId,
  messageTs,
  title,
  broadcastGroupId,
) {
  const res = await dbQuery(
    `
      SELECT *
      FROM tasks
      WHERE team_id=$1
        AND channel_id=$2
        AND message_ts=$3
        AND COALESCE(title, '')=$4
        AND COALESCE(broadcast_group_id, '')=$5
      ORDER BY created_at DESC
      LIMIT 1;
    `,
    [teamId, channelId, messageTs, String(title || ""), String(broadcastGroupId || "")],
  );
  return res.rows[0] || null;
}

// ================================
// Broadcast: usergroup options (external_multi_select)
// ================================
async function searchUsergroups(query) {
  const idToHandle = await getSubteamIdMap("global");
  const groups = Array.from(idToHandle.entries()).map(([id, handle]) => ({
    id,
    handle,
  }));

  const q = String(query || "")
    .toLowerCase()
    .trim();
  const filtered = !q
    ? groups
    : groups.filter((g) => g.handle.toLowerCase().includes(q));

  // 荳企剞縺ｯSlack謗ｨ螂ｨ縺ｫ蜷医ｏ縺帙※驕ｩ蠖薙↓邨槭ｋ
  return filtered.slice(0, 100);
}

// ================================
// Usergroup members cache (for Home dept filter by group_id)
// ================================
const USERGROUP_MEMBERS_CACHE_MS = 10 * 60 * 1000;
const usergroupMembersCache = new Map(); // `${teamId}:${groupId}` -> { at, users: string[] }
const usergroupMembersInflight = new Map();

async function updateUsergroupMembers(teamId, groupId, newUserIds) {
  // usergroups:write にはユーザートークンが必要なワークスペースがあるため
  // SLACK_USER_TOKEN が設定されていればそちらを優先して使う
  const { WebClient } = require("@slack/web-api");
  const client = process.env.SLACK_USER_TOKEN ? new WebClient(process.env.SLACK_USER_TOKEN) : app.client;
  const validIds = newUserIds.filter(id => /^[UW][A-Z0-9]{2,}$/i.test(String(id)));
  if (validIds.length === 0) {
    const err = new Error("cannot_make_group_empty");
    err.code = "cannot_make_group_empty";
    throw err;
  }
  await client.usergroups.users.update({
    usergroup: groupId,
    users: validIds.join(','),
  });
  // キャッシュ無効化
  const key = `${teamId}:${groupId}`;
  usergroupMembersCache.delete(key);
}

async function getUsergroupMembers(teamId, groupId) {
  if (!groupId) return [];
  const key = `${teamId}:${groupId}`;
  const cached = usergroupMembersCache.get(key);
  if (cached && Date.now() - cached.at < USERGROUP_MEMBERS_CACHE_MS)
    return cached.users || [];

  if (usergroupMembersInflight.has(key)) {
    return usergroupMembersInflight.get(key);
  }

  const loadPromise = (async () => {
    const res = await app.client.usergroups.users.list({ usergroup: groupId });
    const users = res?.users || [];
    usergroupMembersCache.set(key, { at: Date.now(), users });
    return users;
  })().catch((e) => {
    console.error("usergroups.users.list error:", e?.data || e);
    usergroupMembersCache.set(key, { at: Date.now(), users: [] });
    return [];
  });

  usergroupMembersInflight.set(key, loadPromise);
  try {
    return await loadPromise;
  } finally {
    usergroupMembersInflight.delete(key);
  }
}

// ================================
// Channel visibility cache・亥盾蜉繝√Ε繝ｳ繝阪Ν縺ｮ縺ｿ陦ｨ遉ｺ・・
// - public 縺ｧ繧ゅ後Θ繝ｼ繧ｶ繝ｼ縺悟盾蜉縺励※縺・↑縺・阪↑繧芽｡ｨ遉ｺ縺励↑縺・
// - private / DM 縺ｯ陦ｨ遉ｺ縺励↑縺・ｼ域里蟄俶婿驥晉ｶｭ謖・ｼ・
// ================================
const CHANNEL_VIS_CACHE_MS = 10 * 60 * 1000;
const channelVisCache = new Map(); // `${teamId}:${channelId}` -> { at, ok }

// user -> joined channels cache・・PI遽邏・ｼ・
const USER_JOINED_CH_CACHE_MS = 60 * 60 * 1000; // 10min → 60min: チャンネル参加状態は頻繁に変わらない
const userJoinedChCache = new Map(); // `${teamId}:${userId}` -> { at, set: Set<string> }
const userJoinedChInflight = new Map(); // 同一ユーザーへの重複API呼び出しを防ぐ

async function listUserJoinedChannelsSet(client, teamId, userId) {
  const key = `${teamId}:${userId}`;
  const cached = userJoinedChCache.get(key);
  if (cached && Date.now() - cached.at < USER_JOINED_CH_CACHE_MS)
    return cached.set;

  // 同じユーザーへの同時リクエストがあれば同じPromiseを使い回す
  if (userJoinedChInflight.has(key)) return userJoinedChInflight.get(key);

  const loadPromise = (async () => {
    const set = new Set();
    let cursor;
    try {
      do {
        const res = await client.users.conversations({
          user: userId,
          types: "public_channel,private_channel",
          limit: 200,
          cursor,
          exclude_archived: true,
        });
        for (const ch of res?.channels || []) {
          if (ch?.id) set.add(ch.id);
        }
        cursor = res?.response_metadata?.next_cursor || null;
      } while (cursor);
    } catch (e) {
      console.error("users.conversations error:", e?.data || e);
    }
    userJoinedChCache.set(key, { at: Date.now(), set });
    return set;
  })();

  userJoinedChInflight.set(key, loadPromise);
  try {
    return await loadPromise;
  } finally {
    userJoinedChInflight.delete(key);
  }
}

async function canUserSeeChannel({ client, teamId, channelId, userId }) {
  if (!channelId) return true;
  if (!userId) return false; // user蜑肴署縺ｮ蛻､螳壹↓蟇・○繧・

  // 縺ｾ縺唔D繝励Ξ繝輔ぅ繝・け繧ｹ縺ｧ鬮倬溷愛螳夲ｼ・PI遽邏・ｼ・
  const id0 = String(channelId)[0];
  if (id0 === "D") return false; // DM
  if (id0 === "G") return false; // private channel・・ome縺ｫ縺ｯ蜃ｺ縺輔↑縺・婿驥晢ｼ・

  // public channel: 蜿ょ刈縺励※縺・ｋ蝣ｴ蜷医・縺ｿ陦ｨ遉ｺ
  if (id0 === "C") {
    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  }

  // 諠ｳ螳壼､悶・ID・井ｾ具ｼ壼・譛峨メ繝｣繝ｳ繝阪Ν遲会ｼ峨・ conversations.info 縺ｧ public 繧堤｢ｺ隱阪＠縺､縺､縲∝盾蜉蛻､螳・
  const key = `${teamId}:${channelId}`;
  const cached = channelVisCache.get(key);
  if (cached && Date.now() - cached.at < CHANNEL_VIS_CACHE_MS) {
    if (!cached.ok) return false;
    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  }

  try {
    const info = await client.conversations.info({ channel: channelId });
    const ch = info?.channel;
    const isPublic = !!ch?.is_channel && !ch?.is_private;
    channelVisCache.set(key, { at: Date.now(), ok: isPublic });
    if (!isPublic) return false;

    const joined = await listUserJoinedChannelsSet(client, teamId, userId);
    return joined.has(channelId);
  } catch (_) {
    channelVisCache.set(key, { at: Date.now(), ok: false });
    return false;
  }
}


app.function(
  "josys_taskify",
  async ({ client, inputs, complete, fail, logger }) => {
    console.log("josys_taskify CALLED", inputs);

    try {
      let teamId = inputs?.team_id || inputs?.teamId || null;
      const requesterUserId =
        inputs?.requester_user_id || inputs?.requesterUserId || null;

      const channelId = inputs?.channel_id || inputs?.channelId || null;

      const assigneeUserIdRaw =
        inputs?.assignee_user_id || inputs?.assigneeUserId || null;
      const assigneeUserIdsRaw =
        inputs?.assignee_user_ids ||
        inputs?.assigneeUserIds ||
        inputs?.assignee_user_ids_csv ||
        inputs?.assigneeUserIdsCsv ||
        null;

      const dueRaw = inputs?.due_date || inputs?.dueDate || null;

      const messageLink =
        inputs?.message_link ||
        inputs?.messageLink ||
        inputs?.message_url ||
        inputs?.messageUrl ||
        inputs?.message_permalink ||
        inputs?.messagePermalink ||
        null;

      let msgTs = inputs?.message_ts || inputs?.messageTs || null;
      if (!msgTs && messageLink) {
        msgTs = extractTsFromPermalink(messageLink);
      }

      if (!teamId) {
        teamId = await getTeamIdViaAuthTest(client);
      }

      // メッセージのユーザーグループメンションからアサイン先を自動判別
      // relay-botと同じロジック：@corp-keiri → 経理G、@corp-soumu → 総務G
      const GROUP_ROUTING = {
        "S08HTC8S2EQ": { id: "S08HTC8S2EQ", handle: "corp-keiri"  }, // 経理
        "S08RD9JL137": { id: "S08RD9JL137", handle: "corp-soumu"  }, // 総務
      };
      let detectedGroup = null;
      if (channelId && msgTs) {
        try {
          const histRes = await client.conversations.history({
            channel: channelId, latest: msgTs, inclusive: true, limit: 1,
          });
          const msg = histRes.messages?.[0];
          if (msg) {
            const text = msg.text || "";
            const mentionedIds = [...text.matchAll(/<!subteam\^(S[0-9A-Z]+)>/g)].map(m => m[1]);
            for (const gid of mentionedIds) {
              if (GROUP_ROUTING[gid]) { detectedGroup = GROUP_ROUTING[gid]; break; }
            }
          }
        } catch (_) {}
      }

      const inputGroupId = String(inputs?.assignee_group_id || inputs?.assigneeGroupId || '').trim() || null;
      const corpGroupId =
        inputGroupId ||
        detectedGroup?.id ||
        process.env.CORP_SOUMU_USERGROUP_ID ||
        process.env.CORP_SYSTEM_USERGROUP_ID ||
        "";
      const corpHandle = (
        inputs?.assignee_group_handle || inputs?.assigneeGroupHandle ||
        detectedGroup?.handle ||
        process.env.CORP_SOUMU_HANDLE ||
        process.env.CORP_SYSTEM_HANDLE ||
        "corp-soumu"
      ).replace(/^@/, "");

      // assignee ids normalize
      const normalizeUserIds = (v) => {
        if (!v) return [];
        if (Array.isArray(v))
          return v.map(String).map((s) => s.trim()).filter(Boolean);
        if (typeof v === "string") {
          return v
            .split(/[\s,]+/g)
            .map((s) => s.trim())
            .filter(Boolean);
        }
        return [];
      };

      const assigneeIds = Array.from(
        new Set(
          [
            ...normalizeUserIds(assigneeUserIdsRaw),
            ...(assigneeUserIdRaw ? [String(assigneeUserIdRaw).trim()] : []),
          ].filter(Boolean),
        ),
      );

      logger?.info?.("debug vars", {
        teamId,
        requesterUserId,
        dueRaw,
        channelId,
        msgTs,
        corpGroupId,
        assigneeIdsCount: assigneeIds.length,
      });

      const missing = [];
      if (!requesterUserId) missing.push("requester_user_id");
      if (!channelId) missing.push("channel_id");
      if (!teamId) missing.push("team_id");

      if (!msgTs) missing.push("message_ts(or message_link parse)");

      if (assigneeIds.length === 0 && !corpGroupId)
        missing.push("CORP_SOUMU_USERGROUP_ID");

      if (missing.length) {
        logger?.warn?.("skipped: missing required", { missing, inputs });
        await complete({
          outputs: {
            task_id: null,
            skipped: `missing:${missing.join(",")}`,
          },
        });
        return;
      }

      const tomorrowYmd = () => {
        const d = new Date();
        d.setDate(d.getDate() + 1);
        return slackDateYmd(d);
      };
      const due = dueRaw ? slackDateYmd(dueRaw) : tomorrowYmd();
      if (!due) {
        logger?.warn?.("skipped: invalid due", { dueRaw });
        await complete({
          outputs: { task_id: null, skipped: "invalid_due" },
        });
        return;
      }

      const existing = await dbGetTaskBySource(teamId, channelId, msgTs);
      if (existing?.id) {
        logger?.info?.("already exists", { taskId: existing.id });
        await complete({
          outputs: { task_id: existing.id, skipped: "already_exists" },
        });
        return;
      }

      const rawText = await fetchMessageTextByTs(client, channelId, msgTs);
      let prettyText = "";
      try {
        prettyText = await prettifySlackText(rawText, teamId);
        prettyText = await prettifyUserMentions(prettyText, teamId);
      } catch (_) {
        prettyText = String(rawText || "");
      }
      const messageFullText = String(prettyText || rawText || "").trim();
      const title = messageFullText || "（本文なし）";
      const description = messageFullText || "";

      let permalink = "";
      try {
        const r = await client.chat.getPermalink({
          channel: channelId,
          message_ts: msgTs,
        });
        permalink = r?.permalink || "";
      } catch (e) {
        logger?.warn?.("getPermalink failed", e);
      }

      const taskId = randomUUID();

      if (assigneeIds.length === 1) {
        const only = assigneeIds[0];
        const [requesterDept, assigneeDept] = await Promise.all([
          resolveDeptForUser(teamId, requesterUserId),
          resolveDeptForUser(teamId, only),
        ]);

        const created = await dbCreateTask({
          id: taskId,
          team_id: teamId,
          channel_id: channelId,
          message_ts: msgTs,
          source_permalink: permalink || null,
          title,
          description,
          requester_user_id: requesterUserId,
          created_by_user_id: requesterUserId,
          assignee_id: only,
          assignee_label: null,
          status: "in_progress",
          due_date: due,
          requester_dept: requesterDept,
          assignee_dept: assigneeDept,
          task_type: "personal",
          broadcast_group_handle: null,
          broadcast_group_id: null,
          total_count: null,
          completed_count: 0,
          notified_at: null,
        });

        try {
          if (only && only !== requesterUserId) {
            await notifyTaskSimpleDM(only, created, "タスクが届いたよ");
          }
        } catch (e) {
          logger?.error?.("workflow step notify error", e);
        }

        try {
          await publishHomeBurst(client, teamId, [requesterUserId, only], 200);
        } catch (e) {
          logger?.warn?.("home publish error", e);
        }

        logger?.info?.("task created", { taskId });
        await complete({ outputs: { task_id: taskId } });
        return;
      }

      let targetList = [];
      if (assigneeIds.length >= 2) {
        targetList = assigneeIds;
      } else {
        const { users: usersFromGroups } = await expandTargetsFromGroups(teamId, [
          corpGroupId,
        ]);
        targetList = Array.from(usersFromGroups || new Set()).filter(Boolean);
      }

      if (!targetList.length) {
        logger?.warn?.("skipped: no targets", { corpGroupId, assigneeIds });
        await complete({ outputs: { task_id: null, skipped: "no_targets" } });
        return;
      }

      const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
      const assigneeLabel =
        assigneeIds.length >= 2
          ? targetList.map((u) => `<@${u}>`).join(" ")
          : `@${corpHandle}`;

      const created = await dbCreateTask({
        id: taskId,
        team_id: teamId,
        channel_id: channelId,
        message_ts: msgTs,
        source_permalink: permalink || null,
        title,
        description,
        requester_user_id: requesterUserId,
        created_by_user_id: requesterUserId,
        assignee_id: null,
        assignee_label: assigneeLabel,
        status: "in_progress",
        due_date: due,
        requester_dept: requesterDept,
        assignee_dept: null,
        task_type: "broadcast",
        broadcast_group_handle: assigneeIds.length >= 2 ? null : `@${corpHandle}`,
        broadcast_group_id: assigneeIds.length >= 2 ? null : corpGroupId,
        total_count: targetList.length,
        completed_count: 0,
        notified_at: null,
      });

      await dbInsertTaskTargets(teamId, taskId, targetList);
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;

      try {
        const toNotify = targetList.filter((u) => u && u !== requesterUserId);
        for (const uid of toNotify) {
          await notifyTaskSimpleDM(uid, created, "タスクが届いたよ");
        }
      } catch (e) {
        logger?.error?.("workflow step notify error", e);
      }

      try {
        const toRefresh = Array.from(
          new Set([requesterUserId, ...targetList].filter(Boolean)),
        );
        await publishHomeBurst(client, teamId, toRefresh, 200);
      } catch (e) {
        logger?.warn?.("home publish error", e);
      }

      logger?.info?.("task created", { taskId });
      await complete({ outputs: { task_id: taskId } });
    } catch (error) {
      logger?.error?.("josys_taskify failed", {
        message: error?.message,
        stack: error?.stack,
      });

      await fail({
        error: `josys_taskify failed: ${error?.message || "unknown error"}`,
      });
    }
  },
);

app.function(
  "bc_contract_send_check_taskify",
  async ({ client, inputs, complete, fail, logger }) => {
    console.log("bc_contract_send_check_taskify CALLED", inputs);

    try {
      let teamId = inputs?.team_id || inputs?.teamId || null;

      let requesterUserId =
        inputs?.requester_user_id ||
        inputs?.requesterUserId ||
        inputs?.user_id ||
        inputs?.userId ||
        null;

      let channelId = inputs?.channel_id || inputs?.channelId || null;

      const messageLink =
        inputs?.message_link ||
        inputs?.messageLink ||
        inputs?.message_url ||
        inputs?.messageUrl ||
        inputs?.message_permalink ||
        inputs?.messagePermalink ||
        null;

      let msgTs = inputs?.message_ts || inputs?.messageTs || null;
      if (!msgTs && messageLink) {
        msgTs = extractTsFromPermalink(messageLink);
      }

      if (!looksLikeSlackChannelId(channelId) && messageLink) {
        channelId = extractChannelIdFromPermalink(messageLink);
      }

      if (!teamId) {
        teamId = await getTeamIdViaAuthTest(client);
      }

      const assigneeIds = uniqIds([
        process.env.BC_CONTRACT_ASSIGNEE_USER_ID_1,
        process.env.BC_CONTRACT_ASSIGNEE_USER_ID_2,
        process.env.BC_CONTRACT_ASSIGNEE_USER_ID_3,
      ]);

      const missing = [];
      if (!requesterUserId || !looksLikeSlackUserId(requesterUserId)) {
        missing.push("requester_user_id(valid Slack user ID)");
      }
      if (!channelId || !looksLikeSlackChannelId(channelId)) {
        missing.push("channel_id(valid Slack channel ID or message_link)");
      }
      if (!teamId || !looksLikeSlackTeamId(teamId)) {
        missing.push("team_id(valid Slack team ID)");
      }
      if (!msgTs) {
        missing.push("message_ts(or message_link parse)");
      }
      if (assigneeIds.length !== 3) {
        missing.push("BC_CONTRACT_ASSIGNEE_USER_ID_1/2/3");
      }

      if (missing.length) {
        logger?.warn?.("skipped: missing required", { missing, inputs });
        await complete({
          outputs: {
            task_id: null,
            skipped: `missing:${missing.join(",")}`,
          },
        });
        return;
      }

      const existing = await dbGetTaskBySource(teamId, channelId, msgTs);
      if (existing?.id) {
        logger?.info?.("already exists", { taskId: existing.id });
        await complete({
          outputs: { task_id: existing.id, skipped: "already_exists" },
        });
        return;
      }

      let rawText = "";
      try {
        rawText = await fetchMessageTextByTs(client, channelId, msgTs);
      } catch (e) {
        logger?.warn?.("fetchMessageTextByTs failed; fallback to blank", {
          channelId,
          msgTs,
          error: e?.message,
        });
        rawText = "";
      }

      const description = String(rawText || "");
      const title = "BC 契約送付チェック";

      let permalink = "";
      try {
        if (channelId && msgTs) {
          const r = await client.chat.getPermalink({
            channel: channelId,
            message_ts: msgTs,
          });
          permalink = r?.permalink || "";
        }
      } catch (e) {
        logger?.warn?.("getPermalink failed", e);
        permalink = String(messageLink || "");
      }

      const postedYmd = jstYmdFromSlackTs(msgTs) || todayJstYmd();
      const due = nextJpBusinessDayFromYmd(postedYmd);
      if (!due) {
        logger?.warn?.("skipped: invalid due", { postedYmd });
        await complete({
          outputs: { task_id: null, skipped: "invalid_due" },
        });
        return;
      }
const taskId = randomUUID();
const [requesterDept, assigneeLabel] = await Promise.all([
  resolveDeptForUser(teamId, requesterUserId),
  buildAssigneeLabelRaw(teamId, assigneeIds, []),
]);

const created = await dbCreateTask({
  id: taskId,
  team_id: teamId,
  channel_id: channelId,
  message_ts: msgTs,
  source_permalink: permalink || messageLink || null,
  title,
  description,
  requester_user_id: requesterUserId,
  created_by_user_id: requesterUserId,
  assignee_id: null,
  assignee_label: assigneeLabel,
  status: "in_progress",
  due_date: due,
  requester_dept: requesterDept,
  assignee_dept: null,
  task_type: "broadcast",
  broadcast_group_handle: null,
  broadcast_group_id: null,
  total_count: assigneeIds.length,
  completed_count: 0,
  notified_at: null,
});

      await dbInsertTaskTargets(teamId, taskId, assigneeIds);
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;

      try {
        const toNotify = assigneeIds.filter((u) => u && u !== requesterUserId);
        for (const uid of toNotify) {
          await notifyTaskSimpleDM(uid, created, "タスクが届いたよ");
        }
      } catch (e) {
        logger?.error?.("workflow step notify error", e);
      }

      try {
        const toRefresh = Array.from(
          new Set([requesterUserId, ...assigneeIds].filter(Boolean)),
        );
        await publishHomeBurst(client, teamId, toRefresh, 200);
      } catch (e) {
        logger?.warn?.("home publish error", e);
      }

      logger?.info?.("task created", { taskId, due, postedYmd });
      await complete({
        outputs: {
          task_id: taskId,
          due_date: due,
        },
      });
    } catch (error) {
      logger?.error?.("bc_contract_send_check_taskify failed", {
        message: error?.message,
        stack: error?.stack,
      });

      await fail({
        error: `bc_contract_send_check_taskify failed: ${error?.message || "unknown error"}`,
      });
    }
  },
);

app.function(
  "cb8d79backoffice_group_taskify",
  async ({ client, inputs, complete, fail, logger }) => {
    try {
      logger?.info?.("cb8d79backoffice_group_taskify called", {
        hasInputs: !!inputs,
        inputKeys: Object.keys(inputs || {}),
      });

      let teamId = inputs?.team_id || inputs?.teamId || null;
      let requesterUserId =
        inputs?.requester_user_id ||
        inputs?.requesterUserId ||
        inputs?.user_id ||
        inputs?.userId ||
        null;
      let channelId = inputs?.channel_id || inputs?.channelId || null;
      const messageLink =
        inputs?.message_link ||
        inputs?.messageLink ||
        inputs?.message_url ||
        inputs?.messageUrl ||
        inputs?.message_permalink ||
        inputs?.messagePermalink ||
        null;
      let msgTs = inputs?.message_ts || inputs?.messageTs || null;
      const title =
        String(inputs?.task_title || inputs?.title || "").trim() || "バックオフィスタスク";
      const descriptionInput = String(
        inputs?.task_description || inputs?.description || "",
      ).trim();
      const assigneeGroupId =
        String(
          inputs?.assignee_group_id ||
            inputs?.assigneeGroupId ||
            inputs?.usergroup_id ||
            inputs?.usergroupId ||
            "",
        ).trim() || null;
      let assigneeGroupHandle =
        String(
          inputs?.assignee_group_handle ||
            inputs?.assigneeGroupHandle ||
            inputs?.usergroup_handle ||
            inputs?.usergroupHandle ||
            "",
        ).trim() || null;
      const dueBusinessDays = Number(
        inputs?.due_business_days || inputs?.dueBusinessDays || 3,
      );

      if (!msgTs && messageLink) {
        msgTs = extractTsFromPermalink(messageLink);
      }
      if (!looksLikeSlackChannelId(channelId) && messageLink) {
        channelId = extractChannelIdFromPermalink(messageLink);
      }
      if (!teamId) {
        teamId = await getTeamIdViaAuthTest(client);
      }

      const missing = [];
      if (!requesterUserId || !looksLikeSlackUserId(requesterUserId)) {
        missing.push("requester_user_id");
      }
      if (!teamId || !looksLikeSlackTeamId(teamId)) {
        missing.push("team_id");
      }
      if (!channelId || !looksLikeSlackChannelId(channelId)) {
        missing.push("channel_id");
      }
      if (!msgTs) {
        missing.push("message_ts or message_link");
      }
      if (!assigneeGroupId) {
        missing.push("assignee_group_id");
      }
      if (missing.length) {
        logger?.warn?.("cb8d79backoffice_group_taskify missing required", {
          missing,
          requesterUserId,
          channelId,
          hasMessageLink: !!messageLink,
          hasMessageTs: !!msgTs,
          assigneeGroupId,
        });
        await complete({
          outputs: {
            task_id: null,
            skipped: `missing:${missing.join(",")}`,
          },
        });
        return;
      }

      if (!assigneeGroupHandle) {
        try {
          const idToHandle = await getSubteamIdMap(teamId);
          assigneeGroupHandle = idToHandle.get(assigneeGroupId) || assigneeGroupId;
        } catch (_) {
          assigneeGroupHandle = assigneeGroupId;
        }
      }
      assigneeGroupHandle = String(assigneeGroupHandle || assigneeGroupId).replace(
        /^@/,
        "",
      );

      const existing = await getTaskBySourceAndBroadcastKey(
        teamId,
        channelId,
        msgTs,
        title,
        assigneeGroupId,
      );
      if (existing?.id) {
        logger?.info?.("cb8d79backoffice_group_taskify already exists", {
          existingTaskId: existing.id,
          teamId,
          channelId,
          msgTs,
          title,
          assigneeGroupId,
        });
        await complete({
          outputs: {
            task_id: existing.id,
            due_date: existing.due_date || null,
            skipped: "already_exists",
          },
        });
        return;
      }

      let rawText = "";
      try {
        rawText = await fetchMessageTextByTs(client, channelId, msgTs);
      } catch (_) {}

      const description = descriptionInput || String(rawText || "");

      let permalink = String(messageLink || "");
      try {
        if (channelId && msgTs) {
          const r = await client.chat.getPermalink({
            channel: channelId,
            message_ts: msgTs,
          });
          permalink = r?.permalink || permalink;
        }
      } catch (e) {
        logger?.warn?.("getPermalink failed", e);
      }

      const postedYmd = jstYmdFromSlackTs(msgTs) || todayJstYmd();
      const due = addJpBusinessDaysYmd(postedYmd, dueBusinessDays);
      if (!due) {
        logger?.warn?.("cb8d79backoffice_group_taskify invalid due", {
          postedYmd,
          dueBusinessDays,
        });
        await complete({
          outputs: { task_id: null, skipped: "invalid_due" },
        });
        return;
      }

      const targetList = uniqIds(await getUsergroupMembers(teamId, assigneeGroupId));
      if (!targetList.length) {
        logger?.warn?.("cb8d79backoffice_group_taskify no targets", {
          teamId,
          assigneeGroupId,
          assigneeGroupHandle,
        });
        await complete({
          outputs: { task_id: null, skipped: "no_targets" },
        });
        return;
      }

      const requesterDept = await resolveDeptForUser(teamId, requesterUserId);
      let taskChannelId = channelId;
      let taskMessageTs = msgTs;

      if (looksLikeSlackChannelId(BACKOFFICE_TASK_THREAD_CHANNEL)) {
        try {
          await ensureBotInChannel({
            client,
            channelId: BACKOFFICE_TASK_THREAD_CHANNEL,
          });
          const root = await client.chat.postMessage({
            channel: BACKOFFICE_TASK_THREAD_CHANNEL,
            text: `タスク管理: ${title}`,
            blocks: [
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text: `*${noMention(title)}*`,
                },
              },
              ...(permalink
                ? [
                    {
                      type: "context",
                      elements: [
                        {
                          type: "mrkdwn",
                          text: `<${permalink}|元メッセージへ>`,
                        },
                      ],
                    },
                  ]
                : []),
            ],
          });
          if (root?.ts) {
            taskChannelId = BACKOFFICE_TASK_THREAD_CHANNEL;
            taskMessageTs = root.ts;
          }
        } catch (e) {
          logger?.warn?.("backoffice thread root post failed", e);
        }
      }

      const taskId = randomUUID();
      const assigneeLabel = await buildAssigneeLabelRaw(teamId, [], [
        assigneeGroupHandle,
      ]);

      const created = await dbCreateTask({
        id: taskId,
        team_id: teamId,
        channel_id: taskChannelId,
        message_ts: taskMessageTs,
        source_permalink: permalink || null,
        title,
        description,
        requester_user_id: requesterUserId,
        created_by_user_id: requesterUserId,
        assignee_id: null,
        assignee_label: assigneeLabel,
        status: "in_progress",
        due_date: due,
        requester_dept: requesterDept,
        assignee_dept: null,
        task_type: "broadcast",
        broadcast_group_handle: `@${assigneeGroupHandle}`,
        broadcast_group_id: assigneeGroupId,
        total_count: targetList.length,
        completed_count: 0,
        notified_at: null,
      });

      await dbInsertTaskTargets(teamId, taskId, targetList);
      const total = await dbCountTargets(teamId, taskId);
      await dbUpdateBroadcastCounts(teamId, taskId, 0, total);
      created.total_count = total;
      created.completed_count = 0;

      try {
        const toNotify = targetList.filter((u) => u && u !== requesterUserId);
        for (const uid of toNotify) {
          await notifyTaskSimpleDM(uid, created, "タスクが届いたよ");
        }
      } catch (e) {
        logger?.error?.("workflow step notify error", e);
      }

      try {
        const toRefresh = Array.from(
          new Set([requesterUserId, ...targetList].filter(Boolean)),
        );
        await publishHomeBurst(client, teamId, toRefresh, 200);
      } catch (e) {
        logger?.warn?.("home publish error", e);
      }

      await complete({
        outputs: {
          task_id: taskId,
          due_date: due,
        },
      });
      logger?.info?.("cb8d79backoffice_group_taskify created", {
        taskId,
        teamId,
        channelId,
        msgTs,
        title,
        assigneeGroupId,
        assigneeGroupHandle,
        targetCount: targetList.length,
        due,
      });
    } catch (error) {
      logger?.error?.("cb8d79backoffice_group_taskify failed", {
        message: error?.message,
        stack: error?.stack,
      });

      await fail({
        error: `cb8d79backoffice_group_taskify failed: ${error?.message || "unknown error"}`,
      });
    }
  },
);

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
    const actorUserId = shortcut.user?.id || "";
    const msgBlocks = shortcut.message?.blocks || null;

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);

    const hasUserMention =
      extractUserIdsFromBlocks(msgBlocks || []).length > 0 ||
      /<@([A-Z0-9]+)(?:\|[^>]+)?>/.test(String(rawText || ""));

    const hasGroupMention =
      extractUserGroupIdsFromBlocks(msgBlocks || []).length > 0 ||
      /<!subteam\^([A-Z0-9]+)(?:\|[^>]+)?>/.test(String(rawText || ""));

    let initialUserIds = [];
    let initialGroupIds = [];
    let initialGroupOptions = [];

    if (hasUserMention || hasGroupMention) {
      const targets = inferTargetsFromMessage(rawText, actorUserId, msgBlocks);

      initialUserIds = Array.isArray(targets.userIds)
        ? targets.userIds.filter(Boolean).slice(0, 10)
        : [];
      initialGroupIds = Array.isArray(targets.groupIds)
        ? targets.groupIds.filter(Boolean).slice(0, 10)
        : [];

      if (initialGroupIds.length) {
        const idToHandle = await getSubteamIdMap(teamId);
        initialGroupOptions = initialGroupIds.map((gid) => {
          const handle = idToHandle.get(gid) || gid;
          return {
            text: {
              type: "plain_text",
              text: `@${String(handle).replace(/^@/, "")}`,
            },
            value: gid,
          };
        });
      }
    }

    const cacheKey = __cacheKey(teamId, channelId, msgTs);
    __cachePut(cacheKey, prettyText || "");

    await openTaskCreateModal(client, {
      trigger_id: shortcut.trigger_id,
      teamId,
      channelId,
      msgTs,
      actorUserId,
      initialUserIds,
      initialGroupOptions,
      includeContentInput: false,
    });
  } catch (e) {
    console.error("shortcut error:", e?.data || e);
  }
});

// ================================
// Global Shortcut: Open Task List (Home-like modal)
// ================================

// ================================
// Slash Command: /dashboard
// ================================
app.command("/dashboard", async ({ ack, body, respond }) => {
  await ack();
  try {
    const teamId = body.team_id;
    const userId = body.user_id;
    if (!teamId || !userId) return;

    const token = generateToken(teamId, userId);
    const baseUrl = (process.env.DASHBOARD_BASE_URL || "https://inrevo-task.com").replace(/\/$/, "");
    const url = `${baseUrl}/dashboard/auth?token=${token}`;

    await respond({
      response_type: "ephemeral",
      text: `管理ダッシュボードを開く: ${url}`,
      blocks: [
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: "📊 *管理ダッシュボード*\n下のボタンからダッシュボードを開けます。\nリンクの有効期限は *1時間* です。",
          },
        },
        {
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: "ダッシュボードを開く" },
              style: "primary",
              url,
            },
          ],
        },
      ],
    });
  } catch (e) {
    console.error("/dashboard command error:", e?.data || e);
  }
});

// ================================
// Express: Dashboard static + API
// ================================
(async () => {
  const port = process.env.PORT ? Number(process.env.PORT) : 3000;
  await dbEnsureSettingsSchema();

  const expressApp = receiver.app;
  expressApp.use(cookieParser());
  expressApp.use(require("express").json({ limit: '10mb' }));

  // Dashboard API
  registerDashboardApi({
    expressApp,
    slackClient: app.client,
    dbQuery,
    dbTransaction,
    getUserDisplayName,
    dbGetDashboardRole,
    dbSetDashboardRole,
    dbListDashboardAdmins,
    dbCreateDashTeam,
    dbListDashTeams,
    dbGetDashTeam,
    dbDeleteDashTeam,
    dbUpdateDashTeam,
    dbAddDashTeamMember,
    dbRemoveDashTeamMember,
    dbListDashTeamMembers,
    dbListDashTeamMembersWithProfile,
    dbUpdateDashTeamMemberRole,
    dbGetUserSlackTitle,
    dbUserHasAdminTeamRole,
    dbGetUserDashTeamRoles,
    dbGetDashTeamSubtree,
    dbUpdateDashTeamFull,
    dbGetUserDashTeams,
    dbListDashboardVisibleUsers,
    dbListDashboardVisibleTeams,
    dbReplaceDashboardVisibleUsers,
    dbReplaceDashboardVisibleTeams,
    dbUpsertDashboardUserDirectoryMember,
    dbSetUserDirectoryActive,
    dbListDashboardUserDirectory,
    dbGetDashboardDirectoryMember,
    dbListWorkloadItems,
    dbGetWorkloadItem,
    dbCreateWorkloadItem,
    dbUpdateWorkloadItem,
    dbDeleteWorkloadItem,
    dbListWorkloadCells,
    dbSetWorkloadCells,
    dbCopyWorkloadMonth,
    dbListWorkloadCategories,
    dbUpsertWorkloadCategory,
    dbUpdateWorkloadCategory,
    dbDeleteWorkloadCategory,
    dbCreateProject,
    dbListProjects,
    dbGetProject,
    dbDeleteProject,
    dbUpdateProject,
    dbAddProjectTask,
    dbRemoveProjectTask,
    dbListProjectTasks,
    // Task operations
    dbGetTaskById,
    dbUpdateStatus,
    dbUpdateTaskEditableFields,
    dbUpdateBroadcastCounts,
    dbListTaskComments,
    dbInsertTaskComment,
    dbCreateTask,
    dbInsertTaskTargets,
    dbListTargetUserIds,
    randomUUID,
    // Integrations
    dbCreateIntegration,
    dbListIntegrations,
    dbGetIntegration,
    dbUpdateIntegration,
    dbDeleteIntegration,
    dbListFieldMappings,
    dbSetFieldMappings,
    dbCreateSyncLog,
    dbUpdateSyncLog,
    dbListSyncLogs,
    getSubteamIdMap,
    getUsergroupMembers,
    updateUsergroupMembers,
    // CRM
    dbCreateClient,
    dbListClients,
    dbGetClient,
    dbUpdateClient,
    dbDeleteClient,
    dbCreateDeal,
    dbListDeals,
    dbGetDeal,
    dbUpdateDeal,
    dbDeleteDeal,
    dbAddDealMember,
    dbRemoveDealMember,
    dbListDealMembers,
    dbIsDealMember,
    dbCreateDealActivity,
    dbListDealActivities,
    dbDeleteDealActivity,
    dbCreateDealPayment,
    dbListDealPayments,
    dbUpdateDealPayment,
    dbDeleteDealPayment,
    dbAddDealTask,
    dbRemoveDealTask,
    dbListDealTasks,
    dbCreateDeliverable,
    dbListDeliverables,
    dbUpdateDeliverable,
    dbDeleteDeliverable,
    dbCreateClientContact,
    dbListClientContacts,
    dbUpdateClientContact,
    dbDeleteClientContact,
    dbCreateDealPosition,
    dbListDealPositions,
    dbUpdateDealPosition,
    dbDeleteDealPosition,
    dbCreateDealMediaPlan,
    dbListDealMediaPlans,
    dbUpdateDealMediaPlan,
    dbDeleteDealMediaPlan,
    dbCreateCalcDef,
    dbListCalcDefs,
    dbUpdateCalcDef,
    dbDeleteCalcDef,
    dbPipelineSummary,
    upsertThreadCard: (...args) => upsertThreadCard(...args),
    buildThreadCardBlocks: (...args) => buildThreadCardBlocks(...args),
    dbListPersonalFilters,
    dbGetPersonalFilterMemberIds,
    boltApp: app,
  });

  // Root redirect
  expressApp.get("/", (_req, res) => res.redirect("/dashboard"));

  // React SPA static files (web/dist)
  const distPath = path.join(__dirname, "web", "dist");
  expressApp.use("/dashboard", require("express").static(distPath));
  // SPA fallback: /dashboard/* → index.html
  expressApp.get("/dashboard/{*splat}", (_req, res) => {
    res.sendFile(path.join(distPath, "index.html"));
  });

  // ── 出勤日報 → HRMOS(IEYASU)自動打刻 ─────────────────────────────────────
  // 出退勤日報チャンネルへの投稿を検知して自動打刻する。
  // チャンネルはカンマ区切りで複数指定可能（テスト用チャンネルの追加などに対応）。
  // WF botのメッセージのみ打刻する（手動投稿での誤打刻を防止）。
  // WFのメッセージにはテキスト中の<@UXXXXX>でユーザーを特定する必要がある。
  const toChSet = (envVal) => new Set((envVal || '').split(',').map(s => s.trim()).filter(Boolean));
  const STAMP_IN_CHS  = toChSet(process.env.HRMOS_STAMP_IN_CHANNELS);
  const STAMP_OUT_CHS = toChSet(process.env.HRMOS_STAMP_OUT_CHANNELS);

  // 一時的エラー（自動リトライ対象）
  const TRANSIENT_REASONS = new Set(['api_error', 'exception']);

  // Slack絵文字リアクション付与
  async function addStampReaction(client, channel, ts, name) {
    if (!channel || !ts) return;
    try { await client.reactions.add({ channel, timestamp: ts, name }); }
    catch (e) {
      // already_reacted は無視
      if (!/already_reacted|reaction_remove|invalid/.test(e?.data?.error || '')) {
        console.warn('[IEYASU] reaction add fail:', e?.data?.error || e.message);
      }
    }
  }

  function stampReasonText(reason) {
    return {
      no_email: 'Slackプロフィールにメールアドレスが登録されていません',
      user_not_found: 'HRMOS側に同じメールの社員アカウントが見つかりません',
      api_error: 'HRMOSのAPIエラーで打刻できませんでした',
      exception: '通信エラーで打刻できませんでした',
      no_token: 'システム設定エラー（管理者へ）',
    }[reason] || `不明なエラー: ${reason}`;
  }

  // 失敗時の本人DM
  async function notifyStampFailure(client, slackUserId, stampType, reason) {
    const label = stampType === 1 ? '出勤' : '退勤';
    const reasonText = stampReasonText(reason);
    try {
      const dm = await client.conversations.open({ users: slackUserId });
      const channel = dm?.channel?.id;
      if (!channel) return;
      await client.chat.postMessage({
        channel,
        text: `⚠️ HRMOS ${label}打刻ができませんでした`,
        blocks: [
          { type: 'section', text: { type: 'mrkdwn', text: `⚠️ *HRMOS ${label}打刻ができませんでした*` } },
          { type: 'section', text: { type: 'mrkdwn', text: `理由: ${reasonText}\n\nお手数ですが HRMOS で手動打刻をお願いします。` } },
        ],
      });
    } catch (e) { console.warn('[IEYASU] DM通知失敗:', e.message); }
  }

  // 失敗時の日報スレッドへの公開返信（人事・上長も気づける）
  async function postStampFailureToThread(client, channelId, messageTs, slackUserId, stampType, reason) {
    if (!channelId || !messageTs) return;
    const label = stampType === 1 ? '出勤' : '退勤';
    const reasonText = stampReasonText(reason);
    try {
      await client.chat.postMessage({
        channel: channelId,
        thread_ts: messageTs,
        text: `⚠️ HRMOS ${label}打刻に失敗`,
        blocks: [
          { type: 'section', text: { type: 'mrkdwn',
            text: `⚠️ *HRMOS ${label}打刻に失敗しました* <@${slackUserId}>\n理由: ${reasonText}\n\n本人は HRMOS で手動打刻をお願いします。人事・上長の方はご確認ください。` } },
        ],
        unfurl_links: false, unfurl_media: false,
      });
    } catch (e) { console.warn('[IEYASU] スレッド通知失敗:', e.message); }
  }

  const doStamp = async (client, message, stampType) => {
    // スレッド返信は除外
    if (message.thread_ts && message.thread_ts !== message.ts) return;
    // WF botのメッセージのみ対象（通常の手動投稿は誤打刻防止のため除外）
    const isWfBot = !!(message.bot_id || message.subtype === 'bot_message');
    if (!isWfBot) return;

    // WF botはuser_idを持たないのでテキスト中の<@UXXXXX>からユーザーを特定
    const match = (message.text || '').match(/<@(U[A-Z0-9]+)>/);
    if (!match) return;
    const targetUserId = match[1];
    const { teamId } = await dbQuery('SELECT DISTINCT team_id FROM tasks LIMIT 1').then(r => ({ teamId: r.rows[0]?.team_id || '' }));
    const label = stampType === 1 ? '出勤' : '退勤';
    console.log(`[IEYASU] ${label}日報受信 ch:${message.channel} user:${targetUserId}`);
    const result = await stampAttendance(client, targetUserId, stampType);

    if (result.ok) {
      // 既打刻スキップ時は青✅、新規打刻時は緑✅で区別
      const reactionName = result.skipped ? 'ballot_box_with_check' : 'white_check_mark';
      await addStampReaction(client, message.channel, message.ts, reactionName);
      if (result.skipped) {
        console.log(`[IEYASU] ${label} 既打刻スキップ: ${result.email} @ ${result.alreadyAt}`);
      }
      await dbQuery(
        `INSERT INTO hrmos_stamps (id, team_id, slack_user_id, stamp_type, ok, hrmos_user_id, channel_id, message_ts, error_reason)
         VALUES (gen_random_uuid(), $1, $2, $3, true, $4, $5, $6, $7)`,
        [teamId, targetUserId, stampType, result.userId || null, message.channel, message.ts, result.skipped ? 'already_stamped' : null]
      ).catch(e => console.error('[IEYASU] DB記録失敗:', e.message));
    } else {
      // 失敗: 一時的なら retry_state='pending' で10分後にリトライ
      const willRetry = TRANSIENT_REASONS.has(result.reason);
      const retryState = willRetry ? 'pending' : 'final_failed';
      await dbQuery(
        `INSERT INTO hrmos_stamps (id, team_id, slack_user_id, stamp_type, ok, hrmos_user_id, error_reason, channel_id, message_ts, retry_state)
         VALUES (gen_random_uuid(), $1, $2, $3, false, $4, $5, $6, $7, $8)`,
        [teamId, targetUserId, stampType, result.userId || null, result.reason, message.channel, message.ts, retryState]
      ).catch(e => console.error('[IEYASU] DB記録失敗:', e.message));
      console.warn(`[IEYASU] ${label}打刻失敗 (retry=${willRetry}):`, result);
      if (!willRetry) {
        // 恒久エラーは即DM＋スレッド返信＋⚠️
        await addStampReaction(client, message.channel, message.ts, 'warning');
        await notifyStampFailure(client, targetUserId, stampType, result.reason);
        await postStampFailureToThread(client, message.channel, message.ts, targetUserId, stampType, result.reason);
      }
      // 一時エラー時はリアクション保留（10分後worker で結果に応じて付与）
    }
  };

  if (STAMP_IN_CHS.size > 0 || STAMP_OUT_CHS.size > 0) {
    app.message(async ({ message, client }) => {
      if (STAMP_IN_CHS.has(message.channel))  await doStamp(client, message, 1);
      if (STAMP_OUT_CHS.has(message.channel)) await doStamp(client, message, 2);
    });

    // 10分後リトライworker（5分おきに走査）
    setInterval(async () => {
      try {
        const { rows } = await dbQuery(
          `SELECT id, team_id, slack_user_id, stamp_type, channel_id, message_ts
           FROM hrmos_stamps
           WHERE retry_state='pending'
             AND stamped_at <= now() - interval '10 minutes'
             AND stamped_at > now() - interval '6 hours'
           ORDER BY stamped_at ASC LIMIT 20`
        );
        if (rows.length === 0) return;
        for (const row of rows) {
          const result = await stampAttendance(app.client, row.slack_user_id, row.stamp_type);
          if (result.ok) {
            const reactionName = result.skipped ? 'ballot_box_with_check' : 'white_check_mark';
            await addStampReaction(app.client, row.channel_id, row.message_ts, reactionName);
            await dbQuery(
              `UPDATE hrmos_stamps SET ok=true, hrmos_user_id=$2, retry_state='retried_ok', error_reason=$3 WHERE id=$1`,
              [row.id, result.userId || null, result.skipped ? 'already_stamped' : null]
            ).catch(() => {});
            console.log(`[IEYASU] retry成功: user=${row.slack_user_id} type=${row.stamp_type} skipped=${!!result.skipped}`);
          } else {
            await addStampReaction(app.client, row.channel_id, row.message_ts, 'warning');
            await notifyStampFailure(app.client, row.slack_user_id, row.stamp_type, result.reason);
            await postStampFailureToThread(app.client, row.channel_id, row.message_ts, row.slack_user_id, row.stamp_type, result.reason);
            await dbQuery(
              `UPDATE hrmos_stamps SET retry_state='final_failed', error_reason=$2 WHERE id=$1`,
              [row.id, result.reason]
            ).catch(() => {});
            console.warn(`[IEYASU] retry最終失敗: user=${row.slack_user_id}`, result);
          }
        }
      } catch (e) { console.error('[IEYASU] retry worker error:', e.message); }
    }, 5 * 60 * 1000);
  }

  // ── 問い合わせフォーム → CRM 自動登録 ──────────────────────────
  if (INQUIRY_CHANNEL_ID) {
    app.message(async ({ message, client }) => {
      if (message.channel !== INQUIRY_CHANNEL_ID || message.subtype) return;
      await handleInquiryMessage(message, client).catch(e =>
        console.error('[CRM Inquiry] handler error:', e.message)
      );
    });
  }

  // ── メンション追跡 ─────────────────────────────────────────────────────────
  // すべてのメッセージを監視し、@メンションをDBに記録する。
  // 返信・リアクションで既読扱い、48時間で自動消滅。
  // @user / @channel / @here / ユーザーグループに対応。
  const MENTION_RE     = /<@(U[A-Z0-9]+)>/g;
  const SUBTEAM_RE     = /<!subteam\^([A-Z0-9]+)(?:\|[^>]+)?>/g;
  const CHANNEL_HERE_RE = /<!channel>|<!here>/;
  const stripMentions = (t) => (t || '').replace(/<@[A-Z0-9]+>/g, '').replace(/<[^>]+>/g, '').trim();

  const saveMention = async (teamId, uid, channelId, msgTs, senderUserId, preview, threadTsRoot, mentionType = 'direct') => {
    if (uid === senderUserId) return;
    const { randomUUID } = require('crypto');
    await dbQuery(`
      INSERT INTO user_mentions (id, team_id, mentioned_user_id, channel_id, message_ts, sender_user_id, text_preview, thread_ts_root, mention_type, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
      ON CONFLICT (team_id, mentioned_user_id, channel_id, message_ts) DO NOTHING
    `, [randomUUID(), teamId, uid, channelId, msgTs, senderUserId, preview, threadTsRoot || msgTs, mentionType]).catch(() => {});
  };

  app.event('message', async ({ event, client, body }) => {
    try {
      if (event.bot_id || event.subtype === 'bot_message' || event.subtype === 'message_deleted') return;
      if (!event.text || !event.user) return;

      const teamId = body?.team_id || body?.team?.id || event.team || '';
      const channelId = event.channel;
      const msgTs = event.ts;
      const senderUserId = event.user;
      const text = event.text;

      // スレッド返信 → そのスレッド内でメンションされていたら既読
      // thread_ts_root でスレッドルートと照合（スレッド内の返信でも正しく消えるよう）
      if (event.thread_ts && event.thread_ts !== msgTs) {
        await dbQuery(`
          UPDATE user_mentions SET dismissed_at=now()
          WHERE mentioned_user_id=$1 AND channel_id=$2
            AND (message_ts=$3 OR thread_ts_root=$3) AND dismissed_at IS NULL
        `, [senderUserId, channelId, event.thread_ts]).catch(() => {});
      }

      const preview = stripMentions(text).slice(0, 100) || '（本文なし）';
      const threadTsRoot = event.thread_ts || msgTs;

      // 直接@メンション（個別ユーザー）
      const directIds = new Set();
      for (const m of text.matchAll(MENTION_RE)) directIds.add(m[1]);
      for (const uid of directIds) {
        await saveMention(teamId, uid, channelId, msgTs, senderUserId, preview, threadTsRoot, 'direct');
      }

      // @channel / @here → チャンネルメンバー全員
      if (CHANNEL_HERE_RE.test(text)) {
        try {
          let cursor;
          do {
            const r = await client.conversations.members({ channel: channelId, limit: 200, cursor });
            for (const uid of (r.members || [])) {
              await saveMention(teamId, uid, channelId, msgTs, senderUserId, preview, threadTsRoot, 'channel');
            }
            cursor = r.response_metadata?.next_cursor;
          } while (cursor);
        } catch { /* チャンネルメンバー取得失敗は無視 */ }
      }

      // ユーザーグループメンション
      for (const m of text.matchAll(SUBTEAM_RE)) {
        try {
          const r = await client.usergroups.users.list({ usergroup: m[1] });
          for (const uid of (r.users || [])) {
            await saveMention(teamId, uid, channelId, msgTs, senderUserId, preview, threadTsRoot, 'group');
          }
        } catch { /* ユーザーグループ取得失敗は無視 */ }
      }

      if (!directIds.size && !CHANNEL_HERE_RE.test(text) && !text.match(SUBTEAM_RE)) return;
    } catch (e) { /* silent */ }
  });

  // リアクションで既読
  app.event('reaction_added', async ({ event, body }) => {
    try {
      if (event.item?.type !== 'message') return;
      await dbQuery(`
        UPDATE user_mentions SET dismissed_at=now()
        WHERE mentioned_user_id=$1 AND channel_id=$2 AND message_ts=$3 AND dismissed_at IS NULL
      `, [event.user, event.item.channel, event.item.ts]).catch(() => {});
    } catch (e) { /* silent */ }
  });

  await app.start(port);
  console.log(`Slack app is running on port ${port}`);

  // 起動時にキャッシュをウォームアップ
  prefetchAll().catch((e) => console.warn("[prefetch] failed:", e.message));
})();

async function prefetchAll() {
  const now = Date.now();

  // ユーザー一覧を一括取得
  let cursor;
  let count = 0;
  do {
    const res = await app.client.users.list({
      limit: 200,
      ...(cursor ? { cursor } : {}),
    });
    for (const u of res?.members || []) {
      if (u.deleted || u.is_bot) continue;
      const teamId = u.team_id;
      const userId = u.id;
      if (!teamId || !userId) continue;
      const key = `${teamId}:${userId}`;
      const name =
        (u?.profile?.display_name && u.profile.display_name.trim()) ||
        (u?.real_name && u.real_name.trim()) ||
        (u?.name && String(u.name).trim()) ||
        userId;
      userNameCache.set(key, { at: now, name });
      const url =
        u?.profile?.image_24 ||
        u?.profile?.image_32 ||
        u?.profile?.image_48 ||
        null;
      if (url) userIconCache.set(key, { at: now, url });
      await dbUpsertDashboardUserDirectoryMember(teamId, u);
      count++;
    }
    cursor = res?.response_metadata?.next_cursor;
  } while (cursor);
  console.log(`[prefetch] loaded ${count} users into cache`);

  // usergroups（subteam）を一括取得してキャッシュ
  try {
    const authRes = await app.client.auth.test();
    const teamId = authRes?.team_id;
    if (teamId) {
      await getSubteamIdMap(teamId);
      await getSubteamIdMap("global");
      console.log(`[prefetch] usergroups warmed for team ${teamId}`);
    }
  } catch (e) {
    console.warn("[prefetch] usergroups failed:", e.message);
  }
}

cron.schedule(
  "15 3 * * *",
  () => {
    prefetchAll().catch((e) => console.warn("[prefetch] scheduled sync failed:", e.message));
  },
  { timezone: "Asia/Tokyo" },
);

