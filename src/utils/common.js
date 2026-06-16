const __msgTextCache = new Map();

function __cacheKey(teamId, channelId, ts) {
  return `${teamId || ""}:${channelId || ""}:${ts || ""}`;
}

function __cachePut(key, text, ttlMs = 10 * 60 * 1000) {
  if (!key) return;
  __msgTextCache.set(key, {
    text: String(text || ""),
    exp: Date.now() + ttlMs,
  });
}

function __cacheGet(key) {
  const v = __msgTextCache.get(key);
  if (!v) return "";
  if (v.exp < Date.now()) {
    __msgTextCache.delete(key);
    return "";
  }
  return v.text || "";
}

async function fetchMessageTextByTs(client, channelId, ts) {
  try {
    if (!channelId || !ts) return "";
    const res = await client.conversations.history({
      channel: channelId,
      latest: ts,
      oldest: ts,
      inclusive: true,
      limit: 1,
    });
    const msg = res?.messages?.[0];
    return String(msg?.text || "");
  } catch (e) {
    console.error("fetchMessageTextByTs error:", e?.data || e);
    return "";
  }
}

function safeJsonParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function parseActionMeta(body, action) {
  const p = safeJsonParse(action?.value || "{}") || {};
  const teamId = p.teamId || getTeamIdFromBody(body);
  const taskId = p.taskId || null;
  return { p, teamId, taskId };
}
function getTeamIdFromBody(body) {
  return (
    body?.team?.id ||
    body?.team_id ||
    body?.team?.team_id ||
    body?.authorizations?.[0]?.team_id ||
    null
  );
}

function getUserIdFromBody(body) {
  return body?.user?.id || body?.user_id || body?.user?.user_id || null;
}

// 通知抑止：@mk 等を表示したいが、メンション通知は飛ばしたくない（※全社タスク発行時は例外でメンションを有効にする）
function noMention(s) {
  if (!s) return "";
  return String(s).replace(/@/g, "＠");
}

// ================================
// UI表示用：社内表示名の "/～" をカットする（例： "田中/John" → "田中" ）
// ================================
function cutAfterSlash(s) {
  if (!s) return "";
  const raw = String(s).trim();
  // "name/xxx/yyy" の可能性もあるので先頭だけ採用
  return raw.split("/")[0].trim();
}

// すでに "@name" の形でも、"name" でもOK → "@name" に整形（/以降はカット）
function toAtShortName(nameOrAtName) {
  const raw = String(nameOrAtName || "")
    .trim()
    .replace(/^@/, "");
  const short = cutAfterSlash(raw);
  return short ? `@${short}` : "-";
}

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

function slackDateYmd(due) {
  if (!due) return null;
  if (typeof due === "string") {
    const m = due.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
    const d = new Date(due);
    if (!Number.isNaN(d.getTime())) {
      const y = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${y}-${mm}-${dd}`;
    }
    return null;
  }
  if (due instanceof Date) {
    const y = due.getFullYear();
    const mm = String(due.getMonth() + 1).padStart(2, "0");
    const dd = String(due.getDate()).padStart(2, "0");
    return `${y}-${mm}-${dd}`;
  }
  return null;
}

function jstYmdFromDate(date) {
  const d = date instanceof Date ? date : new Date(date);
  if (Number.isNaN(d.getTime())) return null;
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function jstYmdFromSlackTs(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  return jstYmdFromDate(new Date(n * 1000));
}

function parseJpHolidaysCsvToSet(csv) {
  return new Set(
    String(csv || "")
      .split(/[\s,]+/)
      .map((s) => s.trim())
      .filter((s) => /^\d{4}-\d{2}-\d{2}$/.test(s)),
  );
}

function addDaysYmd(ymd, days) {
  const m = String(ymd || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  const y = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${mm}-${dd}`;
}

function isJpBusinessDayYmd(
  ymd,
  holidaySet = parseJpHolidaysCsvToSet(process.env.JP_HOLIDAYS_CSV),
) {
  const m = String(ymd || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return false;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  const day = d.getUTCDay();
  if (day === 0 || day === 6) return false;
  if (holidaySet.has(ymd)) return false;
  return true;
}

function nextJpBusinessDayFromYmd(
  baseYmd,
  holidaySet = parseJpHolidaysCsvToSet(process.env.JP_HOLIDAYS_CSV),
) {
  let cur = addDaysYmd(baseYmd, 1);
  for (let i = 0; i < 366; i++) {
    if (cur && isJpBusinessDayYmd(cur, holidaySet)) return cur;
    cur = addDaysYmd(cur, 1);
  }
  return null;
}

const STATUS_OPTIONS = [
  { value: "in_progress", text: "進行中" },
  { value: "done", text: "完了" },
];

function statusLabel(s) {
  if (s === "in_progress") return "進行中";
  const f = STATUS_OPTIONS.find((x) => x.value === s);
  if (f) return f.text;
  if (s === "cancelled") return "取り下げ";
  return s || "-";
}

// broadcast の "対応者ラベル" は "@田中/John @佐藤/Jane" みたいに複数入ることがあるので、
function shortenAssigneeLabel(label) {
  if (!label) return "";

  const s = String(label);

  return s
    .replace(/(@[^@\n]+?)\/[^@\n]+/g, "$1")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

// "@" の数で人数をざっくりカウントする
function countAssigneesFromLabel(label) {
  if (!label) return 0;
  const m = String(label).match(/@/g);
  return m ? m.length : 0;
}

function assigneeDisplay(task) {
  if (task?.task_type === "broadcast") {
    const raw = task.assignee_label || "（複数対象）";

    const count = countAssigneesFromLabel(raw);

    // ✅ 4人以上は要約表示
    if (count >= 4) {
      return "⇒ 対象者多数（詳細を参照）";
    }

    // 1〜3人は名前を短縮表示（/～ をカット）
    return noMention(shortenAssigneeLabel(raw));
  }
  return `<@${task.assignee_id}>`;
}

function extractTsFromPermalink(url) {
  const s = String(url || "");
  const m = s.match(/\/p(\d{10})(\d{6,})/);
  if (!m) return null;
  return `${m[1]}.${m[2].padStart(6, "0")}`;
}

function extractChannelIdFromPermalink(link) {
  const s = String(link || "");
  const m = s.match(/\/archives\/([A-Z0-9]+)\//i);
  return m ? m[1] : null;
}

function looksLikeSlackChannelId(v) {
  return /^[CGD][A-Z0-9]+$/.test(String(v || ""));
}

function looksLikeSlackUserId(v) {
  return /^[UW][A-Z0-9]+$/.test(String(v || ""));
}

function looksLikeSlackTeamId(v) {
  return /^T[A-Z0-9]+$/.test(String(v || ""));
}

function uniqIds(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function todayJstYmd() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

// タスク作成時の期限初期値: JST 15:00 以降は翌日、それ以前は今日
// すべてのタスク作成ルート（モーダル / リアクション / キーワード）で共通使用
function defaultDueYmd() {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  const hour = jst.getUTCHours();
  if (hour >= 15) {
    jst.setUTCDate(jst.getUTCDate() + 1);
  }
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function normalizeHandle(handle) {
  return String(handle || "")
    .trim()
    .replace(/^@/, "")
    .toLowerCase();
}

function pickNoticeText(task) {
  const title = String(task?.title || "").trim();
  if (title) return title;

  const desc = String(task?.description || "").trim();
  if (!desc) return "（本文なし）";

  // ① Slack token/mention を削る
  // - <@Uxxx> / <!subteam^Gxxx|@mk> / <!channel> 等
  let s = desc
    .replace(/<@[^>]+>/g, " ")
    .replace(/<!subteam\^[^>]+>/g, " ")
    .replace(/<!channel>/g, " ")
    .replace(/<!here>/g, " ")
    .replace(/<!everyone>/g, " ");

  // ② @xxx / ＠xxx の連打を削る（日本語/英数字）
  //    ※メンションだけの文を「メンションしか見えない」問題を潰す
  s = s.replace(/(^|\s)[@＠][^\s　]+/g, " ");

  // ③ URL を削る（リンクだけになって読めないのを避ける）
  s = s.replace(/https?:\/\/\S+/g, " ");

  // ④ 余分な空白整理
  s = s.replace(/\s+/g, " ").trim();

  // それでも短すぎたら、元のdescから「最初の1行」だけ拾う（保険）
  if (s.length < 8) {
    const first = desc.split("\n").map((x) => x.trim()).filter(Boolean)[0] || "";
    const cleaned = first
      .replace(/<@[^>]+>/g, " ")
      .replace(/<!subteam\^[^>]+>/g, " ")
      .replace(/(^|\s)[@＠][^\s　]+/g, " ")
      .replace(/https?:\/\/\S+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return cleaned || "（本文なし）";
  }

  return s;
}

// Slack mrkdwn で読みやすいように軽く整形（長すぎると読めない）
function shortenOneLine(s, max = 90) {
  const x = String(s || "").replace(/\r\n/g, "\n").trim();
  const one = x.split("\n").join(" / ");
  if (one.length <= max) return one;
  return one.slice(0, max) + "…";
}

function groupBy(items, keyFn) {
  const m = new Map();
  for (const it of items || []) {
    const k = keyFn(it);
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(it);
  }
  return m;
}

module.exports = {
  __cacheGet,
  __cacheKey,
  __cachePut,
  STATUS_OPTIONS,
  addDaysYmd,
  assigneeDisplay,
  countAssigneesFromLabel,
  cutAfterSlash,
  extractChannelIdFromPermalink,
  extractTsFromPermalink,
  fetchMessageTextByTs,
  formatDueDateOnly,
  getTeamIdFromBody,
  getUserIdFromBody,
  groupBy,
  isJpBusinessDayYmd,
  jstYmdFromDate,
  jstYmdFromSlackTs,
  looksLikeSlackChannelId,
  looksLikeSlackTeamId,
  looksLikeSlackUserId,
  nextJpBusinessDayFromYmd,
  noMention,
  normalizeHandle,
  parseActionMeta,
  parseJpHolidaysCsvToSet,
  pickNoticeText,
  safeJsonParse,
  shortenAssigneeLabel,
  shortenOneLine,
  slackDateYmd,
  statusLabel,
  toAtShortName,
  todayJstYmd,
  defaultDueYmd,
  uniqIds,
};
