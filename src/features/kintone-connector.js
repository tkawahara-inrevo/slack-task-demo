// ================================
// kintone API connector
// ================================

/**
 * config shape:
 * {
 *   subdomain: "example"       // example.cybozu.com
 *   appId: "123"
 *   apiToken: "xxxx"
 * }
 */

function buildBaseUrl(config) {
  const sub = (config.subdomain || "").replace(/\.cybozu\.com.*$/, "").trim();
  if (!sub) throw new Error("kintone subdomain is required");
  return `https://${sub}.cybozu.com`;
}

function buildHeaders(config) {
  return {
    "X-Cybozu-API-Token": config.apiToken,
    "Content-Type": "application/json",
  };
}

// テスト接続 — アプリ情報を取得して接続確認
async function testConnection(config) {
  const base = buildBaseUrl(config);
  const url = `${base}/k/v1/app.json?id=${config.appId}`;
  const res = await fetch(url, { headers: buildHeaders(config) });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`kintone connection failed (${res.status}): ${body}`);
  }
  const data = await res.json();
  return { ok: true, appName: data.name, appId: data.appId };
}

// アプリのフィールド一覧を取得
async function getFields(config) {
  const base = buildBaseUrl(config);
  const url = `${base}/k/v1/app/form/fields.json?app=${config.appId}`;
  const res = await fetch(url, { headers: buildHeaders(config) });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`kintone get fields failed (${res.status}): ${body}`);
  }
  const data = await res.json();
  // { properties: { fieldCode: { type, label, ... }, ... } }
  const fields = Object.entries(data.properties || {}).map(([code, f]) => ({
    code,
    label: f.label || code,
    type: f.type,
  }));
  return fields;
}

// レコード取得（全件 or クエリ指定）
async function getRecords(config, { query, fields, limit = 500 } = {}) {
  const base = buildBaseUrl(config);
  const params = new URLSearchParams({ app: config.appId });
  if (query) params.set("query", query);
  if (fields?.length) params.set("fields", JSON.stringify(fields));
  if (limit) params.set("totalCount", "true");

  const allRecords = [];
  let offset = 0;
  const batchSize = Math.min(limit, 500);

  while (true) {
    const q = query ? `${query} limit ${batchSize} offset ${offset}` : `limit ${batchSize} offset ${offset}`;
    params.set("query", q);
    const url = `${base}/k/v1/records.json?${params}`;
    const res = await fetch(url, { headers: buildHeaders(config) });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`kintone get records failed (${res.status}): ${body}`);
    }
    const data = await res.json();
    allRecords.push(...(data.records || []));
    if (!data.records || data.records.length < batchSize || allRecords.length >= limit) break;
    offset += batchSize;
  }

  return allRecords;
}

// レコード追加
async function addRecords(config, records) {
  const base = buildBaseUrl(config);
  const url = `${base}/k/v1/records.json`;
  // kintone は1回100件まで
  const results = [];
  for (let i = 0; i < records.length; i += 100) {
    const batch = records.slice(i, i + 100);
    const res = await fetch(url, {
      method: "POST",
      headers: buildHeaders(config),
      body: JSON.stringify({ app: config.appId, records: batch }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`kintone add records failed (${res.status}): ${body}`);
    }
    const data = await res.json();
    results.push(...(data.ids || []));
  }
  return results;
}

// レコード更新
async function updateRecords(config, records) {
  const base = buildBaseUrl(config);
  const url = `${base}/k/v1/records.json`;
  const results = [];
  for (let i = 0; i < records.length; i += 100) {
    const batch = records.slice(i, i + 100);
    const res = await fetch(url, {
      method: "PUT",
      headers: buildHeaders(config),
      body: JSON.stringify({ app: config.appId, records: batch }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`kintone update records failed (${res.status}): ${body}`);
    }
    const data = await res.json();
    results.push(...(data.records || []));
  }
  return results;
}

// ================================
// フィールドマッピングを使った変換
// ================================

// ローカルタスク → kintoneレコード
const LOCAL_FIELD_LABELS = {
  title: "タイトル",
  description: "説明",
  status: "ステータス",
  assignee_id: "担当者ID",
  assignee_label: "担当者名",
  due_date: "期限",
  task_type: "種別",
  requester_user_id: "依頼者ID",
  created_at: "作成日時",
  updated_at: "更新日時",
};

const STATUS_MAP_TO_KINTONE = {
  in_progress: "進行中",
  done: "完了",
  cancelled: "キャンセル",
  pending: "保留",
};

const STATUS_MAP_FROM_KINTONE = {};
for (const [k, v] of Object.entries(STATUS_MAP_TO_KINTONE)) {
  STATUS_MAP_FROM_KINTONE[v] = k;
}

function taskToKintoneRecord(task, mappings) {
  const record = {};
  for (const m of mappings) {
    if (m.direction === "from_remote") continue; // remote→localのみのマッピングはスキップ
    let val = task[m.local_field];
    // ステータスは日本語に変換
    if (m.local_field === "status" && val) {
      val = STATUS_MAP_TO_KINTONE[val] || val;
    }
    if (val === null || val === undefined) val = "";
    record[m.remote_field] = { value: String(val) };
  }
  return record;
}

function kintoneRecordToTask(record, mappings) {
  const task = {};
  for (const m of mappings) {
    if (m.direction === "to_remote") continue; // local→remoteのみのマッピングはスキップ
    const field = record[m.remote_field];
    let val = field?.value ?? "";
    // ステータスは英語に変換
    if (m.local_field === "status" && val) {
      val = STATUS_MAP_FROM_KINTONE[val] || val;
    }
    task[m.local_field] = val;
  }
  return task;
}

function getLocalFieldOptions() {
  return Object.entries(LOCAL_FIELD_LABELS).map(([field, label]) => ({
    field,
    label,
  }));
}

module.exports = {
  testConnection,
  getFields,
  getRecords,
  addRecords,
  updateRecords,
  taskToKintoneRecord,
  kintoneRecordToTask,
  getLocalFieldOptions,
  LOCAL_FIELD_LABELS,
  STATUS_MAP_TO_KINTONE,
  STATUS_MAP_FROM_KINTONE,
};
