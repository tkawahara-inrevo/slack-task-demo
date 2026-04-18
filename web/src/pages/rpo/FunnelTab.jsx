// 歩留まり分析タブ
import { useState, useCallback, useRef, useEffect } from 'react';

// ─── 定数 ────────────────────────────────────────────────────────────
const TEMPLATE_KEY = 'rpo_funnel_templates_v1';

const FIXED_STAGES = [
  { id: 'applied', name: '応募',     required: true },
  { id: 'offer',   name: '内定',     required: true },
  { id: 'accept',  name: '内定承諾', required: true },
];
const FIXED_IDS = new Set(FIXED_STAGES.map(s => s.id));

const DEFAULT_TEMPLATES = [
  { id: 'tpl_document',  name: '書類選考' },
  { id: 'tpl_casual',    name: 'カジュアル面談' },
  { id: 'tpl_briefing',  name: '説明会' },
  { id: 'tpl_first',     name: '一次選考' },
  { id: 'tpl_second',    name: '二次選考' },
  { id: 'tpl_final',     name: '最終選考' },
  { id: 'tpl_aptitude',  name: '適性検査' },
  { id: 'tpl_exec',      name: '役員面接' },
  { id: 'tpl_condition', name: '条件面談' },
  { id: 'tpl_offer_i',   name: 'オファー面談' },
];

// ─── ユーティリティ ──────────────────────────────────────────────────
function genId(p) {
  return `${p}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function loadTemplates() {
  try {
    const raw = localStorage.getItem(TEMPLATE_KEY);
    if (!raw) return DEFAULT_TEMPLATES.map(t => ({ ...t }));
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || !parsed.length) return DEFAULT_TEMPLATES.map(t => ({ ...t }));
    return parsed;
  } catch {
    return DEFAULT_TEMPLATES.map(t => ({ ...t }));
  }
}
function saveTemplates(t) { localStorage.setItem(TEMPLATE_KEY, JSON.stringify(t)); }

function createEmptyFunnel() {
  const sv = {};
  FIXED_STAGES.forEach(s => { sv[s.id] = { targetInput: 0, pass: 0, decline: 0, fail: 0, pending: 0 }; });
  return {
    insertedTemplateStages: [],
    stageOrder: FIXED_STAGES.map(s => s.id),
    enabledStages: { applied: true, offer: true, accept: true },
    stageValues: sv,
  };
}

function enforceFixedOrder(order) {
  const middle = order.filter(id => id !== 'applied' && id !== 'accept');
  return ['applied', ...middle, 'accept'];
}

function mergeWithEmpty(funnel) {
  const base = createEmptyFunnel();
  if (!funnel) return base;
  return {
    ...base,
    ...funnel,
    enabledStages: { ...base.enabledStages, ...(funnel.enabledStages || {}) },
    stageValues:   { ...base.stageValues,   ...(funnel.stageValues   || {}) },
    insertedTemplateStages: funnel.insertedTemplateStages || [],
    stageOrder: funnel.stageOrder?.length ? funnel.stageOrder : base.stageOrder,
  };
}

function getOrderedStages(funnel) {
  const map = new Map((funnel.insertedTemplateStages || []).map(s => [s.id, s]));
  return (funnel.stageOrder || [])
    .map(id => {
      if (FIXED_IDS.has(id)) {
        const f = FIXED_STAGES.find(s => s.id === id);
        return { id: f.id, name: f.name, required: true, enabled: !!funnel.enabledStages[id], type: 'fixed' };
      }
      const ins = map.get(id);
      if (!ins) return null;
      return { id: ins.id, name: ins.name, required: false, enabled: !!ins.enabled, type: 'template', templateId: ins.templateId };
    })
    .filter(Boolean);
}

function pct(v, t) { return t > 0 ? `${((v / t) * 100).toFixed(1)}%` : '-'; }

function computeAnalysis(funnel) {
  const ordered = getOrderedStages(funnel);
  const enabled = ordered.filter(s => s.enabled);
  const targets = {};
  const rows    = [];

  enabled.forEach((stage, i) => {
    targets[stage.id] = i === 0
      ? (funnel.stageValues[stage.id]?.targetInput || 0)
      : (funnel.stageValues[enabled[i - 1].id]?.pass || 0);

    const isAccept = stage.id === 'accept';
    const v = funnel.stageValues[stage.id] || {};
    const target = targets[stage.id] || 0;
    rows.push({
      stage, target,
      pass:    isAccept ? 0 : (v.pass    || 0),
      decline: isAccept ? 0 : (v.decline || 0),
      fail:    isAccept ? 0 : (v.fail    || 0),
      pending: isAccept ? 0 : (v.pending || 0),
    });
  });

  const appliedCount  = targets.applied || 0;
  const offerRow      = rows.find(r => r.stage.id === 'offer');
  const acceptRow     = rows.find(r => r.stage.id === 'accept');
  const offerCount    = offerRow  ? offerRow.target  : 0;
  const acceptedCount = acceptRow ? acceptRow.target : 0;

  const analysisRows = rows.map(r => ({
    ...r,
    passRate:    r.stage.id === 'accept' ? '-' : pct(r.pass,    r.target),
    declineRate: r.stage.id === 'accept' ? '-' : pct(r.decline, r.target),
    failRate:    r.stage.id === 'accept' ? '-' : pct(r.fail,    r.target),
    pendingRate: r.stage.id === 'accept' ? '-' : pct(r.pending, r.target),
    arrivalRate: pct(r.target, appliedCount),
  }));

  // ボトルネック
  const cands = rows.filter(r => r.stage.id !== 'accept' && r.target > 0);
  const bot = cands.length ? {
    lowestPassRate:  [...cands].sort((a, b) => (a.pass    / a.target) - (b.pass    / b.target))[0],
    maxDeclineCount: [...cands].sort((a, b) => b.decline  - a.decline)[0],
    maxDeclineRate:  [...cands].sort((a, b) => (b.decline / b.target) - (a.decline / a.target))[0],
    maxFailCount:    [...cands].sort((a, b) => b.fail     - a.fail)[0],
    maxFailRate:     [...cands].sort((a, b) => (b.fail    / b.target) - (a.fail    / a.target))[0],
  } : null;

  // バリデーション（対象人数との整合チェック）
  const errors = [];
  rows.forEach(r => {
    if (r.stage.id === 'accept') return;
    const sum = r.pass + r.decline + r.fail + r.pending;
    if (r.target > 0 && sum !== r.target) {
      errors.push(`「${r.stage.name}」: 合格+辞退+不合格+進行中(${sum}) ≠ 対象人数(${r.target})`);
    }
  });

  return {
    rows, analysisRows, appliedCount, offerCount, acceptedCount,
    offerRate:    pct(offerCount,    appliedCount),
    acceptedRate: pct(acceptedCount, appliedCount),
    bottleneck: bot, errors,
  };
}

// ─── メインコンポーネント ────────────────────────────────────────────
export function FunnelTab({ client, onUpdate }) {
  const [funnel,      setFunnel]      = useState(() => mergeWithEmpty(client.data?.funnel));
  const [templates,   setTemplates]   = useState(loadTemplates);
  const [insertAfter, setInsertAfter] = useState('offer');
  const [selTemplate, setSelTemplate] = useState(() => loadTemplates()[0]?.id || '');
  const [newTplName,  setNewTplName]  = useState('');
  const saveTimer = useRef(null);

  // 案件切り替え時に再初期化
  useEffect(() => {
    setFunnel(mergeWithEmpty(client.data?.funnel));
  }, [client.id]);

  // 800ms デバウンス保存
  const persist = useCallback((f) => {
    clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => onUpdate({ funnel: f }), 800);
  }, [onUpdate]);

  const update = useCallback((updater) => {
    setFunnel(prev => {
      const next = typeof updater === 'function' ? updater(prev) : { ...prev, ...updater };
      persist(next);
      return next;
    });
  }, [persist]);

  const analysis      = computeAnalysis(funnel);
  const orderedStages = getOrderedStages(funnel);

  // ─── ステージ操作 ────────────────────────────────────────────────
  const addStage = () => {
    const tpl = templates.find(t => t.id === selTemplate) || templates[0];
    if (!tpl) return;
    const ns = { id: genId('stg'), templateId: tpl.id, name: tpl.name, enabled: true };
    update(prev => {
      const order = [...prev.stageOrder];
      const idx   = order.indexOf(insertAfter);
      order.splice(idx >= 0 ? idx + 1 : order.length - 1, 0, ns.id);
      return {
        ...prev,
        insertedTemplateStages: [...prev.insertedTemplateStages, ns],
        stageOrder: enforceFixedOrder(order),
        stageValues: { ...prev.stageValues, [ns.id]: { targetInput: 0, pass: 0, decline: 0, fail: 0, pending: 0 } },
      };
    });
  };

  const removeStage = (id) => {
    if (!window.confirm('このステージを削除しますか？')) return;
    update(prev => ({
      ...prev,
      insertedTemplateStages: prev.insertedTemplateStages.filter(s => s.id !== id),
      stageOrder: prev.stageOrder.filter(sid => sid !== id),
      stageValues: Object.fromEntries(Object.entries(prev.stageValues).filter(([k]) => k !== id)),
    }));
  };

  const moveStage = (id, dir) => {
    update(prev => {
      const order = [...prev.stageOrder];
      const idx   = order.indexOf(id);
      const to    = idx + dir;
      if (to < 1 || to >= order.length - 1) return prev;
      [order[idx], order[to]] = [order[to], order[idx]];
      return { ...prev, stageOrder: enforceFixedOrder(order) };
    });
  };

  const toggleStage = (id, enabled) => {
    update(prev => {
      if (FIXED_IDS.has(id)) {
        return { ...prev, enabledStages: { ...prev.enabledStages, [id]: enabled } };
      }
      return {
        ...prev,
        insertedTemplateStages: prev.insertedTemplateStages.map(s => s.id === id ? { ...s, enabled } : s),
      };
    });
  };

  const updateVal = (stageId, key, raw) => {
    const num = Math.max(0, parseInt(raw, 10) || 0);
    update(prev => ({
      ...prev,
      stageValues: {
        ...prev.stageValues,
        [stageId]: { ...prev.stageValues[stageId], [key]: num },
      },
    }));
  };

  // ─── テンプレート操作 ────────────────────────────────────────────
  const addTemplate = () => {
    const name = newTplName.trim();
    if (!name || templates.some(t => t.name === name)) return;
    const nt = [...templates, { id: genId('tpl'), name }];
    setTemplates(nt); saveTemplates(nt); setNewTplName('');
  };

  const removeTemplate = (id) => {
    const nt = templates.filter(t => t.id !== id);
    setTemplates(nt); saveTemplates(nt);
  };

  // ─── CSV出力 ─────────────────────────────────────────────────────
  const exportCsv = () => {
    const rows = [
      [`${client.name} 選考歩留まり分析`],
      [],
      ['フェーズ', '対象人数', '合格数', '辞退数', '不合格数', '進行中数', '合格率', '辞退率', '不合格率', '応募からの到達率'],
      ...analysis.analysisRows.map(r => [
        r.stage.name, r.target, r.pass, r.decline, r.fail, r.pending,
        r.passRate, r.declineRate, r.failRate, r.arrivalRate,
      ]),
      [],
      ['応募数',        analysis.appliedCount],
      ['内定数',        analysis.offerCount],
      ['内定承諾数',    analysis.acceptedCount],
      ['応募→内定率',   analysis.offerRate],
      ['応募→内定承諾率', analysis.acceptedRate],
    ];
    const csv = '\uFEFF' + rows
      .map(r => r.map(c => `"${String(c ?? '').replace(/"/g, '""')}"`).join(','))
      .join('\r\n');
    const a = Object.assign(document.createElement('a'), {
      href: URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' })),
      download: `${client.name}_歩留まり.csv`,
    });
    a.click(); URL.revokeObjectURL(a.href);
  };

  // ─── レンダリング ────────────────────────────────────────────────
  return (
    <div className="funnel-tab">
      {/* ヘッダー行 */}
      <div className="funnel-header">
        <div className="funnel-add-row">
          <div className="funnel-select-group">
            <label>挿入位置</label>
            <select value={insertAfter} onChange={e => setInsertAfter(e.target.value)}>
              {orderedStages.filter(s => s.id !== 'accept').map(s => (
                <option key={s.id} value={s.id}>{s.name}</option>
              ))}
            </select>
          </div>
          <div className="funnel-select-group">
            <label>ステージ追加</label>
            <select value={selTemplate} onChange={e => setSelTemplate(e.target.value)}>
              {templates.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          </div>
          <button className="btn-primary btn-sm" onClick={addStage}>＋ 追加</button>
        </div>
        <button className="btn-secondary btn-sm" onClick={exportCsv}>CSV出力</button>
      </div>

      {/* バリデーション警告 */}
      {analysis.errors.length > 0 && (
        <div className="funnel-errors">
          {analysis.errors.map((e, i) => <div key={i}>⚠ {e}</div>)}
        </div>
      )}

      <div className="funnel-layout">
        {/* ─ 左カラム: ステージ管理 + テンプレート管理 ─ */}
        <div className="funnel-left">
          <div className="funnel-section-title">選考フロー設定</div>
          <div className="funnel-stage-list">
            {orderedStages.map(s => (
              <div key={s.id} className={`funnel-stage-item ${s.enabled ? '' : 'faded'}`}>
                <label className="funnel-stage-check">
                  <input
                    type="checkbox"
                    checked={s.enabled}
                    disabled={s.required}
                    onChange={e => toggleStage(s.id, e.target.checked)}
                  />
                  <span className="funnel-stage-name">{s.name}</span>
                  {s.required && <span className="funnel-required-badge">必須</span>}
                </label>
                {s.type === 'template' && (
                  <div className="funnel-stage-btns">
                    <button className="funnel-icon-btn" onClick={() => moveStage(s.id, -1)} title="上に移動">▲</button>
                    <button className="funnel-icon-btn" onClick={() => moveStage(s.id, 1)}  title="下に移動">▼</button>
                    <button className="funnel-icon-btn danger" onClick={() => removeStage(s.id)}>×</button>
                  </div>
                )}
              </div>
            ))}
          </div>

          <div className="funnel-section-title" style={{ marginTop: 16 }}>テンプレート管理</div>
          <div className="funnel-tpl-add">
            <input
              value={newTplName}
              onChange={e => setNewTplName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && addTemplate()}
              placeholder="新規テンプレ名"
            />
            <button className="btn-secondary btn-sm" onClick={addTemplate}>追加</button>
          </div>
          <div className="funnel-tpl-list">
            {templates.map(t => (
              <div key={t.id} className="funnel-tpl-item">
                <span>{t.name}</span>
                <button className="funnel-icon-btn danger" onClick={() => removeTemplate(t.id)}>×</button>
              </div>
            ))}
          </div>
        </div>

        {/* ─ 右カラム: 入力 + 分析 ─ */}
        <div className="funnel-right">
          {/* 入力テーブル */}
          <div className="funnel-table-wrap">
            <table className="funnel-input-table">
              <thead>
                <tr>
                  <th>選考フロー</th>
                  <th>対象人数</th>
                  <th>合格数</th>
                  <th>辞退数</th>
                  <th>不合格数</th>
                  <th>進行中数</th>
                </tr>
              </thead>
              <tbody>
                {analysis.rows.map(({ stage, target }) => {
                  const isAccept = stage.id === 'accept';
                  const sv = funnel.stageValues[stage.id] || {};
                  const sum = sv.pass + sv.decline + sv.fail + sv.pending;
                  const mismatch = !isAccept && target > 0 && sum !== target;
                  return (
                    <tr key={stage.id} className={mismatch ? 'funnel-row-warn' : ''}>
                      <td className="funnel-stage-cell">{stage.name}</td>
                      <td>
                        {stage.id === 'applied' ? (
                          <input type="number" min="0" value={sv.targetInput || 0}
                            onChange={e => updateVal('applied', 'targetInput', e.target.value)} />
                        ) : (
                          <input type="number" readOnly value={target} className="funnel-readonly" />
                        )}
                      </td>
                      {['pass', 'decline', 'fail', 'pending'].map(key => (
                        <td key={key}>
                          {isAccept
                            ? <span className="funnel-dash">—</span>
                            : <input type="number" min="0" value={sv[key] || 0}
                                onChange={e => updateVal(stage.id, key, e.target.value)} />
                          }
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* サマリー */}
          <div className="funnel-summary">
            {[
              { label: '応募数',        value: analysis.appliedCount  + ' 人' },
              { label: '内定数',        value: analysis.offerCount    + ' 人' },
              { label: '内定承諾数',    value: analysis.acceptedCount + ' 人' },
              { label: '応募→内定率',   value: analysis.offerRate },
              { label: '応募→承諾率',   value: analysis.acceptedRate },
            ].map(({ label, value }) => (
              <div key={label} className="funnel-summary-item">
                <div className="funnel-summary-label">{label}</div>
                <div className="funnel-summary-value">{value}</div>
              </div>
            ))}
          </div>

          {/* ボトルネック */}
          {analysis.bottleneck && (
            <div className="funnel-bottleneck">
              <strong>ボトルネック検出</strong>
              <ul>
                <li>最も合格率が低い: <b>{analysis.bottleneck.lowestPassRate.stage.name}</b>（{pct(analysis.bottleneck.lowestPassRate.pass, analysis.bottleneck.lowestPassRate.target)}）</li>
                <li>辞退が最も多い: <b>{analysis.bottleneck.maxDeclineCount.stage.name}</b>（{analysis.bottleneck.maxDeclineCount.decline} 人）</li>
                <li>最も辞退率が高い: <b>{analysis.bottleneck.maxDeclineRate.stage.name}</b>（{pct(analysis.bottleneck.maxDeclineRate.decline, analysis.bottleneck.maxDeclineRate.target)}）</li>
                <li>不合格が最も多い: <b>{analysis.bottleneck.maxFailCount.stage.name}</b>（{analysis.bottleneck.maxFailCount.fail} 人）</li>
                <li>最も不合格率が高い: <b>{analysis.bottleneck.maxFailRate.stage.name}</b>（{pct(analysis.bottleneck.maxFailRate.fail, analysis.bottleneck.maxFailRate.target)}）</li>
              </ul>
            </div>
          )}

          {/* 分析テーブル */}
          <div className="funnel-table-wrap">
            <table className="funnel-analysis-table">
              <thead>
                <tr>
                  <th>選考フロー</th>
                  <th>合格率</th>
                  <th>辞退率</th>
                  <th>不合格率</th>
                  <th>進行中率</th>
                  <th>応募からの到達率</th>
                </tr>
              </thead>
              <tbody>
                {analysis.analysisRows.map(r => (
                  <tr key={r.stage.id}>
                    <td className="funnel-stage-cell">{r.stage.name}</td>
                    <td>{r.passRate}</td>
                    <td>{r.declineRate}</td>
                    <td>{r.failRate}</td>
                    <td>{r.pendingRate}</td>
                    <td>{r.arrivalRate}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
