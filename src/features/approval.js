// Slack 電子決裁
// - スラッシュコマンド /pochi-approval で起票モーダル
// - チャンネルにカード投稿（承認/否認ボタン付き）
// - 指定された承認者のみボタン押下可
// - 並列モード: 全員揃ったら完了 / 順次モード: 1人ずつ次の人へ
// - スレッドに承認ログを返信
// - 起票者にDM通知（決裁完了 / 否認）

function registerApproval({
  app,
  dbQuery,
  getUserDisplayName,
  getTeamIdFromBody,
  noMention,
  safeJsonParse,
}) {
  const { randomUUID } = require('crypto');

  // ── DB ヘルパー ─────────────────────────────────
  async function createApproval(data) {
    const { id, team_id, requester_user_id, title, description, channel_id, mode, voters, origin_thread_ts } = data;
    await dbQuery(
      `INSERT INTO approvals (id, team_id, requester_user_id, title, description, channel_id, mode, status, origin_thread_ts)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8)`,
      [id, team_id, requester_user_id, title, description || null, channel_id, mode || 'parallel', origin_thread_ts || null]
    );
    for (let i = 0; i < voters.length; i++) {
      await dbQuery(
        `INSERT INTO approval_voters (approval_id, user_id, order_idx, status)
         VALUES ($1,$2,$3,'pending')`,
        [id, voters[i], i]
      );
    }
  }
  async function getApprovalById(id) {
    const r = await dbQuery(`SELECT * FROM approvals WHERE id=$1`, [id]);
    return r.rows[0] || null;
  }
  async function listVoters(id) {
    const r = await dbQuery(`SELECT * FROM approval_voters WHERE approval_id=$1 ORDER BY order_idx ASC`, [id]);
    return r.rows;
  }
  async function setVote(approvalId, userId, status, comment = null) {
    await dbQuery(
      `UPDATE approval_voters SET status=$3, comment=$4, decided_at=now()
       WHERE approval_id=$1 AND user_id=$2`,
      [approvalId, userId, status, comment]
    );
  }
  async function setApprovalStatus(id, status) {
    await dbQuery(
      `UPDATE approvals SET status=$2, completed_at = CASE WHEN $2 IN ('approved','rejected','cancelled') THEN now() ELSE completed_at END
       WHERE id=$1`,
      [id, status]
    );
  }
  async function setApprovalMessageTs(id, ts) {
    await dbQuery(`UPDATE approvals SET message_ts=$2 WHERE id=$1`, [id, ts]);
  }

  // ── Slack ブロック構築 ──────────────────────────
  function statusEmoji(s) {
    return s === 'approved' ? '✅' : s === 'rejected' ? '❌' : '⏳';
  }
  function statusLabel(s) {
    return s === 'approved' ? '決裁完了' : s === 'rejected' ? '否認' : s === 'cancelled' ? '取り下げ' : '承認待ち';
  }
  function modeLabel(m) {
    return m === 'sequential' ? '順次承認（1人ずつ）' : '並列承認（全員）';
  }

  async function buildApprovalCardBlocks({ approval, voters, teamId }) {
    const nameOf = async (uid) => {
      try { return await getUserDisplayName(teamId, uid) || uid; } catch { return uid; }
    };
    const requesterName = await nameOf(approval.requester_user_id);

    // 投票者の表示行
    const voterLines = await Promise.all(voters.map(async (v) => {
      const n = await nameOf(v.user_id);
      const e = statusEmoji(v.status);
      const t = v.decided_at ? new Date(v.decided_at).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit', month: '2-digit', day: '2-digit' }) : '';
      const c = v.comment ? `（${v.comment}）` : '';
      return `${e} ${n}${t ? `　_${t}_` : ''}${c}`;
    }));

    const isDone = approval.status === 'approved' || approval.status === 'rejected' || approval.status === 'cancelled';

    // 順次モードでは次に承認可能なのは1人だけ
    let actableUserIds = null;
    if (!isDone) {
      if (approval.mode === 'sequential') {
        const next = voters.find(v => v.status === 'pending');
        actableUserIds = next ? new Set([next.user_id]) : new Set();
      } else {
        actableUserIds = new Set(voters.filter(v => v.status === 'pending').map(v => v.user_id));
      }
    }

    const blocks = [];
    blocks.push({
      type: 'header',
      text: { type: 'plain_text', text: `${statusEmoji(approval.status)} 電子決裁: ${noMention(approval.title).slice(0, 100)}` },
    });
    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: `*ステータス:* ${statusLabel(approval.status)}　・　*起票:* ${requesterName}　・　*方式:* ${modeLabel(approval.mode)}`,
      }],
    });
    if (approval.description) {
      blocks.push({
        type: 'section',
        text: { type: 'mrkdwn', text: noMention(approval.description).slice(0, 2800) },
      });
    }
    blocks.push({ type: 'divider' });
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `*承認者*\n${voterLines.join('\n')}` },
    });

    // ボタン（決裁完了・否認後は出さない）
    if (!isDone) {
      blocks.push({
        type: 'actions',
        block_id: 'approval_actions',
        elements: [
          {
            type: 'button',
            style: 'primary',
            action_id: 'approval_approve',
            text: { type: 'plain_text', text: '✅ 承認' },
            value: approval.id,
          },
          {
            type: 'button',
            style: 'danger',
            action_id: 'approval_reject',
            text: { type: 'plain_text', text: '❌ 否認' },
            value: approval.id,
          },
        ],
      });
      if (approval.mode === 'sequential') {
        const next = voters.find(v => v.status === 'pending');
        if (next) {
          const nName = await nameOf(next.user_id);
          blocks.push({
            type: 'context',
            elements: [{ type: 'mrkdwn', text: `_今押せるのは ${nName} さんだけです（順次承認）_` }],
          });
        }
      }
    } else {
      blocks.push({
        type: 'context',
        elements: [{ type: 'mrkdwn', text: `_${statusLabel(approval.status)} (${new Date(approval.completed_at || Date.now()).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' })})_` }],
      });
    }
    return { blocks, actableUserIds };
  }

  async function rerenderCard(client, approval) {
    const voters = await listVoters(approval.id);
    const { blocks } = await buildApprovalCardBlocks({ approval, voters, teamId: approval.team_id });
    if (approval.message_ts) {
      try {
        await client.chat.update({
          channel: approval.channel_id,
          ts: approval.message_ts,
          text: `電子決裁: ${approval.title}`,
          blocks,
        });
      } catch (e) {
        console.error('[approval] card update fail:', e?.data || e);
      }
    }
  }

  async function postThreadLog(client, approval, text) {
    try {
      // origin_thread_ts があれば そのスレッドに、なければ approval カード自身をスレッド親として
      const thread_ts = approval.origin_thread_ts || approval.message_ts;
      await client.chat.postMessage({
        channel: approval.channel_id,
        thread_ts,
        text,
        unfurl_links: false,
        unfurl_media: false,
      });
    } catch (e) { console.warn('[approval] log post fail:', e.message); }
  }

  async function notifyRequester(client, approval, kind) {
    try {
      const dm = await client.conversations.open({ users: approval.requester_user_id });
      const channel = dm?.channel?.id;
      if (!channel) return;
      const url = approval.message_ts
        ? `https://slack.com/archives/${approval.channel_id}/p${String(approval.message_ts).replace('.', '')}`
        : null;
      const link = url ? ` <${url}|起票を開く>` : '';
      const text = kind === 'approved'
        ? `✅ あなたが起票した決裁が *全員承認* されました.${link}\n_「${noMention(approval.title)}」_`
        : `❌ あなたが起票した決裁が *否認* されました.${link}\n_「${noMention(approval.title)}」_`;
      await client.chat.postMessage({ channel, text });
    } catch (e) { console.warn('[approval] notify requester fail:', e.message); }
  }

  // ── スラッシュコマンド: 起票モーダル ────────────
  app.command('/pochi-approval', async ({ ack, body, client }) => {
    await ack();
    try {
      await client.views.open({
        trigger_id: body.trigger_id,
        view: buildCreateModal({ defaultChannel: body.channel_id }),
      });
    } catch (e) {
      console.error('[approval] open create modal fail:', e?.data || e);
    }
  });

  // ── メッセージショートカット: スレッドからでも起票可能 ──
  app.shortcut('create_approval', async ({ ack, shortcut, client }) => {
    await ack();
    try {
      const defaultChannel = shortcut.channel?.id;
      // クリックしたメッセージがスレッド内なら thread_ts、トップレベルなら ts を起点に
      const originThreadTs = shortcut.message?.thread_ts || shortcut.message?.ts || null;
      const sourceText = (shortcut.message?.text || '').slice(0, 2800);
      await client.views.open({
        trigger_id: shortcut.trigger_id,
        view: buildCreateModal({ defaultChannel, originThreadTs, initialDescription: sourceText }),
      });
    } catch (e) {
      console.error('[approval] open create modal (shortcut) fail:', e?.data || e);
    }
  });

  // ── グローバルショートカット: どこからでも起票 ──
  app.shortcut('create_approval_global', async ({ ack, shortcut, client }) => {
    await ack();
    try {
      await client.views.open({
        trigger_id: shortcut.trigger_id,
        view: buildCreateModal({}),
      });
    } catch (e) {
      console.error('[approval] open create modal (global) fail:', e?.data || e);
    }
  });

  function buildCreateModal({ defaultChannel, originThreadTs, initialDescription } = {}) {
    // 起点のチャンネル + スレッド情報を private_metadata に保持
    return {
      type: 'modal',
      callback_id: 'approval_create_modal',
      private_metadata: JSON.stringify({ channelId: defaultChannel || null, originThreadTs: originThreadTs || null }),
      title: { type: 'plain_text', text: '電子決裁を起票' },
      submit: { type: 'plain_text', text: '起票' },
      close: { type: 'plain_text', text: 'キャンセル' },
      blocks: [
        {
          type: 'input',
          block_id: 'title',
          label: { type: 'plain_text', text: 'タイトル' },
          element: { type: 'plain_text_input', action_id: 'v', max_length: 100, placeholder: { type: 'plain_text', text: '例: 6月接待費の承認願い' } },
        },
        {
          type: 'input',
          block_id: 'desc',
          optional: true,
          label: { type: 'plain_text', text: '内容・補足' },
          element: {
            type: 'plain_text_input',
            action_id: 'v',
            multiline: true,
            max_length: 3000,
            placeholder: { type: 'plain_text', text: '金額・背景・添付など' },
            ...(initialDescription ? { initial_value: initialDescription } : {}),
          },
        },
        {
          type: 'input',
          block_id: 'voters',
          label: { type: 'plain_text', text: '承認者（複数選択可）' },
          element: { type: 'multi_users_select', action_id: 'v', max_selected_items: 10 },
        },
        {
          type: 'input',
          block_id: 'mode',
          label: { type: 'plain_text', text: '承認方式' },
          element: {
            type: 'static_select',
            action_id: 'v',
            initial_option: { text: { type: 'plain_text', text: '並列承認（全員）' }, value: 'parallel' },
            options: [
              { text: { type: 'plain_text', text: '並列承認（全員）' }, value: 'parallel' },
              { text: { type: 'plain_text', text: '順次承認（1人ずつ）' }, value: 'sequential' },
            ],
          },
        },
      ],
    };
  }

  // ── 起票モーダル送信 ────────────────────────────
  app.view('approval_create_modal', async ({ ack, body, view, client }) => {
    const requester = body.user?.id;
    const teamId = getTeamIdFromBody(body);
    const title = view.state.values.title?.v?.value?.trim() || '';
    const description = view.state.values.desc?.v?.value?.trim() || '';
    const meta = safeJsonParse(view.private_metadata) || {};
    let channelId = meta.channelId;
    const originThreadTs = meta.originThreadTs || null;
    const voters = view.state.values.voters?.v?.selected_users || [];
    const mode = view.state.values.mode?.v?.selected_option?.value || 'parallel';

    const errors = {};
    if (!title) errors.title = 'タイトルを入力してください';
    if (voters.length === 0) errors.voters = '承認者を1人以上選択してください';
    if (voters.includes(requester)) errors.voters = '自分自身を承認者にはできません';
    if (Object.keys(errors).length) {
      await ack({ response_action: 'errors', errors });
      return;
    }
    if (!channelId) {
      // チャンネル情報が取れていない場合（グローバルショートカット等）は起票者のDMにフォールバック
      try {
        const dm = await client.conversations.open({ users: requester });
        const dmChannel = dm?.channel?.id;
        if (!dmChannel) {
          await ack({ response_action: 'errors', errors: { voters: 'チャンネル情報が取得できませんでした。スレッドや投稿欄から起動してください' } });
          return;
        }
        channelId = dmChannel;
        meta.channelId = dmChannel;
      } catch {
        await ack({ response_action: 'errors', errors: { voters: 'チャンネル情報が取得できませんでした' } });
        return;
      }
    }

    // 順次承認 かつ 承認者2人以上の場合は並び順モーダルへ
    if (mode === 'sequential' && voters.length > 1) {
      const reorderView = buildReorderModal({ title, description, channelId, voters, mode, originThreadTs });
      await ack({ response_action: 'push', view: reorderView });
      return;
    }

    await ack();
    await createAndPostApproval(client, { requester, teamId, title, description, channelId, voters, mode, originThreadTs });
  });

  // 順次モード: 並び順指定モーダル
  function buildReorderModal({ title, description, channelId, voters, mode, originThreadTs }) {
    const metadata = JSON.stringify({ title, description, channelId, voters, mode, originThreadTs: originThreadTs || null });
    const blocks = [
      {
        type: 'section',
        text: { type: 'mrkdwn', text: `*順次承認の並び順を指定してください*\n上から順に1人ずつ承認していきます（先に選んだ人が押すまで次の人は押せません）。` },
      },
      { type: 'divider' },
    ];
    voters.forEach((uid, idx) => {
      blocks.push({
        type: 'input',
        block_id: `slot_${idx}`,
        label: { type: 'plain_text', text: `${idx + 1}番目` },
        element: {
          type: 'users_select',
          action_id: 'v',
          initial_user: uid,
        },
      });
    });
    return {
      type: 'modal',
      callback_id: 'approval_reorder_modal',
      private_metadata: metadata,
      title: { type: 'plain_text', text: '承認の並び順' },
      submit: { type: 'plain_text', text: '起票' },
      close: { type: 'plain_text', text: 'キャンセル' },
      blocks,
    };
  }

  // 並び順モーダル送信
  app.view('approval_reorder_modal', async ({ ack, body, view, client }) => {
    const meta = safeJsonParse(view.private_metadata) || {};
    const { title, description, channelId, voters: originalVoters, mode, originThreadTs } = meta;
    const requester = body.user?.id;
    const teamId = getTeamIdFromBody(body);

    // 各スロットから選ばれたユーザーを順番に集める
    const orderedVoters = [];
    for (let i = 0; i < originalVoters.length; i++) {
      const uid = view.state.values[`slot_${i}`]?.v?.selected_user;
      if (uid) orderedVoters.push(uid);
    }

    // バリデーション: 重複 / 元のメンバーと異なる
    const uniq = new Set(orderedVoters);
    if (uniq.size !== orderedVoters.length) {
      const errors = {};
      // 重複している最初のスロットにエラー表示
      const seen = new Set();
      for (let i = 0; i < orderedVoters.length; i++) {
        if (seen.has(orderedVoters[i])) {
          errors[`slot_${i}`] = '同じ人が複数のスロットに設定されています';
          break;
        }
        seen.add(orderedVoters[i]);
      }
      await ack({ response_action: 'errors', errors });
      return;
    }
    if (orderedVoters.length !== originalVoters.length) {
      await ack({ response_action: 'errors', errors: { slot_0: '全てのスロットを設定してください' } });
      return;
    }
    if (orderedVoters.includes(requester)) {
      await ack({ response_action: 'errors', errors: { slot_0: '自分自身を承認者にはできません' } });
      return;
    }

    await ack({ response_action: 'clear' });
    await createAndPostApproval(client, { requester, teamId, title, description, channelId, voters: orderedVoters, mode, originThreadTs });
  });

  // 共通: 決裁レコード作成 + Slack投稿
  async function createAndPostApproval(client, { requester, teamId, title, description, channelId, voters, mode, originThreadTs }) {
    try {
      try { await client.conversations.join({ channel: channelId }); } catch {}

      const id = randomUUID();
      await createApproval({
        id, team_id: teamId,
        requester_user_id: requester,
        title, description,
        channel_id: channelId,
        mode, voters,
        origin_thread_ts: originThreadTs || null,
      });

      const approval = await getApprovalById(id);
      const voterRows = await listVoters(id);
      const { blocks } = await buildApprovalCardBlocks({ approval, voters: voterRows, teamId });
      const postRes = await client.chat.postMessage({
        channel: channelId,
        text: `📋 電子決裁: ${noMention(title)}`,
        blocks,
        ...(originThreadTs ? { thread_ts: originThreadTs } : {}),
      });
      await setApprovalMessageTs(id, postRes.ts);
      approval.message_ts = postRes.ts;

      // スレッドに「承認お願いします」+ メンション
      // 順次モードでは1人目だけメンション、並列モードでは全員
      const targetMentions = mode === 'sequential' ? [voterRows[0].user_id] : voterRows.map(v => v.user_id);
      const mentionLine = targetMentions.map(u => `<@${u}>`).join(' ');
      await postThreadLog(client, approval, `${mentionLine}\n承認をお願いします 🙏`);

      // 起票者にDM
      try {
        const dm = await client.conversations.open({ users: requester });
        const url = `https://slack.com/archives/${channelId}/p${String(postRes.ts).replace('.', '')}`;
        await client.chat.postMessage({
          channel: dm.channel.id,
          text: `📋 電子決裁を起票しました.\n<${url}|決裁を開く>\n_「${noMention(title)}」_`,
        });
      } catch {}
    } catch (e) {
      console.error('[approval] create error:', e?.data || e);
    }
  }

  // ── 承認/否認ボタン共通処理 ────────────────────
  async function handleVote(body, action, client, decision /* 'approved' | 'rejected' */) {
    const approvalId = action.value;
    const actor = body.user?.id;
    const approval = await getApprovalById(approvalId);
    if (!approval) return { error: '決裁が見つかりません' };
    if (approval.status !== 'pending') return { error: 'この決裁はすでに完了しています' };

    const voters = await listVoters(approvalId);
    const voterRow = voters.find(v => v.user_id === actor);
    if (!voterRow) return { error: 'あなたは承認者として指定されていません' };
    if (voterRow.status !== 'pending') return { error: 'あなたはすでに判定済みです' };

    // 順次モードの場合、自分の順番か確認
    if (approval.mode === 'sequential') {
      const next = voters.find(v => v.status === 'pending');
      if (!next || next.user_id !== actor) return { error: 'まだあなたの順番ではありません' };
    }

    return { approval, voters, voterRow };
  }

  async function finalizeVote(client, approvalId, actor, decision, comment) {
    await setVote(approvalId, actor, decision, comment);
    const approval = await getApprovalById(approvalId);
    const voters = await listVoters(approvalId);
    const actorName = await getUserDisplayName(approval.team_id, actor).catch(() => actor);
    const ts = new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' });
    const logIcon = decision === 'approved' ? '✅' : '❌';
    const logComment = comment ? ` ／ ${comment}` : '';
    await postThreadLog(client, approval, `${logIcon} ${actorName} が ${decision === 'approved' ? '承認' : '否認'}しました (${ts})${logComment}`);

    // 完了判定
    if (decision === 'rejected') {
      await setApprovalStatus(approvalId, 'rejected');
      const upd = await getApprovalById(approvalId);
      await rerenderCard(client, upd);
      await notifyRequester(client, upd, 'rejected');
      return;
    }
    // 全員承認なら完了
    const remaining = voters.filter(v => v.user_id !== actor && v.status === 'pending').length;
    if (remaining === 0) {
      await setApprovalStatus(approvalId, 'approved');
      const upd = await getApprovalById(approvalId);
      await rerenderCard(client, upd);
      await notifyRequester(client, upd, 'approved');
    } else {
      // 進行中のまま再描画
      await rerenderCard(client, approval);
      if (approval.mode === 'sequential') {
        const updatedVoters = await listVoters(approvalId);
        const next = updatedVoters.find(v => v.status === 'pending');
        if (next) {
          await postThreadLog(client, approval, `<@${next.user_id}> 次の承認をお願いします 🙏`);
        }
      }
    }
  }

  // 承認ボタン
  app.action('approval_approve', async ({ ack, body, action, client, respond }) => {
    await ack();
    const res = await handleVote(body, action, client, 'approved');
    if (res.error) {
      try {
        await client.chat.postEphemeral({
          channel: body.channel?.id || body.container?.channel_id,
          user: body.user.id,
          text: `⚠️ ${res.error}`,
        });
      } catch {}
      return;
    }
    await finalizeVote(client, action.value, body.user.id, 'approved', null);
  });

  // 否認ボタン → 理由入力モーダル
  app.action('approval_reject', async ({ ack, body, action, client }) => {
    await ack();
    const res = await handleVote(body, action, client, 'rejected');
    if (res.error) {
      try {
        await client.chat.postEphemeral({
          channel: body.channel?.id || body.container?.channel_id,
          user: body.user.id,
          text: `⚠️ ${res.error}`,
        });
      } catch {}
      return;
    }
    try {
      await client.views.open({
        trigger_id: body.trigger_id,
        view: {
          type: 'modal',
          callback_id: 'approval_reject_modal',
          private_metadata: action.value,
          title: { type: 'plain_text', text: '否認の理由' },
          submit: { type: 'plain_text', text: '否認する' },
          close: { type: 'plain_text', text: 'キャンセル' },
          blocks: [
            {
              type: 'input',
              block_id: 'reason',
              label: { type: 'plain_text', text: '理由' },
              element: { type: 'plain_text_input', action_id: 'v', multiline: true, max_length: 500 },
            },
          ],
        },
      });
    } catch (e) { console.error('[approval] reject modal fail:', e?.data || e); }
  });

  app.view('approval_reject_modal', async ({ ack, body, view, client }) => {
    await ack();
    const approvalId = view.private_metadata;
    const reason = view.state.values.reason?.v?.value?.trim() || null;
    await finalizeVote(client, approvalId, body.user.id, 'rejected', reason);
  });

  console.log('[approval] registered (/pochi-approval)');
}

module.exports = { registerApproval };
