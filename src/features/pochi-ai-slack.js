// Slack 上の Pochi AI 機能
// ① :scroll: / :memo: リアクションで要約
// ② /pochi-ask スラッシュコマンドで質問

let _client = null;
function getAnthropic() {
  if (_client) return _client;
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error('ANTHROPIC_API_KEY not set');
  const Anthropic = require('@anthropic-ai/sdk').default;
  _client = new Anthropic({ apiKey: key });
  return _client;
}

// 要約発火リアクション（複数指定可）
const SUMMARIZE_REACTIONS = new Set(['scroll', 'memo']);

function registerPochiAiSlack({ app, getUserDisplayName, getTeamIdFromBody }) {

  // ──────────────────────────────────────────────────
  // ① リアクション → スレッド/メッセージ要約
  // ──────────────────────────────────────────────────
  app.event('reaction_added', async ({ event, client, body }) => {
    try {
      if (!SUMMARIZE_REACTIONS.has(event.reaction)) return;
      const teamId = body?.team_id || event?.item_user || event?.user || null;

      const item = event.item;
      if (!item || item.type !== 'message') return;
      const channelId = item.channel;
      const ts = item.ts;
      const actorUserId = event.user;

      // メッセージ取得（スレッド親かreplyかを判定）
      let messages = [];
      try {
        const r = await client.conversations.replies({ channel: channelId, ts, limit: 200 });
        messages = r.messages || [];
      } catch (e) {
        // スレッドAPI失敗時は単メッセージにフォールバック
        try {
          const h = await client.conversations.history({ channel: channelId, latest: ts, inclusive: true, limit: 1 });
          messages = h.messages || [];
        } catch { messages = []; }
      }

      if (messages.length === 0) return;

      // 親メッセージがthread_tsを持つ場合（リアクションがreplyに付いた）→ 親から取り直し
      const firstMsg = messages[0];
      if (firstMsg?.thread_ts && firstMsg.thread_ts !== ts) {
        try {
          const r2 = await client.conversations.replies({ channel: channelId, ts: firstMsg.thread_ts, limit: 200 });
          if (r2.messages?.length > 0) messages = r2.messages;
        } catch {}
      }

      // ユーザー名解決
      const userIds = new Set();
      for (const m of messages) {
        if (m.user) userIds.add(m.user);
        for (const mm of (m.text || '').matchAll(/<@(U[A-Z0-9]+)>/g)) userIds.add(mm[1]);
      }
      const nameMap = new Map();
      await Promise.all([...userIds].map(async uid => {
        try { nameMap.set(uid, await getUserDisplayName(teamId, uid) || uid); }
        catch { nameMap.set(uid, uid); }
      }));
      const resolveName = (uid) => nameMap.get(uid) || uid;

      // 本文整形（mention置換）
      const replaceMentions = (text) => (text || '').replace(/<@(U[A-Z0-9]+)>/g, (_, u) => `@${resolveName(u)}`);

      // 要約用プロンプト構築
      const lines = messages.map(m => {
        const author = m.user ? resolveName(m.user) : (m.username || (m.bot_profile?.name) || 'bot');
        const text = replaceMentions(m.text || '').replace(/\n+/g, ' ').slice(0, 800);
        return `${author}: ${text}`;
      }).join('\n');

      const anthropic = getAnthropic();
      const SYSTEM = `あなたはSlack上のタスク管理アシスタント「Pochi」の要約機能です。
Slackスレッドの会話を読み取り、忙しい人が30秒で把握できるよう要約します。

# 出力フォーマット（Slack mrkdwn使用）
*📌 議論の要旨*
1-2文で全体像

*✅ 決まったこと*
・箇条書き（無ければ「特になし」）

*❓ 未決事項*
・箇条書き（無ければ「特になし」）

*📝 アクションアイテム*
@担当者名: やること（〜期限）
（無ければ「特になし」）

# ルール
- メッセージ件数が3件未満なら、議論というより単発メッセージなので「📌 要旨」のみ簡潔に。
- 担当者名は本文に出てくる人名・@メンションをそのまま使う。
- 装飾は最小限。emoji と *bold* のみ。`;

      const userMsg = `スレッド（${messages.length}件・参加者${userIds.size}名）の会話:\n\n${lines}\n\n上記を要約してください。`;

      const response = await anthropic.messages.create({
        model: 'claude-haiku-4-5',
        max_tokens: 1024,
        system: SYSTEM,
        messages: [{ role: 'user', content: userMsg }],
      });

      const summary = (response.content || [])
        .filter(b => b.type === 'text').map(b => b.text).join('\n').trim()
        || '（要約生成に失敗しました）';

      // 結果は ephemeral でリアクションした本人にだけ返す
      const targetTs = (messages[0]?.thread_ts || ts);
      try {
        await client.chat.postEphemeral({
          channel: channelId,
          user: actorUserId,
          thread_ts: targetTs,
          text: `🐶 *Pochi要約*\n${summary}`,
        });
      } catch (e) {
        // ephemeral 失敗時はDMフォールバック
        try {
          const dm = await client.conversations.open({ users: actorUserId });
          await client.chat.postMessage({
            channel: dm.channel.id,
            text: `🐶 *Pochi要約*（スレッド: <https://slack.com/archives/${channelId}/p${String(targetTs).replace('.','')}|元メッセージ>）\n\n${summary}`,
          });
        } catch (e2) { console.error('[pochi-ai] notify fail:', e2.message); }
      }
    } catch (e) { console.error('[pochi-ai] reaction summarize error:', e.message); }
  });

  // ──────────────────────────────────────────────────
  // ② /pochi-ask <質問>
  // ──────────────────────────────────────────────────
  app.command('/pochi-ask', async ({ ack, body, client, command, respond }) => {
    await ack();
    try {
      const question = (command?.text || '').trim();
      if (!question) {
        await respond({
          response_type: 'ephemeral',
          text: '🐶 質問を書いてね！\n例: `/pochi-ask このチャンネルでまだ決まってないことを教えて`',
        });
        return;
      }

      const channelId = command.channel_id;
      const userId = command.user_id;
      const teamId = body?.team_id;

      // チャンネル直近の会話履歴を取得（最大50件）
      let messages = [];
      try {
        const h = await client.conversations.history({ channel: channelId, limit: 50 });
        messages = (h.messages || []).reverse(); // 古い順に
      } catch (e) {
        await respond({ response_type: 'ephemeral', text: `🐶 このチャンネルの履歴が取れませんでした: ${e?.data?.error || e.message}` });
        return;
      }

      // ユーザー名解決
      const userIds = new Set();
      for (const m of messages) {
        if (m.user) userIds.add(m.user);
        for (const mm of (m.text || '').matchAll(/<@(U[A-Z0-9]+)>/g)) userIds.add(mm[1]);
      }
      const nameMap = new Map();
      await Promise.all([...userIds].map(async uid => {
        try { nameMap.set(uid, await getUserDisplayName(teamId, uid) || uid); }
        catch { nameMap.set(uid, uid); }
      }));
      const resolveName = (uid) => nameMap.get(uid) || uid;
      const replaceMentions = (text) => (text || '').replace(/<@(U[A-Z0-9]+)>/g, (_, u) => `@${resolveName(u)}`);

      const ctxLines = messages.map(m => {
        const author = m.user ? resolveName(m.user) : (m.username || m.bot_profile?.name || 'bot');
        return `${author}: ${replaceMentions(m.text || '').slice(0, 500)}`;
      }).join('\n');

      const anthropic = getAnthropic();
      const SYSTEM = `あなたはSlack上のアシスタント「Pochi」です。チャンネルの直近の会話を踏まえてユーザーの質問に答えます。

# ルール
- 答えはSlackで読みやすいよう mrkdwn で。emoji と *bold* のみ。
- チャンネル文脈と関係ない一般質問にも答えてOKだが、関係する文脈があれば積極的に参照する。
- 「文脈にないことは推測しない」「分からないことは正直に分からないと言う」。
- 簡潔に。箇条書き優先。`;

      const response = await anthropic.messages.create({
        model: 'claude-haiku-4-5',
        max_tokens: 1024,
        system: SYSTEM,
        messages: [{
          role: 'user',
          content: `# このチャンネルの直近の会話（${messages.length}件）\n${ctxLines}\n\n# 質問\n${question}`,
        }],
      });

      const answer = (response.content || [])
        .filter(b => b.type === 'text').map(b => b.text).join('\n').trim()
        || '（回答生成に失敗しました）';

      await respond({
        response_type: 'ephemeral',
        text: `🐶 *Pochiの回答*\n_質問: ${question}_\n\n${answer}\n\n_※ 直近${messages.length}件の会話を参考にしました_`,
      });
    } catch (e) {
      console.error('[pochi-ai] /pochi-ask error:', e.message);
      try { await respond({ response_type: 'ephemeral', text: `🐶 エラー: ${e.message}` }); } catch {}
    }
  });

  console.log('[pochi-ai] reaction summarize (:scroll:/:memo:) + /pochi-ask registered');
}

module.exports = { registerPochiAiSlack };
