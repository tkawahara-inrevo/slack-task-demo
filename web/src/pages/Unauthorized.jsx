export default function Unauthorized() {
  return (
    <div className="unauthorized">
      <h2>セッションが無効です</h2>
      <p>Slack で <code>/dashboard</code> コマンドを実行して、ダッシュボードを開いてください。</p>
    </div>
  );
}
