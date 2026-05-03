import { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import Pipeline from './Pipeline';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';

export default function CRM() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') || 'dashboard';

  const setTab = (t) => setSearchParams({ tab: t }, { replace: true });

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* タブバー */}
      <div style={{ display: 'flex', borderBottom: '1px solid #e5e7eb', background: '#fff', paddingLeft: 8, flexShrink: 0 }}>
        {[
          { key: 'dashboard',  label: 'ダッシュボード' },
          { key: 'pipeline',  label: 'パイプライン' },
          { key: 'customers', label: '顧客一覧' },
          { key: 'performance', label: '成績' },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{
              padding: '10px 20px', border: 'none', background: 'none', cursor: 'pointer',
              fontSize: '0.88rem', fontWeight: tab === t.key ? 700 : 400,
              color: tab === t.key ? '#1d4ed8' : '#6b7280',
              borderBottom: tab === t.key ? '2px solid #1d4ed8' : '2px solid transparent',
              transition: 'color 0.15s',
            }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* タブコンテンツ */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {tab === 'dashboard'   && <CrmDashboard />}
        {tab === 'pipeline'    && <Pipeline embedded />}
        {tab === 'customers'   && <CustomerList />}
        {tab === 'performance' && <SalesPerformance embedded />}
      </div>
    </div>
  );
}

