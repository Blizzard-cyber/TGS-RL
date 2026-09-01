import { NavLink, Outlet, useLocation } from 'react-router-dom';
import type { ApiClient } from '../api/types';
import { ApiProvider } from './context';
import { Icon, type IconName } from '../components/primitives';

const navigation: Array<{ to: string; label: string; hint: string; icon: IconName }> = [
  { to: '/', label: '运行总览', hint: '全局态势', icon: 'overview' },
  { to: '/jobs', label: '任务中心', hint: '运行与控制', icon: 'jobs' },
  { to: '/traces', label: '链路追踪', hint: '事件因果链', icon: 'trace' },
  { to: '/experiments', label: '实验对比', hint: '基线与变体', icon: 'experiment' },
];

export function AppLayout() {
  return <AppLayoutWithClient />;
}

type AppLayoutWithClientProps = {
  client?: ApiClient;
};

export function AppLayoutWithClient({ client }: AppLayoutWithClientProps) {
  const location = useLocation();
  const mockMode = (import.meta.env.VITE_TGSRL_API_ADAPTER ?? 'http') === 'mock';
  return (
    <ApiProvider client={client}>
      <div className="app-shell">
        <aside className="sidebar">
          <div className="brand-block">
            <div className="brand-mark"><Icon name="pulse" /></div>
            <div>
              <p className="eyebrow">强化学习训练系统</p>
              <h1>TGS-RL</h1>
              <p className="brand-copy">以链路证据连接任务、调度决策与运行资源。</p>
            </div>
          </div>
          <nav className="nav-list" aria-label="主导航">
            {navigation.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) => {
                  const active = item.to === '/traces'
                    ? location.pathname.includes('/traces')
                    : item.to === '/jobs'
                      ? isActive && !location.pathname.includes('/traces')
                      : isActive;
                  return `nav-link${active ? ' active' : ''}`;
                }}
              >
                <Icon name={item.icon} />
                <span><strong>{item.label}</strong><small>{item.hint}</small></span>
              </NavLink>
            ))}
          </nav>
          <div className="sidebar-status">
            <span className="status-beacon" />
            <div>
              <strong>{mockMode ? '演示数据已加载' : '控制面已连接'}</strong>
              <small>{mockMode ? '静态模拟模式' : '实时接口模式'}</small>
            </div>
          </div>
        </aside>
        <main className="content-area">
          <Outlet />
        </main>
      </div>
    </ApiProvider>
  );
}
