import { NavLink, Outlet, useLocation } from 'react-router-dom';
import type { ApiClient } from '../api/types';
import { ApiProvider } from './context';
import { Icon, type IconName } from '../components/primitives';

const navigation: Array<{ group: string; items: Array<{ to: string; label: string; icon: IconName }> }> = [
  {
    group: '运行',
    items: [
      { to: '/', label: '运行总览', icon: 'overview' },
      { to: '/jobs', label: '任务中心', icon: 'jobs' },
    ],
  },
  {
    group: '可观测',
    items: [
      { to: '/traces', label: '链路追踪', icon: 'trace' },
      { to: '/timeline', label: '事件时间线', icon: 'timeline' },
    ],
  },
  {
    group: '资源',
    items: [
      { to: '/topology', label: '资源拓扑', icon: 'topology' },
      { to: '/sandboxes', label: '运行沙箱', icon: 'sandbox' },
    ],
  },
  {
    group: '分析',
    items: [
      { to: '/decisions', label: '调度决策', icon: 'decision' },
      { to: '/experiments', label: '实验对比', icon: 'experiment' },
    ],
  },
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
  const isActiveRoute = (to: string, reactRouterActive: boolean) => {
    if (to === '/') return reactRouterActive;
    if (to === '/jobs') return location.pathname === '/jobs' || /^\/jobs\/[^/]+$/.test(location.pathname);
    return reactRouterActive || location.pathname.endsWith(to);
  };
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
            {navigation.map((section) => (
              <section className="nav-section" key={section.group}>
                <p>{section.group}</p>
                {section.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.to === '/'}
                    className={({ isActive }) => {
                      const active = isActiveRoute(item.to, isActive);
                      return `nav-link${active ? ' active' : ''}`;
                    }}
                  >
                    <Icon name={item.icon} />
                    <strong>{item.label}</strong>
                  </NavLink>
                ))}
              </section>
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
        <div className="workspace">
          <header className="workspace-bar">
            <div><span className="workspace-product">TGS-RL</span><span className="workspace-separator">/</span><strong>本机调度集群</strong></div>
            <div className="workspace-state"><span className="status-beacon" /><strong>{mockMode ? '演示数据' : '控制面在线'}</strong><span>协议 v0.3</span></div>
          </header>
          <main className="content-area">
            <Outlet />
          </main>
        </div>
      </div>
    </ApiProvider>
  );
}
