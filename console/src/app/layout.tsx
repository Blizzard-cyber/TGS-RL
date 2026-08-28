import { NavLink, Outlet } from 'react-router-dom';
import type { ApiClient } from '../api/types';
import { ApiProvider } from './context';

const navigation = [
  { to: '/', label: 'Overview' },
  { to: '/jobs', label: 'Job Detail' },
  { to: '/experiments', label: 'Experiment Compare' },
];

export function AppLayout() {
  return <AppLayoutWithClient />;
}

type AppLayoutWithClientProps = {
  client?: ApiClient;
};

export function AppLayoutWithClient({ client }: AppLayoutWithClientProps) {
  return (
    <ApiProvider client={client}>
      <div className="app-shell">
        <aside className="sidebar">
          <div className="brand-block">
            <p className="eyebrow">Operations Console</p>
            <h1>TGS-RL</h1>
            <p className="brand-copy">
              Trace-first control-plane visibility for jobs, decisions, runtime sandboxes, and experiments.
            </p>
          </div>
          <nav className="nav-list" aria-label="Primary">
            {navigation.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        </aside>
        <main className="content-area">
          <Outlet />
        </main>
      </div>
    </ApiProvider>
  );
}
