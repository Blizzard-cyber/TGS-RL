import type { ButtonHTMLAttributes, PropsWithChildren, ReactNode } from 'react';
import type { LoadStateKind, MetricCard as MetricCardType } from '../api/types';
import { dataKindLabel } from '../app/utils';

export type IconName = 'overview' | 'jobs' | 'trace' | 'experiment' | 'timeline' | 'topology' | 'sandbox' | 'decision' | 'pulse' | 'resource';

export function Icon({ name }: { name: IconName }) {
  const paths: Record<IconName, ReactNode> = {
    overview: <><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></>,
    jobs: <><path d="M4 7.5h16v11H4z"/><path d="M8 7.5V5h8v2.5M4 11h16"/></>,
    trace: <><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="12" cy="18" r="2"/><path d="M7 6h10M6.5 7.5l4.3 8.2M17.5 7.5l-4.3 8.2"/></>,
    experiment: <><path d="M9 3h6M10 3v6l-5 9a2 2 0 0 0 1.8 3h10.4A2 2 0 0 0 19 18l-5-9V3"/><path d="M7.5 15h9"/></>,
    timeline: <><path d="M5 4v16"/><circle cx="5" cy="7" r="2"/><circle cx="5" cy="17" r="2"/><path d="M9 7h10M9 17h7"/></>,
    topology: <><circle cx="12" cy="5" r="2"/><circle cx="5" cy="18" r="2"/><circle cx="19" cy="18" r="2"/><path d="M11 7 6 16M13 7l5 9M7 18h10"/></>,
    sandbox: <><path d="m12 3 8 4.5v9L12 21l-8-4.5v-9z"/><path d="m4 7.5 8 4.5 8-4.5M12 12v9"/></>,
    decision: <><path d="M4 5h7M4 12h12M4 19h7"/><circle cx="17" cy="5" r="2"/><circle cx="20" cy="12" r="2"/><circle cx="14" cy="19" r="2"/></>,
    pulse: <path d="M3 12h4l2-6 4 12 2-6h6"/>,
    resource: <><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M7 9h4v6H7zM15 9h2M15 12h2M15 15h2"/></>,
  };
  return <svg className="icon" viewBox="0 0 24 24" aria-hidden="true">{paths[name]}</svg>;
}

export function ShellFrame({ title, subtitle, actions, children }: PropsWithChildren<{ title: string; subtitle: string; actions?: ReactNode }>) {
  return (
    <div className="shell-frame">
      <header className="page-header">
        <div>
          <p className="eyebrow">调度控制台</p>
          <h1>{title}</h1>
          <p className="page-subtitle">{subtitle}</p>
        </div>
      </header>
      {actions ? <div className="page-toolbar"><span className="toolbar-caption">视图范围</span><div className="page-actions">{actions}</div></div> : null}
      {children}
    </div>
  );
}

export function Panel({ title, subtitle, children, actions, className }: PropsWithChildren<{ title: string; subtitle?: string; actions?: ReactNode; className?: string }>) {
  return (
    <section className={`panel${className ? ` ${className}` : ''}`}>
      <header className="panel-header">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        {actions ? <div>{actions}</div> : null}
      </header>
      {children}
    </section>
  );
}

export function MetricCard({ label, value, delta, tone = 'neutral' }: MetricCardType) {
  return (
    <article className={`metric-card tone-${tone}`}>
      <span className="metric-label">{label}</span>
      <strong className="metric-value">{value}</strong>
      {delta ? <span className="metric-delta">{delta}</span> : null}
    </article>
  );
}

export function Pill({ children, tone = 'neutral' }: PropsWithChildren<{ tone?: 'neutral' | 'good' | 'warn' | 'critical' }>) {
  return <span className={`pill tone-${tone}`}>{children}</span>;
}

export function SourceBadge({ kind }: { kind: 'synthetic' | 'replay' | 'live' }) {
  const tone = kind === 'live' ? 'good' : kind === 'replay' ? 'warn' : 'neutral';
  return (
    <span className={`source-badge tone-${tone}`}>
      <span className="source-dot" />
      {dataKindLabel(kind)}
    </span>
  );
}

export function AsyncState({
  state,
  title,
  message,
  onRetry,
}: {
  state: Exclude<LoadStateKind, 'ready'>;
  title?: string;
  message?: string;
  onRetry?: () => void;
}) {
  const defaults: Record<Exclude<LoadStateKind, 'ready'>, { title: string; message: string }> = {
    loading: {
      title: '正在加载控制面数据',
      message: '正在汇总调度器、运行时与实验状态。',
    },
    empty: {
      title: '暂无记录',
      message: '当前筛选条件下没有可展示的数据。',
    },
    error: {
      title: '请求失败',
      message: '控制台无法从控制面加载该页面，请稍后重试。',
    },
    forbidden: {
      title: '没有访问权限',
      message: '当前会话无权查看这部分数据。',
    },
    'gpu-unavailable': {
      title: '加速卡资源不可用',
      message: '当前查询需要加速卡资源，但调度器没有发现就绪设备。',
    },
    degraded: {
      title: '数据部分降级',
      message: '控制面仅返回了部分数据，请谨慎执行后续操作。',
    },
  };
  const content = defaults[state];
  return (
    <div className={`async-state state-${state}`} role={state === 'error' ? 'alert' : 'status'}>
      <div className="async-grid" />
      <div className="async-copy">
        <p className="eyebrow">页面状态</p>
        <h3>{title ?? content.title}</h3>
        <p>{message ?? content.message}</p>
        {onRetry ? (
          <button className="button" type="button" onClick={onRetry}>
            重新加载
          </button>
        ) : null}
      </div>
    </div>
  );
}

export function DataTable({
  columns,
  rows,
  emptyLabel,
}: {
  columns: string[];
  rows: ReactNode[][];
  emptyLabel?: string;
}) {
  if (rows.length === 0) {
    return <p className="table-empty">{emptyLabel ?? '暂无可展示的数据。'}</p>;
  }
  return (
    <div className="table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} scope="col">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {row.map((cell, cellIndex) => (
                <td key={cellIndex}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function SelectCardButton({
  selected = false,
  className,
  type = 'button',
  ...props
}: Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'aria-pressed'> & { selected?: boolean }) {
  return (
    <button
      {...props}
      aria-pressed={selected}
      className={`select-card${selected ? ' selected' : ''}${className ? ` ${className}` : ''}`}
      type={type}
    />
  );
}
