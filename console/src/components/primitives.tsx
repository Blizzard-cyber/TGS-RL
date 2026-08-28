import type { ButtonHTMLAttributes, PropsWithChildren, ReactNode } from 'react';
import type { LoadStateKind, MetricCard as MetricCardType } from '../api/types';

export function ShellFrame({ title, subtitle, actions, children }: PropsWithChildren<{ title: string; subtitle: string; actions?: ReactNode }>) {
  return (
    <div className="shell-frame">
      <header className="page-header">
        <div>
          <p className="eyebrow">TGS-RL Console</p>
          <h1>{title}</h1>
          <p className="page-subtitle">{subtitle}</p>
        </div>
        {actions ? <div className="page-actions">{actions}</div> : null}
      </header>
      {children}
    </div>
  );
}

export function Panel({ title, subtitle, children, actions }: PropsWithChildren<{ title: string; subtitle?: string; actions?: ReactNode }>) {
  return (
    <section className="panel">
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
  const tone = kind === 'live' ? 'critical' : kind === 'replay' ? 'warn' : 'neutral';
  return (
    <span className={`source-badge tone-${tone}`}>
      <span className="source-dot" />
      {kind}
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
      title: 'Loading control-plane surface',
      message: 'Collecting current scheduler, runtime, and experiment signals.',
    },
    empty: {
      title: 'No retained records',
      message: 'The selected query returned no retained data.',
    },
    error: {
      title: 'Request failed',
      message: 'The console could not load this surface from the control plane.',
    },
    forbidden: {
      title: 'Access forbidden',
      message: 'Your current session is not authorized for this surface.',
    },
    'gpu-unavailable': {
      title: 'GPU capacity unavailable',
      message: 'The query needs GPU-backed resources, but the scheduler reports none are READY.',
    },
    degraded: {
      title: 'Degraded data quality',
      message: 'The control plane returned partial data; use caution when acting on it.',
    },
  };
  const content = defaults[state];
  return (
    <div className={`async-state state-${state}`} role={state === 'error' ? 'alert' : 'status'}>
      <div className="async-grid" />
      <div className="async-copy">
        <p className="eyebrow">Surface State</p>
        <h3>{title ?? content.title}</h3>
        <p>{message ?? content.message}</p>
        {onRetry ? (
          <button className="button" type="button" onClick={onRetry}>
            Retry
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
    return <p className="table-empty">{emptyLabel ?? 'No rows to display.'}</p>;
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
