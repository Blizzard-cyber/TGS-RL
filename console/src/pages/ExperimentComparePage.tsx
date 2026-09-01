import { useState } from 'react';
import { useApiClient } from '../app/apiContext';
import { useQuery } from '../app/hooks';
import { dataKindOptions, formatTimestamp, simulationOptions, toQueryFilters, titleCase } from '../app/utils';
import { SurfaceStateBoundary, SurfaceStateControl } from '../app/surface';
import { AsyncState, DataTable, Panel, Pill, SelectCardButton, ShellFrame, SourceBadge } from '../components/primitives';

export function ExperimentComparePage() {
  const client = useApiClient();
  const [dataKind, setDataKind] = useState('all');
  const [mode, setMode] = useState('ready');
  const [selectedExperimentId, setSelectedExperimentId] = useState('');
  const { result, retry } = useQuery((signal) => client.listExperiments({ filters: toQueryFilters({ data_kind: dataKind !== 'all' ? dataKind : undefined, mode }), signal }), [client, dataKind, mode]);
  const selectedExperiment = result.data?.find((entry) => entry.id === selectedExperimentId) ?? result.data?.[0];
  return <ShellFrame title="实验对比" subtitle="对比基线与变体运行，同时保持真实、回放和合成数据边界。" actions={<div className="control-row"><label><span>数据来源</span><select value={dataKind} onChange={(event) => setDataKind(event.target.value)}>{dataKindOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label><SurfaceStateControl mode={mode} onChange={setMode} options={simulationOptions} /></div>}>
    <div className="two-column"><Panel title="实验列表" subtitle="选择实验后查看其中的运行与指标。"><SurfaceStateBoundary result={result} retry={retry}>{(data) => <div className="stack-list">{(data ?? []).map((experiment) => <SelectCardButton key={experiment.id} selected={selectedExperiment?.id === experiment.id} onClick={() => setSelectedExperimentId(experiment.id)}><div className="list-card-header"><div><h3>{experiment.name}</h3><p>{experiment.id} · {formatTimestamp(experiment.createdAt)}</p></div><Pill tone={experiment.state === 'completed' ? 'good' : experiment.state === 'failed' ? 'critical' : 'warn'}>{titleCase(experiment.state)}</Pill></div><p className="body-copy">{experiment.summary}</p></SelectCardButton>)}</div>}</SurfaceStateBoundary></Panel>
    <Panel title="运行对比" subtitle="指标与来源、策略和配置一一对应，不混合聚合。">{selectedExperiment ? <><div className="list-card"><div className="list-card-header"><div><h3>{selectedExperiment.name}</h3><code>{selectedExperiment.id}</code></div><Pill tone={selectedExperiment.state === 'completed' ? 'good' : selectedExperiment.state === 'failed' ? 'critical' : 'warn'}>{titleCase(selectedExperiment.state)}</Pill></div><p className="body-copy">{selectedExperiment.summary}</p></div><DataTable columns={['运行', '来源', '策略', '配置', '指标']} rows={selectedExperiment.runs.map((run) => [<div key={`${run.id}-meta`}><strong>{run.label}</strong><div className="subtle">{run.runId}</div></div>, <SourceBadge key={`${run.id}-source`} kind={run.dataKind} />, run.policyVersion, run.configHash, <div key={`${run.id}-metrics`} className="metric-inline-list">{run.metrics.map((metric) => <span key={metric.label}>{metric.label}：{metric.value}</span>)}</div>])} emptyLabel="该实验没有保留运行记录。" /></> : <AsyncState state="empty" message="请选择一个实验查看运行对比。" />}</Panel></div>
  </ShellFrame>;
}
