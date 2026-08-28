export const dataKindOptions = [
  { value: 'all', label: 'All sources' },
  { value: 'live', label: 'Live' },
  { value: 'replay', label: 'Replay' },
  { value: 'synthetic', label: 'Synthetic' },
] as const;

export const simulationOptions = [
  { value: 'ready', label: 'Ready' },
  { value: 'empty', label: 'Empty' },
  { value: 'error', label: 'Error' },
  { value: 'forbidden', label: 'Forbidden' },
  { value: 'gpu-unavailable', label: 'GPU unavailable' },
  { value: 'degraded', label: 'Degraded' },
] as const;

export function toQueryFilters(input: Record<string, string | undefined>) {
  return Object.fromEntries(Object.entries(input).filter((entry): entry is [string, string] => Boolean(entry[1])));
}

export function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'UTC',
  }).format(new Date(value));
}

export function titleCase(value: string) {
  return value.replace(/[_-]/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}
