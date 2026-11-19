/** 聚合落库 → 结果落地数据流接口：检查点覆盖、完整性报告、重试与完成闸口。 */
import { request } from './client'

export type IntegrityTaskReport = {
  task: string
  status: string
  checkpoint_id: string | null
  replay_version: number
  success_partitions: string[]
  failed_partitions: string[]
  failed_records: { record_id: number; partition: string; reason: string }[]
  pending_repair: { record_id: number; partition: string; reason: string }[]
  missing_partitions: string[]
  planned: number
  success_count: number
  complete: boolean
  blocked: boolean
  reason?: string
}

export type IntegrityReport = {
  ok: boolean
  generated_at: number
  task_count: number
  blocked_tasks: string[]
  reports: IntegrityTaskReport[]
}

export type CheckpointCoverage = {
  ok: boolean
  checkpoint_id: string
  task: string
  replay_version: number
  declared_success_partitions: string[]
  verified_success_partitions: string[]
  missing_partitions: string[]
  failed_partitions: string[]
  blocking_partitions: string[]
  failed_records: { record_id: number; partition: string; reason: string }[]
  failed_count: number
  blocked: boolean
  message: string
}

export type ReplayableCheckpoint = {
  ok: boolean
  task: string
  checkpoint_id: string | null
  replay_version: number
  success_partitions: string[]
  missing_partitions: string[]
  missing_count: number
  complete: boolean
  blocked: boolean
  repair_log: { record_id: number; partition: string; repaired_at: number; strategy: string }[]
  status: string
  error?: string
}

export type RetryResult = {
  ok: boolean
  task: string
  checkpoint_id: string
  replay_version: number
  repaired: number
  still_failed: number
  success_partitions: string[]
  failed_partitions: string[]
  complete: boolean
}

export type CompletionResult = {
  ok: boolean
  task: string
  blocked: boolean
  message: string
  checkpoint_id?: string | null
  replay_version?: number
  missing_partitions?: string[]
  failed_partitions?: string[]
}

export type AccessOverview = {
  ok: boolean
  team: string
  teams: string[]
  scopes: Record<string, { label: string; description: string }>
  records: {
    module: string
    id: number
    name: string
    owner: string
    scope: string
    scope_label: string
    visible: boolean
    read_only: boolean
  }[]
}

async function readJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = (await response.json().catch(() => ({}))) as T & {
    error?: string
    detail?: string
    code?: string
  }
  if (!response.ok) {
    const reason = payload?.error ?? payload?.detail ?? `接口返回 ${response.status}`
    throw new Error(reason)
  }
  return payload as T
}

export function loadAggregateIntegrity(): Promise<IntegrityReport> {
  return readJson<IntegrityReport>('/api/flows/aggregate-sink')
}

export function loadTaskIntegrity(task: string): Promise<IntegrityReport> {
  return readJson<IntegrityReport>(`/api/flows/aggregate-sink/${encodeURIComponent(task)}/integrity`)
}

export function loadReplayableCheckpoint(task: string): Promise<ReplayableCheckpoint> {
  return readJson<ReplayableCheckpoint>(
    `/api/flows/aggregate-sink/${encodeURIComponent(task)}/checkpoints`)
}

export function loadCheckpointCoverage(checkpointId: string): Promise<CheckpointCoverage> {
  return readJson<CheckpointCoverage>(
    `/api/flows/sink-output/checkpoints/${encodeURIComponent(checkpointId)}`)
}

export function retryTask(task: string, strategy: 'append' | 'rebuild'): Promise<RetryResult> {
  return readJson<RetryResult>(
    `/api/flows/aggregate-sink/${encodeURIComponent(task)}/retry`,
    { method: 'POST', body: JSON.stringify({ strategy }) },
  )
}

export function completeTask(task: string): Promise<CompletionResult> {
  return readJson<CompletionResult>(
    `/api/flows/sink-output/${encodeURIComponent(task)}/complete`,
    { method: 'POST' },
  )
}

export function loadAccessOverview(): Promise<AccessOverview> {
  return readJson<AccessOverview>('/api/flows/access/overview')
}
