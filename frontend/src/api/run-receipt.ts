/**
 * 运行历史 × Schema检验回执接口封装。
 * 与后端 app/routers/run_receipt.py 的路径与字段保持一致。
 */
import { request } from './client'

export type ReceiptConclusion = 'pass' | 'fail' | 'incomplete'

export type ReceiptViolation = {
  field: string
  rule_id: number
  规则编号: string
  规则版本: string
  类型约束: string
  实际值: unknown
  实际类型: string
  原因: string
  message: string
}

export type AppliedRule = {
  rule_id: number
  规则编号: string
  目标数据: string
  字段名: string
  类型约束: string
  格式约束: string | null
  空值策略: string | null
  异常归类: string | null
  规则版本: string
}

export type SchemaReceipt = {
  receipt_id: string
  manifest_id?: string
  run_id: number
  round: number
  target_module: string
  trigger: string
  conclusion: ReceiptConclusion
  input_snapshot: Record<string, unknown>
  rules_applied: AppliedRule[]
  rule_versions: string[]
  violations: ReceiptViolation[]
  status: string
  created_at: string
}

export type ReceiptSummary = {
  receipt_id: string
  round: number
  conclusion: ReceiptConclusion
  trigger: string
  status: string
  violation_count: number
  rule_versions: string[]
  created_at: string
}

export type RunDetail = {
  id: number
  status: string
  check_state: '采集中' | '待续检' | '检验不通过' | '数据可用'
  data_available: boolean
  resume_available: boolean
  latest_receipt_id: string | null
  receipts: ReceiptSummary[]
  [field: string]: unknown
}

export type RecheckResult = {
  receipt_id: string
  run_id: number
  round: number
  target_module: string
  conclusion: ReceiptConclusion
  matches_original: boolean
  original_conclusion: ReceiptConclusion
  input_snapshot: Record<string, unknown>
  rules_applied: AppliedRule[]
  rule_versions: string[]
  violations: ReceiptViolation[]
  rechecked_at: string
}

async function asJson<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`接口返回 ${response.status}，检验数据未更新`)
  return (await response.json()) as T
}

/** 确认采集完成并触发首轮Schema检验；明细写入失败时后端返回 ok=false 而非500。 */
export async function confirmRun(runId: number): Promise<{
  ok: boolean
  message: string
  entry?: Record<string, unknown>
}> {
  const response = await request(`/api/run-history/${runId}/confirm`, {
    method: 'POST',
    body: JSON.stringify({ action: '确认完成' }),
  })
  return asJson(response)
}

export async function fetchRunDetail(runId: number): Promise<RunDetail> {
  return asJson(await request(`/api/run-history/${runId}/detail`))
}

export async function listReceipts(runId: number): Promise<{ run_id: number; items: SchemaReceipt[] }> {
  return asJson(await request(`/api/run-history/${runId}/receipts`))
}

export async function fetchReceipt(runId: number, round: number): Promise<SchemaReceipt> {
  return asJson(await request(`/api/run-history/${runId}/receipts/${round}`))
}

/** 续检：采集已结束但检验未完成时补一轮，结果作为新一轮回执单独记录。 */
export async function resumeCheck(runId: number): Promise<{
  ok: boolean
  message: string
  receipt: SchemaReceipt | null
}> {
  return asJson(
    await request(`/api/run-history/${runId}/resume-check`, { method: 'POST' }),
  )
}

/** 主动发起独立检验：按当前最新规则另开一轮，历史回执保持不变。 */
export async function startIndependentCheck(runId: number): Promise<{
  ok: boolean
  message: string
  receipt: SchemaReceipt | null
}> {
  return asJson(
    await request(`/api/run-history/${runId}/independent-check`, { method: 'POST' }),
  )
}

/** 按原回执固化的被检输入与规则版本复核历史轮次。 */
export async function recheckReceipt(runId: number, round: number): Promise<{
  ok: boolean
  message: string
  recheck: RecheckResult
}> {
  return asJson(
    await request(`/api/run-history/${runId}/receipts/${round}/recheck`, { method: 'POST' }),
  )
}
