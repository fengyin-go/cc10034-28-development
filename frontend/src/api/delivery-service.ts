/** 投递尝试与回执接口封装：路由分发的单事件发送记录、回执登记、审计详情与重放。 */
import { request } from './client'

export type DeliveryReceipt = {
  receipt_id: string
  sender: string
  target: string
  role: string
  result: string
  received_at: string
  attempt_id: string
}

export type DeliveryAttempt = {
  attempt_id: string
  event_id: string
  target: string
  role: string
  outcome: string
  sent_at: string
  receipts: DeliveryReceipt[]
}

export type DeliverySummary = {
  event_id: string
  status: string
  delivery_count: number
  effective_receipt_id: string | null
  attempt_count: number
  receipt_count: number
  claimable: boolean
  updated_at: string
}

export type DeliveryDetail = DeliverySummary & {
  attempts: DeliveryAttempt[]
  payload: Record<string, unknown>
  created_at: string
  effective_receipt: DeliveryReceipt | null
}

export type ApiResult<T> = {
  ok: boolean
  message: string
  result: T | null
}

export type DispatchResult = {
  event_id: string
  attempts: DeliveryAttempt[]
  failovered: boolean
  status: string
}

export type ReceiptResult = {
  event_id: string
  duplicate: boolean
  receipt?: DeliveryReceipt
  delivery_count: number
  status: string
  effective_receipt_id: string | null
}

export type ReplayResult = {
  replayed: boolean
  claimable: boolean
  event_id: string
  delivery_count?: number
  effective_receipt?: DeliveryReceipt | null
  attempt?: DeliveryAttempt
}

export type AuditEventDetail = {
  event_id: string
  found: boolean
  delivery: {
    status: string
    delivery_count: number
    effective_receipt_id: string | null
    claimable: boolean
    attempt_count: number
  } | null
  receipts: DeliveryReceipt[]
  audit_entries: Record<string, string | number>[]
  replay_needed: boolean
  message: string
}

async function postJson<T>(path: string, body: unknown): Promise<ApiResult<T>> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return (await response.json()) as ApiResult<T>
}

export function dispatchEvent(payload: {
  event_id: string
  primary_target: string
  backup_target: string
  primary_outcome: string
  payload?: Record<string, unknown>
}): Promise<ApiResult<DispatchResult>> {
  return postJson<DispatchResult>('/api/route_dispatcher/dispatch', payload)
}

export function submitReceipt(payload: {
  event_id: string
  target: string
  receipt_id: string
  sender: string
  result: string
}): Promise<ApiResult<ReceiptResult>> {
  return postJson<ReceiptResult>('/api/route_dispatcher/receipts', payload)
}

export async function listDeliveries(): Promise<{ items: DeliverySummary[]; total: number }> {
  const response = await request('/api/route_dispatcher/deliveries')
  if (!response.ok) throw new Error('投递汇总读取失败')
  return (await response.json()) as { items: DeliverySummary[]; total: number }
}

export async function deliveryDetail(eventId: string): Promise<DeliveryDetail> {
  const response = await request(`/api/route_dispatcher/deliveries/${encodeURIComponent(eventId)}`)
  if (!response.ok) throw new Error('回执详情读取失败')
  return (await response.json()) as DeliveryDetail
}

export function replayEvent(eventId: string): Promise<ApiResult<ReplayResult>> {
  return postJson<ReplayResult>(
    `/api/route_dispatcher/deliveries/${encodeURIComponent(eventId)}/replay`,
    {},
  )
}

export async function auditEventDetail(eventId: string): Promise<AuditEventDetail> {
  const response = await request(`/api/audit_trail/events/${encodeURIComponent(eventId)}`)
  if (!response.ok) throw new Error('审计详情读取失败')
  return (await response.json()) as AuditEventDetail
}
