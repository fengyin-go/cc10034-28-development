<template>
  <section class="panel" data-testid="delivery-panel">
    <h3>投递尝试与回执</h3>
    <p class="panel-desc">
      每次发送按实际发送位置落库，重开进程后仍能查到每次尝试；回执按真实发送身份保存，同一逻辑事件只完成一次。
    </p>

    <form class="filter-bar" data-testid="dispatch-form" @submit.prevent="doDispatch">
      <label class="filter-item">
        <span>事件ID</span>
        <input v-model="dispatchForm.event_id" data-testid="dispatch-event-id" placeholder="如 evt-81" required />
      </label>
      <label class="filter-item">
        <span>主目标通道</span>
        <input v-model="dispatchForm.primary_target" data-testid="dispatch-primary" placeholder="如 orders-east" required />
      </label>
      <label class="filter-item">
        <span>备用目标</span>
        <input v-model="dispatchForm.backup_target" data-testid="dispatch-backup" placeholder="如 orders-backup" />
      </label>
      <label class="filter-item">
        <span>主端结果</span>
        <select v-model="dispatchForm.primary_outcome" data-testid="dispatch-outcome">
          <option value="已发送">已发送</option>
          <option value="超时">超时</option>
          <option value="失败">失败</option>
        </select>
      </label>
      <button class="btn primary" data-testid="dispatch-submit" type="submit" :disabled="busy">发起分发</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>事件ID</th>
          <th>状态</th>
          <th>逻辑交付计数</th>
          <th>投递尝试</th>
          <th>已收回执</th>
          <th>有效接收凭证</th>
          <th>还可领取</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in deliveries" :key="item.event_id" :data-testid="`delivery-${item.event_id}`">
          <td>{{ item.event_id }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.delivery_count }}</td>
          <td>{{ item.attempt_count }}</td>
          <td>{{ item.receipt_count }}</td>
          <td>{{ item.effective_receipt_id ?? '—' }}</td>
          <td>{{ item.claimable ? '是' : '否' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`detail-${item.event_id}`" @click="openDetail(item.event_id)">
              回执详情
            </button>
            <button class="link" type="button" :data-testid="`replay-${item.event_id}`" @click="doReplay(item.event_id)">
              重放
            </button>
          </td>
        </tr>
        <tr v-if="!deliveries.length">
          <td colspan="8" class="empty-state">暂无投递记录，可先在上方发起分发</td>
        </tr>
      </tbody>
    </table>

    <div v-if="detail" class="modal-backdrop" data-testid="delivery-detail-modal">
      <div class="modal">
        <h3>回执详情：{{ detail.event_id }}</h3>
        <p>
          状态：{{ detail.status }} ｜ 逻辑交付计数：{{ detail.delivery_count }} ｜
          有效接收凭证：{{ detail.effective_receipt_id ?? '—' }} ｜
          还可领取：{{ detail.claimable ? '是' : '否' }}
        </p>
        <table class="data-table">
          <thead>
            <tr>
              <th>尝试编号</th>
              <th>发送位置</th>
              <th>角色</th>
              <th>发送结果</th>
              <th>发送时间</th>
              <th>回执</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="attempt in detail.attempts" :key="attempt.attempt_id">
              <td>{{ attempt.attempt_id }}</td>
              <td>{{ attempt.target }}</td>
              <td>{{ attempt.role }}</td>
              <td>{{ attempt.outcome }}</td>
              <td>{{ attempt.sent_at }}</td>
              <td>
                <div v-for="receipt in attempt.receipts" :key="receipt.receipt_id">
                  {{ receipt.receipt_id }}（{{ receipt.sender }} · {{ receipt.result }}）
                </div>
                <span v-if="!attempt.receipts.length">—</span>
              </td>
            </tr>
          </tbody>
        </table>

        <form class="filter-bar" data-testid="receipt-form" @submit.prevent="doSubmitReceipt">
          <label class="filter-item">
            <span>回执位置</span>
            <select v-model="receiptForm.target" data-testid="receipt-target">
              <option v-for="target in attemptTargets" :key="target" :value="target">{{ target }}</option>
            </select>
          </label>
          <label class="filter-item">
            <span>回执编号</span>
            <input v-model="receiptForm.receipt_id" data-testid="receipt-id" placeholder="如 rcpt-bk-81" required />
          </label>
          <label class="filter-item">
            <span>发送身份</span>
            <input v-model="receiptForm.sender" data-testid="receipt-sender" placeholder="真实发送身份" required />
          </label>
          <label class="filter-item">
            <span>回执结果</span>
            <select v-model="receiptForm.result" data-testid="receipt-result">
              <option value="成功">成功</option>
              <option value="失败">失败</option>
            </select>
          </label>
          <button class="btn primary" data-testid="receipt-submit" type="submit" :disabled="busy">登记回执</button>
        </form>

        <div class="page-actions">
          <button class="btn" type="button" data-testid="detail-refresh" @click="refreshDetail">刷新</button>
          <button class="btn" type="button" data-testid="detail-replay" @click="doReplay(detail.event_id)">重放</button>
          <button class="btn ghost" type="button" data-testid="detail-close" @click="detail = null">关闭</button>
        </div>
        <p v-if="detailMessage" class="panel-message" data-testid="detail-message">{{ detailMessage }}</p>
      </div>
    </div>

    <p v-if="message" class="panel-message" data-testid="delivery-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="delivery-error">{{ errorMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  deliveryDetail,
  dispatchEvent,
  listDeliveries,
  replayEvent,
  submitReceipt,
  type DeliveryDetail,
  type DeliverySummary,
} from '@/api/delivery-service'

const deliveries = ref<DeliverySummary[]>([])
const detail = ref<DeliveryDetail | null>(null)
const message = ref('')
const detailMessage = ref('')
const errorMessage = ref('')
const busy = ref(false)

const dispatchForm = ref({
  event_id: '',
  primary_target: '',
  backup_target: '',
  primary_outcome: '超时',
})
const receiptForm = ref({ target: '', receipt_id: '', sender: '', result: '成功' })

const attemptTargets = computed(() => {
  if (!detail.value) return []
  return [...new Set(detail.value.attempts.map((attempt) => attempt.target))]
})

async function reload() {
  try {
    const payload = await listDeliveries()
    deliveries.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '投递汇总读取失败'
  }
}

async function doDispatch() {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await dispatchEvent({ ...dispatchForm.value })
    if (!result.ok) throw new Error(result.message)
    message.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发失败'
  } finally {
    busy.value = false
  }
}

async function openDetail(eventId: string) {
  detailMessage.value = ''
  errorMessage.value = ''
  try {
    detail.value = await deliveryDetail(eventId)
    receiptForm.value = {
      target: attemptTargets.value[0] ?? '',
      receipt_id: '',
      sender: '',
      result: '成功',
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执详情读取失败'
  }
}

async function refreshDetail() {
  if (!detail.value) return
  detailMessage.value = ''
  try {
    detail.value = await deliveryDetail(detail.value.event_id)
    detailMessage.value = '已刷新，每次投递尝试与回执均以落库数据为准'
  } catch (error) {
    detailMessage.value = error instanceof Error ? error.message : '刷新失败'
  }
}

async function doSubmitReceipt() {
  if (!detail.value) return
  busy.value = true
  detailMessage.value = ''
  try {
    const result = await submitReceipt({
      event_id: detail.value.event_id,
      ...receiptForm.value,
    })
    if (!result.ok) throw new Error(result.message)
    detailMessage.value = result.message
    await refreshDetail()
    await reload()
  } catch (error) {
    detailMessage.value = error instanceof Error ? error.message : '回执登记失败'
  } finally {
    busy.value = false
  }
}

async function doReplay(eventId: string) {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await replayEvent(eventId)
    if (!result.ok) throw new Error(result.message)
    const receipt = result.result?.effective_receipt
    message.value = receipt
      ? `${result.message}；本次选用的有效接收凭证：${receipt.receipt_id}（${receipt.sender}）`
      : result.message
    if (detail.value && detail.value.event_id === eventId) await refreshDetail()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放失败'
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.panel {
  border: 1px solid var(--border);
  border-radius: 8px;
  background: #fff;
  padding: 12px;
  margin-bottom: 12px;
}
.panel-desc {
  color: var(--muted);
  font-size: 12px;
}
.panel-message {
  color: var(--brand);
  font-size: 13px;
}
.row-actions {
  display: flex;
  gap: 8px;
}
select {
  padding: 4px 6px;
}
</style>
