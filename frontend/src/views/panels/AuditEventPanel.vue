<template>
  <section class="panel" data-testid="audit-event-panel">
    <h3>事件审计详情</h3>
    <p class="panel-desc">
      按原事件号关联本次收到的真实回执；重开详情时自动恢复落库回执的审计关联，供值班员核对是否还要重放。
    </p>

    <form class="filter-bar" data-testid="audit-event-form" @submit.prevent="loadDetail">
      <label class="filter-item">
        <span>事件ID</span>
        <input v-model="eventId" data-testid="audit-event-id" placeholder="如 evt-81" required />
      </label>
      <button class="btn primary" data-testid="audit-event-query" type="submit" :disabled="busy">查询</button>
      <button
        v-if="detail?.found"
        class="btn"
        type="button"
        data-testid="audit-event-refresh"
        :disabled="busy"
        @click="loadDetail"
      >
        刷新
      </button>
    </form>

    <template v-if="detail">
      <p class="panel-message" data-testid="audit-event-message">{{ detail.message }}</p>

      <div v-if="detail.delivery" class="stat-row">
        <article class="stat-card">
          <span class="stat-label">逻辑交付计数</span>
          <strong class="stat-value" data-testid="audit-delivery-count">{{ detail.delivery.delivery_count }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">有效接收凭证</span>
          <strong class="stat-value" data-testid="audit-effective-receipt">
            {{ detail.delivery.effective_receipt_id ?? '—' }}
          </strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">是否还要重放</span>
          <strong class="stat-value" data-testid="audit-replay-needed">{{ detail.replay_needed ? '是' : '否' }}</strong>
        </article>
      </div>

      <h4>真实回执（{{ detail.receipts.length }}）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>回执编号</th>
            <th>真实发送身份</th>
            <th>发送位置</th>
            <th>角色</th>
            <th>结果</th>
            <th>接收时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="receipt in detail.receipts" :key="receipt.receipt_id" :data-testid="`audit-receipt-${receipt.receipt_id}`">
            <td>{{ receipt.receipt_id }}</td>
            <td>{{ receipt.sender }}</td>
            <td>{{ receipt.target }}</td>
            <td>{{ receipt.role }}</td>
            <td>{{ receipt.result }}</td>
            <td>{{ receipt.received_at }}</td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`resend-${receipt.receipt_id}`"
                :disabled="busy"
                @click="resendReceipt(receipt)"
              >
                重送回执
              </button>
            </td>
          </tr>
          <tr v-if="!detail.receipts.length">
            <td colspan="7" class="empty-state">该事件暂未收到回执</td>
          </tr>
        </tbody>
      </table>

      <h4>关联审计轨迹（{{ detail.audit_entries.length }}）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>审计时间</th>
            <th>操作类型</th>
            <th>变更摘要</th>
            <th>结果状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in detail.audit_entries" :key="String(entry.id)">
            <td>{{ entry['审计时间'] }}</td>
            <td>{{ entry['操作类型'] }}</td>
            <td>{{ entry['变更摘要'] }}</td>
            <td>{{ entry['结果状态'] }}</td>
          </tr>
          <tr v-if="!detail.audit_entries.length">
            <td colspan="4" class="empty-state">该事件暂无关联审计记录</td>
          </tr>
        </tbody>
      </table>
    </template>

    <p v-if="errorMessage" class="error-text" data-testid="audit-event-error">{{ errorMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  auditEventDetail,
  submitReceipt,
  type AuditEventDetail,
  type DeliveryReceipt,
} from '@/api/delivery-service'

const eventId = ref('')
const detail = ref<AuditEventDetail | null>(null)
const errorMessage = ref('')
const busy = ref(false)

async function loadDetail() {
  if (!eventId.value.trim()) return
  busy.value = true
  errorMessage.value = ''
  try {
    detail.value = await auditEventDetail(eventId.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '审计详情读取失败'
  } finally {
    busy.value = false
  }
}

async function resendReceipt(receipt: DeliveryReceipt) {
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await submitReceipt({
      event_id: eventId.value.trim(),
      target: receipt.target,
      receipt_id: receipt.receipt_id,
      sender: receipt.sender,
      result: receipt.result,
    })
    if (!result.ok) throw new Error(result.message)
    await loadDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重送回执失败'
  } finally {
    busy.value = false
  }
}
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
h4 {
  margin: 12px 0 6px;
}
</style>
