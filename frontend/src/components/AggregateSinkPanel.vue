<template>
  <section class="flow-panel" data-testid="aggregate-integrity-panel">
    <header class="flow-panel-head">
      <div>
        <h3>聚合落库失败分区</h3>
        <p class="flow-desc">
          检查点只覆盖已成功写入的分区；失败记录留在输出表并进入输出完整性报告，任务状态跟随完整性。
        </p>
      </div>
      <button class="btn" type="button" data-testid="aggregate-refresh" @click="reload">刷新</button>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="aggregate-panel-error">{{ errorMessage }}</p>
    <p v-else-if="!report" class="flow-empty">正在读取聚合落库完整性…</p>
    <p v-else-if="!report.reports.length" class="flow-empty">当前团队暂无可查看的聚合落库任务</p>

    <table v-else class="data-table" data-testid="aggregate-integrity-table">
      <thead>
        <tr>
          <th>任务</th><th>状态</th><th>检查点</th><th>回放版本</th>
          <th>成功分区</th><th>失败分区</th><th>缺失分区</th><th>完整性</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in report.reports" :key="item.task" :data-testid="`aggregate-row-${item.task}`">
          <td>{{ item.task }}</td>
          <td>{{ item.status }}</td>
          <td class="mono">{{ item.checkpoint_id ?? '—' }}</td>
          <td>{{ item.replay_version || '—' }}</td>
          <td>{{ item.success_partitions.join('、') || '—' }}</td>
          <td>
            <span
              v-for="partition in item.failed_partitions"
              :key="partition"
              class="badge danger"
              :data-testid="`failed-partition-${item.task}-${partition}`"
            >{{ partition }}</span>
            <span v-if="!item.failed_partitions.length">—</span>
          </td>
          <td>{{ item.missing_partitions.join('、') || '—' }}</td>
          <td>
            <span class="badge" :class="item.blocked ? 'danger' : 'ok'">
              {{ item.blocked ? (item.reason || '待修复') : '完整' }}
            </span>
          </td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { loadAggregateIntegrity } from '@/api/flows'
import type { IntegrityReport } from '@/api/flows'

const report = ref<IntegrityReport | null>(null)
const errorMessage = ref('')

async function reload() {
  errorMessage.value = ''
  try {
    report.value = await loadAggregateIntegrity()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '聚合落库完整性读取失败'
  }
}

onMounted(reload)
</script>
