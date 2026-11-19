<template>
  <section class="flow-panel" data-testid="sink-checkpoint-panel">
    <header class="flow-panel-head">
      <div>
        <h3>检查点覆盖核对</h3>
        <p class="flow-desc">
          结果落地按检查点读取聚合落库数据：核对成功分区与失败记录，缺记录时阻止任务完成。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="sink-refresh" @click="reload">刷新</button>
        <button
          class="btn primary"
          type="button"
          data-testid="sink-complete"
          :disabled="!taskName.trim()"
          @click="complete"
        >
          标记完成
        </button>
      </div>
    </header>

    <div class="filter-bar">
      <label class="filter-item">
        <span>聚合任务</span>
        <input v-model="taskName" data-testid="sink-task-input" placeholder="按任务名核对检查点" />
      </label>
      <button class="btn" type="button" data-testid="sink-load" @click="reload">核对</button>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="sink-panel-error">{{ errorMessage }}</p>
    <p v-if="completionMessage" class="flow-note" data-testid="sink-completion">{{ completionMessage }}</p>

    <template v-if="checkpoint">
      <dl class="flow-kv" data-testid="sink-checkpoint-summary">
        <dt>检查点</dt><dd class="mono">{{ checkpoint.checkpoint_id }}</dd>
        <dt>回放版本</dt><dd>{{ replayable?.replay_version ?? '—' }}</dd>
        <dt>成功分区</dt><dd>{{ checkpoint.declared_success_partitions.join('、') || '—' }}</dd>
        <dt>核对通过</dt><dd>{{ checkpoint.verified_success_partitions.join('、') || '—' }}</dd>
        <dt>缺口分区</dt>
        <dd data-testid="sink-missing-partitions">
          <span
            v-for="partition in checkpoint.blocking_partitions"
            :key="partition"
            class="badge danger"
          >{{ partition }}</span>
          <span v-if="!checkpoint.blocking_partitions.length">无</span>
        </dd>
        <dt>结论</dt>
        <dd>
          <span class="badge" :class="checkpoint.blocked ? 'danger' : 'ok'" data-testid="sink-blocked-badge">
            {{ checkpoint.blocked ? '阻止完成' : '可回放' }}
          </span>
          <span class="flow-message">{{ checkpoint.message }}</span>
        </dd>
      </dl>

      <table v-if="checkpoint.failed_records.length" class="data-table" data-testid="sink-failed-records">
        <thead><tr><th>失败记录</th><th>分区</th><th>原因</th></tr></thead>
        <tbody>
          <tr v-for="record in checkpoint.failed_records" :key="record.record_id">
            <td>{{ record.record_id }}</td>
            <td>{{ record.partition }}</td>
            <td>{{ record.reason }}</td>
          </tr>
        </tbody>
      </table>

      <div class="page-actions">
        <button class="btn" type="button" data-testid="retry-append" @click="retry('append')">
          重试并在原检查点追加修复
        </button>
        <button class="btn" type="button" data-testid="retry-rebuild" @click="retry('rebuild')">
          重试并重建同一检查点
        </button>
      </div>
      <p v-if="retryMessage" class="flow-note" data-testid="retry-note">{{ retryMessage }}</p>
    </template>
    <p v-else-if="!errorMessage" class="flow-empty">输入聚合任务名后核对检查点覆盖情况</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { completeTask, loadCheckpointCoverage, loadReplayableCheckpoint, retryTask } from '@/api/flows'
import type { CheckpointCoverage, ReplayableCheckpoint } from '@/api/flows'

const taskName = ref('')
const checkpoint = ref<CheckpointCoverage | null>(null)
const replayable = ref<ReplayableCheckpoint | null>(null)
const errorMessage = ref('')
const completionMessage = ref('')
const retryMessage = ref('')

async function reload() {
  errorMessage.value = ''
  completionMessage.value = ''
  retryMessage.value = ''
  checkpoint.value = null
  replayable.value = null
  if (!taskName.value.trim()) return
  try {
    const replay = await loadReplayableCheckpoint(taskName.value.trim())
    replayable.value = replay
    if (!replay.checkpoint_id) {
      errorMessage.value = replay.error ?? '任务尚未生成检查点'
      return
    }
    checkpoint.value = await loadCheckpointCoverage(replay.checkpoint_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点核对失败'
  }
}

async function retry(strategy: 'append' | 'rebuild') {
  retryMessage.value = ''
  try {
    const result = await retryTask(taskName.value.trim(), strategy)
    retryMessage.value = result.ok
      ? `重试完成，回放版本更新为 v${result.replay_version}，检查点仍为同一份`
      : `仍有 ${result.still_failed} 条失败记录，检查点未新增版本`
    await reload()
  } catch (error) {
    retryMessage.value = error instanceof Error ? error.message : '重试失败'
  }
}

async function complete() {
  completionMessage.value = ''
  try {
    const result = await completeTask(taskName.value.trim())
    completionMessage.value = result.message
  } catch (error) {
    completionMessage.value = error instanceof Error ? error.message : '任务完成校验失败'
  }
}

onMounted(reload)
</script>
