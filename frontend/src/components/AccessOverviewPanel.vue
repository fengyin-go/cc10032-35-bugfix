<template>
  <section class="flow-panel" data-testid="access-overview">
    <header class="flow-panel-head">
      <div>
        <h3>权限页面</h3>
        <p class="flow-desc">
          当前团队：<strong data-testid="access-team">{{ overview?.team ?? '—' }}</strong>。
          团队私有记录只对所属团队可见；受控只读记录可查看但不能改动，越权接口直接拒绝返回结果。
        </p>
      </div>
      <div class="page-actions">
        <select v-model="currentTeam" data-testid="access-team-select" class="flow-select" @change="reload">
          <option v-for="item in teams" :key="item" :value="item">{{ item }}</option>
        </select>
        <button class="btn" type="button" data-testid="access-refresh" @click="reload">刷新</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="access-error">{{ errorMessage }}</p>

    <div v-if="overview" class="stat-row">
      <article v-for="(scope, key) in overview.scopes" :key="key" class="stat-card">
        <span class="stat-label">{{ scope.label }}</span>
        <strong class="stat-value">{{ countScope(String(key)) }}</strong>
        <p class="flow-desc">{{ scope.description }}</p>
      </article>
    </div>

    <table v-if="overview" class="data-table" data-testid="access-table">
      <thead>
        <tr><th>模块</th><th>记录</th><th>所属团队</th><th>权限范围</th><th>当前团队可见</th><th>访问方式</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in overview.records" :key="`${row.module}-${row.id}`">
          <td>{{ row.module }}</td>
          <td>{{ row.name }}</td>
          <td>{{ row.owner }}</td>
          <td><span class="badge" :class="row.scope === 'shared_read_only' ? 'warn' : 'ok'">{{ row.scope_label }}</span></td>
          <td>{{ row.visible ? '是' : '否' }}</td>
          <td>{{ row.read_only ? '受控只读' : (row.visible ? '可读写' : '越权拒绝') }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { loadAccessOverview } from '@/api/flows'
import type { AccessOverview } from '@/api/flows'
import { currentTeam, useTeam } from '@/data/team'

const overview = ref<AccessOverview | null>(null)
const teams = ref<string[]>([])
const errorMessage = ref('')

function countScope(scope: string) {
  return overview.value?.records.filter((row) => row.scope === scope && row.visible).length ?? 0
}

async function reload() {
  errorMessage.value = ''
  useTeam(currentTeam.value)
  try {
    overview.value = await loadAccessOverview()
    teams.value = overview.value.teams
    if (!currentTeam.value) useTeam(overview.value.team)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '权限页面读取失败'
  }
}

onMounted(reload)
</script>
