/** 当前值守团队：权限页面切换后，聚合落库 / 结果落地页面共用同一个团队上下文。 */
import { ref } from 'vue'

import { setRequestTeam } from '@/api/client'

export const currentTeam = ref('')

export function useTeam(team: string): void {
  currentTeam.value = team
  setRequestTeam(team)
}
