/** 统一请求封装：拼后端地址、抛网络错误、给页脚留一句可读的说明。
 *
 * 团队用查询参数传递：HTTP 头里放中文团队名会被浏览器按 latin-1 编码，
 * 后端只能看到乱码；查询参数天然是 UTF-8。
 */
const API_BASE = import.meta.env.VITE_API_BASE ?? ''

let currentTeam = ''

/** 权限页面切换团队后，后续所有 /api 请求都带上同一个团队。 */
export function setRequestTeam(team: string): void {
  currentTeam = team
}

function withTeam(url: string): string {
  if (!currentTeam || !url.includes('/api/')) return url
  const join = url.includes('?') ? '&' : '?'
  return `${url}${join}team=${encodeURIComponent(currentTeam)}`
}

export function request(path: string, init?: RequestInit): Promise<Response> {
  const url = path.startsWith('http') ? path : `${API_BASE}${path}`
  return fetch(withTeam(url), {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  }).catch((error: unknown) => {
    const detail = error instanceof Error ? error.message : '请求未送达'
    throw new Error(`接口请求失败：${detail}`)
  })
}

export async function fetchJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}，数据未更新`)
  }
  return (await response.json()) as T
}
