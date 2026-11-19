"""团队维度访问控制：聚合落库与结果落地的记录可见性、共享只读和越权拒绝。

设计要点：
- 记录归属由 ``所属团队`` 决定，缺省归属当前值守团队；
- 聚合落库记录（含 35 号记录）只对所属团队可见，其他团队访问直接拒绝；
- 结果落地中的共享记录允许其他团队查看，但只能以受控只读方式读取，
  写动作一律拒绝，且拒绝后不改变记录归属和状态；
- 共享数据只暴露"完整可回放版本"，所属团队可以看到最新工作版本（可能含失败分区）。
"""
from __future__ import annotations

from typing import Any

DEFAULT_TEAM = "数据工程组"
TEAM_HEADERS = ("X-Operator-Team", "X-Team-Id")
SHARED_MODES = {"受控只读", "团队共享", "团队共享只读", "read_only", "shared_read_only"}


class PermissionDenied(Exception):
    """越权访问：路由层据此返回 403，服务层保证事务不落地。"""

    def __init__(self, message: str, *, code: str = "team_scope_denied", status_code: int = 403):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code

    def as_dict(self) -> dict[str, Any]:
        return {"ok": False, "code": self.code, "error": self.message}


def current_team(request: Any = None, explicit: str | None = None) -> str:
    """解析当前团队：显式参数 > 请求头 > 查询参数 > 默认值守团队。

    HTTP 头按 latin-1 解码，中文团队名需要还原成 UTF-8；查询参数本身就是 UTF-8。
    """
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    if request is not None:
        headers = getattr(request, "headers", None)
        if headers is not None:
            for name in TEAM_HEADERS:
                value = headers.get(name)
                if value and str(value).strip():
                    return _normalize_header(str(value).strip())
        query = getattr(request, "query_params", None)
        if query is not None:
            value = query.get("team")
            if value and str(value).strip():
                return str(value).strip()
    return DEFAULT_TEAM


def _normalize_header(value: str) -> str:
    """把 latin-1 解出来的 UTF-8 字节还原成正常中文；失败则原样返回。"""
    try:
        return value.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


def owner_of(row: dict[str, Any]) -> str:
    return str(row.get("所属团队") or DEFAULT_TEAM)


def is_owner(row: dict[str, Any], team: str) -> bool:
    return owner_of(row) == team


def is_shared(row: dict[str, Any]) -> bool:
    return str(row.get("共享模式") or "") in SHARED_MODES


def visible_in_list(module: str, row: dict[str, Any], team: str) -> bool:
    """列表可见性：私有记录仅归属团队可见，共享记录可被其他团队受控查看。"""
    if module in ("aggregate_sink", "sink_output"):
        return is_owner(row, team) or is_shared(row)
    return True


def require_view(module: str, row: dict[str, Any], team: str) -> None:
    if visible_in_list(module, row, team):
        return
    raise PermissionDenied(
        f"{module} 记录 {row.get('id')} 仅限所属团队 {owner_of(row)} 查看，"
        f"当前团队 {team} 越权访问，拒绝返回结果",
        code="record_scope_denied",
    )


def require_write(module: str, row: dict[str, Any], team: str, action: str = "写入") -> None:
    """写权限：共享记录对非归属团队只读；归属团队始终可写。"""
    if is_owner(row, team):
        return
    if is_shared(row):
        raise PermissionDenied(
            f"{module} 记录 {row.get('id')} 为 {owner_of(row)} 共享的受控只读数据，"
            f"当前团队 {team} 无权{action}",
            code="shared_read_only",
        )
    raise PermissionDenied(
        f"{module} 记录 {row.get('id')} 仅限所属团队 {owner_of(row)} 操作，"
        f"当前团队 {team} 无权{action}，事务已拒绝并保持原归属",
        code="team_scope_denied",
    )


def require_owner(row: dict[str, Any], team: str, action: str = "访问") -> None:
    if is_owner(row, team):
        return
    raise PermissionDenied(
        f"记录 {row.get('id')} 仅限所属团队 {owner_of(row)} {action}，"
        f"当前团队 {team} 越权访问，拒绝返回结果",
        code="record_scope_denied",
    )
