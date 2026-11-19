"""模块服务基类：共享查询、登记、动作和统计骨架，业务差异留在各模块文件。"""
from __future__ import annotations

from typing import Any

from app.access import current_team, is_owner, is_shared, require_owner, require_view, require_write
from app.store import store


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []
    # 归属维度：默认不启用团队隔离，聚合落库/结果落地在子类中打开
    SCOPE_ENABLED = False
    SHARE_READ_ONLY = False

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        team: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        if self.SCOPE_ENABLED:
            viewer = current_team(explicit=team)
            rows = [row for row in rows if self._visible(row, viewer)]
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int, team: str | None = None) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and self.SCOPE_ENABLED:
            require_view(self.MODULE, entry, current_team(explicit=team))
        return entry

    def _visible(self, row: dict[str, Any], team: str) -> bool:
        """归属团队可见；共享记录对其他团队开放受控只读查看。"""
        return is_owner(row, team) or (self.SHARE_READ_ONLY and is_shared(row))

    def create_entry(
        self, values: dict[str, Any], *,
        team: str | None = None,
        submission_key: str | None = None,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        key = submission_key or str(values.get("_submission_key") or "").strip() or None
        with store.lock:
            # 同一提交键只登记一次，重复提交直接回放第一次的结果
            existing = store.find_submission(self.MODULE, key)
            if existing is not None:
                self._mark_duplicate(existing, store.submission_count(self.MODULE, key))
                return existing, []
            missing = [
                field for field in self.REQUIRED_FIELDS
                if not str(values.get(field) or "").strip()
            ]
            if missing:
                return None, missing
            rows = store.rows(self.MODULE)
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            entry.update({field: values.get(field) for field in self.LIST_FIELDS})
            entry["status"] = self.STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            if self.SCOPE_ENABLED:
                entry["所属团队"] = str(values.get("所属团队") or current_team(explicit=team))
                entry["共享模式"] = str(values.get("共享模式") or "团队私有")
            rows.append(entry)
            store.remember_submission(self.MODULE, key, entry["id"])
            return entry, []

    @staticmethod
    def _mark_duplicate(entry: dict[str, Any], count: int = 1) -> None:
        entry["duplicate_submission"] = True
        entry["submission_count"] = int(count)

    def run_action(
        self, entry_id: int, action: str, *,
        team: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        with store.lock:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if self.SCOPE_ENABLED:
                require_write(self.MODULE, entry, current_team(explicit=team), action)
            target = self.ACTION_RULES.get(action)
            if target is None:
                return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
            entry["status"] = target
            entry["pending"] = target != self.STATUS_ORDER[-1]
            entry["abnormal"] = action in self.NEGATIVE_ACTIONS
            return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # 兼容旧调用：子类需要只读校验时可直接使用
    _require_owner = staticmethod(require_owner)
