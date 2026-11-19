"""回放引擎领域服务：消费检查点数据执行数据回放与恢复验证。

数据流: checkpoint_mgr（检查点管理）→ replay_engine（回放引擎）→ restore_plan（故障恢复）
核心逻辑: 检查点快照回放、状态恢复验证、位点对齐、回放进度管理。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "回放任务"
    LIST_FIELDS = ["任务名称", "源检查点", "目标拓扑", "回放模式", "起始位点", "结果验证", "进度"]
    REQUIRED_FIELDS = ["任务名称", "源检查点", "回放模式"]
    STATUS_ORDER = ["草稿", "待回放", "回放中", "已完成", "验证失败"]
    ACTION_RULES = {"启动回放": "待回放", "暂停回放": "回放中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = ["暂停回放"]

    def create_replay_job(
        self,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
    ) -> dict[str, Any]:
        """基于检查点创建数据回放任务。

        消费 checkpoint_mgr 中的检查点快照，结合 task_topology 的拓扑配置，
        创建回放任务并写入恢复方案的引用。
        """
        # 1. 验证检查点存在
        cp_row = None
        for r in store.rows("checkpoint_mgr"):
            cid = r.get("id", "")
            if str(cid) == str(checkpoint_id):
                cp_row = r
                break
        if cp_row is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        if cp_row.get("status") != "已启用":
            return {"ok": False, "error": f"检查点「{checkpoint_id}」未启用"}

        # 只有完整可回放的版本才能作为消费来源：失败分区未修复时拒绝回放
        if cp_row.get("incomplete") or cp_row.get("pending_repair"):
            return {
                "ok": False,
                "error": "检查点仍存在缺失分区或失败记录，不能作为回放版本",
                "checkpoint_id": checkpoint_id,
                "missing_partitions": sorted({
                    item.get("partition", "")
                    for item in cp_row.get("pending_repair", [])
                    if item.get("partition")
                }),
            }

        # 2. 验证拓扑配置
        topo_found = any(
            r.get("拓扑名称") == topology_name
            for r in store.rows("task_topology")
        )
        if not topo_found and topology_name:
            return {"ok": False, "error": f"拓扑配置「{topology_name}」不存在"}

        # 3. 创建回放任务
        task_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
        ) + 1
        replay_task = {
            "id": task_id,
            "任务名称": job_name,
            "源检查点": str(checkpoint_id),
            "目标拓扑": topology_name,
            "回放模式": mode,
            "起始位点": cp_row.get("timestamp", time.time() - 3600),
            "结果验证": "待验证",
            "进度": "0%",
            "status": "待回放",
            "pending": True,
            "abnormal": False,
            "_checkpoint_snapshot": copy.deepcopy(cp_row),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(replay_task)

        return {
            "ok": True,
            "task_id": task_id,
            "task_name": job_name,
            "checkpoint_id": checkpoint_id,
            "topology": topology_name,
            "mode": mode,
        }

    def execute_replay(self, task_id: int) -> dict[str, Any]:
        """执行回放任务：检查点状态快照回放 + 结果验证。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        if task.get("status") not in ("待回放", "回放中"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 不可回放"}

        task["status"] = "回放中"

        # 获取检查点快照中的状态数据
        cp_snapshot = task.get("_checkpoint_snapshot", {})
        state_data = cp_snapshot.get("state", cp_snapshot.get("operator_states", {}))

        # 执行回放：对每个算子状态复制到 state_store
        replay_results = []
        if state_data:
            for op_name, op_state in state_data.items():
                state_entry = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                    ) + 1,
                    "配置名称": f"replay_{op_name}_{task_id}",
                    "后端类型": "memory",
                    "状态键模式": f"replay://{op_name}",
                    "TTL时长": "3600",
                    "检查点间隔": "30",
                    "增量模式": "full",
                    "清理策略": "after_replay",
                    "status": "已生效",
                    "pending": True,
                    "abnormal": False,
                    "_replay_task_id": task_id,
                    "_replay_state": copy.deepcopy(op_state),
                    "_created_at": time.time(),
                }
                store.rows("state_store").append(state_entry)
                replay_results.append({
                    "operator": op_name,
                    "state_keys": len(op_state) if isinstance(op_state, dict) else 0,
                    "restored_to": "state_store",
                })

        # 验证回放结果
        verification = self._verify_replay_results(task_id, replay_results)

        task["结果验证"] = verification["status"]
        task["进度"] = "100%"
        task["status"] = "已完成" if verification["all_passed"] else "验证失败"
        task["_replay_results"] = replay_results

        return {
            "ok": True,
            "task_id": task_id,
            "restored_operators": len(replay_results),
            "replay_results": replay_results,
            "verification": verification,
        }

    def _verify_replay_results(
        self, task_id: int, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """验证回放结果的完整性：检查 state_store 中是否正确落入了回放状态。"""
        restored_ops = set(r["operator"] for r in results)
        found_in_store = set()
        for row in store.rows("state_store"):
            if row.get("_replay_task_id") == task_id:
                cfg_name = row.get("配置名称", "")
                parts = cfg_name.split("_")
                if len(parts) >= 3:
                    found_in_store.add(parts[1])

        missing = restored_ops - found_in_store
        all_passed = len(missing) == 0
        return {
            "status": "passed" if all_passed else "partial",
            "all_passed": all_passed,
            "expected": len(restored_ops),
            "found": len(found_in_store),
            "missing_operators": list(missing),
        }

    def list_replay_history(self) -> list[dict[str, Any]]:
        """列出所有回放任务历史记录及恢复方案关联。"""
        history = []
        for task in store.rows(self.MODULE):
            entry = {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "源检查点": task.get("源检查点", ""),
                "回放模式": task.get("回放模式", ""),
                "结果验证": task.get("结果验证", "待验证"),
                "进度": task.get("进度", "0%"),
                "status": task.get("status", ""),
            }
            # 查找关联的恢复方案
            for rp in store.rows("restore_plan"):
                if task.get("目标拓扑", "") in rp.get("任务范围", ""):
                    entry["linked_restore_plan"] = rp.get("方案名称", "")
                    break
            history.append(entry)
        return history

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加回放成功率。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        completed = sum(1 for t in rows if t.get("status") == "已完成")
        failed = sum(1 for t in rows if t.get("status") == "验证失败")
        total_done = completed + failed
        base["replay_success_rate"] = completed / max(total_done, 1)
        base["completed_replays"] = completed
        base["failed_replays"] = failed
        return base