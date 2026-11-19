"""聚合落库领域服务：将实时聚合结果按定时频率批量写入目标存储表。

修复要点（序号35）：
- 逐条写入后先判定每条记录的写入状态，失败记录留在输出表（``write_status=failed``）；
- 检查点只声明**已成功写入**的分区与数量，不再用"计划写入数量"冒充快照；
- 失败分区进入输出完整性报告，缺记录时阻止任务完成（``待重试`` -> 不能标成已完成）；
- 重试跨过已创建的检查点时**更新同一检查点**（追加修复记录 + 版本号），不新建可回放版本；
- 同一提交键只落库一次，重复提交返回同一检查点与同一批次。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.access import (
    PermissionDenied,
    current_team,
    is_owner,
    require_view,
    require_write,
)
from app.services.base import BaseModuleService
from app.store import store

CHECKPOINT_COLLECTION = "checkpoint_mgr"
SINK_COLLECTION = "sink_output"


class AggregateSinkService(BaseModuleService):
    MODULE = "aggregate_sink"
    ENTITY = "聚合任务"
    LIST_FIELDS = ["任务名称", "源指标集", "聚合粒度", "输出表名", "落库频率", "覆盖模式", "分区键"]
    REQUIRED_FIELDS = ["任务名称", "源指标集", "聚合粒度"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "写入失败"]
    ACTION_RULES = {"启动任务": "待启动", "停止任务": "运行中"}
    NEGATIVE_ACTIONS = []
    SCOPE_ENABLED = True
    SHARE_READ_ONLY = True

    # ------------------------------------------------------------------
    # 配置校验
    # ------------------------------------------------------------------

    def _find_task(self, task_name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(self.MODULE) if r.get("任务名称") == task_name),
            None,
        )

    def validate_sink_task(self, task_name: str) -> dict[str, Any]:
        """校验聚合落库任务的配置完整性。"""
        task = self._find_task(task_name)
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        checks = []
        granularity = task.get("聚合粒度", "")
        valid_granularities = {"minute", "hour", "day", "week", "month"}
        checks.append({
            "check": "granularity",
            "ok": granularity.lower() in valid_granularities,
            "detail": granularity,
        })

        try:
            frequency = int(task.get("落库频率", "60"))
            checks.append({
                "check": "frequency",
                "ok": frequency >= 10 and frequency <= 86400,
                "detail": f"{frequency}s",
            })
        except (ValueError, TypeError):
            checks.append({"check": "frequency", "ok": False, "detail": "格式错误"})

        partition_key = task.get("分区键", "")
        checks.append({
            "check": "partition_key",
            "ok": bool(partition_key and partition_key.strip()),
            "detail": partition_key,
        })

        mode = task.get("覆盖模式", "append")
        checks.append({
            "check": "mode",
            "ok": mode in ("append", "overwrite", "merge"),
            "detail": mode,
        })

        return {
            "ok": all(c["ok"] for c in checks),
            "task": task_name,
            "checks": checks,
        }

    # ------------------------------------------------------------------
    # 聚合落库：逐条写入 + 成功分区检查点
    # ------------------------------------------------------------------

    def batch_flush_to_target(
        self,
        task_name: str,
        aggregated: list[dict[str, Any]],
        *,
        submission_key: str | None = None,
        partition: str | None = None,
        team: str | None = None,
    ) -> dict[str, Any]:
        """将聚合结果批量写入目标存储表，并只把成功分区写入检查点。

        ``partition`` 为可选的外部分区标签；不传时按落库频率对应的当前小时分片。
        每条记录独立判定 ``write_status``，失败记录保留在输出表中等待重试。
        """
        with store.lock:
            task = self._find_task(task_name)
            if task is None:
                return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}
            if self.SCOPE_ENABLED:
                require_write(self.MODULE, task, current_team(explicit=team), "落库")

            # 幂等：同一提交键返回首次落库的批次和检查点，不重复写入
            seen = task.get("_batches_by_submission", {})
            if submission_key and submission_key in seen:
                prior = dict(seen[submission_key])
                prior["duplicate_submission"] = True
                return prior

            partition_key = task.get("分区键", "dt")
            mode = task.get("覆盖模式", "append")
            batch_id = self._next_batch_id(task_name)
            default_partition = partition or time.strftime("%Y%m%d%H")

            written = 0
            failed = 0
            partition_ok: dict[str, int] = {}
            partition_bad: dict[str, int] = {}
            failed_records: list[dict[str, Any]] = []
            for item in aggregated:
                partition_val = self._record_partition(item, default_partition)
                sink_record = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows(SINK_COLLECTION)), default=0
                    ) + 1,
                    "配置名称": f"agg_{task_name}_{partition_val}",
                    "输出类型": "aggregate_batch",
                    "目标地址": f"table://{task.get('输出表名', task_name)}",
                    "写入模式": mode,
                    "分区策略": partition_key,
                    "格式转换": "json",
                    "status": "已生效",
                    "sink": f"agg_{task_name}",
                    "batch_id": batch_id,
                    "partition": partition_val,
                    "data": item,
                    "written_at": time.time(),
                    "所属团队": task.get("所属团队") or current_team(explicit=team),
                }
                # 逐条判定写入结果，失败记录留在输出表里，不删除也不换算成成功
                success, reason = self._write_one(item)
                sink_record["write_status"] = "success" if success else "failed"
                if success:
                    written += 1
                    partition_ok[partition_val] = partition_ok.get(partition_val, 0) + 1
                else:
                    sink_record["异常标记"] = reason
                    sink_record["_retryable"] = True
                    # 只有显式声明"重试也无法修复"的记录才持续失败（毒记录）
                    sink_record["_force_fail"] = bool(
                        item.get("force_fail") or item.get("write_ok") is False)
                    sink_record["_retry_ok"] = bool(item.get("retry_ok", True))
                    failed += 1
                    partition_bad[partition_val] = partition_bad.get(partition_val, 0) + 1
                    failed_records.append({
                        "record_id": sink_record["id"],
                        "partition": partition_val,
                        "reason": reason,
                    })
                store.rows(SINK_COLLECTION).append(sink_record)

            # 分区只要有任意一条失败就不算"成功写入"，不写进检查点
            complete_partitions = [
                p for p in partition_ok if p not in partition_bad
            ]
            failed_partitions = sorted(partition_bad)

            # 只有全部成功才允许把任务置为运行中；存在失败分区时进入写入失败待重试
            cp_entry = self._sync_checkpoint(
                task=task,
                batch_id=batch_id,
                planned=len(aggregated),
                success_partitions=complete_partitions,
                failed_partitions=failed_partitions,
                failed_records=failed_records,
                team=team,
            )

            if failed:
                task["status"] = "写入失败"
                task["pending"] = True
                task["abnormal"] = True
                status = "写入失败"
            else:
                task["status"] = "运行中"
                task["pending"] = True
                task["abnormal"] = False
                status = "运行中"
            task["last_batch_id"] = batch_id
            task["last_checkpoint_id"] = cp_entry["checkpoint_id"]
            task["last_completeness"] = (
                "complete" if not failed else "incomplete"
            )

            store.remember_submission(self.MODULE, submission_key, task["id"])
            task.setdefault("batches", []).append(batch_id)

            result = {
                "ok": failed == 0,
                "task": task_name,
                "written": written,
                "failed": failed,
                "planned": len(aggregated),
                "checkpoint_id": cp_entry["checkpoint_id"],
                "batch_id": batch_id,
                "success_partitions": complete_partitions,
                "failed_partitions": failed_partitions,
                "failed_records": failed_records,
                "status": status,
                "complete": failed == 0,
            }
            if submission_key:
                # 只记摘要，重复提交时回放同一批次与同一检查点
                task.setdefault("_batches_by_submission", {})[submission_key] = {
                    "ok": result["ok"],
                    "task": task_name,
                    "written": written,
                    "failed": failed,
                    "planned": len(aggregated),
                    "checkpoint_id": cp_entry["checkpoint_id"],
                    "batch_id": batch_id,
                    "status": status,
                    "complete": failed == 0,
                }
            return result

    def _next_batch_id(self, task_name: str) -> str:
        return f"batch_{task_name}_{int(time.time() * 1000)}"

    @staticmethod
    def _record_partition(item: dict[str, Any], default: str) -> str:
        """记录分区：允许批次内按记录携带分区，缺省落到批次分区。"""
        if isinstance(item, dict):
            explicit = item.get("partition") or item.get("dt")
            if explicit:
                return str(explicit)
        return default

    @staticmethod
    def _write_one(item: dict[str, Any]) -> tuple[bool, str]:
        """单条写入判定：由记录自带的故障标记决定成败，保证可重复测试。"""
        if isinstance(item, dict):
            if item.get("force_fail"):
                return False, "目标表写入被拒绝"
            if item.get("write_ok") is False:
                return False, "源记录标记为不可写"
        return True, ""

    # ------------------------------------------------------------------
    # 检查点：只声明成功分区，重试时更新同一检查点
    # ------------------------------------------------------------------

    def _find_checkpoint(self, checkpoint_id: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(CHECKPOINT_COLLECTION)
             if r.get("checkpoint_id") == checkpoint_id),
            None,
        )

    def _sync_checkpoint(
        self,
        *,
        task: dict[str, Any],
        batch_id: str,
        planned: int,
        success_partitions: list[str],
        failed_partitions: list[str] | None = None,
        failed_records: list[dict[str, Any]],
        team: str | None = None,
    ) -> dict[str, Any]:
        """写入或更新检查点：同一落库批次始终复用同一个可回放版本。"""
        task_name = task.get("任务名称", "")
        existing_id = task.get("last_checkpoint_id")
        cp_entry = self._find_checkpoint(existing_id) if existing_id else None
        now = time.time()

        if cp_entry is None:
            numeric_ids = [
                int(r.get("id"))
                for r in store.rows(CHECKPOINT_COLLECTION)
                if isinstance(r.get("id"), int) or str(r.get("id", "")).isdigit()
            ]
            cp_entry = {
                "id": max(numeric_ids, default=0) + 1,
                "配置名称": f"checkpoint_{task_name}_{int(now)}",
                "任务名称": task_name,
                "间隔时间": task.get("落库频率", "60"),
                "超时时长": "120",
                "最大并发": "3",
                "对齐策略": "exactly_once",
                "保留数量": 5,
                "外部化路径": f"/data/checkpoints/{task_name}",
                "status": "已启用",
                "checkpoint_id": f"cp_agg_{task_name}_{int(now)}",
                "replay_version": 1,
                "repair_log": [],
                "snapshot_ts": now,
                "所属团队": task.get("所属团队") or current_team(explicit=team),
                "共享模式": task.get("共享模式") or "团队私有",
            }
            store.rows(CHECKPOINT_COLLECTION).append(cp_entry)

        # 只记录成功写入的分区；失败分区和失败记录单独进入完整性报告
        covered = list(cp_entry.get("success_partitions", []))
        for p in success_partitions:
            if p not in covered:
                covered.append(p)
        cp_entry["success_partitions"] = covered
        cp_entry["declared_failed_partitions"] = sorted(set(failed_partitions or []))
        cp_entry["written"] = len(covered)
        cp_entry["success_count"] = int(cp_entry.get("success_count", 0))
        cp_entry["planned_at_last_batch"] = planned
        cp_entry["batch_id"] = batch_id
        cp_entry["operator_states"] = {
            "task": task_name,
            "written": len(covered),
            "success_partitions": covered,
            "failed": len(failed_records),
        }
        cp_entry["snapshot_ts"] = now

        if failed_records:
            cp_entry["status"] = "已启用"
            cp_entry["incomplete"] = True
            cp_entry["pending_repair"] = [
                {"record_id": r["record_id"], "partition": r["partition"], "reason": r["reason"]}
                for r in failed_records
            ]
            cp_entry["last_report"] = self._build_report(
                cp_entry,
                planned=planned,
                failed_partitions=failed_partitions or [],
                failed_records=failed_records,
            )
        else:
            cp_entry["incomplete"] = False
            cp_entry["pending_repair"] = []
            cp_entry["last_report"] = self._build_report(
                cp_entry, planned=planned, failed_partitions=[], failed_records=[])

        return cp_entry

    def _build_report(
        self,
        cp_entry: dict[str, Any],
        *,
        planned: int,
        failed_partitions: list[str],
        failed_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        covered = cp_entry.get("success_partitions", [])
        missing_partitions = sorted({
            r["partition"] for r in failed_records
            if r["partition"] not in covered
        } | {p for p in failed_partitions if p not in covered})
        return {
            "checkpoint_id": cp_entry.get("checkpoint_id"),
            "planned": planned,
            "success_count": cp_entry.get("written", 0),
            "success_partitions": list(covered),
            "failed_count": len(failed_records),
            "failed_records": copy.deepcopy(failed_records),
            "missing_partitions": missing_partitions,
            "complete": len(failed_records) == 0,
            "generated_at": time.time(),
        }

    # ------------------------------------------------------------------
    # 失败重试：跨检查点回退/重建，或在原检查点追加修复记录
    # ------------------------------------------------------------------

    def retry_failed_writes(
        self,
        task_name: str,
        *,
        strategy: str = "append",
        team: str | None = None,
    ) -> dict[str, Any]:
        """重试失败记录，并保证检查点仍然指向一个完整可回放的版本。

        strategy:
          - ``append``：在原检查点上追加修复记录，版本号 +1，不新建可回放版本；
          - ``rebuild``：回退到失败批次前的检查点基线并重建同一检查点。
        """
        with store.lock:
            task = self._find_task(task_name)
            if task is None:
                return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}
            if self.SCOPE_ENABLED:
                require_write(self.MODULE, task, current_team(explicit=team), "重试")

            failed_rows = [
                r for r in store.rows(SINK_COLLECTION)
                if r.get("sink") == f"agg_{task_name}"
                and r.get("write_status") == "failed"
            ]
            repaired: list[dict[str, Any]] = []
            still_failed: list[dict[str, Any]] = []
            for row in failed_rows:
                ok, reason = self._retry_one(row)
                if ok:
                    row["write_status"] = "success"
                    row["repaired_at"] = time.time()
                    row.pop("_retryable", None)
                    repaired.append({
                        "record_id": row.get("id"),
                        "partition": row.get("partition"),
                        "batch_id": row.get("batch_id", ""),
                    })
                else:
                    row["异常标记"] = reason
                    still_failed.append({
                        "record_id": row.get("id"),
                        "partition": row.get("partition"),
                        "reason": reason,
                    })

            checkpoint_id = task.get("last_checkpoint_id")
            cp_entry = self._find_checkpoint(checkpoint_id) if checkpoint_id else None
            if cp_entry is None:
                # 失败重试跨过还没有检查点的批次：补建一个检查点，只覆盖成功分区
                cp_entry = self._sync_checkpoint(
                    task=task,
                    batch_id=task.get("last_batch_id", ""),
                    planned=len(failed_rows),
                    success_partitions=[],
                    failed_partitions=[],
                    failed_records=still_failed,
                    team=team,
                )
                checkpoint_id = cp_entry["checkpoint_id"]

            # 以"检查点已声明的分区 + 本批次出现过的分区"为候选，
            # 分区下只要还有失败记录就不算成功写入
            covered = list(cp_entry.get("success_partitions", []))
            declared_failed = list(cp_entry.get("declared_failed_partitions", []))
            still_failed_partitions = sorted({
                r["partition"] for r in still_failed if r.get("partition")
            })
            candidates = list(covered)
            for p in [*declared_failed, *(item["partition"] for item in repaired)]:
                if p and p not in candidates:
                    candidates.append(p)
            new_success = [p for p in candidates if p not in still_failed_partitions]
            failing = [p for p in candidates if p in still_failed_partitions]

            repair_entries = [
                {
                    "record_id": item["record_id"],
                    "partition": item["partition"],
                    "repaired_at": time.time(),
                    "strategy": strategy,
                }
                for item in repaired
            ]

            if strategy == "rebuild":
                # 回退到失败批次前的成功分区基线，再按当前核验结果重建同一检查点
                baseline = list(cp_entry.get("baseline_partitions") or covered)
                cp_entry["baseline_partitions"] = baseline
                verified = [
                    p for p in baseline
                    if p not in still_failed_partitions
                ]
                for p in new_success:
                    if p not in verified:
                        verified.append(p)
                cp_entry["success_partitions"] = verified
                cp_entry["rebuild_count"] = int(cp_entry.get("rebuild_count", 0)) + 1
            else:
                cp_entry["success_partitions"] = new_success

            cp_entry["declared_failed_partitions"] = failing
            cp_entry.setdefault("repair_log", []).extend(repair_entries)
            cp_entry["repair_strategy"] = strategy
            cp_entry["written"] = len(cp_entry["success_partitions"])
            cp_entry["success_count"] = int(cp_entry.get("success_count", 0)) + len(repaired)
            cp_entry["operator_states"] = {
                "task": task_name,
                "written": cp_entry["written"],
                "success_partitions": list(cp_entry["success_partitions"]),
                "failed": len(still_failed),
            }
            cp_entry["replay_version"] = int(cp_entry.get("replay_version", 1)) + 1
            cp_entry["snapshot_ts"] = time.time()
            cp_entry["incomplete"] = len(still_failed) > 0
            cp_entry["pending_repair"] = list(still_failed)
            cp_entry["last_report"] = self._build_report(
                cp_entry,
                planned=int(cp_entry.get("planned_at_last_batch", 0)),
                failed_partitions=failing,
                failed_records=still_failed,
            )

            # 任务状态跟随完整性，绝不因为写了检查点就标成运行中/已完成
            if still_failed:
                task["status"] = "写入失败"
                task["abnormal"] = True
            else:
                task["status"] = "已完成"
                task["abnormal"] = False
            task["pending"] = bool(still_failed)
            task["last_checkpoint_id"] = cp_entry["checkpoint_id"]
            task["last_completeness"] = "complete" if not still_failed else "incomplete"

            return {
                "ok": not still_failed,
                "task": task_name,
                "checkpoint_id": cp_entry["checkpoint_id"],
                "replay_version": cp_entry["replay_version"],
                "repaired": len(repaired),
                "still_failed": len(still_failed),
                "repaired_records": repaired,
                "pending_repair": still_failed,
                "success_partitions": list(cp_entry["success_partitions"]),
                "failed_partitions": failing,
                "complete": not still_failed,
            }

    @staticmethod
    def _retry_one(row: dict[str, Any]) -> tuple[bool, str]:
        """重试判定：写入失败不是毒记录时默认可在重试中修复。"""
        if row.get("_force_fail") and not row.get("_retry_ok", True):
            return False, "目标表仍拒绝写入"
        if row.get("force_fail"):
            return False, "目标表仍拒绝写入"
        if row.get("write_ok") is False:
            return False, "源记录标记为不可写"
        return True, ""

    # ------------------------------------------------------------------
    # 完整性报告与消费视图
    # ------------------------------------------------------------------

    def output_integrity_report(
        self,
        task_name: str | None = None,
        *,
        team: str | None = None,
    ) -> dict[str, Any]:
        """生成输出完整性报告：成功分区、失败记录和缺失分区一目了然。"""
        viewer = current_team(explicit=team)
        tasks = [
            t for t in store.rows(self.MODULE)
            if t.get("任务名称") and isinstance(t.get("id"), int)
            and (task_name is None or t.get("任务名称") == task_name)
        ]
        if task_name is not None and not tasks:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在或无权查看"}
        if self.SCOPE_ENABLED:
            tasks = [t for t in tasks if is_owner(t, viewer)]
        if task_name is not None and not tasks:
            raise PermissionDenied(
                f"聚合任务「{task_name}」仅限所属团队查看，当前团队 {viewer} 越权访问，"
                "拒绝返回结果",
                code="record_scope_denied",
            )

        reports = []
        for task in tasks:
            name = task.get("任务名称", "")
            checkpoint_id = task.get("last_checkpoint_id")
            cp_entry = self._find_checkpoint(checkpoint_id) if checkpoint_id else None
            covered = list((cp_entry or {}).get("success_partitions", []))
            failed_rows = [
                r for r in store.rows(SINK_COLLECTION)
                if r.get("sink") == f"agg_{name}" and r.get("write_status") == "failed"
            ]
            failed_partitions = sorted({
                r.get("partition", "") for r in failed_rows if r.get("partition")
            })
            pending = list((cp_entry or {}).get("pending_repair", []))
            missing = sorted({
                p.get("partition", "") for p in pending
                if p.get("partition") and p.get("partition") not in covered
            } | {p for p in failed_partitions if p not in covered})
            # 没有检查点的任务同样不能声称完整：没有可回放版本就阻挡完成
            blocked = bool(pending) or bool((cp_entry or {}).get("incomplete")) or not cp_entry
            reports.append({
                "task": name,
                "status": task.get("status", ""),
                "checkpoint_id": checkpoint_id,
                "replay_version": (cp_entry or {}).get("replay_version", 0),
                "success_partitions": covered,
                "failed_partitions": failed_partitions,
                "failed_records": [
                    {
                        "record_id": r.get("id"),
                        "partition": r.get("partition", ""),
                        "reason": r.get("异常标记", ""),
                    }
                    for r in failed_rows
                ],
                "pending_repair": pending,
                "missing_partitions": missing,
                "planned": (cp_entry or {}).get("planned_at_last_batch", 0),
                "success_count": len(covered),
                "complete": not blocked,
                "blocked": blocked,
                "reason": (
                    "尚未生成检查点，没有可回放版本"
                    if not cp_entry else
                    ("存在失败分区或失败记录" if blocked else "")
                ),
            })

        return {
            "ok": True,
            "generated_at": time.time(),
            "task_count": len(reports),
            "blocked_tasks": [r["task"] for r in reports if r["blocked"]],
            "reports": reports,
        }

    def list_replayable_checkpoints(self, task_name: str, team: str | None = None) -> dict[str, Any]:
        """结果落地消费视图：每个任务只有一个完整可回放版本。

        共享的聚合落库数据允许其他团队按检查点受控查看，但重试和完成仍只归所属团队。
        """
        task = self._find_task(task_name)
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}
        viewer = current_team(explicit=team)
        if self.SCOPE_ENABLED:
            require_view(self.MODULE, task, viewer)
        checkpoint_id = task.get("last_checkpoint_id")
        cp_entry = self._find_checkpoint(checkpoint_id) if checkpoint_id else None
        if cp_entry is None:
            return {
                "ok": False,
                "task": task_name,
                "checkpoint_id": None,
                "replay_version": 0,
                "success_partitions": [],
                "missing_partitions": [],
                "missing_count": 0,
                "complete": False,
                "blocked": True,
                "error": f"聚合任务「{task_name}」尚未生成检查点",
                "repair_log": [],
                "status": "incomplete",
            }

        covered = list(cp_entry.get("success_partitions", []))
        missing = list(cp_entry.get("pending_repair", []))
        blocking = bool(missing) or bool(cp_entry.get("incomplete"))
        return {
            "ok": not blocking,
            "task": task_name,
            "checkpoint_id": cp_entry.get("checkpoint_id"),
            "replay_version": cp_entry.get("replay_version", 1),
            "success_partitions": covered,
            "missing_partitions": sorted({m.get("partition", "") for m in missing}),
            "missing_count": len(missing),
            "complete": not blocking,
            "blocked": blocking,
            "repair_log": copy.deepcopy(cp_entry.get("repair_log", [])),
            "status": "complete" if not blocking else "incomplete",
        }

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------

    def compute_storage_metrics(self) -> dict[str, Any]:
        """计算存储指标：所有聚合任务的写入统计。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if isinstance(r.get("id"), int) and r.get("id", 0) < 100
        ]
        metrics = {}
        total_records = 0
        for task in tasks:
            name = task.get("任务名称", "")
            count = sum(
                1 for s in store.rows(SINK_COLLECTION)
                if name and name in str(s.get("sink", ""))
                and s.get("write_status") == "success"
            )
            total_records += count
            metrics[name] = count

        return {
            "ok": True,
            "task_count": len(tasks),
            "total_records": total_records,
            "per_task": metrics,
            "avg_per_task": total_records / max(len(tasks), 1),
        }

    def list_sink_status(self) -> list[dict[str, Any]]:
        """列出所有聚合落库任务的状态及关联检查点信息。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if isinstance(r.get("id"), int) and r.get("id", 0) < 100
        ]
        result = []
        for t in tasks:
            name = t.get("任务名称", "")
            checkpoints = [
                c for c in store.rows(CHECKPOINT_COLLECTION)
                if name and name in str(c.get("配置名称", ""))
            ]
            result.append({
                "task": name,
                "granularity": t.get("聚合粒度", ""),
                "mode": t.get("覆盖模式", ""),
                "status": t.get("status", ""),
                "checkpoints": len(checkpoints),
                "checkpoint_id": t.get("last_checkpoint_id"),
                "completeness": t.get("last_completeness", ""),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["failed"] = sum(1 for r in rows if r.get("status") == "写入失败")
        base["incomplete_checkpoints"] = sum(
            1 for c in store.rows(CHECKPOINT_COLLECTION)
            if c.get("checkpoint_id") and c.get("incomplete")
        )
        return base
