"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator / aggregate_sink → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验

修复要点（序号35）：
- 按检查点读取聚合落库数据，核对 ``success_partitions`` 与失败记录；
- 检查点指向的成功分区在输出表中缺记录时 **阻止任务完成**；
- 失败重试跨过已创建的检查点，只更新同一检查点，不新建可回放版本；
- 共享的聚合落库数据以受控只读方式展示给其他团队，写操作越权直接拒绝。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.access import (
    current_team,
    is_owner,
    require_owner,
    require_view,
    require_write,
)
from app.services.aggregate_sink import AggregateSinkService
from app.services.base import BaseModuleService
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]
    SCOPE_ENABLED = True
    SHARE_READ_ONLY = True

    def __init__(self) -> None:
        self._aggregate = AggregateSinkService()

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
        *, team: str | None = None,
    ) -> dict[str, Any]:
        """将计算结果批量写入目标存储，支持分区和格式转换。"""
        with store.lock:
            sink = next(
                (r for r in store.rows(self.MODULE) if r.get("配置名称") == sink_name),
                None,
            )
            if sink is None:
                return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
            require_write(self.MODULE, sink, current_team(explicit=team), "写入")
            if sink.get("status") == "已停用":
                return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

            try:
                batch_size = int(sink.get("批量大小", "100"))
            except (ValueError, TypeError):
                batch_size = 100

            partition_strategy = sink.get("分区策略", "round_robin")
            write_mode = sink.get("写入模式", "append")
            output_type = sink.get("输出类型", "database")
            format_type = sink.get("格式转换", "json")

            written = 0
            failed = 0
            for i, item in enumerate(results):
                partition_key = self._calc_partition(item, partition_strategy, i)
                converted = self._convert_format(item, format_type)
                record = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
                    ) + 1,
                    "sink": sink_name,
                    "output_type": output_type,
                    "partition": partition_key,
                    "format": format_type,
                    "mode": write_mode,
                    "data": converted,
                    "written_at": time.time(),
                    "status": "已生效",
                    "所属团队": sink.get("所属团队") or current_team(explicit=team),
                }
                if self._simulate_write(partition_key):
                    record["write_status"] = "success"
                    store.rows(self.MODULE).append(record)
                    written += 1
                else:
                    record["write_status"] = "failed"
                    record["异常标记"] = "写入异常"
                    store.rows(self.MODULE).append(record)
                    failed += 1

            if failed > 0:
                sink["status"] = "写入异常"
                sink["pending"] = True
                sink["abnormal"] = True

            return {
                "ok": failed == 0,
                "sink": sink_name,
                "total": len(results),
                "written": written,
                "failed": failed,
                "partition_strategy": partition_strategy,
                "batch_size": batch_size,
            }

    @staticmethod
    def _calc_partition(item: dict[str, Any], strategy: str, index: int) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(item: dict[str, Any], fmt: str) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    @staticmethod
    def _simulate_write(partition: str) -> bool:
        """模拟写入成功（绝大部分成功，偶发失败模拟异常）。"""
        return hash(partition) % 20 != 0

    # ---- 按检查点读取聚合落库数据 ----

    def read_checkpoint_outputs(
        self,
        checkpoint_id: str,
        *,
        team: str | None = None,
    ) -> dict[str, Any]:
        """按检查点读取聚合落库数据，核对成功分区和失败记录。

        结果落地只信任检查点声明的成功分区：分区在输出表里缺记录时标记 ``missing``，
        并在返回中标记 ``blocked``，调用方据此阻止任务完成。
        """
        viewer = current_team(explicit=team)
        cp_entry = next(
            (r for r in store.rows("checkpoint_mgr")
             if r.get("checkpoint_id") == checkpoint_id),
            None,
        )
        if cp_entry is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在", "blocked": True}
        require_view("aggregate_sink", cp_entry, viewer)

        covered = list(cp_entry.get("success_partitions", []))
        task_name = str(cp_entry.get("任务名称") or "").removeprefix("checkpoint_")

        # 逐条读回该任务的全部输出记录：重试修复过的记录状态已更新为 success，
        # 仍未修复的失败记录必须继续阻塞完成。
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == f"agg_{task_name}"
            and r.get("write_status") in ("success", "failed")
        ]
        verified_partitions = {
            r.get("partition") for r in records if r.get("write_status") == "success"
        }
        failed_records = [
            {
                "record_id": r.get("id"),
                "partition": r.get("partition", ""),
                "reason": r.get("异常标记", ""),
            }
            for r in records
            if r.get("write_status") == "failed"
        ]
        # 检查点声明的成功分区在输出表里必须能逐条核对到，缺一条就阻塞
        missing = [p for p in covered if p not in verified_partitions]
        failed_partitions = sorted({
            r["partition"] for r in failed_records if r.get("partition")
        })
        blocking_partitions = sorted(set(missing) | set(failed_partitions))

        blocked = bool(blocking_partitions) or bool(cp_entry.get("incomplete"))
        if missing:
            message = "检查点声明的成功分区在输出表中缺记录，任务不能标记完成"
        elif failed_records:
            message = "存在失败记录，任务不能标记完成"
        else:
            message = "检查点与输出数据一致"
        return {
            "ok": not blocked,
            "checkpoint_id": checkpoint_id,
            "task": task_name,
            "replay_version": cp_entry.get("replay_version", 1),
            "declared_success_partitions": covered,
            "verified_success_partitions": sorted(p for p in verified_partitions if p in covered),
            "missing_partitions": missing,
            "failed_partitions": failed_partitions,
            "blocking_partitions": blocking_partitions,
            "failed_records": failed_records,
            "failed_count": len(failed_records),
            "blocked": blocked,
            "message": message,
        }

    def verify_output_integrity(
        self, sink_name: str, *, team: str | None = None,
    ) -> dict[str, Any]:
        """验证指定 sink 下的输出记录完整性和写入成功率。"""
        viewer = current_team(explicit=team)
        config = next(
            (r for r in store.rows(self.MODULE) if r.get("配置名称") == sink_name),
            None,
        )
        if config is not None:
            require_view(self.MODULE, config, viewer)

        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name
        ]
        total = len(records)
        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = total - success
        failed_partitions = sorted({
            r.get("partition", "") for r in records if r.get("write_status") == "failed"
        })
        success_partitions = sorted({
            r.get("partition", "") for r in records if r.get("write_status") == "success"
        })
        return {
            "ok": True,
            "sink": sink_name,
            "total": total,
            "success": success,
            "failed": failed,
            "success_rate": success / max(total, 1),
            "partitions": len({r.get("partition", "") for r in records}),
            "success_partitions": success_partitions,
            "failed_partitions": failed_partitions,
            "needs_retry": failed > 0,
            # 有失败记录时输出不完整，不能作为完成态交付
            "complete": failed == 0,
            "blocks_completion": failed > 0,
        }

    def retry_failed_writes(
        self, sink_name: str, *, strategy: str = "append", team: str | None = None,
    ) -> dict[str, Any]:
        """重试失败的写入记录；跨检查点时更新同一检查点而不是新建版本。"""
        with store.lock:
            viewer = current_team(explicit=team)
            if sink_name.startswith("agg_"):
                task_name = sink_name[4:]
                self._require_aggregate_owner(task_name, viewer, "重试")
                # 交给聚合落库统一处理检查点修复，保证只有一个可回放版本
                return self._aggregate.retry_failed_writes(
                    task_name, strategy=strategy, team=viewer)

            config = next(
                (r for r in store.rows(self.MODULE) if r.get("配置名称") == sink_name),
                None,
            )
            if config is not None:
                require_write(self.MODULE, config, viewer, "重试")
            records = [
                r for r in store.rows(self.MODULE)
                if r.get("sink") == sink_name and r.get("write_status") == "failed"
            ]
            retried = 0
            still_failed = 0
            for record in records:
                if record.get("force_fail") or record.get("write_ok") is False:
                    still_failed += 1
                    continue
                record["write_status"] = "success"
                record["repaired_at"] = time.time()
                retried += 1
            return {
                "ok": still_failed == 0,
                "sink": sink_name,
                "total_failed": len(records),
                "retried_success": retried,
                "still_failed": still_failed,
            }

    def _require_aggregate_owner(self, task_name: str, team: str, action: str) -> None:
        for task in store.rows("aggregate_sink"):
            if task.get("任务名称") == task_name:
                require_owner(task, team, action)
                return

    # ---- 任务完成闸口 ----

    def complete_sink_task(self, task_name: str, *, team: str | None = None) -> dict[str, Any]:
        """任务完成闸口：检查点缺记录或仍有失败记录时拒绝完成。"""
        viewer = current_team(explicit=team)
        self._require_aggregate_owner(task_name, viewer, "完成任务")
        replayable = self._aggregate.list_replayable_checkpoints(task_name, team=viewer)
        checkpoint_id = replayable.get("checkpoint_id")
        if not checkpoint_id:
            return {
                "ok": False,
                "task": task_name,
                "blocked": True,
                "message": replayable.get("error") or "任务尚未生成检查点，不能完成",
                "missing_partitions": replayable.get("missing_partitions", []),
            }
        verification = self.read_checkpoint_outputs(checkpoint_id, team=viewer)
        if verification.get("blocked"):
            return {
                "ok": False,
                "task": task_name,
                "blocked": True,
                "message": verification["message"],
                "checkpoint_id": checkpoint_id,
                "missing_partitions": verification.get("missing_partitions", []),
                "failed_partitions": verification.get("failed_partitions", []),
                "failed_records": verification.get("failed_records", []),
            }
        for task in store.rows("aggregate_sink"):
            if task.get("任务名称") == task_name:
                task["status"] = "已完成"
                task["pending"] = False
                task["abnormal"] = False
                break
        return {
            "ok": True,
            "task": task_name,
            "blocked": False,
            "checkpoint_id": checkpoint_id,
            "replay_version": replayable.get("replay_version"),
            "message": "检查点覆盖的成功分区在输出表中均可核对，任务完成",
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if isinstance(r.get("id"), int) and r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            records_count = sum(
                1 for r in store.rows(self.MODULE)
                if r.get("sink") == name and r.get("write_status") == "success"
            )
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": records_count,
                "shared_mode": c.get("共享模式", ""),
                "owner": c.get("所属团队", ""),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_records"] = sum(1 for r in rows if r.get("write_status"))
        base["failed_records"] = sum(1 for r in rows if r.get("write_status") == "failed")
        base["blocked_tasks"] = sum(
            1 for r in store.rows("aggregate_sink")
            if r.get("last_completeness") == "incomplete"
        )
        return base
