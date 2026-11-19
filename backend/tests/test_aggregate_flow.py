"""序号35：聚合落库 → 结果落地 数据流测试。

覆盖：
  - 逐条写入后判定写入状态，失败记录留在输出表；
  - 检查点只声明成功写入的分区，不再记录"计划写入数量"；
  - 失败记录进入输出完整性报告，缺记录时阻止任务完成；
  - 重试跨越已创建的检查点时更新同一检查点，不新建可回放版本；
  - 重复提交只生效一次；
  - 团队可见性：35号记录仅所属团队可见，共享聚合数据受控只读，越权拒绝。
"""
from __future__ import annotations

import json

import pytest

from app.access import PermissionDenied
from app.routers import flows, resource
from app.schemas import ActionPayload, EntryPayload
from app.services import service_for
from app.store import store

TEAM = "数据工程组"
OTHER = "交易风控组"


class FakeRequest:
    """只实现路由用到的 request.headers / request.query_params。"""

    def __init__(self, team: str | None = None, **query: str) -> None:
        self.headers = {"X-Operator-Team": team} if team else {}
        self.query_params = dict(query)


def _records(*specs: tuple[str, bool, bool]) -> list[dict]:
    """(分区, 是否失败, 重试时是否可修复)。"""
    return [
        {
            "key": f"r{i}",
            "value": i,
            "partition": partition,
            "force_fail": failed,
            "retry_ok": retry_ok,
        }
        for i, (partition, failed, retry_ok) in enumerate(specs)
    ]


@pytest.fixture()
def aggregate():
    return service_for("aggregate_sink")


@pytest.fixture()
def sink():
    return service_for("sink_output")


def _denial_payload(response) -> dict:
    """FastAPI 路由直接返回 JSONResponse 时取出状态码和响应体。"""
    assert response.status_code == 403
    return json.loads(bytes(response.body).decode("utf-8"))


# ---------------------------------------------------------------------------
# 逐条写入与检查点覆盖
# ---------------------------------------------------------------------------

class TestCheckpointCoverage:
    def test_failed_records_stay_in_output_table(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        assert result["ok"] is False
        assert result["written"] == 1
        assert result["failed"] == 1
        rows = [
            r for r in store.rows("sink_output")
            if r.get("sink") == "agg_agg_order_daily"
            and r.get("batch_id") == result["batch_id"]
        ]
        assert len(rows) == 2, "失败记录必须留在输出表里，不能只写成功记录"
        assert {r["write_status"] for r in rows} == {"success", "failed"}

    def test_checkpoint_only_covers_successful_partitions(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        checkpoint = next(
            c for c in store.rows("checkpoint_mgr")
            if c.get("checkpoint_id") == result["checkpoint_id"]
        )
        assert checkpoint["success_partitions"] == ["dt=P1"]
        assert "dt=P2" not in checkpoint["success_partitions"]
        assert checkpoint["operator_states"]["written"] == 1
        # 计划写入数量单独记录，不再冒充检查点覆盖量
        assert checkpoint["planned_at_last_batch"] == 2

    def test_partition_with_any_failure_not_declared_successful(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P1", True, True)),
            partition="dt=P1",
        )
        assert result["success_partitions"] == []
        assert result["failed_partitions"] == ["dt=P1"]

    def test_task_not_marked_running_when_write_failed(self, aggregate):
        aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", True, True)),
            partition="dt=P1",
        )
        task = next(
            t for t in store.rows("aggregate_sink")
            if t.get("任务名称") == "agg_order_daily"
        )
        assert task["status"] == "写入失败"
        assert task["last_completeness"] == "incomplete"

    def test_all_success_marks_task_running(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P1", False, True)),
            partition="dt=P1",
        )
        assert result["ok"] is True
        task = next(
            t for t in store.rows("aggregate_sink")
            if t.get("任务名称") == "agg_order_daily"
        )
        assert task["status"] == "运行中"

    def test_integrity_report_lists_failed_records(self, aggregate):
        aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        report = aggregate.output_integrity_report("agg_order_daily", team=TEAM)
        item = report["reports"][0]
        assert item["blocked"] is True
        assert item["success_partitions"] == ["dt=P1"]
        assert item["failed_partitions"] == ["dt=P2"]
        assert item["failed_records"], "失败记录必须进入输出完整性报告"
        assert "agg_order_daily" in report["blocked_tasks"]


# ---------------------------------------------------------------------------
# 结果落地读取与完成闸口
# ---------------------------------------------------------------------------

class TestSinkConsumption:
    def test_reads_checkpoint_and_detects_missing_records(self, aggregate, sink):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", False, True)),
            partition="dt=P1",
        )
        checkpoint_id = result["checkpoint_id"]
        # 人为制造"检查点声明了分区、输出表里却缺记录"的场景
        for row in store.rows("sink_output"):
            if row.get("partition") == "dt=P1" and row.get("sink") == "agg_agg_order_daily":
                row["write_status"] = "failed"

        view = sink.read_checkpoint_outputs(checkpoint_id, team=TEAM)
        assert view["blocked"] is True
        assert view["missing_partitions"] == ["dt=P1"]

    def test_completion_blocked_while_records_missing(self, aggregate, sink):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        completion = sink.complete_sink_task("agg_order_daily", team=TEAM)
        assert completion["ok"] is False
        assert completion["blocked"] is True
        assert completion["checkpoint_id"] == result["checkpoint_id"]

    def test_completion_allowed_after_retry(self, aggregate, sink):
        aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        aggregate.retry_failed_writes("agg_order_daily", team=TEAM)
        completion = sink.complete_sink_task("agg_order_daily", team=TEAM)
        assert completion["ok"] is True
        assert completion["blocked"] is False

    def test_shared_sink_detail_readable_by_other_team(self):
        entry = resource.get_entry(
            "sink_output", 2, FakeRequest(OTHER))
        assert entry is not None
        assert entry["共享模式"] == "受控只读"

    def test_shared_sink_write_rejected_and_owner_kept(self):
        before = dict(store.find("sink_output", 2))
        response = resource.run_action(
            "sink_output", 2, ActionPayload(action="停用输出"), FakeRequest(OTHER))
        payload = _denial_payload(response)
        assert payload["code"] == "shared_read_only"
        after = store.find("sink_output", 2)
        assert after["status"] == before["status"]
        assert after["所属团队"] == before["所属团队"] == TEAM

    def test_private_sink_hidden_from_other_team(self):
        response = resource.get_entry("sink_output", 1, FakeRequest(OTHER))
        assert _denial_payload(response)["code"] == "record_scope_denied"


# ---------------------------------------------------------------------------
# 检查点版本与重试
# ---------------------------------------------------------------------------

class TestCheckpointVersioning:
    def test_retry_updates_same_checkpoint(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        checkpoint_id = result["checkpoint_id"]
        retry = aggregate.retry_failed_writes("agg_order_daily", team=TEAM)
        assert retry["checkpoint_id"] == checkpoint_id
        assert retry["replay_version"] == 2

        checkpoints = [
            c for c in store.rows("checkpoint_mgr")
            if c.get("任务名称") == "agg_order_daily"
        ]
        assert len(checkpoints) == 1, "重试不能新建另一个可回放版本"
        assert checkpoints[0]["repair_log"], "修复记录必须留在原检查点上"
        assert set(checkpoints[0]["success_partitions"]) == {"dt=P1", "dt=P2"}

    def test_rebuild_keeps_single_replayable_checkpoint(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True), ("dt=P2", True, True)),
            partition="dt=P1",
        )
        retry = aggregate.retry_failed_writes(
            "agg_order_daily", strategy="rebuild", team=TEAM)
        assert retry["checkpoint_id"] == result["checkpoint_id"]
        checkpoints = [
            c for c in store.rows("checkpoint_mgr")
            if c.get("任务名称") == "agg_order_daily"
        ]
        assert len(checkpoints) == 1
        assert checkpoints[0]["rebuild_count"] == 1
        assert retry["ok"] is True

    def test_retry_still_failing_keeps_incomplete(self, aggregate):
        aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P2", True, False)),
            partition="dt=P2",
        )
        retry = aggregate.retry_failed_writes("agg_order_daily", team=TEAM)
        assert retry["ok"] is False
        assert retry["still_failed"] == 1
        report = aggregate.output_integrity_report("agg_order_daily", team=TEAM)
        assert report["reports"][0]["blocked"] is True

    def test_duplicate_submission_writes_once(self, aggregate):
        records = _records(("dt=P1", False, True), ("dt=P1", False, True))
        first = aggregate.batch_flush_to_target(
            "agg_order_daily", records, partition="dt=P1", submission_key="sub-1")
        second = aggregate.batch_flush_to_target(
            "agg_order_daily", records, partition="dt=P1", submission_key="sub-1")
        assert second["duplicate_submission"] is True
        assert second["batch_id"] == first["batch_id"]
        assert second["checkpoint_id"] == first["checkpoint_id"]
        rows = [
            r for r in store.rows("sink_output")
            if r.get("sink") == "agg_agg_order_daily"
            and r.get("batch_id") == first["batch_id"]
        ]
        assert len(rows) == 2, "重复提交不能重复落库"


# ---------------------------------------------------------------------------
# 团队权限
# ---------------------------------------------------------------------------

class TestTeamScope:
    def test_other_team_cannot_read_aggregate_record(self):
        response = resource.get_entry("aggregate_sink", 3, FakeRequest(TEAM))
        assert _denial_payload(response)["code"] == "record_scope_denied"

    def test_aggregate_list_scoped_by_team(self):
        own, _ = service_for("aggregate_sink").list_entries(team=TEAM, size=50)
        other, _ = service_for("aggregate_sink").list_entries(team=OTHER, size=50)
        own_names = {row["任务名称"] for row in own}
        other_names = {row["任务名称"] for row in other}
        assert "agg_risk_minute" not in own_names, "团队私有记录不能跨团队可见"
        assert "agg_risk_minute" in other_names
        assert "agg_region_hourly" not in other_names, "团队私有记录不能跨团队可见"
        # 共享的聚合落库记录可以出现在对方列表里，但只允许受控只读
        assert "agg_order_daily" in other_names
        shared_row = next(r for r in other if r["任务名称"] == "agg_order_daily")
        assert shared_row["共享模式"] == "受控只读"
        assert shared_row["所属团队"] == TEAM

    def test_cross_team_retry_rejected(self, aggregate):
        with pytest.raises(PermissionDenied):
            aggregate.retry_failed_writes("agg_order_daily", team=OTHER)

    def test_shared_checkpoint_viewable_but_not_writable_by_other_team(self, aggregate):
        result = aggregate.batch_flush_to_target(
            "agg_order_daily",
            _records(("dt=P1", False, True)),
            partition="dt=P1",
        )
        # 共享的聚合落库数据按检查点受控只读：其他团队可以核对覆盖情况
        view = aggregate.list_replayable_checkpoints("agg_order_daily", team=OTHER)
        assert view["checkpoint_id"] == result["checkpoint_id"]
        # 但仍不能重试或改动检查点
        with pytest.raises(PermissionDenied):
            aggregate.retry_failed_writes("agg_order_daily", team=OTHER)

    def test_private_task_checkpoint_hidden_from_other_team(self, aggregate):
        aggregate.batch_flush_to_target(
            "agg_region_hourly",
            _records(("dt=P1", False, True)),
            partition="dt=P1",
        )
        with pytest.raises(PermissionDenied):
            aggregate.list_replayable_checkpoints("agg_region_hourly", team=OTHER)

    def test_access_overview_reports_two_scopes(self):
        payload = flows.access_overview(FakeRequest(OTHER))
        assert set(payload["scopes"]) == {"team_private", "shared_read_only"}
        shared = [r for r in payload["records"] if r["scope"] == "shared_read_only"]
        assert shared, "受控只读范围需要在权限页面可见"
        assert any(r["read_only"] for r in shared if r["owner"] != OTHER)

    def test_duplicate_create_submission_registers_once(self):
        service = service_for("window_config")
        values = {
            "策略名称": "窗口-幂等",
            "窗口类型": "滚动",
            "窗口大小": "60",
        }
        payload = EntryPayload(values=values, submission_key="entry-sub-1")
        first = resource.create_entry("window_config", payload, FakeRequest(TEAM))
        second = resource.create_entry("window_config", payload, FakeRequest(TEAM))
        assert first.ok and second.ok
        assert second.code == "duplicate_submission"
        rows = [r for r in store.rows("window_config") if r.get("策略名称") == "窗口-幂等"]
        assert len(rows) == 1
        assert service.get_entry(rows[0]["id"]) is not None
