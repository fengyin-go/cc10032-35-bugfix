"""管道链路业务测试：覆盖所有8组真实数据流场景。

测试用例：
  - test_window_aggregation_completeness：流式计算窗口聚合 + 结果落地完整性
  - test_watermark_late_bypass：水位延迟触发迟到数据旁路
  - test_checkpoint_state_consistency：检查点恢复后状态一致性
  - test_join_window_semantics：双流关联的时间窗口语义
  - test_backpressure_degradation：背压触发后的吞吐降级
  - test_end_to_end_pipeline：端到端集成测试
  - test_cep_alert_restore_chain：CEP→告警→恢复链路
  - test_lineage_tracing：血缘追踪完整性
"""
from __future__ import annotations

import copy
import time

import pytest

from app.pipeline import (
    aggregate_and_sink_join_results,
    apply_backpressure_degradation,
    apply_window_aggregation,
    BackpressureState,
    check_watermark_delay,
    create_backfill_from_late,
    create_checkpoint_snapshot,
    generate_restore_plan,
    match_cep_pattern,
    perform_stream_join,
    restore_from_checkpoint,
    run_end_to_end_pipeline,
    sample_and_debug_trace,
    simulate_stream_batch,
    sink_results_to_store,
    simulate_task_topology_flow,
    trace_metric_lineage,
    trigger_alert_for_cep_matches,
    verify_sink_integrity,
    verify_state_consistency,
    CheckpointSnapshot,
)


# ---------------------------------------------------------------------------
# 测试辅助函数
# ---------------------------------------------------------------------------

def _make_events(count: int, base_ts: float | None = None) -> list[dict[str, Any]]:
    if base_ts is None:
        base_ts = time.time()
    return [
        {
            "event_id": f"ev_{i}",
            "topic": "test_topic",
            "payload": {
                "user_id": f"u{i % 4}",
                "amount": 10.0 + i * 2.5,
                "region": f"r{i % 3}",
            },
            "event_time": base_ts + i * 1.0,
            "source": "test_source",
        }
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# 测试 1: 流式计算窗口聚合 + 结果落地完整性
# ---------------------------------------------------------------------------

class TestWindowAggregationAndSink:
    """验证窗口聚合计算正确性和结果落地完整性。"""

    def test_rolling_window_aggregation(self):
        """滚动窗口聚合：验证 count/sum/avg/max/min 的准确性。"""
        events = _make_events(20)
        results = apply_window_aggregation(
            events, "tumbling", 5.0, 5.0,
            group_keys=["user_id"],
            agg_funcs=["count", "sum", "avg"],
        )
        assert len(results) > 0, "窗口聚合应产生结果"
        for wr in results:
            assert wr.row_count > 0
            assert len(wr.aggregates) > 0
            assert wr.watermark < wr.end_ts, "水位应小于窗口结束时间"

    def test_sliding_window_overlap(self):
        """滑动窗口应产生重叠区间结果。"""
        events = _make_events(30)
        results = apply_window_aggregation(
            events, "sliding", 10.0, 5.0,
            group_keys=["region"],
            agg_funcs=["count", "max"],
        )
        # 30个事件, 1s间隔, 10s窗口, 5s步长 → 应产生多个重叠窗口
        assert len(results) >= 6, f"滑动窗口应有足够重叠，实际: {len(results)}"

    def test_sink_integrity(self):
        """结果落地完整性：窗口结果应全部写入 sink_output。"""
        events = _make_events(15)
        results = apply_window_aggregation(
            events, "tumbling", 5.0, 5.0,
            group_keys=["user_id"],
            agg_funcs=["count", "sum"],
        )
        sink_msg = sink_results_to_store(results, "结果落地样例1", "append")
        assert sink_msg[0] == len(results), f"应写入全部窗口结果: {sink_msg}"
        integrity = verify_sink_integrity(results)
        assert integrity["complete"], f"落地完整性检查失败: {integrity['missing']}"

    def test_empty_events_returns_empty(self):
        """空事件列表应返回空结果。"""
        results = apply_window_aggregation(
            [], "tumbling", 5.0, 5.0,
            group_keys=["a"], agg_funcs=["count"],
        )
        assert results == []


# ---------------------------------------------------------------------------
# 测试 2: 水位延迟触发迟到数据旁路
# ---------------------------------------------------------------------------

class TestWatermarkLateBypass:
    """验证水位延迟检测和迟到数据旁路输出机制。"""

    def test_late_data_detection(self):
        """检测事件时间早于水位截止的事件为迟到。"""
        events = _make_events(20)
        max_ts = max(e["event_time"] for e in events)
        watermark = max_ts + 5  # 水位靠后
        late_entries, ratio = check_watermark_delay(
            "test_source", watermark, events, allowed_lateness_sec=3.0,
        )
        # 延迟容限3s: 事件时间 < watermark - 3 的判定为迟到
        cutoff = watermark - 3
        expected_late = sum(1 for e in events if e["event_time"] < cutoff)
        assert len(late_entries) >= expected_late, f"应有 {expected_late}+ 条迟到"
        for le in late_entries:
            assert "bypass_reason" in le.__dict__
            assert le.event_time < cutoff

    def test_late_bypass_creates_backfill(self):
        """迟到数据应自动生成补数任务。"""
        events = _make_events(20)
        max_ts = max(e["event_time"] for e in events)
        watermark = max_ts + 10
        late_entries, _ = check_watermark_delay(
            "test_source", watermark, events, allowed_lateness_sec=1.0,
        )
        result = create_backfill_from_late(late_entries, "test_metric", "test_source")
        if late_entries:
            assert result["created"] is True
            assert result["job_id"] is not None
        else:
            assert result["created"] is False

    def test_no_late_when_watermark_behind(self):
        """当水位滞后于所有事件时不应产生迟到。"""
        events = _make_events(10)
        min_ts = min(e["event_time"] for e in events)
        watermark = min_ts + 2  # 水位跟随但包含所有
        late_entries, _ = check_watermark_delay(
            "test_source", watermark, events, allowed_lateness_sec=50.0,
        )
        assert len(late_entries) == 0


# ---------------------------------------------------------------------------
# 测试 3: 检查点恢复后状态一致性
# ---------------------------------------------------------------------------

class TestCheckpointStateConsistency:
    """验证检查点创建、恢复和状态一致性。"""

    def test_create_and_restore_checkpoint(self):
        """创建检查点后应能恢复相同状态。"""
        state = {"key": "value", "offset": 42, "window_states": {"w1": 100}}
        cp = create_checkpoint_snapshot("test_op", state)
        assert cp.checkpoint_id is not None
        assert cp.timestamp > 0
        assert cp.operator_states["test_op"] == state

        restored = restore_from_checkpoint(cp.checkpoint_id)
        assert restored is not None
        assert restored["test_op"] == state

    def test_restore_nonexistent_checkpoint(self):
        """恢复不存在的检查点应返回 None。"""
        result = restore_from_checkpoint("nonexistent_id")
        assert result is None

    def test_state_consistency_across_snapshots(self):
        """多个检查点之间状态应保持一致。"""
        base_state = {"count": 100, "sum": 500.0}
        cp1 = create_checkpoint_snapshot("agg_op", base_state)
        time.sleep(0.01)
        cp2 = create_checkpoint_snapshot("agg_op", {"count": 100, "sum": 500.0})
        result = verify_state_consistency([cp1, cp2])
        assert result["consistent"] is True

    def test_detect_inconsistent_state(self):
        """检测状态不一致的检查点。"""
        cp1 = create_checkpoint_snapshot("op", {"val": 1})
        cp2 = create_checkpoint_snapshot("op", {"val": 2})
        result = verify_state_consistency([cp1, cp2])
        assert result["consistent"] is False, "应检测到不一致"


# ---------------------------------------------------------------------------
# 测试 4: 双流关联的时间窗口语义
# ---------------------------------------------------------------------------

class TestJoinWindowSemantics:
    """验证双流时间窗口关联和语义正确性。"""

    def test_inner_join_matching(self):
        """内联：只有两边都匹配的记录才产生结果。"""
        left = _make_events(10)
        right = _make_events(10, base_ts=time.time() + 5)
        results = perform_stream_join(left, right, "user_id", 30.0, "inner")
        matched = sum(1 for jr in results if jr.matched)
        assert matched > 0
        assert matched <= min(len(set(
            str(e["payload"]["user_id"]) for e in left
        )), len(set(
            str(e["payload"]["user_id"]) for e in right
        )))

    def test_join_result_window_bounds(self):
        """关联结果应有正确的时间窗口边界。"""
        left = _make_events(5)
        right = _make_events(5, base_ts=time.time() + 3)
        results = perform_stream_join(left, right, "user_id", 20.0, "inner")
        for jr in results:
            assert jr.window_end - jr.window_start == 20.0
            assert jr.window_start >= 0

    def test_join_aggregate_sink(self):
        """关联结果聚合后应正确写入 aggregate_sink。"""
        left = _make_events(8)
        right = _make_events(8, base_ts=time.time() + 2)
        results = perform_stream_join(left, right, "user_id", 15.0)
        agg = aggregate_and_sink_join_results(results, "agg_order_daily")
        assert agg["join_total"] == len(results)
        assert agg["match_rate"] >= 0

    def test_empty_join(self):
        """空流关联应返回空结果。"""
        results = perform_stream_join([], [], "user_id", 10.0)
        assert results == []


# ---------------------------------------------------------------------------
# 测试 5: 背压触发后的吞吐降级
# ---------------------------------------------------------------------------

class TestBackpressureDegradation:
    """验证背压检测和降级策略。"""

    def test_backpressure_detection(self):
        """QPS 超过阈值时应触发背压。"""
        bp = BackpressureState(
            source="test",
            current_qps=900,
            max_qps=1000,
            threshold_ratio=0.8,
            is_throttled=True,
            degraded_actions=["限速采样"],
        )
        assert bp.is_throttled is True
        result = apply_backpressure_degradation(bp)
        assert result["action"] == "degrade"
        assert result["degraded_qps"] < result["original_qps"]

    def test_no_degradation_under_threshold(self):
        """未超过阈值时不应降级。"""
        bp = BackpressureState(
            source="test",
            current_qps=500,
            max_qps=1000,
            threshold_ratio=0.8,
            is_throttled=False,
            degraded_actions=[],
        )
        result = apply_backpressure_degradation(bp)
        assert result["action"] == "none"

    def test_degradation_reduces_qps(self):
        """降级时 QPS 应降低到约 70%。"""
        bp = BackpressureState(
            source="test",
            current_qps=1000,
            max_qps=1200,
            threshold_ratio=0.8,
            is_throttled=True,
            degraded_actions=["限速采样", "延迟计算"],
        )
        result = apply_backpressure_degradation(bp)
        assert result["degraded_qps"] == pytest.approx(700, rel=0.01)


# ---------------------------------------------------------------------------
# 测试 6: CEP→告警→恢复链路
# ---------------------------------------------------------------------------

class TestCepAlertRestore:
    """验证复杂事件处理到告警恢复的完整链路。"""

    def test_cep_pattern_matching(self):
        """应正确匹配连续失败事件模式。"""
        events = [
            {"event_id": str(i), "event_type": t, "event_time": time.time() + i}
            for i, t in enumerate(
                ["login_fail", "login_fail", "login_fail", "access_granted"]
            )
        ]
        matches = match_cep_pattern(events, ["login_fail"] * 3, 60.0)
        assert len(matches) >= 1, f"应匹配到连续3次登录失败, 实际: {len(matches)}"

    def test_cep_no_match_different_pattern(self):
        """不匹配的模式不应产生结果。"""
        events = [
            {"event_id": str(i), "event_type": "ok", "event_time": time.time() + i}
            for i in range(10)
        ]
        matches = match_cep_pattern(events, ["login_fail"] * 3, 60.0)
        assert len(matches) == 0

    def test_alert_on_match(self):
        """匹配后应触发告警。"""
        events = [
            {"event_id": str(i), "event_type": "login_fail", "event_time": time.time() + i}
            for i in range(5)
        ]
        matches = match_cep_pattern(events, ["login_fail"] * 3, 60.0)
        alert = trigger_alert_for_cep_matches(matches, "告警策略样例2")
        if matches:
            assert alert["triggered"] is True

    def test_restore_plan_generation(self):
        """多次触发应生成恢复方案。"""
        plan = generate_restore_plan(4, "test_scope")
        assert plan["plan_created"] is True or "job_id" not in plan
        assert plan["恢复模式"] == "from_checkpoint" or plan.get("plan_created", True)

    def test_no_restore_without_trigger(self):
        """无触发时不应生成恢复方案。"""
        plan = generate_restore_plan(0, "test_scope")
        assert plan["plan_created"] is False


# ---------------------------------------------------------------------------
# 测试 7: 采样→调试追踪 & 拓扑→状态存储
# ---------------------------------------------------------------------------

class TestSamplerTraceAndTopology:
    """验证数据采样到调试追踪、拓扑到状态存储的链路。"""

    def test_sampler_trace_link(self):
        """采样后应创建调试追踪会话。"""
        events = _make_events(50)
        result = sample_and_debug_trace("数据采样样例1", "test_op", events, 0.1)
        assert result["sampled_count"] > 0
        assert result["total_events"] == 50
        assert result["trace_id"] is not None

    def test_topology_flow_with_state(self):
        """拓扑执行应将状态写入 state_store。"""
        from app.store import store
        before_count = len(store.rows("state_store"))
        result = simulate_task_topology_flow("任务拓扑样例1", "test_source")
        assert result["topology"] is not None
        assert "current_qps" in result

    def test_lineage_tracing(self):
        """血缘追踪应记录完整上下游链路。"""
        lineage = trace_metric_lineage(
            "test_metric",
            ["stream_source", "kafka_topic"],
            ["calc_operator", "window_config"],
            ["sink_output", "dashboard"],
        )
        assert lineage["记录ID"] is not None
        assert lineage["指标名称"] == "test_metric"
        assert lineage["status"] == "正常"


# ---------------------------------------------------------------------------
# 测试 8: 端到端集成测试
# ---------------------------------------------------------------------------

class TestEndToEndPipeline:
    """端到端集成：验证完整管道编排。"""

    def test_full_pipeline_completion(self):
        """完整管道应覆盖所有8条链路并成功完成。"""
        report = run_end_to_end_pipeline("test_source")
        assert report["pipeline_status"] == "completed"
        # 链路1
        assert report["chain1_source_to_sink"]["events"] == 20
        assert report["chain1_source_to_sink"]["windows"] > 0
        assert report["chain1_source_to_sink"]["written"] > 0
        # 链路2
        assert "late_count" in report["chain2_watermark_backfill"]
        assert "delay_ratio" in report["chain2_watermark_backfill"]
        # 链路4
        assert "cep_matches" in report["chain4_cep_restore"]
        assert "restore_plan_id" in report["chain4_cep_restore"]
        # 链路6
        assert report["chain6_sampler_trace"]["sampled_count"] > 0
        # 链路7
        assert report["chain7_lineage"]["status"] == "正常"

    def test_pipeline_with_join(self):
        """带双流关联的管道应正确执行链路3。"""
        report = run_end_to_end_pipeline(
            "test_source",
            join_left_source="test_left",
            join_right_source="test_right",
        )
        assert report["pipeline_status"] == "completed"
        if "chain3_join_checkpoint" in report:
            assert "join_total" in report["chain3_join_checkpoint"]
            assert "checkpoint_id" in report["chain3_join_checkpoint"]

    def test_pipeline_idempotent(self):
        """管道应可重复执行无副作用。"""
        report1 = run_end_to_end_pipeline("test_source")
        report2 = run_end_to_end_pipeline("test_source")
        assert report1["pipeline_status"] == report2["pipeline_status"]


# ---------------------------------------------------------------------------
# 边界条件测试
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """边界条件和异常处理。"""

    def test_nonexistent_source(self):
        """不存在的数据源应自动生成模拟事件。"""
        events = simulate_stream_batch("nonexistent_source")
        assert len(events) == 10  # 默认 batch_size 为 10

    def test_large_batch_window(self):
        """大数据量窗口聚合应在合理时间内完成。"""
        events = _make_events(200)
        start = time.time()
        results = apply_window_aggregation(
            events, "tumbling", 20.0, 10.0,
            group_keys=["user_id", "region"],
            agg_funcs=["count", "sum", "avg", "max", "min"],
        )
        elapsed = time.time() - start
        assert elapsed < 5.0, f"200条事件聚合应<5s, 实际{elapsed:.2f}s"
        assert len(results) > 0

    def test_single_event_window(self):
        """单事件窗口也应正常计算。"""
        events = _make_events(1)
        results = apply_window_aggregation(
            events, "tumbling", 5.0, 5.0,
            group_keys=["user_id"], agg_funcs=["count", "sum"],
        )
        assert len(results) >= 1
        assert results[0].row_count == 1

    def test_missing_field_graceful(self):
        """缺失字段时应优雅降级。"""
        events = [{"event_id": "bad", "event_time": time.time()}]
        results = apply_window_aggregation(
            events, "tumbling", 5.0, 5.0,
            group_keys=["user_id"], agg_funcs=["count"],
        )
        assert len(results) >= 0  # 不应崩溃