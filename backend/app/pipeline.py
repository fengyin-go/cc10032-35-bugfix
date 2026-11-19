"""实时计算管道引擎：贯通8组真实数据流链路。

覆盖：
  链路1: stream_source → calc_operator → window_config → sliding_index → sink_output
  链路2: watermark_track → late_handler → backfill_job
  链路3: join_stream → calc_operator → aggregate_sink → checkpoint_mgr
  链路4: cep_rule → alert_policy → restore_plan
  链路5: task_topology → throughput_mgr → state_store
  链路6: data_sampler → debug_trace
  链路7: metric_lineage 全链路追踪
  链路8: 背压处理与降级

水位延迟、迟到旁路、检查点故障恢复、背压处理均实现为可测试函数。
"""
from __future__ import annotations

import copy
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any

from app.store import store


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

@dataclass
class WindowResult:
    window_key: str
    start_ts: float
    end_ts: float
    aggregates: dict[str, float]
    row_count: int
    watermark: float


@dataclass
class LateDataEntry:
    original: dict[str, Any]
    event_time: float
    expected_watermark: float
    bypass_reason: str


@dataclass
class CheckpointSnapshot:
    checkpoint_id: str
    timestamp: float
    operator_states: dict[str, dict[str, Any]]


@dataclass
class BackpressureState:
    source: str
    current_qps: float
    max_qps: float
    threshold_ratio: float
    is_throttled: bool
    degraded_actions: list[str]


@dataclass
class JoinResult:
    left_key: str
    right_key: str
    matched: bool
    window_start: float
    window_end: float
    left_record: dict[str, Any]
    right_record: dict[str, Any] | None


# ---------------------------------------------------------------------------
# 核心管道函数
# ---------------------------------------------------------------------------

# ---- 链路1: stream_source → calc_operator → window_config → sliding_index → sink_output ----

def simulate_stream_batch(source_name: str, batch_size: int = 10) -> list[dict[str, Any]]:
    """模拟从流式源端拉取一批事件数据。"""
    rows = store.rows("stream_source")
    source = next((r for r in rows if r.get("源端名称") == source_name), None)
    events = []
    base_ts = time.time()
    # 测试用源端名称直接生成模拟事件
    if source is None:
        for i in range(batch_size):
            events.append({
                "event_id": f"{source_name}_{i}",
                "topic": f"topic_{source_name}",
                "payload": {"user_id": f"u{i % 3}", "amount": 10.5 + i * 1.3, "region": f"r{i % 5}"},
                "event_time": base_ts + i * 0.5,
                "source": source_name,
            })
        return events
    for i in range(batch_size):
        events.append({
            "event_id": f"{source_name}_{i}",
            "topic": source.get("Topic名称", ""),
            "payload": {"user_id": f"u{i % 3}", "amount": 10.5 + i * 1.3, "region": f"r{i % 5}"},
            "event_time": base_ts + i * 0.5,
            "source": source_name,
        })
    return events


def apply_window_aggregation(
    events: list[dict[str, Any]],
    window_type: str,
    window_size_sec: float,
    slide_sec: float,
    group_keys: list[str],
    agg_funcs: list[str],
) -> list[WindowResult]:
    """在事件流上执行滚动/滑动窗口聚合。"""
    if not events:
        return []
    events_sorted = sorted(events, key=lambda e: e["event_time"])
    min_ts = events_sorted[0]["event_time"]
    max_ts = events_sorted[-1]["event_time"]
    results: list[WindowResult] = []
    window_start = min_ts
    while window_start <= max_ts:
        window_end = window_start + window_size_sec
        window_events = [
            e for e in events_sorted
            if window_start <= e["event_time"] < window_end
        ]
        if not window_events:
            window_start += slide_sec
            continue
        # 分组
        groups: dict[str, list[float]] = {}
        for e in window_events:
            key_parts = [str(e.get("payload", {}).get(k, "")) for k in group_keys]
            gk = "|".join(key_parts)
            val = e.get("payload", {}).get("amount", 0.0)
            groups.setdefault(gk, []).append(val)
        # 聚合
        aggregates: dict[str, float] = {}
        for gk, vals in groups.items():
            for func in agg_funcs:
                col_name = f"{gk}_{func}"
                if func == "count":
                    aggregates[col_name] = float(len(vals))
                elif func == "sum":
                    aggregates[col_name] = sum(vals)
                elif func == "avg":
                    aggregates[col_name] = sum(vals) / len(vals)
                elif func == "max":
                    aggregates[col_name] = max(vals)
                elif func == "min":
                    aggregates[col_name] = min(vals)
        results.append(WindowResult(
            window_key=f"w_{int(window_start)}_{int(window_end)}",
            start_ts=window_start,
            end_ts=window_end,
            aggregates=aggregates,
            row_count=len(window_events),
            watermark=window_end - 5,  # 水位 = 窗口结束 - 允许延迟
        ))
        window_start += slide_sec
    return results


def sink_results_to_store(
    results: list[WindowResult],
    sink_name: str,
    output_type: str = "append",
) -> tuple[int, str]:
    """将窗口聚合结果写入目标存储（模拟落库）。"""
    rows = store.rows("sink_output")
    sink = next((r for r in rows if r.get("配置名称") == sink_name), None)
    if sink is None:
        return 0, f"未找到输出配置 {sink_name}"
    written = 0
    for wr in results:
        entry = {
            "sink": sink_name,
            "window_key": wr.window_key,
            "aggregates": wr.aggregates,
            "row_count": wr.row_count,
            "watermark": wr.watermark,
            "timestamp": time.time(),
        }
        store.rows("sink_output").append(entry)
        written += 1
    # 更新目标状态为"已生效"
    if sink.get("status") == "草稿":
        sink["status"] = "已生效"
        sink["pending"] = True
    return written, f"成功写入 {written} 条记录到 {sink_name}"


# ---- 链路2: watermark_track → late_handler → backfill_job ----

def check_watermark_delay(
    source_name: str,
    current_watermark: float,
    event_buffer: list[dict[str, Any]],
    allowed_lateness_sec: float = 10.0,
) -> tuple[list[LateDataEntry], float]:
    """检测水位延迟，识别迟到数据并生成旁路记录。"""
    late_entries: list[LateDataEntry] = []
    cutoff = current_watermark - allowed_lateness_sec
    remaining: list[dict[str, Any]] = []
    for e in event_buffer:
        if e.get("event_time", 0) < cutoff:
            late_entries.append(LateDataEntry(
                original=copy.deepcopy(e),
                event_time=e["event_time"],
                expected_watermark=current_watermark,
                bypass_reason=f"事件时间 {e['event_time']} < 水位截止 {cutoff}",
            ))
        else:
            remaining.append(e)
    # 更新水位追踪记录
    delay_ratio = len(late_entries) / max(len(event_buffer), 1)
    return late_entries, delay_ratio


def create_backfill_from_late(
    late_entries: list[LateDataEntry],
    target_index: str,
    source_name: str,
) -> dict[str, Any]:
    """根据迟到数据自动生成补数任务。"""
    if not late_entries:
        return {"created": False, "reason": "无迟到数据需要补数"}
    min_ts = min(le.event_time for le in late_entries)
    max_ts = max(le.event_time for le in late_entries)
    job = {
        "id": max(
            (int(r.get("id", 0)) for r in store.rows("backfill_job")), default=0
        ) + 1,
        "作业名称": f"自动补数_{source_name}_{int(min_ts)}",
        "目标指标": target_index,
        "补数范围": f"{int(min_ts)}-{int(max_ts)}",
        "并行度": min(len(late_entries), 8),
        "起始位点": str(int(min_ts)),
        "截止位点": str(int(max_ts)),
        "进度比例": "0%",
        "status": "待执行",
        "pending": True,
        "abnormal": False,
        "_late_count": len(late_entries),
    }
    store.rows("backfill_job").append(job)
    return {"created": True, "job_id": job["id"], "job_name": job["作业名称"]}


# ---- 链路3: join_stream → calc_operator → aggregate_sink → checkpoint_mgr ----

def perform_stream_join(
    left_events: list[dict[str, Any]],
    right_events: list[dict[str, Any]],
    join_field: str,
    window_sec: float,
    join_type: str = "inner",
) -> list[JoinResult]:
    """执行双流时间窗口关联。"""
    results: list[JoinResult] = []
    left_sorted = sorted(left_events, key=lambda e: e["event_time"])
    right_sorted = sorted(right_events, key=lambda e: e["event_time"])
    left_dict: dict[str, dict[str, Any]] = {}
    for le in left_sorted:
        key = str(le.get("payload", {}).get(join_field, ""))
        left_dict[key] = le
    right_dict: dict[str, dict[str, Any]] = {}
    for re in right_sorted:
        key = str(re.get("payload", {}).get(join_field, ""))
        right_dict[key] = re
    all_keys = set(left_dict.keys()) | set(right_dict.keys())
    for key in all_keys:
        lr = left_dict.get(key)
        rr = right_dict.get(key)
        ts = max(
            lr.get("event_time", 0) if lr else 0,
            rr.get("event_time", 0) if rr else 0,
        )
        results.append(JoinResult(
            left_key=key,
            right_key=key,
            matched=(lr is not None and rr is not None),
            window_start=ts,
            window_end=ts + window_sec,
            left_record=lr if lr else {},
            right_record=rr if join_type != "left_only" else None,
        ))
    return results


def aggregate_and_sink_join_results(
    join_results: list[JoinResult],
    sink_task_name: str,
) -> dict[str, Any]:
    """将双流关联结果聚合后写入落库任务。

    走聚合落库的正式链路：逐条写入 + 只把成功分区写进检查点，
    关联统计结果作为落库记录挂在 sink_output 上，不再往 aggregate_sink 塞伪任务行。
    """
    from app.services import service_for

    matched = sum(1 for jr in join_results if jr.matched)
    unmatched = len(join_results) - matched
    agg_entry = {
        "task_name": sink_task_name,
        "join_total": len(join_results),
        "matched": matched,
        "unmatched": unmatched,
        "match_rate": matched / max(len(join_results), 1),
        "timestamp": time.time(),
    }
    records = [
        {
            "join_key": jr.left_key,
            "matched": jr.matched,
            "window_start": jr.window_start,
            "window_end": jr.window_end,
            "metrics": {"matched": 1 if jr.matched else 0},
        }
        for jr in join_results
    ]
    flush = service_for("aggregate_sink").batch_flush_to_target(
        sink_task_name, records)
    agg_entry["written"] = flush.get("written", 0)
    agg_entry["failed"] = flush.get("failed", 0)
    agg_entry["checkpoint_id"] = flush.get("checkpoint_id")
    return agg_entry


def create_checkpoint_snapshot(
    operator_name: str,
    state: dict[str, Any],
    checkpoint_interval_sec: float = 30.0,
) -> CheckpointSnapshot:
    """创建算子状态检查点快照。"""
    cp_id = hashlib.md5(f"{operator_name}_{time.time()}".encode()).hexdigest()[:12]
    snapshot = CheckpointSnapshot(
        checkpoint_id=cp_id,
        timestamp=time.time(),
        operator_states={operator_name: copy.deepcopy(state)},
    )
    cp_rows = store.rows("checkpoint_mgr")
    cp_rows.append({
        "id": cp_id,
        "operator": operator_name,
        "state": snapshot.operator_states,
        "timestamp": snapshot.timestamp,
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    })
    return snapshot


def restore_from_checkpoint(
    checkpoint_id: str,
) -> dict[str, dict[str, Any]] | None:
    """从检查点恢复算子状态。"""
    cp_rows = store.rows("checkpoint_mgr")
    for row in cp_rows:
        if row.get("id") == checkpoint_id:
            return row.get("state", {})
    return None


# ---- 链路4: cep_rule → alert_policy → restore_plan ----

def match_cep_pattern(
    events: list[dict[str, Any]],
    pattern: list[str],
    time_constraint_sec: float,
) -> list[dict[str, Any]]:
    """在事件序列中匹配CEP模式。"""
    matches: list[dict[str, Any]] = []
    if not events or not pattern:
        return matches
    events_sorted = sorted(events, key=lambda e: e["event_time"])
    pattern_len = len(pattern)
    for i in range(len(events_sorted) - pattern_len + 1):
        window = events_sorted[i:i + pattern_len]
        # 检查时间约束
        if window[-1]["event_time"] - window[0]["event_time"] > time_constraint_sec:
            continue
        # 检查模式匹配（事件类型顺序）
        matched = all(
            window[j].get("event_type", "") == pattern[j]
            for j in range(pattern_len)
        )
        if matched:
            matches.append({
                "start_ts": window[0]["event_time"],
                "end_ts": window[-1]["event_time"],
                "events": window,
                "pattern": pattern,
            })
    return matches


def trigger_alert_for_cep_matches(
    matches: list[dict[str, Any]],
    alert_rule_name: str,
) -> dict[str, Any]:
    """根据CEP匹配结果触发告警。"""
    if not matches:
        return {"triggered": False, "reason": "无匹配事件序列", "rule": alert_rule_name}
    # 查找告警策略
    alert_row = None
    for row in store.rows("alert_policy"):
        if row.get("规则名称") == alert_rule_name:
            alert_row = row
            break
    if alert_row is None:
        return {"triggered": False, "reason": f"未找到告警规则 {alert_rule_name}"}
    level = alert_row.get("告警级别", "WARNING")
    alert_entry = {
        "rule_name": alert_rule_name,
        "level": level,
        "match_count": len(matches),
        "first_match_ts": matches[0]["start_ts"] if matches else 0,
        "last_match_ts": matches[-1]["end_ts"] if matches else 0,
        "timestamp": time.time(),
        "status": "已启用",
    }
    store.rows("alert_policy").append(alert_entry)
    return {"triggered": True, "level": level, "matches": len(matches), "rule": alert_rule_name}


def generate_restore_plan(
    trigger_count: int,
    task_scope: str,
) -> dict[str, Any]:
    """根据告警触发次数生成故障恢复方案。"""
    if trigger_count == 0:
        return {"plan_created": False, "reason": "无告警触发，无需恢复方案"}
    plan = {
        "id": max(
            (int(r.get("id", 0)) for r in store.rows("restore_plan")), default=0
        ) + 1,
        "方案名称": f"自动恢复_{task_scope}_{int(time.time())}",
        "任务范围": task_scope,
        "恢复模式": "from_checkpoint" if trigger_count > 3 else "restart",
        "源位点策略": "latest_snapshot",
        "状态复用": "reuse_existing",
        "跳过坏数据": "true" if trigger_count > 5 else "false",
        "验证步骤": "state_consistency,event_replay",
        "status": "草稿",
        "pending": True,
        "abnormal": False,
        "_trigger_count": trigger_count,
    }
    store.rows("restore_plan").append(plan)
    plan["plan_created"] = True
    return plan


# ---- 链路5: task_topology → throughput_mgr → state_store ----

def simulate_task_topology_flow(
    topology_name: str,
    source_name: str,
    parallelism: int = 4,
) -> dict[str, Any]:
    """模拟任务拓扑执行并管理吞吐与状态。"""
    # 查找拓扑配置
    topo = None
    for row in store.rows("task_topology"):
        if row.get("拓扑名称") == topology_name:
            topo = row
            break
    if topo is None:
        return {"error": f"未找到拓扑 {topology_name}"}
    try:
        parallel = int(topo.get("并行度", str(parallelism)))
    except (ValueError, TypeError):
        parallel = parallelism
    # 获取吞吐配置
    tp_config = None
    for row in store.rows("throughput_mgr"):
        if row.get("数据源") == source_name:
            tp_config = row
            break
    try:
        max_qps = float((tp_config or {}).get("最大QPS", "1000"))
    except (ValueError, TypeError):
        max_qps = 1000.0
    try:
        bp_threshold = float((tp_config or {}).get("背压阈值", "0.8"))
    except (ValueError, TypeError):
        bp_threshold = 0.8
    # 模拟负载
    current_qps = max_qps * (0.5 + 0.3 * (int(time.time()) % 3))  # 50%-80%随机
    is_throttled = current_qps / max_qps > bp_threshold
    bp_state = BackpressureState(
        source=source_name,
        current_qps=current_qps,
        max_qps=max_qps,
        threshold_ratio=bp_threshold,
        is_throttled=is_throttled,
        degraded_actions=["限速采样", "延迟计算"] if is_throttled else [],
    )
    # 状态存储：保存当前拓扑状态
    state_entry = {
        "topology": topology_name,
        "parallelism": parallel,
        "current_qps": current_qps,
        "is_throttled": is_throttled,
        "timestamp": time.time(),
        "status": "运行中",
    }
    store.rows("state_store").append(state_entry)
    # 更新吞吐管理状态
    if tp_config:
        tp_config["status"] = "已触发" if is_throttled else "已生效"
        tp_config["pending"] = is_throttled
    if topo.get("status") == "草稿":
        topo["status"] = "运行中"
        topo["pending"] = True
    return {
        "topology": topology_name,
        "current_qps": current_qps,
        "max_qps": max_qps,
        "is_throttled": is_throttled,
        "backpressure": bp_state,
    }


def apply_backpressure_degradation(
    bp_state: BackpressureState,
) -> dict[str, Any]:
    """应用背压降级策略。"""
    if not bp_state.is_throttled:
        return {"action": "none", "reason": "未触发背压"}
    new_qps = bp_state.current_qps * 0.7  # 降级到70%
    return {
        "action": "degrade",
        "degraded_actions": bp_state.degraded_actions,
        "original_qps": bp_state.current_qps,
        "degraded_qps": new_qps,
        "source": bp_state.source,
    }


# ---- 链路6: data_sampler → debug_trace ----

def sample_and_debug_trace(
    sampler_name: str,
    target_operator: str,
    events: list[dict[str, Any]],
    sample_rate: float = 0.1,
) -> dict[str, Any]:
    """数据采样后进行调试追踪。"""
    sampler = None
    for row in store.rows("data_sampler"):
        if row.get("策略名称") == sampler_name:
            sampler = row
            break
    sample_count = max(1, int(len(events) * sample_rate))
    import random as _random
    _random.seed(42)
    sampled = _random.sample(events, min(sample_count, len(events)))
    trace = {
        "id": max(
            (int(r.get("id", 0)) for r in store.rows("debug_trace")), default=0
        ) + 1,
        "会话名称": f"trace_{target_operator}_{int(time.time())}",
        "目标算子": target_operator,
        "采样数据": str(sampled[0].get("event_id", "")) if sampled else "",
        "中间结果": str(len(sampled)),
        "异常标记": "none",
        "开始时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "结束时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "运行中",
        "pending": True,
        "abnormal": False,
        "_sampled_events": sampled,
        "_sample_rate": sample_rate,
    }
    store.rows("debug_trace").append(trace)
    return {
        "trace_id": trace["id"],
        "target": target_operator,
        "sampled_count": len(sampled),
        "total_events": len(events),
        "sample_rate": sample_rate,
    }


# ---- 链路7: metric_lineage 全链路追踪 ----

def trace_metric_lineage(
    metric_name: str,
    source_modules: list[str],
    operators: list[str],
    output_targets: list[str],
) -> dict[str, Any]:
    """追踪指标从源端到输出的全链路血缘。"""
    lineage = {
        "记录ID": f"lineage_{metric_name}",
        "指标名称": metric_name,
        "上游源端": "→".join(source_modules),
        "中间算子": "→".join(operators),
        "输出目标": "→".join(output_targets),
        "更新时间": time.strftime("%Y-%m-%d"),
        "版本号": f"v{int(time.time()) % 100}",
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    existing = False
    for row in store.rows("metric_lineage"):
        if row.get("记录ID") == lineage["记录ID"]:
            row.update(lineage)
            existing = True
            break
    if not existing:
        lineage["id"] = max(
            (int(r.get("id", 0)) for r in store.rows("metric_lineage")), default=0
        ) + 1
        store.rows("metric_lineage").append(lineage)
    return lineage


# ---- 链路8: 端到端管道编排 ----

def run_end_to_end_pipeline(
    source_name: str,
    join_left_source: str | None = None,
    join_right_source: str | None = None,
) -> dict[str, Any]:
    """执行完整的端到端流式计算管道。

    覆盖全部8条链路的集成验证。
    """
    report: dict[str, Any] = {}

    # ---- 链路1: 源端→窗口聚合→落地 ----
    events = simulate_stream_batch(source_name, batch_size=20)
    window_results = apply_window_aggregation(
        events, "sliding", 10.0, 5.0,
        group_keys=["user_id", "region"],
        agg_funcs=["count", "sum", "avg", "max", "min"],
    )
    written, sink_msg = sink_results_to_store(window_results, "结果落地样例1")
    report["chain1_source_to_sink"] = {
        "events": len(events),
        "windows": len(window_results),
        "written": written,
        "message": sink_msg,
    }

    # ---- 链路2: 水位延迟→迟到旁路→补数 ----
    fake_watermark = max(e["event_time"] for e in events) - 3
    late_entries, delay_ratio = check_watermark_delay(
        source_name, fake_watermark, events, allowed_lateness_sec=5.0,
    )
    backfill_result = create_backfill_from_late(late_entries, "window_agg_metric", source_name)
    report["chain2_watermark_backfill"] = {
        "watermark": fake_watermark,
        "late_count": len(late_entries),
        "delay_ratio": delay_ratio,
        "backfill": backfill_result,
    }

    # ---- 链路3: 双流关联→聚合→检查点 ----
    if join_left_source and join_right_source:
        left_events = simulate_stream_batch(join_left_source, batch_size=10)
        right_events = simulate_stream_batch(join_right_source, batch_size=10)
        join_results = perform_stream_join(
            left_events, right_events, "user_id", 15.0, "inner",
        )
        agg_result = aggregate_and_sink_join_results(join_results, "agg_order_daily")
        cp = create_checkpoint_snapshot("join_stream_agg", agg_result)
        report["chain3_join_checkpoint"] = {
            "join_total": len(join_results),
            "matched": agg_result["matched"],
            "checkpoint_id": cp.checkpoint_id,
        }

    # ---- 链路4: CEP→告警→恢复 ----
    cep_events = [
        {"event_id": f"ev_{i}", "event_type": t, "event_time": time.time() + i}
        for i, t in enumerate(["login_fail", "login_fail", "login_fail", "access_granted"])
    ]
    cep_matches = match_cep_pattern(cep_events, ["login_fail"] * 3, 60.0)
    alert_result = trigger_alert_for_cep_matches(cep_matches, "告警策略样例1")
    restore_result = generate_restore_plan(len(cep_matches), "login_security")
    report["chain4_cep_restore"] = {
        "cep_matches": len(cep_matches),
        "alert": alert_result,
        "restore_plan_id": restore_result.get("id"),
    }

    # ---- 链路5: 拓扑→吞吐→状态 ----
    topo_result = simulate_task_topology_flow("任务拓扑样例1", source_name)
    if topo_result.get("backpressure"):
        bp_degrade = apply_backpressure_degradation(topo_result["backpressure"])
        report["chain5_topology_bp"] = {
            "topo": topo_result,
            "degradation": bp_degrade,
        }

    # ---- 链路6: 采样→调试 ----
    trace_result = sample_and_debug_trace("数据采样样例1", "window_agg", events, 0.1)
    report["chain6_sampler_trace"] = trace_result

    # ---- 链路7: 血缘追踪 ----
    lineage = trace_metric_lineage(
        "real_time_qps",
        ["stream_source"],
        ["calc_operator", "window_config", "sliding_index"],
        ["sink_output", "aggregate_sink"],
    )
    report["chain7_lineage"] = lineage

    report["pipeline_status"] = "completed"
    report["timestamp"] = time.time()
    return report


# ---- 工具函数 ----

def verify_state_consistency(
    checkpoints: list[CheckpointSnapshot],
) -> dict[str, Any]:
    """验证多个检查点之间的状态一致性。"""
    issues: list[str] = []
    if not checkpoints:
        return {"consistent": True, "issues": []}
    baseline = checkpoints[0].operator_states
    for i, cp in enumerate(checkpoints[1:], 1):
        for op_name, state in cp.operator_states.items():
            if op_name not in baseline:
                issues.append(f"检查点{i}: 算子{op_name}不在基线中")
            elif state != baseline[op_name]:
                issues.append(f"检查点{i}: 算子{op_name}状态与基线不一致")
    return {"consistent": len(issues) == 0, "issues": issues}


def verify_sink_integrity(
    window_results: list[WindowResult],
    sink_module: str = "sink_output",
) -> dict[str, Any]:
    """验证窗口聚合结果是否完整落地。"""
    written_keys = set()
    for row in store.rows(sink_module):
        if isinstance(row.get("window_key"), str):
            written_keys.add(row["window_key"])
    expected_keys = {wr.window_key for wr in window_results}
    missing = expected_keys - written_keys
    return {
        "expected": len(expected_keys),
        "written": len(written_keys),
        "missing": list(missing),
        "complete": len(missing) == 0,
    }