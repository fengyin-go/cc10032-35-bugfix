"""聚合落库 → 结果落地 数据流接口：落库、完整性报告、检查点读取、重试和完成闸口。

路由刻意围绕"检查点只有一个完整可回放版本"组织：
- ``POST /api/flows/aggregate-sink/{task}/flush`` 逐条落库并生成/更新检查点；
- ``GET  /api/flows/aggregate-sink/{task}/integrity`` 输出完整性报告（成功分区 + 失败记录）；
- ``POST /api/flows/aggregate-sink/{task}/retry`` 重试失败记录（append/rebuild）；
- ``GET  /api/flows/sink-output/checkpoints/{checkpoint_id}`` 按检查点读取落库数据；
- ``POST /api/flows/sink-output/{task}/complete`` 结果落地完成闸口，缺记录时阻止完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.access import PermissionDenied, current_team
from app.services import service_for
from app.services.aggregate_sink import AggregateSinkService
from app.services.sink_output import SinkOutputService
from app.store import store

router = APIRouter(prefix="/api/flows", tags=["聚合落库数据流"])


class FlushPayload(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)
    partition: str | None = None
    submission_key: str | None = None


class RetryPayload(BaseModel):
    strategy: str = "append"


def _denied(error: PermissionDenied) -> JSONResponse:
    return JSONResponse(status_code=error.status_code, content=error.as_dict())


def _aggregate() -> AggregateSinkService:
    service = service_for("aggregate_sink")
    assert isinstance(service, AggregateSinkService)
    return service


def _sink() -> SinkOutputService:
    service = service_for("sink_output")
    assert isinstance(service, SinkOutputService)
    return service


@router.post("/aggregate-sink/{task_name}/flush")
def flush_aggregate(task_name: str, payload: FlushPayload, request: Request) -> Any:
    try:
        return _aggregate().batch_flush_to_target(
            task_name,
            payload.records,
            submission_key=payload.submission_key,
            partition=payload.partition,
            team=current_team(request),
        )
    except PermissionDenied as error:
        return _denied(error)


@router.get("/aggregate-sink")
def aggregate_integrity_all(request: Request) -> Any:
    """聚合页面的失败分区总览：当前团队可见任务的完整性报告。"""
    try:
        return _aggregate().output_integrity_report(team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.get("/aggregate-sink/{task_name}/integrity")
def aggregate_integrity(task_name: str, request: Request) -> Any:
    try:
        return _aggregate().output_integrity_report(
            task_name, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.get("/access/overview")
def access_overview(request: Request) -> Any:
    """权限页面数据：两类权限范围（团队私有 / 受控只读）及当前团队可见记录。"""
    viewer = current_team(request)
    scopes = {
        "team_private": {
            "label": "团队私有",
            "description": "仅所属团队可查看和写入，其他团队读取直接拒绝",
        },
        "shared_read_only": {
            "label": "受控只读",
            "description": "所属团队可写，其他团队只能受控查看，不能改动数据",
        },
    }
    records = []
    for module in ("aggregate_sink", "sink_output"):
        for row in store.rows(module):
            if not isinstance(row.get("id"), int):
                continue
            owner = str(row.get("所属团队") or "数据工程组")
            shared = str(row.get("共享模式") or "团队私有")
            scope = "shared_read_only" if shared in ("受控只读", "团队共享", "read_only") else "team_private"
            visible = owner == viewer or (module == "sink_output" and scope == "shared_read_only")
            records.append({
                "module": module,
                "id": row.get("id"),
                "name": row.get("任务名称") or row.get("配置名称"),
                "owner": owner,
                "scope": scope,
                "scope_label": scopes[scope]["label"],
                "visible": visible,
                "read_only": scope == "shared_read_only" and owner != viewer,
            })
    return {
        "ok": True,
        "team": viewer,
        "teams": sorted({r["owner"] for r in records} | {viewer}),
        "scopes": scopes,
        "records": records,
    }


@router.get("/aggregate-sink/{task_name}/checkpoints")
def replayable_checkpoint(task_name: str, request: Request) -> Any:
    try:
        return _aggregate().list_replayable_checkpoints(
            task_name, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.post("/aggregate-sink/{task_name}/retry")
def retry_aggregate(task_name: str, payload: RetryPayload, request: Request) -> Any:
    try:
        return _aggregate().retry_failed_writes(
            task_name, strategy=payload.strategy, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.get("/sink-output/checkpoints/{checkpoint_id}")
def read_checkpoint_outputs(checkpoint_id: str, request: Request) -> Any:
    try:
        return _sink().read_checkpoint_outputs(checkpoint_id, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.get("/sink-output/{sink_name}/integrity")
def sink_integrity(sink_name: str, request: Request) -> Any:
    try:
        return _sink().verify_output_integrity(sink_name, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)


@router.post("/sink-output/{task_name}/complete")
def complete_sink_task(task_name: str, request: Request) -> Any:
    try:
        return _sink().complete_sink_task(task_name, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)
