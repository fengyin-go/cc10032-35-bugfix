"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。

团队范围：
- 当前团队取请求头 ``X-Operator-Team``（或查询参数 ``team``），缺省为值守团队；
- 聚合落库 / 结果落地记录带团队归属，越权读取返回 403 且不返回数据；
- 共享记录只允许受控只读访问，写动作被拒绝后记录保持原归属和原状态。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.access import PermissionDenied, current_team
from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _denied(error: PermissionDenied) -> JSONResponse:
    """越权统一返回 403，响应体带可机读的错误码，不夹带任何记录内容。"""
    return JSONResponse(status_code=error.status_code, content=error.as_dict())


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size,
        team=current_team(request))
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000,
        team=current_team(request))
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int, request: Request) -> dict[str, Any]:
    service = require_service(module)
    try:
        entry = service.get_entry(entry_id, team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload, request: Request) -> ActionResult:
    service = require_service(module)
    try:
        entry, missing = service.create_entry(
            payload.values,
            team=current_team(request),
            submission_key=payload.submission_key,
        )
    except PermissionDenied as error:
        return _denied(error)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    if entry is not None and entry.get("duplicate_submission"):
        return ActionResult(
            ok=True,
            message=f"{service.ENTITY}已存在，重复提交未重复登记",
            entry=entry,
            code="duplicate_submission",
        )
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload, request: Request) -> ActionResult:
    service = require_service(module)
    try:
        entry, message = service.run_action(
            entry_id, payload.action.strip(), team=current_team(request))
    except PermissionDenied as error:
        return _denied(error)
    return ActionResult(ok=entry is not None, message=message, entry=entry)
