"""投递尝试与回执接口：路由分发的单事件发送记录、回执登记、审计详情与重放。

所有投递尝试持久化在投递台账（app.delivery），重开进程后仍可查询；
回执按真实发送身份保存，同一逻辑事件只完成一次，相同回执重试不增加
逻辑交付计数。

注意：本路由必须早于通用资源路由注册，否则 /api/route_dispatcher/deliveries
等路径会被 /api/{module}/{entry_id} 抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import DispatchPayload, ReceiptPayload
from app.services import service_for
from app.services.audit_trail import AuditTrailService
from app.services.route_dispatcher import RouteDispatcherService

router = APIRouter(prefix="/api", tags=["投递尝试与回执"])


def _dispatcher() -> RouteDispatcherService:
    service = service_for("route_dispatcher")
    assert isinstance(service, RouteDispatcherService)
    return service


def _audit() -> AuditTrailService:
    service = service_for("audit_trail")
    assert isinstance(service, AuditTrailService)
    return service


@router.post("/route_dispatcher/dispatch")
def dispatch_event(payload: DispatchPayload) -> dict[str, Any]:
    """分发单个事件：主端超时/失败后切换备用目标，每次发送落投递尝试。"""
    result, message = _dispatcher().dispatch_event(
        event_id=payload.event_id,
        primary_target=payload.primary_target,
        backup_target=payload.backup_target,
        primary_outcome=payload.primary_outcome,
        payload=payload.payload,
    )
    return {"ok": result is not None, "message": message, "result": result}


@router.post("/route_dispatcher/receipts")
def submit_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    """登记下游真实回执：幂等，相同回执重试不增加逻辑交付计数。"""
    result, message = _dispatcher().submit_receipt(
        event_id=payload.event_id,
        target=payload.target,
        receipt_id=payload.receipt_id,
        sender=payload.sender,
        result=payload.result,
    )
    return {"ok": result is not None, "message": message, "result": result}


@router.get("/route_dispatcher/deliveries")
def list_deliveries() -> dict[str, Any]:
    """投递事件汇总：每条含逻辑交付计数与是否还可领取重放。"""
    items = _dispatcher().list_deliveries()
    return {"items": items, "total": len(items)}


@router.get("/route_dispatcher/deliveries/{event_id}")
def delivery_detail(event_id: str) -> dict[str, Any]:
    """回执详情：每次投递尝试与已收到的真实回执，刷新后仍可查到。"""
    detail = _dispatcher().delivery_detail(event_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 没有投递记录")
    return detail


@router.post("/route_dispatcher/deliveries/{event_id}/replay")
def replay_event(event_id: str) -> dict[str, Any]:
    """重放事件：已完成的事件不再领取数据，返回本次选用的有效接收凭证。"""
    result, message = _dispatcher().replay_event(event_id)
    return {"ok": result is not None, "message": message, "result": result}


@router.get("/audit_trail/events/{event_id}")
def audit_event_detail(event_id: str) -> dict[str, Any]:
    """审计详情：按原事件号关联本次收到的真实回执，供值班员核对是否还要重放。"""
    return _audit().event_detail(event_id)
