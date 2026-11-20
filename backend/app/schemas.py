"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class DispatchPayload(BaseModel):
    """路由分发单事件投递请求。"""

    event_id: str
    primary_target: str
    backup_target: str = ""
    primary_outcome: str = "已发送"
    payload: dict[str, Any] = Field(default_factory=dict)


class ReceiptPayload(BaseModel):
    """下游消费端回执登记请求。"""

    event_id: str
    target: str
    receipt_id: str
    sender: str
    result: str = "成功"
