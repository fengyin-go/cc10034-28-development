"""路由分发领域服务：按规则将变更事件路由到下游消费通道。

上游：dedup_window(去重) → event_filter(过滤) → route_dispatcher(分发)
消费：下游Kafka/MQ消费端

投递尝试与回执由 app.delivery 的持久化台账承载：每次发送按实际发送
位置落库，重开进程后仍能查到每次尝试；回执按真实发送身份保存，
同一逻辑事件只完成一次。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.delivery import (
    OUTCOME_SENT,
    ROLE_BACKUP,
    ROLE_PRIMARY,
    STATUS_COMPLETED,
    STATUS_PENDING,
    DeliveryLedger,
    delivery_ledger,
    sync_receipt_audits,
)
from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class RouteDispatcherService(BaseModuleService):
    MODULE = "route_dispatcher"
    ENTITY = "路由规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "匹配条件", "目标通道", "分发策略", "是否广播", "权重", "备用目标"]
    REQUIRED_FIELDS = ["规则名称", "匹配条件", "目标通道"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已启用"}
    NEGATIVE_ACTIONS = []

    def route_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个事件执行路由分发。

        根据规则匹配条件和分发策略，将事件定向到目标通道。
        支持广播、加权分发和备通道降级。
        """
        condition = config.get("匹配条件", "")
        target = config.get("目标通道", "default_sink")
        strategy = config.get("分发策略", "round_robin")
        broadcast = config.get("是否广播", "否") == "是"
        weight = int(config.get("权重", "1"))
        backup = config.get("备用目标", "")

        event_str = str(event)
        if condition and condition not in event_str and condition not in event.get("来源表", ""):
            return {"routed": False, "reason": "condition_mismatch"}

        route_result: dict[str, Any] = {
            "routed": True,
            "event_id": event.get("事件ID", ""),
            "rule": config.get("规则名称"),
            "strategy": strategy,
            "broadcast": broadcast,
            "routed_at": datetime.now().isoformat(),
        }

        if broadcast:
            route_result["targets"] = [target]
        else:
            route_result["targets"] = [target]
            route_result["weight"] = weight

        if backup:
            route_result["backup_target"] = backup

        pipeline_bus.routing_table.append({
            "name": config.get("规则名称"),
            "condition": condition,
            "target": target,
            "enabled": config.get("status") == "已启用",
        })

        pipeline_bus.pipeline_metrics["routed"] += 1
        pipeline_bus.write_audit(
            "路由分发", target,
            f"事件 {event.get('事件ID','')} 路由到 {target}",
            "route_dispatcher", "已路由"
        )
        return route_result

    def batch_route(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量路由事件。

        按策略对事件流执行批量分发，支持负载均衡模式。
        """
        strategy = config.get("分发策略", "round_robin")
        targets = config.get("目标通道", "default").split(",")
        weight = int(config.get("权重", "1"))
        broadcast = config.get("是否广播", "否") == "是"

        by_target: dict[str, int] = {}
        for i, event in enumerate(events):
            if broadcast:
                target = targets[0]
            else:
                idx = (i // weight) % len(targets)
                target = targets[idx]
            by_target[target] = by_target.get(target, 0) + 1

        pipeline_bus.write_audit(
            "批量路由", f"{len(events)}条事件",
            f"批量路由 {len(events)} 条事件到 {len(by_target)} 个目标",
            "route_dispatcher", "已路由"
        )
        return {
            "total_routed": len(events),
            "by_target": by_target,
            "strategy": strategy,
            "broadcast": broadcast,
        }

    def compute_route_distribution(self, configs: list[dict[str, Any]]) -> dict[str, Any]:
        """计算路由分发比例的预期分布。"""
        total_weight = sum(int(c.get("权重", "1")) for c in configs if c.get("status") == "已启用")
        dist: dict[str, Any] = {}
        for c in configs:
            if c.get("status") == "已启用":
                w = int(c.get("权重", "1"))
                name = c.get("规则名称", "")
                dist[name] = {
                    "target": c.get("目标通道"),
                    "weight": w,
                    "share_pct": round(w / max(total_weight, 1) * 100, 1),
                }
        return {"total_weight": total_weight, "distribution": dist}

    def find_fallback_route(self, event: dict[str, Any], primary_target: str) -> dict[str, Any]:
        """查找备选路由。"""
        fallback = None
        for rule in pipeline_bus.routing_table:
            if rule.get("target") == primary_target:
                pass
            elif rule.get("enabled"):
                fallback = rule
                break
        if fallback:
            return {"found": True, "fallback": fallback["target"], "condition": fallback["condition"]}
        return {"found": False}

    # ---------- 投递尝试与回执（持久化台账） ----------

    def __init__(self, ledger: DeliveryLedger | None = None) -> None:
        self._ledger = ledger or delivery_ledger

    def dispatch_event(
        self,
        event_id: str,
        primary_target: str,
        backup_target: str = "",
        primary_outcome: str = OUTCOME_SENT,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """分发单个事件：主端超时/失败后自动切换备用目标，每次发送都落投递尝试。"""
        event_id = (event_id or "").strip()
        primary_target = (primary_target or "").strip()
        backup_target = (backup_target or "").strip()
        if not event_id or not primary_target:
            return None, "事件ID与主目标通道不能为空"
        existing = self._ledger.get_event(event_id)
        if existing and existing.get("status") == STATUS_COMPLETED:
            return None, f"事件 {event_id} 已完成投递，不允许重复分发"

        attempts: list[dict[str, Any]] = []
        try:
            attempts.append(
                self._ledger.record_attempt(
                    event_id, primary_target, ROLE_PRIMARY, primary_outcome, payload
                )
            )
            failovered = False
            if primary_outcome != OUTCOME_SENT and backup_target:
                attempts.append(
                    self._ledger.record_attempt(
                        event_id, backup_target, ROLE_BACKUP, OUTCOME_SENT, payload
                    )
                )
                failovered = True
        except ValueError as exc:
            return None, str(exc)

        summary = f"事件 {event_id} 发往 {primary_target}（{primary_outcome}）"
        if failovered:
            summary += f"，{primary_outcome}后切换备用通道 {backup_target}"
        pipeline_bus.pipeline_metrics["routed"] += 1
        pipeline_bus.write_audit(
            "路由分发", event_id, summary, "route_dispatcher", "已路由"
        )
        return {
            "event_id": event_id,
            "attempts": attempts,
            "failovered": failovered,
            "status": STATUS_PENDING,
        }, f"事件 {event_id} 已分发，投递尝试 {len(attempts)} 笔已落库"

    def submit_receipt(
        self,
        event_id: str,
        target: str,
        receipt_id: str,
        sender: str,
        result: str = "成功",
    ) -> tuple[dict[str, Any] | None, str]:
        """登记下游回执：按真实发送身份保存，幂等，完成后同步审计关联。"""
        event_id = (event_id or "").strip()
        target = (target or "").strip()
        receipt_id = (receipt_id or "").strip()
        sender = (sender or "").strip()
        if not all([event_id, target, receipt_id, sender]):
            return None, "事件ID、回执位置、回执编号、发送身份均不能为空"
        try:
            record, receipt, is_new = self._ledger.record_receipt(
                event_id, target, receipt_id, sender, result
            )
        except ValueError as exc:
            return None, str(exc)
        # 台账已落库，再把回执关联进审计轨迹（幂等）；若进程在两者之间
        # 中断，重开后由恢复逻辑按原事件号补齐同一条关联。
        sync_receipt_audits()
        count = record.get("delivery_count", 0)
        if not is_new:
            return {
                "event_id": event_id,
                "duplicate": True,
                "delivery_count": count,
                "status": record.get("status"),
                "effective_receipt_id": record.get("effective_receipt_id"),
            }, f"回执 {receipt_id} 已存在，未重复计数（逻辑交付计数 {count}）"
        return {
            "event_id": event_id,
            "duplicate": False,
            "receipt": receipt,
            "delivery_count": count,
            "status": record.get("status"),
            "effective_receipt_id": record.get("effective_receipt_id"),
        }, f"回执 {receipt_id} 已登记（逻辑交付计数 {count}）"

    def list_deliveries(self) -> list[dict[str, Any]]:
        """投递事件汇总列表：每条含逻辑交付计数与是否还可领取。"""
        records = self._ledger.list_events()
        records.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return [DeliveryLedger.summarize(record) for record in records]

    def delivery_detail(self, event_id: str) -> dict[str, Any] | None:
        """回执详情：每次投递尝试与已收真实回执；读取前幂等补齐审计关联。"""
        sync_receipt_audits()
        record = self._ledger.get_event(event_id)
        if record is None:
            return None
        detail = DeliveryLedger.summarize(record)
        detail["attempts"] = record.get("attempts", [])
        detail["payload"] = record.get("payload", {})
        detail["created_at"] = record.get("created_at", "")
        detail["effective_receipt"] = (
            self._ledger.find_receipt(event_id, record["effective_receipt_id"])
            if record.get("effective_receipt_id")
            else None
        )
        return detail

    def replay_event(self, event_id: str) -> tuple[dict[str, Any] | None, str]:
        """重放事件。

        已完成的事件不再领取数据重放，返回本次选用的有效接收凭证供核对；
        未完成的事件重新投递一笔尝试（优先备用目标）。
        """
        record = self._ledger.get_event(event_id)
        if record is None:
            return None, f"事件 {event_id} 没有投递记录"
        if record.get("status") == STATUS_COMPLETED:
            effective = self._ledger.find_receipt(
                event_id, record.get("effective_receipt_id") or ""
            )
            return {
                "replayed": False,
                "claimable": False,
                "event_id": event_id,
                "delivery_count": record.get("delivery_count", 0),
                "effective_receipt": effective,
            }, f"事件 {event_id} 已完成投递，不再领取重放"
        attempts = record.get("attempts", [])
        backup = next(
            (a for a in reversed(attempts) if a.get("role") == ROLE_BACKUP), None
        )
        target = (backup or attempts[-1])["target"] if attempts else ""
        if not target:
            return None, f"事件 {event_id} 没有可重放的投递目标"
        role = (backup or attempts[-1]).get("role", ROLE_BACKUP)
        try:
            attempt = self._ledger.record_attempt(
                event_id, target, role, OUTCOME_SENT, record.get("payload")
            )
        except ValueError as exc:
            return None, str(exc)
        pipeline_bus.write_audit(
            "事件重放",
            event_id,
            f"事件 {event_id} 重放到 {target}（尝试 {attempt['attempt_id']}）",
            "route_dispatcher",
            "已重放",
        )
        return {
            "replayed": True,
            "claimable": True,
            "event_id": event_id,
            "attempt": attempt,
        }, f"事件 {event_id} 已重新投递到 {target}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            target = entry.get("目标通道", "")
            pipeline_bus.routing_table.append({
                "name": entry.get("规则名称"),
                "condition": entry.get("匹配条件"),
                "target": target,
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "路由启用", target,
                f"路由规则 {entry.get('规则名称')} 已启用，指向 {target}",
                "route_dispatcher", "已启用"
            )
        return entry, msg