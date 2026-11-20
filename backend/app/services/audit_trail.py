"""审计轨迹领域服务：记录CDC管道配置变更和事件处理的全链审计。

上游：所有服务模块都会写入审计日志到 pipeline_bus
消费：审计查询消费端通过本服务查询历史轨迹

事件级审计详情按原事件号关联本次收到的真实回执（来自路由分发的
持久化投递台账），值班员重开详情即可核对事件是否还要重放。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.delivery import STATUS_COMPLETED, delivery_ledger, sync_receipt_audits
from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class AuditTrailService(BaseModuleService):
    MODULE = "audit_trail"
    ENTITY = "审计日志"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["审计时间", "操作类型", "目标对象", "变更摘要", "操作来源", "结果状态", "审计备注"]
    REQUIRED_FIELDS = ["审计时间", "操作类型", "目标对象"]
    STATUS_ORDER = ["已记录", "已复核", "异常标记"]
    ACTION_RULES = {"复核记录": "已记录", "标记异常": "已记录"}
    NEGATIVE_ACTIONS = []

    def query_audit_logs(self, from_time: str = "", to_time: str = "",
                         operation_type: str = "", source_module: str = "",
                         limit: int = 100) -> list[dict[str, Any]]:
        """查询审计日志。”

        支持按时间范围、操作类型、来源模块筛选。
        """
        result: list[dict[str, Any]] = []
        for entry in reversed(pipeline_bus.audit_log):
            entry_time = entry.get("审计时间", "")
            if from_time and entry_time < from_time:
                continue
            if to_time and entry_time > to_time:
                continue
            if operation_type and operation_type not in entry.get("操作类型", ""):
                continue
            if source_module and source_module != entry.get("操作来源", ""):
                continue
            result.append(dict(entry))
            if len(result) >= limit:
                break
        return result

    def audit_summary(self) -> dict[str, Any]:
        """审计日志汇总统计。"""
        logs = pipeline_bus.audit_log
        by_module: dict[str, int] = {}
        by_operation: dict[str, int] = {}
        by_status: dict[str, int] = {}
        for entry in logs:
            module = entry.get("操作来源", "unknown")
            op = entry.get("操作类型", "unknown")
            status = entry.get("结果状态", "unknown")
            by_module[module] = by_module.get(module, 0) + 1
            by_operation[op] = by_operation.get(op, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "total_entries": len(logs),
            "by_module": by_module,
            "by_operation": by_operation,
            "by_status": by_status,
            "last_entry_time": logs[-1]["审计时间"] if logs else "",
            "generated_at": datetime.now().isoformat(),
        }

    def find_anomalies(self, window_count: int = 500) -> list[dict[str, Any]]:
        """从审计日志中检测异常模式。"""
        recent = pipeline_bus.audit_log[-window_count:]
        anomalies = []
        error_keywords = ["失败", "异常", "错误", "failed", "error"]
        for entry in recent:
            summary = entry.get("变更摘要", "")
            status = entry.get("结果状态", "")
            if any(kw in summary for kw in error_keywords) or status == "异常标记":
                anomalies.append({
                    "entry": entry,
                    "risk": "high" if "失败" in summary or "严重" in summary else "medium",
                    "detected_at": datetime.now().isoformat(),
                })
        return anomalies

    def compliance_check(self) -> dict[str, Any]:
        """合规性检查。"""
        logs = pipeline_bus.audit_log
        total = len(logs)
        required_ops = ["DDL分析", "Schema同步", "通道启动", "路由分发"]
        op_coverage: dict[str, bool] = {}
        for op in required_ops:
            op_coverage[op] = any(op in entry.get("操作类型", "") for entry in logs)
        all_covered = all(op_coverage.values())
        return {
            "total_audit_entries": total,
            "required_operation_coverage": op_coverage,
            "all_required_covered": all_covered,
            "compliance_status": "合规" if all_covered else "部分覆盖",
        }

    def recover_receipt_associations(self) -> int:
        """从投递台账恢复回执的审计关联（幂等），返回本次补写条数。

        回执随投递尝试落库后，若进程在审计关联可查之前中断，重开时
        按原事件号重建关联，避免查不到真实接收而把已完成事件再次重放。
        """
        return sync_receipt_audits()

    def event_detail(self, event_id: str) -> dict[str, Any]:
        """事件审计详情：按原事件号汇总真实回执、审计轨迹与重放判定。

        读取前先幂等补齐回执关联，保证值班员重新进入详情时看到的
        是恢复后的完整状态，而不是崩溃瞬间的半成品。
        """
        event_id = (event_id or "").strip()
        sync_receipt_audits()
        record = delivery_ledger.get_event(event_id)
        entries = [
            dict(entry)
            for entry in pipeline_bus.audit_log
            if entry.get("目标对象") == event_id
        ]
        if record is None:
            return {
                "event_id": event_id,
                "found": False,
                "delivery": None,
                "receipts": [],
                "audit_entries": entries,
                "replay_needed": False,
                "message": f"事件 {event_id} 没有投递记录",
            }
        receipts: list[dict[str, Any]] = []
        for attempt in record.get("attempts", []):
            for receipt in attempt.get("receipts", []):
                receipts.append(dict(receipt))
        completed = record.get("status") == STATUS_COMPLETED
        delivery = {
            "status": record.get("status"),
            "delivery_count": record.get("delivery_count", 0),
            "effective_receipt_id": record.get("effective_receipt_id"),
            "claimable": not completed,
            "attempt_count": len(record.get("attempts", [])),
        }
        if completed:
            message = (
                f"事件 {event_id} 已完成投递（逻辑交付计数 "
                f"{delivery['delivery_count']}），无需重放"
            )
        else:
            message = f"事件 {event_id} 尚未完成投递，可领取重放"
        return {
            "event_id": event_id,
            "found": True,
            "delivery": delivery,
            "receipts": receipts,
            "audit_entries": entries,
            "replay_needed": not completed,
            "message": message,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "复核记录":
            pipeline_bus.write_audit(
                "审计复核", entry.get("目标对象", ""),
                f"审计记录 {entry_id} 已复核",
                "audit_trail", "已复核"
            )
        return entry, msg