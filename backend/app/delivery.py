"""投递尝试台账：路由分发的单事件发送记录与回执的持久化存储。

设计要点：
- 每次发送（主端/备端）都按实际发送位置落一条投递尝试记录，重开进程后仍能查到；
- 回执按真实发送身份挂在对应尝试上，同一逻辑事件只能完成一次（逻辑交付计数 0/1）；
- 相同回执重复提交不改变逻辑交付计数；两个物理回执都完整保留；
- 台账 JSON 原子写落盘，审计关联可从台账幂等恢复，避免崩溃后已完成事件被再次领取重放。
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_LEDGER_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "delivery_ledger.json"
)

OUTCOME_SENT = "已发送"
OUTCOME_TIMEOUT = "超时"
OUTCOME_FAILED = "失败"

STATUS_PENDING = "待确认"
STATUS_COMPLETED = "已完成"

ROLE_PRIMARY = "主端"
ROLE_BACKUP = "备端"

RESULT_OK = "成功"


def _now() -> str:
    return datetime.now().isoformat()


class DeliveryLedger:
    """单事件投递台账：持久化投递尝试与回执，保证重开可查、完成幂等。"""

    def __init__(self, path: Path | None = None) -> None:
        env_path = os.environ.get("DELIVERY_LEDGER_PATH", "")
        self._path = Path(env_path) if env_path else (path or DEFAULT_LEDGER_PATH)
        self._lock = threading.RLock()
        self._events: dict[str, dict[str, Any]] = {}
        self._seq = 0
        self._load()

    # ---------- 持久化 ----------

    def _load(self) -> None:
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return
        events = raw.get("events")
        if isinstance(events, dict):
            self._events = {str(k): v for k, v in events.items()}
        self._seq = int(raw.get("seq", 0))

    def _save(self) -> None:
        """原子落盘：先写临时文件再替换，避免崩溃留下半个文件。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        payload = {"version": 1, "seq": self._seq, "events": self._events}
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(tmp, self._path)

    @staticmethod
    def _copy(record: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(record, ensure_ascii=False))

    # ---------- 查询 ----------

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._events.get(event_id)
            return self._copy(record) if record else None

    def list_events(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._copy(record) for record in self._events.values()]

    def find_receipt(self, event_id: str, receipt_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._events.get(event_id)
            if not record:
                return None
            for attempt in record.get("attempts", []):
                for receipt in attempt.get("receipts", []):
                    if receipt.get("receipt_id") == receipt_id:
                        return self._copy(receipt)
            return None

    def all_receipts(self) -> list[dict[str, Any]]:
        """展开台账里的全部回执（带原事件号），供审计关联恢复使用。"""
        flattened: list[dict[str, Any]] = []
        with self._lock:
            for record in self._events.values():
                for attempt in record.get("attempts", []):
                    for receipt in attempt.get("receipts", []):
                        item = self._copy(receipt)
                        item["event_id"] = record["event_id"]
                        flattened.append(item)
        return flattened

    # ---------- 写入 ----------

    def record_attempt(
        self,
        event_id: str,
        target: str,
        role: str,
        outcome: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """按实际发送位置落一条投递尝试记录；已完成的事件拒绝再投递。"""
        with self._lock:
            record = self._events.get(event_id)
            if record and record.get("status") == STATUS_COMPLETED:
                raise ValueError(f"事件 {event_id} 已完成投递，不允许再次发送")
            if record is None:
                record = {
                    "event_id": event_id,
                    "status": STATUS_PENDING,
                    "delivery_count": 0,
                    "effective_receipt_id": None,
                    "payload": payload or {},
                    "created_at": _now(),
                    "updated_at": _now(),
                    "attempts": [],
                }
                self._events[event_id] = record
            self._seq += 1
            attempt = {
                "attempt_id": f"att-{self._seq:04d}",
                "event_id": event_id,
                "target": target,
                "role": role,
                "outcome": outcome,
                "sent_at": _now(),
                "receipts": [],
            }
            record["attempts"].append(attempt)
            record["updated_at"] = _now()
            self._save()
            return self._copy(attempt)

    def record_receipt(
        self,
        event_id: str,
        target: str,
        receipt_id: str,
        sender: str,
        result: str,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        """登记回执。

        返回 (事件记录, 回执, 是否新回执)。相同 receipt_id 重复提交时幂等：
        直接返回既有状态，不增加逻辑交付计数。
        """
        with self._lock:
            record = self._events.get(event_id)
            if record is None:
                raise ValueError(f"事件 {event_id} 没有投递记录，回执被拒绝")
            existing = self.find_receipt(event_id, receipt_id)
            if existing is not None:
                return self._copy(record), existing, False
            attempt = self._latest_attempt_for(record, target)
            if attempt is None:
                raise ValueError(
                    f"事件 {event_id} 没有发往 {target} 的投递尝试，回执被拒绝"
                )
            receipt = {
                "receipt_id": receipt_id,
                "sender": sender,
                "target": target,
                "role": attempt.get("role", ""),
                "result": result,
                "received_at": _now(),
                "attempt_id": attempt["attempt_id"],
            }
            attempt["receipts"].append(receipt)
            # 同一逻辑事件只能完成一次：首个成功回执选定有效接收凭证，
            # 之后到达的物理回执（如主端迟到的成功回执）仍按真实发送身份保存，
            # 但不再改变逻辑交付计数与已选用的有效凭证。
            if result == RESULT_OK and record.get("status") != STATUS_COMPLETED:
                record["status"] = STATUS_COMPLETED
                record["delivery_count"] = 1
                record["effective_receipt_id"] = receipt_id
            record["updated_at"] = _now()
            self._save()
            return self._copy(record), self._copy(receipt), True

    @staticmethod
    def _latest_attempt_for(
        record: dict[str, Any], target: str
    ) -> dict[str, Any] | None:
        for attempt in reversed(record.get("attempts", [])):
            if attempt.get("target") == target:
                return attempt
        return None

    # ---------- 视图辅助 ----------

    @staticmethod
    def summarize(record: dict[str, Any]) -> dict[str, Any]:
        attempts = record.get("attempts", [])
        return {
            "event_id": record["event_id"],
            "status": record["status"],
            "delivery_count": record.get("delivery_count", 0),
            "effective_receipt_id": record.get("effective_receipt_id"),
            "attempt_count": len(attempts),
            "receipt_count": sum(len(a.get("receipts", [])) for a in attempts),
            "claimable": record.get("status") != STATUS_COMPLETED,
            "updated_at": record.get("updated_at", ""),
        }


def _ledger_path() -> Path:
    env_path = os.environ.get("DELIVERY_LEDGER_PATH", "")
    return Path(env_path) if env_path else DEFAULT_LEDGER_PATH


delivery_ledger = DeliveryLedger(_ledger_path())


def sync_receipt_audits() -> int:
    """把台账中所有回执的审计关联补齐到审计日志（幂等）。

    回执随投递尝试落库后，若进程在审计关联可查之前中断，重开时
    通过本函数按原事件号重建关联；值班员重新进入详情即可核对，
    已完成的事件不会因此再次进入重放。
    """
    from app.pipeline import pipeline_bus

    existing = {
        entry.get("审计备注")
        for entry in pipeline_bus.audit_log
        if str(entry.get("审计备注", "")).startswith("receipt:")
    }
    recovered = 0
    for receipt in delivery_ledger.all_receipts():
        tag = f"receipt:{receipt['receipt_id']}"
        if tag in existing:
            continue
        pipeline_bus.write_audit(
            "回执登记",
            receipt["event_id"],
            f"收到 {receipt['sender']}（{receipt['target']}）对事件 "
            f"{receipt['event_id']} 的{receipt['result']}回执",
            "route_dispatcher",
            "已确认",
            remark=tag,
        )
        existing.add(tag)
        recovered += 1
    return recovered
