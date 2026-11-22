"""Schema校验领域服务：数据结构、字段类型、空值策略校验。

运行历史确认完成时由本服务产出**持久化的Schema检验回执**：
- 同一次检验的结论、被检输入快照、规则版本快照必须可核对；
- 只计入目标数据（运行历史）的已生效规则，其他目标的规则不参与；
- 回执先落盘后生效，明细写入失败不会产生可交给下游的通过回执；
- 规则后续被修改/停用不影响历史回执，旧违规可按原版本复核；
- 每一轮检验单独记录，重复确认不会复制已发布的回执。
"""
from __future__ import annotations

import hashlib
import json
import threading
from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app import persistence as persist

RECEIPT_FILE = "schema_check_receipts.jsonl"
PENDING_FILE = "schema_check_pending.jsonl"
TARGET_RUN_HISTORY = "run_history"

# 同一运行的确认/续检串行化，避免并发确认产生重复回执或读到半写状态。
_RECEIPT_LOCKS: dict[int, threading.RLock] = {}
_LOCK_GUARD = threading.Lock()

# 测试/演练用故障注入：在回执明细落盘前抛出，模拟“检验明细写入报错”。
# 键为 run_id，值为剩余触发次数；生产路径默认不注入。
_WRITE_FAULTS: dict[int, int] = {}


def run_lock(run_id: int) -> threading.RLock:
    """同一运行的确认/续检/复核共用的串行锁。"""
    with _LOCK_GUARD:
        lock = _RECEIPT_LOCKS.get(run_id)
        if lock is None:
            lock = threading.RLock()
            _RECEIPT_LOCKS[run_id] = lock
        return lock


def arm_write_fault(run_id: int, times: int = 1) -> None:
    """安排下一次（或多次）该运行的回执落盘直接失败，用于验证无伪通过残留。"""
    _WRITE_FAULTS[run_id] = _WRITE_FAULTS.get(run_id, 0) + times


def clear_write_fault(run_id: int) -> None:
    _WRITE_FAULTS.pop(run_id, None)


def _rule_version(rule: dict[str, Any]) -> str:
    """规则版本：优先取登记的规则版本，否则按规则内容生成内容指纹。"""
    declared = str(rule.get("规则版本") or "").strip()
    if declared:
        return declared
    raw = repr(sorted((k, str(v)) for k, v in rule.items() if k not in ("id", "status", "pending", "abnormal")))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _snapshot_rule(rule: dict[str, Any]) -> dict[str, Any]:
    """固化检验时实际使用的规则，保证回执可按原版本复核。"""
    return {
        "rule_id": rule.get("id"),
        "规则编号": rule.get("规则编号", ""),
        "目标数据": rule.get("目标数据", ""),
        "字段名": rule.get("字段名", ""),
        "类型约束": rule.get("类型约束", ""),
        "格式约束": rule.get("格式约束", ""),
        "空值策略": rule.get("空值策略", ""),
        "异常归类": rule.get("异常归类", ""),
        "规则版本": _rule_version(rule),
    }


def _snapshot_input(record: dict[str, Any]) -> dict[str, Any]:
    """固化被检输入，只保留业务字段与采集状态，剥离运行期易变元数据。"""
    skip = {"pending", "abnormal", "version", "updated_at", "last_action", "previous_status"}
    return {k: v for k, v in record.items() if k not in skip}


def _check_one(rule_snapshot: dict[str, Any], record: dict[str, Any]) -> dict[str, Any] | None:
    """对单条规则执行检验，返回违规明细；无违规返回None。"""
    field = rule_snapshot.get("字段名", "")
    expected_type = str(rule_snapshot.get("类型约束") or "").strip().lower()
    format_constraint = rule_snapshot.get("格式约束") or ""
    null_policy = rule_snapshot.get("空值策略") or ""
    value = record.get(field)

    if value is None or (isinstance(value, str) and not value.strip()):
        if null_policy == "禁止":
            return {
                "field": field,
                "rule_id": rule_snapshot.get("rule_id"),
                "规则编号": rule_snapshot.get("规则编号", ""),
                "规则版本": rule_snapshot.get("规则版本", ""),
                "类型约束": expected_type,
                "实际值": value,
                "实际类型": type(value).__name__,
                "原因": "空值",
                "message": f"字段{field}不允许为空（规则{rule_snapshot.get('规则编号')}）",
            }
        return None

    if expected_type == "int":
        # 严格整数规则：bool是int子类但语义上不是采集行数；字符串"bad"必须判违规。
        is_int = isinstance(value, int) and not isinstance(value, bool)
        if not is_int:
            return {
                "field": field,
                "rule_id": rule_snapshot.get("rule_id"),
                "规则编号": rule_snapshot.get("规则编号", ""),
                "规则版本": rule_snapshot.get("规则版本", ""),
                "类型约束": "int",
                "实际值": value,
                "实际类型": type(value).__name__,
                "原因": "类型不符",
                "message": f"字段{field}期望int类型，实际为{type(value).__name__}（规则{rule_snapshot.get('规则编号')}）",
            }
    elif expected_type == "str":
        # 严格文本规则：数值/布尔不冒充字符串。
        if not isinstance(value, str):
            return {
                "field": field,
                "rule_id": rule_snapshot.get("rule_id"),
                "规则编号": rule_snapshot.get("规则编号", ""),
                "规则版本": rule_snapshot.get("规则版本", ""),
                "类型约束": "str",
                "实际值": value,
                "实际类型": type(value).__name__,
                "原因": "类型不符",
                "message": f"字段{field}期望str类型，实际为{type(value).__name__}（规则{rule_snapshot.get('规则编号')}）",
            }
    elif expected_type == "float":
        is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
        if not is_number:
            return {
                "field": field,
                "rule_id": rule_snapshot.get("rule_id"),
                "规则编号": rule_snapshot.get("规则编号", ""),
                "规则版本": rule_snapshot.get("规则版本", ""),
                "类型约束": "float",
                "实际值": value,
                "实际类型": type(value).__name__,
                "原因": "类型不符",
                "message": f"字段{field}期望float类型，实际为{type(value).__name__}（规则{rule_snapshot.get('规则编号')}）",
            }
    elif expected_type == "date":
        if not isinstance(value, str) or (format_constraint and len(value) < 8):
            return {
                "field": field,
                "rule_id": rule_snapshot.get("rule_id"),
                "规则编号": rule_snapshot.get("规则编号", ""),
                "规则版本": rule_snapshot.get("规则版本", ""),
                "类型约束": "date",
                "实际值": value,
                "实际类型": type(value).__name__,
                "原因": "格式不符",
                "message": f"字段{field}日期格式不符合{format_constraint or 'date'}（规则{rule_snapshot.get('规则编号')}）",
            }
    return None


class SchemaCheckService(BaseModuleService):
    MODULE = "schema_check"
    ENTITY = "Schema规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标数据", "字段名", "类型约束", "格式约束", "空值策略", "异常归类", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "目标数据", "字段名"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ------------------------------------------------------------------
    # 持久化回执读写
    # ------------------------------------------------------------------
    def _read_receipts(self) -> list[dict[str, Any]]:
        return persist.read_jsonl("reports", RECEIPT_FILE)

    def _write_receipt(self, receipt: dict[str, Any]) -> None:
        persist.append_jsonl("reports", RECEIPT_FILE, receipt)

    def _read_pending(self) -> list[dict[str, Any]]:
        return persist.read_jsonl("reports", PENDING_FILE)

    def _write_pending(self, manifest: dict[str, Any]) -> None:
        persist.append_jsonl("reports", PENDING_FILE, manifest)

    def _consume_pending(self, manifest_id: str) -> None:
        """把待发布清单标记为已消费；append-only 仓库通过追加状态行表达覆盖。"""
        rows = self._read_pending()
        for row in rows:
            if row.get("manifest_id") == manifest_id:
                row["status"] = "consumed"
                row["consumed_at"] = persist.now_iso()
        path = persist.data_path("reports", PENDING_FILE)
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def _open_pending(self, run_id: int, round_no: int) -> dict[str, Any] | None:
        """读取某轮尚未发布的待检验清单（首次明细写入失败后的续检依据）。"""
        for row in reversed(self._read_pending()):
            if (
                row.get("run_id") == run_id
                and row.get("round") == round_no
                and row.get("status") == "open"
            ):
                return row
        return None

    def _find_round(self, receipts: list[dict[str, Any]], run_id: int, round_no: int) -> dict[str, Any] | None:
        for receipt in receipts:
            if receipt.get("run_id") == run_id and receipt.get("round") == round_no:
                return receipt
        return None

    def latest_receipt(self, run_id: int) -> dict[str, Any] | None:
        """读取某运行最新一轮的持久化回执。"""
        matched = [r for r in self._read_receipts() if r.get("run_id") == run_id]
        if not matched:
            return None
        return max(matched, key=lambda r: int(r.get("round", 0)))

    def get_receipt(self, run_id: int, round_no: int) -> dict[str, Any] | None:
        return self._find_round(self._read_receipts(), run_id, round_no)

    def list_receipts(self, run_id: int) -> list[dict[str, Any]]:
        return sorted(
            (r for r in self._read_receipts() if r.get("run_id") == run_id),
            key=lambda r: int(r.get("round", 0)),
        )

    # ------------------------------------------------------------------
    # 检验执行
    # ------------------------------------------------------------------
    def _active_rules_for(self, target_module: str) -> list[dict[str, Any]]:
        """只取目标数据匹配且已生效的规则，其他目标的规则不计入。"""
        return [
            r for r in store.rows(self.MODULE)
            if r.get("目标数据") == target_module and r.get("status") == "已生效"
        ]

    def run_check(
        self,
        record: dict[str, Any],
        target_module: str,
        *,
        run_id: int,
        trigger: str,
        round_no: int | None = None,
        reuse_pending: bool = False,
    ) -> dict[str, Any]:
        """执行一轮检验并发布持久化回执。

        结论只有 pass / fail / incomplete：
        - 没有匹配目标的已生效规则时，不能假装通过，记为 incomplete（检验未完成）；
        - 被检输入与规则版本在明细写入前就固化为待发布清单，首次写入报错后，
          同一轮重试（reuse_pending=True）沿用原快照，规则中途被修改不改变原轮结论；
          新一轮独立检验不复用旧清单，按当前规则重新固化；
        - 明细在内存中全部算完后一次性落盘，落盘失败向上抛出，不留下伪通过；
        - 同一轮次重复执行直接返回已发布回执，保证重复确认后只有单份。
        """
        lock = run_lock(run_id)
        with lock:
            receipts = self._read_receipts()
            if round_no is None:
                rounds = [int(r.get("round", 0)) for r in receipts if r.get("run_id") == run_id]
                round_no = (max(rounds) + 1) if rounds else 1
            else:
                existing = self._find_round(receipts, run_id, round_no)
                if existing is not None:
                    return existing

            # 仅同一轮重试才沿用首次固化的输入与规则版本；新轮次一律重取当前规则。
            pending = self._open_pending(run_id, round_no) if reuse_pending else None
            if pending is not None:
                input_snapshot = pending["input_snapshot"]
                rule_snapshots = pending["rules_applied"]
                manifest_id = pending["manifest_id"]
            else:
                rules = self._active_rules_for(target_module)
                rule_snapshots = [_snapshot_rule(r) for r in rules]
                input_snapshot = _snapshot_input(record)
                manifest_id = f"PEND-R{run_id:04d}-{round_no:02d}"
                # 先固化再算明细、再发布：明细写入报错时原版本仍可核对。
                self._write_pending({
                    "manifest_id": manifest_id,
                    "run_id": run_id,
                    "round": round_no,
                    "target_module": target_module,
                    "trigger": trigger,
                    "input_snapshot": input_snapshot,
                    "rules_applied": rule_snapshots,
                    "rule_versions": [s["规则版本"] for s in rule_snapshots],
                    "status": "open",
                    "created_at": persist.now_iso(),
                })

            details: list[dict[str, Any]] = []
            for snapshot in rule_snapshots:
                violation = _check_one(snapshot, input_snapshot)
                if violation is not None:
                    details.append(violation)

            if not rule_snapshots:
                conclusion = "incomplete"
            elif details:
                conclusion = "fail"
            else:
                conclusion = "pass"

            receipt = {
                "receipt_id": f"SC-R{run_id:04d}-{round_no:02d}",
                "manifest_id": manifest_id,
                "run_id": run_id,
                "round": round_no,
                "target_module": target_module,
                "trigger": trigger,
                "conclusion": conclusion,
                "input_snapshot": input_snapshot,
                "rules_applied": rule_snapshots,
                "rule_versions": [s["规则版本"] for s in rule_snapshots],
                "violations": details,
                "status": "published",
                "created_at": persist.now_iso(),
            }
            # 先持久化再对外可见：写入报错时调用方拿不到任何通过回执，
            # 已发布回执文件里也不会出现半条记录，后续确认仍按原快照重试本轮。
            remaining = _WRITE_FAULTS.get(run_id, 0)
            if remaining > 0:
                _WRITE_FAULTS[run_id] = remaining - 1
                if _WRITE_FAULTS[run_id] <= 0:
                    _WRITE_FAULTS.pop(run_id, None)
                raise OSError(f"运行{run_id}检验明细写入失败（模拟故障注入）")
            self._write_receipt(receipt)
            self._consume_pending(manifest_id)

            # 兼容旧的违规流水消费方（仅在确有违规时追加）。
            if details:
                persist.append_jsonl("reports", "schema_violations.jsonl", {
                    "target_module": target_module,
                    "run_id": run_id,
                    "round": round_no,
                    "receipt_id": receipt["receipt_id"],
                    "record_summary": str(input_snapshot)[:100],
                    "violations": [d["message"] for d in details],
                    "timestamp": receipt["created_at"],
                })
            return receipt

    def resume_check(self, run_id: int) -> dict[str, Any]:
        """续检入口：对采集已结束但检验未完成（incomplete/无回执）的运行补一轮检验。"""
        record = store.find("run_history", run_id)
        if record is None:
            raise ValueError(f"运行记录 {run_id} 不存在或已归档")
        return self.run_check(
            record,
            TARGET_RUN_HISTORY,
            run_id=run_id,
            trigger="manual_resume",
            reuse_pending=True,
        )

    def recheck_round(self, run_id: int, round_no: int) -> dict[str, Any]:
        """按原回执固化的输入与规则版本复核历史轮次，规则后续变更不影响复核结论。"""
        lock = run_lock(run_id)
        with lock:
            original = self.get_receipt(run_id, round_no)
            if original is None:
                raise ValueError(f"运行 {run_id} 第{round_no}轮检验回执不存在，无法复核")

            input_snapshot = original.get("input_snapshot", {})
            rechecked: list[dict[str, Any]] = []
            for snapshot in original.get("rules_applied", []):
                violation = _check_one(snapshot, input_snapshot)
                if violation is not None:
                    rechecked.append(violation)

            if not original.get("rules_applied"):
                conclusion = "incomplete"
            elif rechecked:
                conclusion = "fail"
            else:
                conclusion = "pass"

            return {
                "receipt_id": original.get("receipt_id"),
                "run_id": run_id,
                "round": round_no,
                "target_module": original.get("target_module"),
                "conclusion": conclusion,
                "matches_original": conclusion == original.get("conclusion"),
                "original_conclusion": original.get("conclusion"),
                "input_snapshot": input_snapshot,
                "rules_applied": original.get("rules_applied", []),
                "rule_versions": original.get("rule_versions", []),
                "violations": rechecked,
                "rechecked_at": persist.now_iso(),
            }

    def validate_record(self, record: dict[str, Any], target_module: str) -> list[str]:
        """对单条记录执行所有已生效的Schema校验规则，返回违规消息列表。"""
        rules = self._active_rules_for(target_module)
        input_snapshot = _snapshot_input(record)
        messages: list[str] = []
        for rule in rules:
            violation = _check_one(_snapshot_rule(rule), input_snapshot)
            if violation is not None:
                messages.append(violation["message"])
        return messages

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        receipts = self._read_receipts()
        base["total_receipts"] = len(receipts)
        base["pass_receipts"] = sum(1 for r in receipts if r.get("conclusion") == "pass")
        base["fail_receipts"] = sum(1 for r in receipts if r.get("conclusion") == "fail")
        base["incomplete_receipts"] = sum(1 for r in receipts if r.get("conclusion") == "incomplete")
        # 兼容旧统计口径
        violations = persist.read_jsonl("reports", "schema_violations.jsonl")
        base["total_violations"] = len(violations)
        # 关联的质量防护规则
        guards = store.rows("quality_guard")
        base["linked_quality_rules"] = sum(1 for g in guards if g.get("status") == "已生效")
        # 关联的导出通道
        channels = store.rows("export_channel")
        base["linked_export_channels"] = len(channels)
        return base
