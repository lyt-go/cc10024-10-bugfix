"""运行历史领域服务：采集运行记录的统一查询和审计。

运行记录「确认完成」时：
1. 采集本身结束（状态流转为已完成）；
2. 触发 page_dedup 去重与 schema_check 检验，Schema 检验产出**持久化回执**；
3. 运行记录只关联已发布回执的编号/轮次/结论，并据此判断本次采集数据是否可交给下游；
4. 检验未完成（无匹配规则或明细未落盘）时保留续检入口，绝不产生通过回执。

并发与重复确认：同一运行的确认按 run 维度加锁，已发布回执的轮次不会被重复执行，
重复确认后回执仍保持单份。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app.services.schema_check import TARGET_RUN_HISTORY, run_lock
from app import persistence as persist


def _schema_service():
    """延迟取用注册表中的单例，避免 services 包初始化期的循环导入。"""
    from app.services import SERVICES
    return SERVICES["schema_check"]


class RunHistoryService(BaseModuleService):
    MODULE = "run_history"
    ENTITY = "运行记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "关联计划", "开始时间", "结束时间", "采集行数", "失败行数", "执行日志", "异常摘要"]
    REQUIRED_FIELDS = ["记录编号", "关联计划", "开始时间"]
    STATUS_ORDER = ["运行中", "已完成", "部分成功", "已失败"]
    ACTION_RULES = {"确认完成": "已完成", "标记部分完成": "部分成功", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 动作
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action != "确认完成":
            return super().run_action(entry_id, action)

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        schema_svc = _schema_service()
        with run_lock(entry_id):
            existing = schema_svc.latest_receipt(entry_id)
            if existing is not None and existing.get("status") == "published":
                # 幂等：重复确认只刷新关联，不复制已发布回执、不发起新一轮检验。
                self._link_receipt(entry, existing)
                return entry, (
                    f"{self.ENTITY}已确认完成，检验回执{existing['receipt_id']}已存在"
                    f"（第{existing['round']}轮，结论{self._conclusion_label(existing['conclusion'])}），未重复检验"
                )

            # 首次确认（或上一次确认在回执落盘前失败）：先完成采集状态流转。
            result_entry, message = super().run_action(entry_id, action)
            self._run_dedup(result_entry)

            # 首轮检验固定为第1轮；落盘失败会抛出异常，运行停留在“已结束/待检验”。
            # 重试同一轮时复用首次固化的输入与规则版本，规则中途变更不改变原结论。
            receipt = schema_svc.run_check(
                result_entry,
                TARGET_RUN_HISTORY,
                run_id=entry_id,
                trigger="run_history_confirm",
                round_no=1,
                reuse_pending=True,
            )
            self._link_receipt(result_entry, receipt)
            message += f"，Schema检验第1轮结论：{self._conclusion_label(receipt['conclusion'])}"
            if receipt["conclusion"] == "fail":
                message += f"，违规{len(receipt['violations'])}项"
            elif receipt["conclusion"] == "incomplete":
                message += "，检验未完成，可通过续检入口补检"
            return result_entry, message

    # ------------------------------------------------------------------
    # 回执关联与数据可用性
    # ------------------------------------------------------------------
    def _link_receipt(self, entry: dict[str, Any], receipt: dict[str, Any]) -> None:
        """把持久化回执关联到运行记录，可用性只认真正落盘的 pass 回执。"""
        entry["schema_receipt_id"] = receipt["receipt_id"]
        entry["schema_round"] = receipt["round"]
        entry["schema_conclusion"] = receipt["conclusion"]
        entry["schema_violation_count"] = len(receipt.get("violations", []))
        entry["data_available"] = receipt.get("conclusion") == "pass" and receipt.get("status") == "published"
        entry["check_state"] = self._check_state(entry, receipt)

    @staticmethod
    def _conclusion_label(conclusion: str) -> str:
        return {"pass": "通过", "fail": "不通过", "incomplete": "未完成"}.get(conclusion, conclusion)

    def _check_state(self, entry: dict[str, Any], receipt: dict[str, Any] | None) -> str:
        """供运行负责人分辨：采集中 / 待续检 / 检验不通过 / 数据可用。"""
        if entry.get("status") == "运行中":
            return "采集中"
        if receipt is None or receipt.get("conclusion") == "incomplete":
            return "待续检"
        if receipt.get("conclusion") == "fail":
            return "检验不通过"
        if receipt.get("conclusion") == "pass":
            return "数据可用"
        return "待续检"

    def get_run_detail(self, entry_id: int) -> dict[str, Any] | None:
        """运行详情：运行记录 + 持久化回执清单 + 基于回执的可用性结论。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        schema_svc = _schema_service()
        receipts = schema_svc.list_receipts(entry_id)
        latest = receipts[-1] if receipts else None
        detail = dict(entry)
        detail["receipts"] = [
            {
                "receipt_id": r.get("receipt_id"),
                "round": r.get("round"),
                "conclusion": r.get("conclusion"),
                "trigger": r.get("trigger"),
                "status": r.get("status"),
                "violation_count": len(r.get("violations", [])),
                "rule_versions": r.get("rule_versions", []),
                "created_at": r.get("created_at"),
            }
            for r in receipts
        ]
        detail["latest_receipt_id"] = latest.get("receipt_id") if latest else None
        detail["check_state"] = self._check_state(entry, latest)
        # 可用性每次都以持久化回执为准重新计算，不信内存标记。
        detail["data_available"] = bool(
            latest and latest.get("status") == "published" and latest.get("conclusion") == "pass"
        )
        detail["resume_available"] = detail["check_state"] == "待续检"
        return detail

    def resume_schema_check(self, entry_id: int) -> dict[str, Any]:
        """续检入口：采集已结束但检验未完成时补一轮，结果作为新一轮回执单独记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            raise ValueError(f"{self.ENTITY} {entry_id} 不存在或已归档")
        if entry.get("status") == "运行中":
            raise ValueError("采集仍在运行中，需先确认完成再检验")
        schema_svc = _schema_service()
        with run_lock(entry_id):
            receipt = schema_svc.resume_check(entry_id)
            self._link_receipt(entry, receipt)
            return receipt

    def start_independent_check(self, entry_id: int) -> dict[str, Any]:
        """主动发起独立检验：使用当前最新规则另开一轮，不覆盖历史回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            raise ValueError(f"{self.ENTITY} {entry_id} 不存在或已归档")
        schema_svc = _schema_service()
        with run_lock(entry_id):
            receipt = schema_svc.run_check(
                entry, TARGET_RUN_HISTORY, run_id=entry_id, trigger="manual_independent"
            )
            self._link_receipt(entry, receipt)
            return receipt

    def recheck_receipt(self, entry_id: int, round_no: int) -> dict[str, Any]:
        """按原回执固化的输入与规则版本复核，规则中途被修改也应还原原结论。"""
        schema_svc = _schema_service()
        with run_lock(entry_id):
            return schema_svc.recheck_round(entry_id, round_no)

    # ------------------------------------------------------------------
    # 去重（保留原有跨模块副作用）
    # ------------------------------------------------------------------
    def _run_dedup(self, entry: dict[str, Any]) -> None:
        dedup_rows = store.rows("page_dedup")
        active_dedup = [d for d in dedup_rows if d.get("status") == "已生效"]
        if not active_dedup:
            return
        snap_files = [f for f in persist.list_files("snapshots", ".json") if f.startswith("page_")]
        if not snap_files:
            return
        pages_data = []
        for sf in snap_files[:5]:  # 采样前5份快照
            snap = persist.read_json("snapshots", sf)
            if snap:
                pages_data.append(snap)
        if not pages_data:
            return
        fingerprint = hashlib.md5(str(pages_data).encode()).hexdigest()[:16]
        persist.append_jsonl("reports", "dedup_report.jsonl", {
            "trigger": "run_history_confirm",
            "run_id": entry.get("id"),
            "timestamp": persist.now_iso(),
            "snapshots_checked": len(pages_data),
            "fingerprint": fingerprint,
        })

    # ------------------------------------------------------------------
    # 查询与快照
    # ------------------------------------------------------------------
    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的运行记录，优先从JSONL文件读取完整历史。"""
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        memory_runs = store.rows(self.MODULE)
        # 合并内存和文件中的记录，按时间排序
        combined = file_runs[-limit:] + [
            {
                "module": self.MODULE,
                "entity": self.ENTITY,
                "entry_id": r.get("id"),
                "status": r.get("status"),
                "version": r.get("version", ""),
                "action": r.get("status", ""),
                "timestamp": r.get("updated_at", ""),
                "abnormal": r.get("abnormal", False),
            } for r in memory_runs
        ]
        return combined[-limit:]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 文件中的运行记录
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        base["file_records"] = len(file_runs)
        base["file_abnormal"] = sum(1 for r in file_runs if r.get("abnormal"))
        # 关联的去重报告
        dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["triggered_dedup"] = len([r for r in dedup_reports if r.get("trigger") == "run_history_confirm"])
        # 关联的持久化 Schema 检验回执
        schema_svc = _schema_service()
        receipts = schema_svc._read_receipts()
        run_ids = {r.get("id") for r in store.rows(self.MODULE)}
        linked = [r for r in receipts if r.get("run_id") in run_ids]
        base["linked_schema_receipts"] = len(linked)
        base["schema_pass_runs"] = len({r["run_id"] for r in linked if r.get("conclusion") == "pass"})
        base["schema_fail_runs"] = len({r["run_id"] for r in linked if r.get("conclusion") == "fail"})
        base["schema_pending_runs"] = sum(
            1 for run in store.rows(self.MODULE)
            if self.get_run_detail(int(run["id"]))["check_state"] == "待续检"
        )
        base["data_available_runs"] = sum(
            1 for run in store.rows(self.MODULE)
            if self.get_run_detail(int(run["id"]))["data_available"]
        )
        return base
