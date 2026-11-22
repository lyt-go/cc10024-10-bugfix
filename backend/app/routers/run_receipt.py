"""运行历史 × Schema检验回执接口。

除通用资源路由外，运行记录需要围绕持久化检验回执提供确认、续检、
独立检验和按原版本复核的入口，并向运行负责人暴露可核对的检验明细。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas import ActionPayload, ActionResult
from app.services import service_for

router = APIRouter(prefix="/api/run-history", tags=["运行检验回执"])


def _run_service():
    service = service_for("run_history")
    if service is None:  # pragma: no cover - 注册表固定包含
        raise HTTPException(status_code=404, detail="运行历史模块未注册")
    return service


@router.post("/{run_id}/confirm", response_model=ActionResult)
def confirm_run(run_id: int, payload: ActionPayload) -> ActionResult:
    """确认采集完成并触发首轮Schema检验。

    检验明细写入失败时返回 ok=False 的结构化回执：采集状态已结束，
    但没有任何通过回执，运行停留在待续检，不能让异常冒泡成 500。
    """
    service = _run_service()
    if service.get_entry(run_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    try:
        entry, message = service.run_action(run_id, payload.action.strip() or "确认完成")
    except OSError as exc:
        return ActionResult(
            ok=False,
            message=(
                f"采集已确认结束，但Schema检验明细写入失败：{exc}；"
                "未产生检验回执，数据暂不可用，可通过续检入口重试本轮检验"
            ),
            entry=service.get_entry(run_id),
        )
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{run_id}/detail")
def run_detail(run_id: int) -> dict:
    """运行详情：采集状态、检验状态、回执清单与数据可用性一次看清。"""
    detail = _run_service().get_run_detail(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    return detail


@router.get("/{run_id}/receipts")
def list_receipts(run_id: int) -> dict:
    """列出该运行全部轮次的持久化检验回执（含违规明细、输入与规则版本快照）。"""
    service = _run_service()
    if service.get_entry(run_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    schema_svc = service_for("schema_check")
    return {"run_id": run_id, "items": schema_svc.list_receipts(run_id)}


@router.get("/{run_id}/receipts/{round_no}")
def get_receipt(run_id: int, round_no: int) -> dict:
    """读取指定轮次的单份回执。"""
    service = _run_service()
    if service.get_entry(run_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    receipt = service_for("schema_check").get_receipt(run_id, round_no)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"第{round_no}轮检验回执不存在")
    return receipt


@router.post("/{run_id}/resume-check")
def resume_check(run_id: int) -> dict:
    """续检入口：采集已结束但检验未完成时补检，结果作为新一轮回执单独记录。"""
    service = _run_service()
    if service.get_entry(run_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    try:
        receipt = service.resume_schema_check(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        return {"ok": False, "message": f"续检明细写入失败：{exc}；原回执未被覆盖，请稍后重试", "receipt": None}
    return {"ok": True, "message": "续检完成", "receipt": receipt}


@router.post("/{run_id}/independent-check")
def independent_check(run_id: int) -> dict:
    """主动发起独立检验：按当前最新规则另开一轮，历史回执保持不变。"""
    service = _run_service()
    if service.get_entry(run_id) is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在或已归档")
    try:
        receipt = service.start_independent_check(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        return {"ok": False, "message": f"独立检验明细写入失败：{exc}；历史回执保持不变", "receipt": None}
    return {"ok": True, "message": "独立检验完成", "receipt": receipt}


@router.post("/{run_id}/receipts/{round_no}/recheck")
def recheck_receipt(run_id: int, round_no: int) -> dict:
    """按原回执固化的被检输入与规则版本复核，结论须与原轮一致才可核对通过。"""
    try:
        result = _run_service().recheck_receipt(run_id, round_no)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "message": "复核完成", "recheck": result}
