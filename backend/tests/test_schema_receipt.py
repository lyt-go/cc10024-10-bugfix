"""Schema检验回执端到端测试。

复现任务场景：
1. 待确认运行记录的「采集行数」填为文本 bad，运行历史整数规则应判违规；
2. 为抽取模板另配一条无关生效规则，确认其不计入运行历史检验；
3. 打断首次明细写入（故障注入），确认无通过回执残留、运行停在待续检；
4. 同一运行并发两次确认，最终只发布单份回执；
5. 读运行详情后把规则改为文本再重新确认原轮，仍看到原整数版本违规；
6. 用新规则主动发起独立检验（新一轮单独记录），再按原版本复核原回执。
"""
from __future__ import annotations

import os
import sys
import threading

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist

# 每个用例都从干净的数据目录开始，避免历史 JSONL 干扰轮次编号。
for _sub in ["runs", "snapshots", "reports", "exports"]:
    _p = persist.data_path(_sub, "")
    if os.path.isdir(_p):
        for _f in os.listdir(_p):
            os.remove(os.path.join(_p, _f))


def _create_active_rule(svc, values):
    entry, missing = svc.create_entry(values)
    assert entry is not None, f"规则创建失败: {missing}"
    result, msg = svc.run_action(entry["id"], "标记生效")
    assert result is not None and result["status"] == "已生效", msg
    return result


def _cleanup_reports():
    for sub in ["runs", "snapshots", "reports", "exports"]:
        p = persist.data_path(sub, "")
        if os.path.isdir(p):
            for f in os.listdir(p):
                os.remove(os.path.join(p, f))


def _reset_target_rules(schema_svc, target="run_history"):
    """停用内存仓库里目标数据的遗留生效规则，保证各用例规则集独立。"""
    from app.store import store
    for rule in store.rows(schema_svc.MODULE):
        if rule.get("目标数据") == target and rule.get("status") == "已生效":
            rule["status"] = "已停用"
            rule["pending"] = True


def test_bad_row_count_fails_and_unrelated_rule_ignored():
    """bad 文本违反整数规则 → fail；抽取模板的无关规则不计入。"""
    _cleanup_reports()
    from app.services.run_history import RunHistoryService
    from app.services.schema_check import SchemaCheckService, TARGET_RUN_HISTORY

    schema_svc = SchemaCheckService()
    run_svc = RunHistoryService()
    _reset_target_rules(schema_svc)

    # 面向运行历史的整数规则
    int_rule = _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-INT",
        "目标数据": TARGET_RUN_HISTORY,
        "字段名": "采集行数",
        "类型约束": "int",
        "格式约束": "",
        "空值策略": "允许",
        "异常归类": "类型错误",
        "规则版本": "int-v1",
    })
    # 面向抽取模板的无关生效规则
    _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-EXTRACT",
        "目标数据": "data_extract",
        "字段名": "抽取字段",
        "类型约束": "str",
        "空值策略": "禁止",
        "规则版本": "extract-v1",
    })

    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-001",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T09:00:00",
        "采集行数": "bad",
    })
    run_id = run["id"]

    entry, message = run_svc.run_action(run_id, "确认完成")
    assert entry["status"] == "已完成"

    receipt = schema_svc.latest_receipt(run_id)
    assert receipt is not None
    assert receipt["conclusion"] == "fail", "bad 文本必须判违规，不能伪通过"
    assert receipt["round"] == 1
    # 只计入 run_history 目标的规则
    targets = {r["目标数据"] for r in receipt["rules_applied"]}
    assert targets == {TARGET_RUN_HISTORY}
    assert len(receipt["rules_applied"]) == 1
    used_rule = receipt["rules_applied"][0]
    assert used_rule["规则版本"] == "int-v1"
    assert used_rule["类型约束"] == "int"
    # 违规字段、实际值与实际类型都可核对
    assert len(receipt["violations"]) == 1
    violation = receipt["violations"][0]
    assert violation["field"] == "采集行数"
    assert violation["实际值"] == "bad"
    assert violation["实际类型"] == "str"
    assert violation["规则版本"] == "int-v1"
    # 被检输入快照保留原始 bad 值
    assert receipt["input_snapshot"]["采集行数"] == "bad"
    assert "不通过" in message

    # fail 回执不得让数据对下游可用
    detail = run_svc.get_run_detail(run_id)
    assert detail["check_state"] == "检验不通过"
    assert detail["data_available"] is False
    assert detail["latest_receipt_id"] == receipt["receipt_id"]


def test_write_fault_leaves_no_pass_and_retry_uses_original_version():
    """首次明细写入被打断：无通过回执；改规则后重试仍按原整数版本判违规。"""
    _cleanup_reports()
    from app.services.schema_check import SchemaCheckService, arm_write_fault
    from app.services.run_history import RunHistoryService

    schema_svc = SchemaCheckService()
    run_svc = RunHistoryService()
    _reset_target_rules(schema_svc)

    int_rule = _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-FAULT",
        "目标数据": "run_history",
        "字段名": "采集行数",
        "类型约束": "int",
        "空值策略": "允许",
        "规则版本": "int-v1",
    })

    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-FAULT",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T10:00:00",
        "采集行数": "bad",
    })
    run_id = run["id"]

    # 打断首次明细写入
    arm_write_fault(run_id, times=1)
    raised = False
    try:
        run_svc.run_action(run_id, "确认完成")
    except OSError:
        raised = True
    assert raised, "明细写入报错必须向上抛出，不能吞掉后伪通过"

    # 采集已结束、检验未完成；没有任何已发布回执，更没有通过回执
    run_row = run_svc.get_entry(run_id)
    assert run_row["status"] == "已完成"
    assert schema_svc.latest_receipt(run_id) is None
    published = persist.read_jsonl("reports", "schema_check_receipts.jsonl")
    assert all(r.get("run_id") != run_id for r in published)
    detail = run_svc.get_run_detail(run_id)
    assert detail["check_state"] == "待续检"
    assert detail["data_available"] is False
    assert detail["resume_available"] is True
    # 待发布清单已固化原输入与原规则版本
    pending = schema_svc._open_pending(run_id, 1)
    assert pending is not None
    assert pending["input_snapshot"]["采集行数"] == "bad"
    assert pending["rules_applied"][0]["规则版本"] == "int-v1"

    # 两次确认之间有人把规则改成文本并停用原整数规则
    int_rule["类型约束"] = "str"
    int_rule["规则版本"] = "text-v2"
    schema_svc.run_action(int_rule["id"], "停用规则")

    # 重新确认原轮：沿用待发布清单的原整数版本，仍应看到原违规
    entry, message = run_svc.run_action(run_id, "确认完成")
    assert entry is not None
    receipt = schema_svc.latest_receipt(run_id)
    assert receipt["round"] == 1
    assert receipt["conclusion"] == "fail"
    assert receipt["rules_applied"][0]["规则版本"] == "int-v1"
    assert receipt["rules_applied"][0]["类型约束"] == "int"
    assert receipt["violations"][0]["field"] == "采集行数"
    assert receipt["input_snapshot"]["采集行数"] == "bad"
    assert "int-v1" in message or "不通过" in message
    # 待发布清单已消费，回执只发布一次
    assert schema_svc._open_pending(run_id, 1) is None


def test_concurrent_confirmations_publish_single_receipt():
    """同一运行并发两次确认：串行化后只产生单份已发布回执。"""
    _cleanup_reports()
    from app.services.schema_check import SchemaCheckService, arm_write_fault
    from app.services.run_history import RunHistoryService

    schema_svc = SchemaCheckService()
    run_svc = RunHistoryService()
    _reset_target_rules(schema_svc)
    _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-CONC",
        "目标数据": "run_history",
        "字段名": "采集行数",
        "类型约束": "int",
        "空值策略": "允许",
        "规则版本": "int-v1",
    })
    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-CONC",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T11:00:00",
        "采集行数": 120,
    })
    run_id = run["id"]

    # 首个线程在明细写入处失败一次，模拟并发下第一次确认写明细报错
    arm_write_fault(run_id, times=1)
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, object]] = []

    def confirm(tag: str):
        barrier.wait()
        try:
            entry, msg = run_svc.run_action(run_id, "确认完成")
            outcomes.append((tag, ("ok", msg)))
        except OSError as exc:
            outcomes.append((tag, ("fault", str(exc))))

    t1 = threading.Thread(target=confirm, args=("first",))
    t2 = threading.Thread(target=confirm, args=("second",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    tags = {tag: kind for tag, (kind, _) in outcomes}
    assert sorted(tags.values()) == ["fault", "ok"], outcomes

    receipts = schema_svc.list_receipts(run_id)
    assert len(receipts) == 1, f"并发确认后回执必须单份，实际 {len(receipts)}"
    receipt = receipts[0]
    assert receipt["round"] == 1 and receipt["status"] == "published"
    assert receipt["conclusion"] == "pass"
    detail = run_svc.get_run_detail(run_id)
    assert detail["data_available"] is True
    assert detail["check_state"] == "数据可用"

    # 再次重复确认：不新增轮次、不复制回执
    run_svc.run_action(run_id, "确认完成")
    assert len(schema_svc.list_receipts(run_id)) == 1


def test_independent_check_is_new_round_and_recheck_uses_original():
    """新规则发起独立检验单独成轮；复核原回执还原原整数版本结论。"""
    _cleanup_reports()
    from app.services.schema_check import SchemaCheckService
    from app.services.run_history import RunHistoryService

    schema_svc = SchemaCheckService()
    run_svc = RunHistoryService()
    _reset_target_rules(schema_svc)
    rule = _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-ROUND",
        "目标数据": "run_history",
        "字段名": "采集行数",
        "类型约束": "int",
        "空值策略": "允许",
        "规则版本": "int-v1",
    })
    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-ROUND",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T12:00:00",
        "采集行数": "bad",
    })
    run_id = run["id"]
    run_svc.run_action(run_id, "确认完成")

    first = schema_svc.get_receipt(run_id, 1)
    assert first["conclusion"] == "fail"
    round_one = first["rules_applied"]
    assert len(round_one) == 1
    assert round_one[0]["规则编号"] == "E2E-SCHE-ROUND"
    assert round_one[0]["规则版本"] == "int-v1"

    # 规则改为文本后，用新规则主动发起独立检验 → 第2轮，单独记录且通过
    rule["类型约束"] = "str"
    rule["规则版本"] = "text-v2"
    second = run_svc.start_independent_check(run_id)
    assert second["round"] == 2
    assert second["trigger"] == "manual_independent"
    # 本轮实际使用的规则就是改后的文本版本（只核对本用例登记的那条）
    applied = [r for r in second["rules_applied"] if r["规则编号"] == "E2E-SCHE-ROUND"]
    assert len(applied) == 1
    assert applied[0]["规则版本"] == "text-v2"
    assert applied[0]["类型约束"] == "str"
    # 本用例规则在文本约束下对 "bad" 无违规
    own_violations = [v for v in second["violations"] if v["规则编号"] == "E2E-SCHE-ROUND"]
    assert own_violations == []
    assert second["conclusion"] == "pass"

    # 两份回执并存，原轮结论不被覆盖
    receipts = schema_svc.list_receipts(run_id)
    assert [r["round"] for r in receipts] == [1, 2]
    assert schema_svc.get_receipt(run_id, 1)["conclusion"] == "fail"

    # 复核原回执：按原输入与原规则版本，仍判 fail，且与原结论一致
    recheck = run_svc.recheck_receipt(run_id, 1)
    assert recheck["conclusion"] == "fail"
    assert recheck["matches_original"] is True
    recheck_rule = recheck["rules_applied"][0]
    assert recheck_rule["规则编号"] == "E2E-SCHE-ROUND"
    assert recheck_rule["规则版本"] == "int-v1"
    assert recheck_rule["类型约束"] == "int"
    assert recheck["violations"][0]["field"] == "采集行数"
    assert recheck["input_snapshot"]["采集行数"] == "bad"

    # 运行详情同时列出两轮，最新轮决定可用性
    detail = run_svc.get_run_detail(run_id)
    assert [r["round"] for r in detail["receipts"]] == [1, 2]
    assert detail["data_available"] is True
    assert detail["check_state"] == "数据可用"


def test_no_matching_rule_is_incomplete_not_pass():
    """无匹配目标的生效规则时不得给通过回执，保留续检入口。"""
    _cleanup_reports()
    from app.services.schema_check import SchemaCheckService
    from app.services.run_history import RunHistoryService

    schema_svc = SchemaCheckService()
    run_svc = RunHistoryService()
    _reset_target_rules(schema_svc)
    # 只有其他目标的规则
    _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-OTHER",
        "目标数据": "data_extract",
        "字段名": "抽取字段",
        "类型约束": "str",
        "空值策略": "禁止",
        "规则版本": "other-v1",
    })
    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-EMPTY",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T13:00:00",
        "采集行数": 10,
    })
    run_id = run["id"]
    run_svc.run_action(run_id, "确认完成")

    receipt = schema_svc.latest_receipt(run_id)
    assert receipt["conclusion"] == "incomplete"
    assert receipt["rules_applied"] == []
    detail = run_svc.get_run_detail(run_id)
    assert detail["check_state"] == "待续检"
    assert detail["data_available"] is False

    # 补上运行历史规则后续检，产生新一轮并通过
    _create_active_rule(schema_svc, {
        "规则编号": "E2E-SCHE-LATE",
        "目标数据": "run_history",
        "字段名": "采集行数",
        "类型约束": "int",
        "空值策略": "允许",
        "规则版本": "int-v1",
    })
    resumed = run_svc.resume_schema_check(run_id)
    assert resumed["round"] == 2
    assert resumed["conclusion"] == "pass"
    assert run_svc.get_run_detail(run_id)["data_available"] is True


def test_running_state_distinguished_from_pending_check():
    """运行中 = 采集中；确认后才进入检验态，三种状态可分辨。"""
    _cleanup_reports()
    from app.services.run_history import RunHistoryService

    run_svc = RunHistoryService()
    run, _ = run_svc.create_entry({
        "记录编号": "E2E-RUN-STATES",
        "关联计划": "E2E-PLAN",
        "开始时间": "2026-11-22T14:00:00",
        "采集行数": "bad",
    })
    run_id = run["id"]
    assert run_svc.get_run_detail(run_id)["check_state"] == "采集中"

    # 续检采集中的运行应被拒绝
    blocked = False
    try:
        run_svc.resume_schema_check(run_id)
    except ValueError:
        blocked = True
    assert blocked


if __name__ == "__main__":
    test_bad_row_count_fails_and_unrelated_rule_ignored()
    print("PASS: test_bad_row_count_fails_and_unrelated_rule_ignored")
    test_write_fault_leaves_no_pass_and_retry_uses_original_version()
    print("PASS: test_write_fault_leaves_no_pass_and_retry_uses_original_version")
    test_concurrent_confirmations_publish_single_receipt()
    print("PASS: test_concurrent_confirmations_publish_single_receipt")
    test_independent_check_is_new_round_and_recheck_uses_original()
    print("PASS: test_independent_check_is_new_round_and_recheck_uses_original")
    test_no_matching_rule_is_incomplete_not_pass()
    print("PASS: test_no_matching_rule_is_incomplete_not_pass")
    test_running_state_distinguished_from_pending_check()
    print("PASS: test_running_state_distinguished_from_pending_check")
    print("\nALL 6 SCHEMA RECEIPT E2E TESTS PASSED")
