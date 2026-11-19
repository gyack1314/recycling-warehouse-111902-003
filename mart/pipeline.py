"""集市构建管道：读取原件、逐步应用动作、裁决待核实并产出物理表行集。

管道只做纯内存数据处理，不直接写文件；写入与发布由 build.py 负责，
这样同一份输入可以确定性地重建任意发布版本。
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import date

from .allocation import allocate, scale_weight

# 待核实原因码
OVER_CREDIT = "over_credit"                # 同币种累计红冲超过可冲金额
CURRENCY_MISMATCH = "currency_mismatch"    # 跨币种替代/红冲，无换算依据
REF_MISSING = "ref_missing"                # 引用的费用行/修订或货票不存在
DUP_REVISION = "dup_revision"              # 同 line/revision 重复
SCOPE_TICKET_MISSING = "scope_ticket_missing"
BASIS_MISSING = "basis_missing"            # 声明范围内存在基数缺失
BASIS_TOTAL_ZERO = "basis_total_zero"      # 基数显式全为 0，无精确比例
RETURN_TICKET_MISSING = "return_ticket_missing"


def _parse_target(target: str) -> tuple[str, int]:
    line, _, rev = target.partition("/")
    return line, int(rev)


@dataclass
class _CargoVersion:
    version: int
    row: dict
    valid_from_step: int
    valid_to_step: int | None = None


@dataclass
class _Charge:
    row: dict
    source_step: int
    accepted: bool = True
    superseded_at_step: int | None = None


@dataclass
class _State:
    cargo: dict[str, list[_CargoVersion]] = field(default_factory=dict)
    returns: list[dict] = field(default_factory=list)
    charges: dict[tuple[str, int], _Charge] = field(default_factory=dict)
    credits: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)
    credit_used: dict[tuple[str, int], int] = field(default_factory=dict)
    _event_seq: int = 0

    def quarantine_event(self, step: int, table: str, key: str, reason: str, detail: str) -> None:
        self._event_seq += 1
        self.quarantine.append({
            "event_id": self._event_seq,
            "source_step": step,
            "entity_table": table,
            "entity_key": key,
            "reason_code": reason,
            "detail": detail,
        })


def run_pipeline(seed: dict, actions: list[dict], step_cutoff: int | None = None) -> dict[str, list[dict]]:
    """从 seed 出发应用 actions（step <= step_cutoff），返回全部物理表行集。

    输入一律深拷贝：调用方可能复用同一 JSON 对象连续构建多个发布，
    管道的任何状态变化都不得泄漏回输入。
    """
    st = _State()
    _ingest_seed(st, copy.deepcopy(seed))
    for action in sorted(copy.deepcopy(actions), key=lambda a: a["step"]):
        # cutoff 为 None 表示只发布 seed，一个后续动作都不应用。
        if step_cutoff is None or action["step"] > step_cutoff:
            continue
        _apply_action(st, action)
    return _materialize(st)


def _ingest_seed(st: _State, seed: dict) -> None:
    for row in seed["cargo"]:
        st.cargo[row["ticket"]] = [_CargoVersion(1, dict(row), 0)]
    for row in seed["charges"]:
        st.charges[(row["line"], row["revision"])] = _Charge(dict(row), 0)
    for row in seed["credit_notes"]:
        _accept_credit(st, dict(row), 0)


def _apply_action(st: _State, action: dict) -> None:
    step, table, op = action["step"], action["table"], action["op"]
    if table == "cargo" and op == "replace":
        row = copy.deepcopy(action["row"])
        ticket = action["key"]
        if ticket not in st.cargo:
            st.quarantine_event(step, "cargo", ticket, REF_MISSING,
                                "replace 指向不存在的货票，已挂起待核实")
            return
        chain = st.cargo[ticket]
        chain[-1].valid_to_step = step - 1
        chain.append(_CargoVersion(chain[-1].version + 1, row, step))
    elif table == "cargo" and op == "return":
        ticket = action["key"]
        chain = st.cargo.get(ticket)
        if not chain or chain[-1].row["trip"] != action["trip"]:
            st.quarantine_event(step, "cargo", ticket, RETURN_TICKET_MISSING,
                                f"退运引用的货票/运次不存在: {ticket}@{action['trip']}")
            return
        st.returns.append({
            "return_id": len(st.returns) + 1,
            "ticket": ticket,
            "trip": action["trip"],
            "return_weight_text": action["weight"],
            "return_weight_scaled": scale_weight(action["weight"]),
            "effective_date": date.fromisoformat(action["effective"]),
            "source_step": step,
        })
    elif table == "charges" and op == "append":
        row = copy.deepcopy(action["row"])
        key = (row["line"], row["revision"])
        if key in st.charges:
            st.quarantine_event(step, "charges", f"{row['line']}/{row['revision']}",
                                DUP_REVISION, "费用修订身份重复，拒绝并入")
            return
        target_line, target_rev = _parse_target(row["supersedes"])
        target = st.charges.get((target_line, target_rev))
        if target is None or not target.accepted:
            st.quarantine_event(step, "charges", f"{row['line']}/{row['revision']}",
                                REF_MISSING, f"supersedes 引用无有效依据: {row['supersedes']}")
            return
        if target_line != row["line"]:
            st.quarantine_event(step, "charges", f"{row['line']}/{row['revision']}",
                                REF_MISSING, "supersedes 必须指向同一费用行")
            return
        if target.row["currency"] != row["currency"]:
            # 跨币种替代没有换算依据，不能让两个币种的修订混在一条费用线上。
            st.quarantine_event(step, "charges", f"{row['line']}/{row['revision']}",
                                CURRENCY_MISMATCH,
                                f"替代修订币种 {row['currency']} 与原修订 "
                                f"{target.row['currency']} 不一致，挂起待核实")
            return
        st.charges[key] = _Charge(row, step)
        target.superseded_at_step = step
    elif table == "credit_notes" and op == "append":
        _accept_credit(st, dict(action["row"]), step)
    else:
        st.quarantine_event(step, table, str(action.get("key", "")), REF_MISSING,
                            f"未识别的动作: {table}/{op}")


def _accept_credit(st: _State, row: dict, step: int) -> None:
    row = copy.deepcopy(row)
    line, rev = _parse_target(row["target"])
    target = st.charges.get((line, rev))
    record = {"id": row["id"], "line": line, "revision": rev,
              "currency": row["currency"], "minor": row["minor"],
              "source_step": step}
    if target is None or not target.accepted:
        record["accepted"] = False
        st.credits.append(record)
        st.quarantine_event(step, "credit_notes", row["id"], REF_MISSING,
                            f"红冲目标不存在或已挂起: {row['target']}")
        return
    if target.row["currency"] != row["currency"]:
        record["accepted"] = False
        st.credits.append(record)
        st.quarantine_event(step, "credit_notes", row["id"], CURRENCY_MISMATCH,
                            f"红冲币种 {row['currency']} 与费用币种 "
                            f"{target.row['currency']} 不一致")
        return
    used = st.credit_used.get((line, rev), 0)
    if used + row["minor"] > target.row["minor"]:
        record["accepted"] = False
        st.credits.append(record)
        st.quarantine_event(step, "credit_notes", row["id"], OVER_CREDIT,
                            f"同币种累计红冲 {used + row['minor']} 超过可冲金额 "
                            f"{target.row['minor']}（{row['target']}），该红冲不影响其他行")
        return
    record["accepted"] = True
    st.credits.append(record)
    st.credit_used[(line, rev)] = used + row["minor"]


def _current_cargo(st: _State) -> dict[str, _CargoVersion]:
    return {ticket: chain[-1] for ticket, chain in st.cargo.items()}


def _materialize(st: _State) -> dict[str, list[dict]]:
    tables: dict[str, list[dict]] = {name: [] for name in (
        "cargo_ticket_history", "cargo_return", "charge_revision", "credit_note",
        "quarantine_event", "allocation_bridge", "unallocated_charge",
        "charge_reconciliation", "trip_transport",
    )}
    tables["quarantine_event"] = list(st.quarantine)
    tables["cargo_return"] = list(st.returns)

    # 货票版本链
    for ticket, chain in sorted(st.cargo.items()):
        for v in chain:
            r = v.row
            tables["cargo_ticket_history"].append({
                "ticket": ticket,
                "ticket_version": v.version,
                "trip": r["trip"],
                "material": r["material"],
                "station": r["station"],
                "weight_text": r["weight"],
                "weight_scaled": scale_weight(r["weight"]),
                "pieces": r["pieces"],
                "active": r["active"],
                "valid_from_step": v.valid_from_step,
                "valid_to_step": v.valid_to_step,
                "is_current": v.valid_to_step is None,
            })

    # 费用修订链
    effective_keys = set()
    for (line, rev), ch in sorted(st.charges.items()):
        if not ch.accepted:
            continue
        is_effective = ch.superseded_at_step is None
        if is_effective:
            effective_keys.add((line, rev))
        tables["charge_revision"].append({
            "line": line, "revision": rev, "trip": ch.row["trip"],
            "tickets": list(ch.row["tickets"]), "basis": ch.row["basis"],
            "currency": ch.row["currency"], "minor": ch.row["minor"],
            "supersedes": ch.row["supersedes"],
            "is_effective": is_effective, "source_step": ch.source_step,
            "superseded_at_step": ch.superseded_at_step,
        })

    tables["credit_note"] = [{
        "id": c["id"], "line": c["line"], "revision": c["revision"],
        "trip": st.charges[(c["line"], c["revision"])].row["trip"]
        if (c["line"], c["revision"]) in st.charges else None,
        "currency": c["currency"], "minor": c["minor"],
        "accepted": c["accepted"], "source_step": c["source_step"],
    } for c in sorted(st.credits, key=lambda c: (c["source_step"], c["id"]))]

    current = _current_cargo(st)
    current_by_trip: dict[str, list[str]] = {}
    for ticket, v in current.items():
        if v.row["active"]:
            current_by_trip.setdefault(v.row["trip"], []).append(ticket)

    # 有效费用修订的红冲汇总与分摊
    credited = {}
    for c in st.credits:
        if c["accepted"]:
            credited[(c["line"], c["revision"])] = (
                credited.get((c["line"], c["revision"]), 0) + c["minor"])

    for (line, rev), ch in sorted(st.charges.items()):
        if not ch.accepted:
            continue
        row = ch.row
        gross = row["minor"]
        cred = credited.get((line, rev), 0)
        net = gross - cred
        recon = {
            "line": line, "revision": rev, "trip": row["trip"],
            "currency": row["currency"], "gross_minor": gross,
            "credited_minor": cred, "net_minor": net,
            "allocated_minor": 0, "unallocated_minor": 0,
            "is_effective": (line, rev) in effective_keys,
        }
        if not recon["is_effective"]:
            # 被替代的修订不在当前发布分摊；余额由同币种替代修订承接。
            recon["balanced"] = True
            tables["charge_reconciliation"].append(recon)
            continue

        scope = list(row["tickets"]) if row["tickets"] else sorted(current_by_trip.get(row["trip"], []))
        scope_records = []
        if not scope:
            # 运次没有任何当前有效货票：不存在分摊基数，整笔挂账而非按 0 处理。
            _park_unallocated(tables, row, scope, gross, cred, net,
                              SCOPE_TICKET_MISSING, [],
                              "费用声明的运次没有当前有效货票，无可分摊基数")
            recon["unallocated_minor"] = net
            recon["balanced"] = False
            tables["charge_reconciliation"].append(recon)
            continue
        bad_scope = [t for t in scope if t not in current or current[t].row["trip"] != row["trip"]]
        if bad_scope:
            _park_unallocated(tables, row, scope, gross, cred, net,
                              SCOPE_TICKET_MISSING, sorted(bad_scope),
                              "分摊范围引用了不属于本运次或不存在的货票")
            recon["unallocated_minor"] = net
            recon["balanced"] = False
            tables["charge_reconciliation"].append(recon)
            continue

        missing = []
        for ticket in scope:
            cr = current[ticket].row
            if row["basis"] == "weight":
                basis = scale_weight(cr["weight"])
            elif row["basis"] == "pieces":
                basis = cr["pieces"]
            else:
                # 未声明的业务基数类型按未知处理，不当作 0。
                basis = None
            if basis is None:
                missing.append(ticket)
            scope_records.append((ticket, current[ticket].version, basis))

        if missing:
            # 缺失基数不是 0：整笔保留为待分摊，不排除未知票后偷偷归一。
            _park_unallocated(tables, row, scope, gross, cred, net,
                              BASIS_MISSING, sorted(missing),
                              f"基数 {row['basis']} 在范围内货票上缺失，缺失不是 0")
            recon["unallocated_minor"] = net
            recon["balanced"] = False
            tables["charge_reconciliation"].append(recon)
            continue

        alloc_rows, zero_tickets = allocate(net, scope_records)
        if zero_tickets:
            _park_unallocated(tables, row, scope, gross, cred, net,
                              BASIS_TOTAL_ZERO, zero_tickets,
                              "范围基数合计为 0，不存在精确比例")
            recon["unallocated_minor"] = net
            recon["balanced"] = False
            tables["charge_reconciliation"].append(recon)
            continue

        total_allocated = 0
        for a in alloc_rows:
            cr = current[a.ticket].row
            tables["allocation_bridge"].append({
                "line": line, "revision": rev, "trip": row["trip"],
                "tickets_scope": list(scope), "basis": row["basis"],
                "currency": row["currency"], "gross_minor": gross,
                "credited_minor": cred, "net_minor": net,
                "ticket": a.ticket, "ticket_version": a.ticket_version,
                "material": cr["material"], "station": cr["station"],
                "basis_value_scaled": a.basis_value_scaled,
                "basis_total_scaled": a.basis_total_scaled,
                "basis_num": a.basis_num, "basis_den": a.basis_den,
                "floor_minor": a.floor_minor,
                "remainder_scaled": a.remainder_scaled,
                "remainder_rank": a.remainder_rank,
                "got_remainder": a.got_remainder,
                "allocated_minor": a.allocated_minor,
            })
            total_allocated += a.allocated_minor
        recon["allocated_minor"] = total_allocated
        recon["balanced"] = total_allocated == net
        tables["charge_reconciliation"].append(recon)

    tables["trip_transport"] = _build_trip_transport(st, current, current_by_trip)
    return tables


def _park_unallocated(tables, row, scope, gross, cred, net, reason, missing_tickets, detail):
    tables["unallocated_charge"].append({
        "line": row["line"], "revision": row["revision"], "trip": row["trip"],
        "tickets_scope": list(scope), "basis": row["basis"],
        "currency": row["currency"], "gross_minor": gross,
        "credited_minor": cred, "unallocated_minor": net,
        "reason_code": reason, "missing_tickets": missing_tickets,
        "detail": detail,
    })


def _build_trip_transport(st, current, current_by_trip) -> list[dict]:
    return_weight: dict[str, int] = {}
    for ret in st.returns:
        return_weight[ret["trip"]] = return_weight.get(ret["trip"], 0) + ret["return_weight_scaled"]

    out = []
    for trip in sorted(current_by_trip):
        tickets = sorted(current_by_trip[trip])
        weights = [scale_weight(current[t].row["weight"]) for t in tickets]
        pieces = [current[t].row["pieces"] for t in tickets]
        weight_known = all(w is not None for w in weights)
        pieces_known = all(p is not None for p in pieces)
        weight_sum = sum(weights) if weight_known else None
        pieces_sum = sum(pieces) if pieces_known else None
        ret_sum = return_weight.get(trip, 0)
        out.append({
            "trip": trip,
            "ticket_count": len(tickets),
            "weight_known": weight_known,
            "weight_scaled": weight_sum,
            "pieces_known": pieces_known,
            "pieces": pieces_sum,
            "return_weight_scaled": ret_sum,
            "net_weight_scaled": (weight_sum - ret_sum) if weight_known else None,
        })
    return out
