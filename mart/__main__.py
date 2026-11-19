"""命令行入口。

子命令：
  build     [--all-actions | --steps N] [--version V]  构建不可变发布
  claim     --version V                                 领取固定集市版本
  verify    [--version V]                               哈希核对 + 真实表对账
  query     <name> [--version V] [--currency C]         受控查询入口
  sql       <sql> [--version V]                         在已发布真实表上直接执行 SQL
  versions                                            列出已登记发布
"""
from __future__ import annotations

import argparse
import json

from .build import build_release, claim_version, verify_release, claimed_version
from . import query as q


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="mart", description="回收运输费用分摊集市")
    p.add_argument("--store", default="mart_store", help="集市根目录（默认 mart_store）")
    p.add_argument("--inputs", default="inputs", help="原始资料目录（默认 inputs）")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="构建发布")
    g = b.add_mutually_exclusive_group()
    g.add_argument("--all-actions", action="store_true", help="应用 actions.json 全部动作")
    g.add_argument("--steps", type=int, default=None, help="只应用前 N 个动作")
    b.add_argument("--version", default=None)

    c = sub.add_parser("claim", help="领取固定版本")
    c.add_argument("--version", required=True)

    v = sub.add_parser("verify", help="核对发布并在真实表上对账")
    v.add_argument("--version", default=None)

    qp = sub.add_parser("query", help="受控查询")
    qp.add_argument("name", choices=[
        "material-cost", "trip-volume", "trip-combined", "audit-chain",
        "reconcile", "unallocated", "quarantine", "station-material-trip"])
    qp.add_argument("--version", default=None)
    qp.add_argument("--currency", default=None)
    qp.add_argument("--line", default=None)

    sp = sub.add_parser("sql", help="在发布真实表/视图上执行只读 SQL")
    sp.add_argument("sql_text")
    sp.add_argument("--version", default=None)

    sub.add_parser("versions", help="列出已登记发布")

    args = p.parse_args(argv)

    if args.cmd == "build":
        cutoff = None
        if args.all_actions:
            actions = json.loads(open(f"{args.inputs}/actions.json", encoding="utf-8").read())
            cutoff = max(a["step"] for a in actions)
        elif args.steps is not None:
            cutoff = args.steps
        _print(build_release(args.inputs, args.store, args.version, cutoff))
    elif args.cmd == "claim":
        _print(claim_version(args.store, args.version))
    elif args.cmd == "verify":
        _print(verify_release(args.store, args.version))
    elif args.cmd == "versions":
        from pathlib import Path
        reg = Path(args.store) / "registry.json"
        _print(json.loads(reg.read_text(encoding="utf-8")) if reg.exists() else {"releases": []})
    elif args.cmd == "query":
        con = q.connect(args.store, args.version)
        if args.name == "material-cost":
            _print(q.cost_by_material(con, args.currency))
        elif args.name == "trip-volume":
            _print(q.volume_by_trip(con))
        elif args.name == "trip-combined":
            _print(q.combined_trip_cost_and_volume(con))
        elif args.name == "audit-chain":
            _print(q.audit_chain(con, args.line))
        elif args.name == "reconcile":
            _print(q.independent_reconciliation(con))
        elif args.name == "unallocated":
            _print([dict(zip([d[0] for d in con.description], r)) for r in con.execute(
                "SELECT * FROM v_unallocated ORDER BY line, revision").fetchall()])
        elif args.name == "quarantine":
            _print([dict(zip([d[0] for d in con.description], r)) for r in con.execute(
                "SELECT * FROM v_quarantine ORDER BY event_id").fetchall()])
        elif args.name == "station-material-trip":
            _print([dict(zip([d[0] for d in con.description], r)) for r in con.execute(
                """SELECT 'cost' AS side, station, material, trip, currency,
                          allocated_minor AS measure
                   FROM v_cost_by_station_material_trip
                   UNION ALL
                   SELECT 'volume', station, material, trip, NULL, weight_scaled
                   FROM v_volume_by_station_material_trip
                   ORDER BY trip, material, side""").fetchall()])
    elif args.cmd == "sql":
        con = q.connect(args.store, args.version)
        cur = con.execute(args.sql_text)
        cols = [d[0] for d in con.description]
        _print([dict(zip(cols, r)) for r in cur.fetchall()])


if __name__ == "__main__":
    main()
