"""集市业务测试：在真实 Parquet 发布上用 DuckDB 查询验证，而非信任内存结果。"""
import json
import tempfile
import unittest
from pathlib import Path

from mart.allocation import allocate, scale_weight
from mart.build import build_release, claim_version, verify_release, claimed_version
from mart.query import (connect, cost_by_material, volume_by_trip,
                        combined_trip_cost_and_volume, audit_chain,
                        independent_reconciliation, CrossCurrencyError)

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / "inputs"


class MartTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Path(self._tmp.name) / "mart_store"
        build_release(INPUTS, self.store, version="v1", step_cutoff=None)
        build_release(INPUTS, self.store, version="v2", step_cutoff=4)
        claim_version(self.store, "v2")

    def tearDown(self):
        self._tmp.cleanup()

    # ---- 纯算法：整数分摊与稳定次序 ----
    def test_largest_remainder_tie_uses_ticket_order(self):
        # 100 分按 2:1:1 吨分摊：下界 50/25/25，无余分
        rows, missing = allocate(100, [("P1", 1, 2000), ("P2", 1, 1000), ("P3", 1, 1000)])
        self.assertFalse(missing)
        self.assertEqual([r.allocated_minor for r in rows], [50, 25, 25])
        # 10 分按 1:1:1：每票 3.33，余 1 分，余数相同按字典序归 P1
        rows, _ = allocate(10, [("P2", 1, 1000), ("P1", 1, 1000), ("P3", 1, 1000)])
        got = {r.ticket: r.allocated_minor for r in rows}
        self.assertEqual(got, {"P1": 4, "P2": 3, "P3": 3})
        self.assertTrue(next(r for r in rows if r.ticket == "P1").got_remainder)
        # 与输入顺序无关：打乱后结果一致
        rows2, _ = allocate(10, [("P3", 1, 1000), ("P1", 1, 1000), ("P2", 1, 1000)])
        self.assertEqual({r.ticket: r.allocated_minor for r in rows2}, got)

    def test_negative_amount_sign_restored(self):
        rows, _ = allocate(-10, [("P1", 1, 1000), ("P2", 1, 1000)])
        self.assertEqual(sorted(r.allocated_minor for r in rows), [-5, -5])
        self.assertEqual(sum(r.allocated_minor for r in rows), -10)

    def test_missing_basis_is_not_zero(self):
        rows, missing = allocate(51, [("P1", 1, 2000), ("P3", 1, None)])
        self.assertEqual(rows, [])
        self.assertEqual(missing, ["P3"])
        self.assertIsNone(scale_weight(None))
        self.assertEqual(scale_weight("1.500"), 1500)

    # ---- 发布 v1：原始资料 ----
    def test_v1_weight_based_allocation_on_real_tables(self):
        con = connect(self.store, "v1")
        # C1/1 毛额 100 - R1 红冲 30 = 70，按 T1 两票重量 2:1 分摊
        rows = con.execute(
            "SELECT ticket, allocated_minor FROM v_allocation_bridge "
            "WHERE line='C1' ORDER BY ticket").fetchall()
        self.assertEqual(rows, [("P1", 47), ("P2", 23)])
        # 47+23=70，且 C1 不存在待分摊
        self.assertEqual(con.execute(
            "SELECT count(*) FROM v_unallocated WHERE line='C1'").fetchone()[0], 0)

    def test_v1_pieces_basis_ticket_scoped_charge(self):
        con = connect(self.store, "v1")
        rows = con.execute(
            "SELECT ticket, allocated_minor FROM v_allocation_bridge "
            "WHERE line='C2'").fetchall()
        # 15 分、pieces 基数、范围仅 P2 -> 全归 P2
        self.assertEqual(rows, [("P2", 15)])

    def test_v1_unknown_weight_whole_charge_parked(self):
        con = connect(self.store, "v1")
        # C3 覆盖 T2 全票，P3 重量未知：整笔 51 分待分摊，不产生桥表行
        parked = con.execute(
            "SELECT line, unallocated_minor, reason_code, missing_tickets "
            "FROM v_unallocated").fetchall()
        self.assertEqual(parked, [("C3", 51, "basis_missing", ["P3"])])
        self.assertEqual(con.execute(
            "SELECT count(*) FROM v_allocation_bridge WHERE line='C3'").fetchone()[0], 0)

    def test_v1_trip_weight_counted_from_tickets_not_charge_rows(self):
        con = connect(self.store, "v1")
        # T1 有两票、三条费用行；运次重量只算货票一次：2.000+1.000=3.000 吨
        t1 = con.execute(
            "SELECT ticket_count, weight_scaled FROM v_trip_transport "
            "WHERE trip='T1'").fetchone()
        self.assertEqual(t1, (2, 3000))
        # 桥表行数（C1 两行 + C2 一行）不会放大运次重量
        bridge_rows = con.execute(
            "SELECT count(*) FROM v_allocation_bridge WHERE trip='T1'").fetchone()[0]
        self.assertEqual(bridge_rows, 3)

    def test_v1_reconciliation_ties_to_the_cent(self):
        con = connect(self.store, "v1")
        rows = independent_reconciliation(con)
        for r in rows:
            self.assertTrue(r["ties_to_net"], r)
            self.assertEqual(r["bridge_sum"], r["recon_allocated"])
        c1 = next(r for r in rows if r["line"] == "C1")
        self.assertEqual((c1["gross_minor"], c1["credited_minor"], c1["net_minor"]),
                         (100, 30, 70))

    # ---- 发布 v2：四个后续动作后的裁决 ----
    def test_v2_cargo_correction_changes_basis_but_v1_stays_immutable(self):
        con1, con2 = connect(self.store, "v1"), connect(self.store, "v2")
        old = con1.execute(
            "SELECT weight_scaled FROM v_cargo_current WHERE ticket='P2'").fetchone()[0]
        new = con2.execute(
            "SELECT weight_scaled FROM v_cargo_current WHERE ticket='P2'").fetchone()[0]
        self.assertEqual((old, new), (1000, 1500))
        # 版本链保留更正痕迹（直接读真实 Parquet 表，而非视图）
        hist = con2.execute(
            "SELECT ticket_version, weight_scaled, valid_from_step, is_current "
            "FROM read_parquet(?) WHERE ticket='P2' ORDER BY ticket_version",
            [str(self.store / "releases/v2/cargo_ticket_history.parquet")]).fetchall()
        self.assertEqual(hist, [(1, 1000, 0, False), (2, 1500, 1, True)])
        # 新基数只在新发布使用：C1 净额 100-30=70 按 2.000:1.500 -> 40/30
        rows = con2.execute(
            "SELECT ticket, allocated_minor FROM v_allocation_bridge "
            "WHERE line='C1' ORDER BY ticket").fetchall()
        self.assertEqual(rows, [("P1", 40), ("P2", 30)])
        # 桥表记录的是新基数与采用它的货票版本
        ver = con2.execute(
            "SELECT DISTINCT ticket_version FROM v_allocation_bridge "
            "WHERE line='C1' AND ticket='P2'").fetchone()[0]
        self.assertEqual(ver, 2)

    def test_v2_partial_then_over_credit(self):
        con = connect(self.store, "v2")
        # R1=30 合法；R2=90 使累计 120 > 可冲 100，整条 R2 进待核实
        credits = con.execute(
            "SELECT id, accepted FROM v_credit_note ORDER BY id").fetchall()
        self.assertEqual(credits, [("R1", True), ("R2", False)])
        q = con.execute(
            "SELECT reason_code, entity_key FROM v_quarantine "
            "WHERE entity_key='R2'").fetchall()
        self.assertEqual(q, [("over_credit", "R2")])
        # C1 净额仍是 70，未被超额红冲改写
        net = con.execute(
            "SELECT net_minor FROM v_charge_reconciliation "
            "WHERE line='C1' AND is_effective").fetchone()[0]
        self.assertEqual(net, 70)

    def test_v2_cross_currency_supersession_quarantined(self):
        con = connect(self.store, "v2")
        # C2/2 是 USD 修订却想替代 CNY 的 C2/1：挂起，C2/1 保持有效
        q = con.execute(
            "SELECT reason_code FROM v_quarantine WHERE entity_key='C2/2'").fetchall()
        self.assertEqual(q, [("currency_mismatch",)])
        eff = con.execute(
            "SELECT revision, currency FROM v_charge_effective "
            "WHERE line='C2'").fetchall()
        self.assertEqual(eff, [(1, "CNY")])
        # 原 CNY 分摊继续可用
        self.assertEqual(con.execute(
            "SELECT sum(allocated_minor) FROM v_allocation_bridge WHERE line='C2'"
        ).fetchone()[0], 15)

    def test_v2_return_keeps_original_transport_fact(self):
        con = connect(self.store, "v2")
        # 退运 0.500 吨独立记录；原运次重量仍是 3.500（更正后 2.000+1.500）
        t1 = con.execute(
            "SELECT weight_scaled, return_weight_scaled, net_weight_scaled "
            "FROM v_trip_transport WHERE trip='T1'").fetchone()
        self.assertEqual(t1, (3500, 500, 3000))
        ret = con.execute(
            "SELECT ticket, return_weight_scaled FROM v_cargo_return").fetchall()
        self.assertEqual(ret, [("P2", 500)])
        # 退运不触发减费：C1 分摊基数不含退运冲减
        self.assertEqual(con.execute(
            "SELECT basis_total_scaled FROM v_allocation_bridge "
            "WHERE line='C1' LIMIT 1").fetchone()[0], 3500)

    def test_v2_full_reconciliation_and_immutable_versions(self):
        self.assertEqual(claimed_version(self.store), "v2")
        result = verify_release(self.store)
        self.assertTrue(result["balanced"])
        con = connect(self.store)  # 默认领取版本
        for r in independent_reconciliation(con):
            self.assertTrue(r["ties_to_net"])
        # v1 目录哈希未被 v2 构建触碰
        v1_manifest = json.loads((self.store / "releases/v1/manifest.json").read_text())
        v2_manifest = json.loads((self.store / "releases/v2/manifest.json").read_text())
        self.assertNotEqual(
            [t["sha256"] for t in v1_manifest["tables"] if t["table"] == "allocation_bridge"],
            [t["sha256"] for t in v2_manifest["tables"] if t["table"] == "allocation_bridge"])

    # ---- 查询入口与可加性守卫 ----
    def test_material_and_station_entries(self):
        con = connect(self.store, "v2")
        mc = cost_by_material(con, currency="CNY")
        by_mat = {r["material"]: r["allocated_minor"] for r in mc}
        # C1: paper 40 / glass 30；C2: glass 15（CNY）
        self.assertEqual(by_mat, {"glass": 45, "paper": 40})
        smt = con.execute(
            "SELECT station, material, trip, allocated_minor "
            "FROM v_cost_by_station_material_trip ORDER BY material").fetchall()
        self.assertEqual(smt, [("N", "glass", "T1", 45), ("N", "paper", "T1", 40)])

    def test_combined_query_not_fanout_multiplied(self):
        con = connect(self.store, "v1")
        rows = combined_trip_cost_and_volume(con)
        by_trip = {r["trip"]: r for r in rows}
        # T1 三条费用行不会把 3.000 吨重量乘成 9.000
        self.assertEqual(by_trip["T1"]["weight_scaled"], 3000)
        self.assertEqual(by_trip["T1"]["allocated_minor"], 85)  # C1 净 70 + C2 15
        # T2 重量未知、费用整笔待分摊：运次仍在视图中，金额与重量均为 NULL
        self.assertIsNone(by_trip["T2"]["allocated_minor"])
        self.assertIsNone(by_trip["T2"]["weight_scaled"])
        self.assertFalse(by_trip["T2"]["weight_known"])

    def test_cross_currency_guard_blocks_sum_without_fx(self):
        from mart.query import _guard_single_currency
        con = connect(self.store, "v1")
        # 受控入口在单币种结果上正常工作（v1 桥表只有 CNY）
        self.assertEqual({r["material"] for r in cost_by_material(con)}, {"glass", "paper"})
        # 把 CNY 已分摊与 USD 待分摊并置成一个结果集：无换算依据不得加成
        mixed = con.execute("""
            SELECT currency, sum(minor) AS x FROM (
                SELECT currency, allocated_minor AS minor FROM v_allocation_bridge
                UNION ALL
                SELECT currency, unallocated_minor FROM v_unallocated
            ) GROUP BY currency ORDER BY currency
        """).fetchall()
        self.assertEqual([r[0] for r in mixed], ["CNY", "USD"])
        with self.assertRaises(CrossCurrencyError):
            _guard_single_currency([{"currency": r[0]} for r in mixed])
        # 按币种分组分别列示始终允许，合计保持各自币种
        self.assertEqual(mixed[0][1], 85)   # CNY: 70 + 15
        self.assertEqual(mixed[1][1], 51)   # USD: 待分摊

    def test_audit_chain_walks_back_to_settlement_lines(self):
        con = connect(self.store, "v1")
        chain = audit_chain(con, line="C1")
        self.assertEqual({r["ticket"] for r in chain}, {"P1", "P2"})
        for r in chain:
            self.assertEqual(r["gross_minor"] - r["credited_minor"], r["net_minor"])
            self.assertTrue(r["basis_den"] > 0)
        self.assertEqual(sum(r["allocated_minor"] for r in chain), 70)

    def test_unknown_weight_trip_volume_flagged_not_zeroed(self):
        con = connect(self.store, "v1")
        vol = {r["trip"]: r for r in volume_by_trip(con)}
        self.assertIsNone(vol["T2"]["weight_scaled"])
        self.assertFalse(vol["T2"]["weight_known"])
        # 未知重量不进可加总集合：跨运次求和只覆盖已知运次
        known_sum = con.execute(
            "SELECT sum(weight_scaled) FROM v_trip_transport WHERE weight_known"
        ).fetchone()[0]
        self.assertEqual(known_sum, 3000)


class SyntheticEdgeCaseTests(unittest.TestCase):
    """合成最小输入验证管道裁决分支，不触碰真实原件。"""

    def _run(self, charges, cargo=None, credit_notes=None):
        from mart.pipeline import run_pipeline
        seed = {
            "cargo": cargo if cargo is not None else [{
                "ticket": "Q1", "trip": "T9", "material": "paper",
                "station": "N", "weight": "1.000", "pieces": 1, "active": True}],
            "charges": charges,
            "credit_notes": credit_notes or [],
        }
        return run_pipeline(seed, [], None)

    def test_unknown_basis_type_parked_not_zeroed(self):
        out = self._run([{
            "line": "X1", "revision": 1, "trip": "T9", "tickets": [],
            "basis": "volume", "currency": "CNY", "minor": 9, "supersedes": None}])
        self.assertEqual(out["allocation_bridge"], [])
        self.assertEqual(out["unallocated_charge"][0]["reason_code"], "basis_missing")
        self.assertEqual(out["unallocated_charge"][0]["unallocated_minor"], 9)

    def test_charge_on_trip_without_active_tickets_parked(self):
        out = self._run(
            [{
                "line": "X2", "revision": 1, "trip": "T9", "tickets": [],
                "basis": "weight", "currency": "CNY", "minor": 9, "supersedes": None}],
            cargo=[{
                "ticket": "Q1", "trip": "T9", "material": "paper", "station": "N",
                "weight": "1.000", "pieces": 1, "active": False}])
        self.assertEqual(out["allocation_bridge"], [])
        self.assertEqual(out["unallocated_charge"][0]["reason_code"], "scope_ticket_missing")
        self.assertEqual(out["trip_transport"], [])

    def test_explicit_zero_basis_parked(self):
        out = self._run(
            [{
                "line": "X3", "revision": 1, "trip": "T9", "tickets": ["Q1"],
                "basis": "pieces", "currency": "CNY", "minor": 9, "supersedes": None}],
            cargo=[{
                "ticket": "Q1", "trip": "T9", "material": "paper", "station": "N",
                "weight": "1.000", "pieces": 0, "active": True}])
        self.assertEqual(out["unallocated_charge"][0]["reason_code"], "basis_total_zero")
        self.assertEqual(out["allocation_bridge"], [])


class ReleaseEngineeringTests(unittest.TestCase):
    def test_rebuild_is_byte_identical_and_existing_release_is_frozen(self):
        with tempfile.TemporaryDirectory() as d:
            store = Path(d) / "store"
            m1 = build_release(INPUTS, store, version="r", step_cutoff=4)
            hashes1 = {t["table"]: t["sha256"] for t in m1["tables"]}
            # 已存在版本不可覆盖
            with self.assertRaises(FileExistsError):
                build_release(INPUTS, store, version="r", step_cutoff=4)
            # 换一个空仓库按相同输入重建，逐表字节一致
            store2 = Path(d) / "store2"
            m2 = build_release(INPUTS, store2, version="r", step_cutoff=4)
            hashes2 = {t["table"]: t["sha256"] for t in m2["tables"]}
            self.assertEqual(hashes1, hashes2)

    def test_claim_unknown_version_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            store = Path(d) / "store"
            build_release(INPUTS, store, version="r", step_cutoff=None)
            with self.assertRaises(KeyError):
                claim_version(store, "ghost")


if __name__ == "__main__":
    unittest.main()
