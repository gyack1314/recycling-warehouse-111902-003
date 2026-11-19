"""DuckDB 查询入口：在已领取发布上加载视图并提供受控查询函数。

金额可加性规则（见 docs/mart-contract.md）：
- 同币种的 minor 金额可以相加；不同币种没有换算依据，禁止加成一个金额；
- 重量只有 weight_known=真 时可相加，未知基数不参与求和；
- 费用与运输量必须各自聚合后再连接，不能拿桥表明细直接乘货票重量。
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from .build import claimed_version

_VIEWS_SQL = Path(__file__).with_name("views.sql")


class CrossCurrencyError(ValueError):
    """未选择换算依据时试图把多币种加成单一金额。"""


def connect(store_dir: str | Path, version: str | None = None) -> duckdb.DuckDBPyConnection:
    store_dir = Path(store_dir).resolve()
    version = version or claimed_version(store_dir)
    release_dir = (store_dir / "releases" / version).resolve()
    if not release_dir.is_dir():
        raise KeyError(f"发布目录不存在: {version}")
    con = duckdb.connect()
    # DuckDB 的 CREATE VIEW 不接受绑定参数，这里只做 SQL 字符串内字面量替换；
    # 路径来自本地已登记发布目录，单引号按 SQL 字面量规则转义。
    literal = "'" + release_dir.as_posix().replace("'", "''") + "'"
    sql = _VIEWS_SQL.read_text(encoding="utf-8").replace(":release_dir", literal)
    for stmt in [s.strip() for s in stmt_split(sql)]:
        if stmt:
            con.execute(stmt)
    return con


def stmt_split(sql: str) -> list[str]:
    """按分号切语句，跳过单引号字符串内的分号。"""
    out, buf, in_str = [], [], False
    for ch in sql:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    out.append("".join(buf))
    return out


def _guard_single_currency(rows, currency_col: str = "currency"):
    currencies = {r[currency_col] for r in rows if r[currency_col] is not None}
    if len(currencies) > 1:
        raise CrossCurrencyError(
            f"结果包含多个币种 {sorted(currencies)}，未选择换算依据，不能加成一个金额；"
            "请按 currency 分组分别列示")


def _dicts(con, sql: str, params: list | None = None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def cost_by_material(con, currency: str | None = None) -> list[dict]:
    sql = "SELECT material, currency, charge_lines, tickets, allocated_minor FROM v_allocation_by_material"
    params = []
    if currency:
        sql += " WHERE currency = ?"
        params.append(currency)
    rows = _dicts(con, sql + " ORDER BY material, currency", params)
    if currency is None:
        _guard_single_currency(rows)
    return rows


def volume_by_trip(con) -> list[dict]:
    return _dicts(con, """
        SELECT trip, ticket_count, weight_known, weight_scaled,
               pieces_known, pieces, return_weight_scaled, net_weight_scaled
        FROM v_trip_transport ORDER BY trip""")


def combined_trip_cost_and_volume(con) -> list[dict]:
    """费用与运输量同查询并置：两侧已在视图内各自聚合，按币种分行。"""
    rows = _dicts(con, """
        SELECT trip, currency, allocated_minor, weight_known, weight_scaled
        FROM v_trip_cost_and_volume ORDER BY trip, currency""")
    _guard_single_currency(rows)
    return rows


def audit_chain(con, line: str | None = None) -> list[dict]:
    """沿分摊行回查结算行，并独立核对已分摊+待分摊=净额。"""
    sql = "SELECT * FROM v_audit_chain"
    params = []
    if line:
        sql += " WHERE line = ?"
        params.append(line)
    return _dicts(con, sql + " ORDER BY line, ticket", params)


def independent_reconciliation(con) -> list[dict]:
    """独立于构建管道：只对真实 Parquet 表做分组对账。"""
    return _dicts(con, """
        WITH b AS (
            SELECT line, revision, sum(allocated_minor) AS bridge_allocated
            FROM v_allocation_bridge GROUP BY 1, 2
        ),
        u AS (
            SELECT line, revision, sum(unallocated_minor) AS parked
            FROM v_unallocated GROUP BY 1, 2
        )
        SELECT r.line, r.revision, r.currency,
               r.gross_minor, r.credited_minor, r.net_minor,
               r.allocated_minor AS recon_allocated,
               coalesce(b.bridge_allocated, 0) AS bridge_sum,
               r.unallocated_minor AS recon_unallocated,
               coalesce(u.parked, 0) AS unallocated_sum,
               (coalesce(b.bridge_allocated, 0) + coalesce(u.parked, 0) = r.net_minor)
                   AS ties_to_net,
               r.balanced
        FROM v_charge_reconciliation r
        LEFT JOIN b USING (line, revision)
        LEFT JOIN u USING (line, revision)
        WHERE r.is_effective
        ORDER BY r.line, r.revision""")
