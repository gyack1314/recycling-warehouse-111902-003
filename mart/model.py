"""物理表定义：Parquet schema 与表名清单。

设计原则：
- 金额列全部为 int64，语义为币种最小单位（minor）；
- 重量以 weight_scaled（int64，1000 倍缩放）精确表示，原字符串保留为 weight_text；
- 每张事实表只承载一个 grain，禁止把费用行与货票明细直接连接后落地；
- 发布版本不写进数据行，版本由发布目录与 manifest 承载，保证表内容不可变。
"""
from __future__ import annotations

import pyarrow as pa

# 维度/标识统一字符串，状态用布尔，整数金额与基数避免任何精度损失。
_TICKET = pa.field("ticket", pa.string(), nullable=False)
_TRIP = pa.field("trip", pa.string(), nullable=False)
_MATERIAL = pa.field("material", pa.string(), nullable=False)
_STATION = pa.field("station", pa.string(), nullable=False)
_LINE = pa.field("line", pa.string(), nullable=False)
_REVISION = pa.field("revision", pa.int32(), nullable=False)
_CURRENCY = pa.field("currency", pa.string(), nullable=False)
_MINOR = pa.field("minor", pa.int64(), nullable=False)

SCHEMAS: dict[str, pa.Schema] = {
    # 货票版本链（SCD2，按 step 维护有效范围）；退运不改写本链。
    "cargo_ticket_history": pa.schema([
        _TICKET,
        pa.field("ticket_version", pa.int32(), nullable=False),
        _TRIP,
        _MATERIAL,
        _STATION,
        pa.field("weight_text", pa.string(), nullable=True),
        pa.field("weight_scaled", pa.int64(), nullable=True),   # None=重量未知，不是 0
        pa.field("pieces", pa.int64(), nullable=True),          # None=件数未知，不是 0
        pa.field("active", pa.bool_(), nullable=False),
        pa.field("valid_from_step", pa.int32(), nullable=False),
        pa.field("valid_to_step", pa.int32(), nullable=True),
        pa.field("is_current", pa.bool_(), nullable=False),
    ]),
    # 退运：独立的返程运输记录，不抹掉原运次。
    "cargo_return": pa.schema([
        pa.field("return_id", pa.int32(), nullable=False),
        _TICKET,
        _TRIP,
        pa.field("return_weight_text", pa.string(), nullable=False),
        pa.field("return_weight_scaled", pa.int64(), nullable=False),
        pa.field("effective_date", pa.date32(), nullable=False),
        pa.field("source_step", pa.int32(), nullable=False),
    ]),
    # 费用行×修订（同一 line 的替代链保留全部修订）。
    "charge_revision": pa.schema([
        _LINE,
        _REVISION,
        _TRIP,
        pa.field("tickets", pa.list_(pa.string()), nullable=False),  # 空数组=运次全票
        pa.field("basis", pa.string(), nullable=False),
        _CURRENCY,
        _MINOR,
        pa.field("supersedes", pa.string(), nullable=True),
        pa.field("is_effective", pa.bool_(), nullable=False),
        pa.field("source_step", pa.int32(), nullable=False),
        pa.field("superseded_at_step", pa.int32(), nullable=True),
    ]),
    # 红冲：按 id 留存，被拒红冲仍以 accepted=false 保留可查。
    "credit_note": pa.schema([
        pa.field("id", pa.string(), nullable=False),
        _LINE,
        _REVISION,
        _TRIP,
        _CURRENCY,
        _MINOR,
        pa.field("accepted", pa.bool_(), nullable=False),
        pa.field("source_step", pa.int32(), nullable=False),
    ]),
    # 待核实事件：异常红冲、跨币种替代、无依据引用等，不改写其他有效费用。
    "quarantine_event": pa.schema([
        pa.field("event_id", pa.int32(), nullable=False),
        pa.field("source_step", pa.int32(), nullable=False),
        pa.field("entity_table", pa.string(), nullable=False),
        pa.field("entity_key", pa.string(), nullable=False),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("detail", pa.string(), nullable=False),
    ]),
    # 分摊桥表：grain = 有效费用修订 × 范围内货票。
    "allocation_bridge": pa.schema([
        _LINE,
        _REVISION,
        _TRIP,
        pa.field("tickets_scope", pa.list_(pa.string()), nullable=False),
        pa.field("basis", pa.string(), nullable=False),
        _CURRENCY,
        pa.field("gross_minor", pa.int64(), nullable=False),
        pa.field("credited_minor", pa.int64(), nullable=False),
        pa.field("net_minor", pa.int64(), nullable=False),
        _TICKET,
        pa.field("ticket_version", pa.int32(), nullable=False),
        _MATERIAL,
        _STATION,
        pa.field("basis_value_scaled", pa.int64(), nullable=False),
        pa.field("basis_total_scaled", pa.int64(), nullable=False),
        pa.field("basis_num", pa.int64(), nullable=False),
        pa.field("basis_den", pa.int64(), nullable=False),
        pa.field("floor_minor", pa.int64(), nullable=False),
        pa.field("remainder_scaled", pa.int64(), nullable=False),
        pa.field("remainder_rank", pa.int32(), nullable=False),
        pa.field("got_remainder", pa.bool_(), nullable=False),
        pa.field("allocated_minor", pa.int64(), nullable=False),
    ]),
    # 待分摊：有效费用暂时分不下去的金额，独立可查，不均摊给已到仓的票。
    "unallocated_charge": pa.schema([
        _LINE,
        _REVISION,
        _TRIP,
        pa.field("tickets_scope", pa.list_(pa.string()), nullable=False),
        pa.field("basis", pa.string(), nullable=False),
        _CURRENCY,
        pa.field("gross_minor", pa.int64(), nullable=False),
        pa.field("credited_minor", pa.int64(), nullable=False),
        pa.field("unallocated_minor", pa.int64(), nullable=False),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("missing_tickets", pa.list_(pa.string()), nullable=False),
        pa.field("detail", pa.string(), nullable=False),
    ]),
    # 费用行对账：按费用行×修订×币种，已分摊+待分摊=扣红冲后净额。
    "charge_reconciliation": pa.schema([
        _LINE,
        _REVISION,
        _TRIP,
        _CURRENCY,
        pa.field("gross_minor", pa.int64(), nullable=False),
        pa.field("credited_minor", pa.int64(), nullable=False),
        pa.field("net_minor", pa.int64(), nullable=False),
        pa.field("allocated_minor", pa.int64(), nullable=False),
        pa.field("unallocated_minor", pa.int64(), nullable=False),
        pa.field("is_effective", pa.bool_(), nullable=False),
        pa.field("balanced", pa.bool_(), nullable=False),
    ]),
    # 运次运输量事实：grain = 运次；重量只按有效货票求和，与费用行无关。
    "trip_transport": pa.schema([
        _TRIP,
        pa.field("ticket_count", pa.int64(), nullable=False),
        pa.field("weight_known", pa.bool_(), nullable=False),
        pa.field("weight_scaled", pa.int64(), nullable=True),
        pa.field("pieces_known", pa.bool_(), nullable=False),
        pa.field("pieces", pa.int64(), nullable=True),
        pa.field("return_weight_scaled", pa.int64(), nullable=False),
        pa.field("net_weight_scaled", pa.int64(), nullable=True),
    ]),
}

TABLE_NAMES = tuple(SCHEMAS)
