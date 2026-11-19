"""精确整数分摊算法。

规则（见 docs/input-contract.md）：
- 先对绝对金额取整数下界，余分按余数降序分派；
- 余数相同时按货票 ticket 字典序（资料中的稳定货票顺序）；
- 最后还原正负号；
- 基数缺失不是 0：只要范围内存在缺失基数的货票，调用方应整笔挂入待分摊，
  本模块不做任何排除后重新归一。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

WEIGHT_SCALE = 1000


def scale_weight(value) -> int | None:
    """把吨数字符串/Decimal 精确缩放为整数（毫克吨，即千分之一吨）。

    None 表示基数未知，原样返回 None，绝不转成 0。
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = Decimal(value)
    elif isinstance(value, int):
        return value * WEIGHT_SCALE
    scaled = value * Decimal(WEIGHT_SCALE)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"重量精度超过 {WEIGHT_SCALE} 分之一吨，无法精确缩放: {value}")
    return int(scaled)


@dataclass(frozen=True)
class AllocationRow:
    ticket: str
    ticket_version: int
    basis_value_scaled: int
    basis_total_scaled: int
    basis_num: int          # 精确比例分子（即本票基数）
    basis_den: int          # 精确比例分母（即范围基数合计）
    floor_minor: int
    remainder_scaled: int   # abs(金额)*基数 除以基数合计后的余数
    remainder_rank: int     # 余分分派次序，从 1 开始；未参与分派为 NULL 语义的 -1
    got_remainder: bool
    allocated_minor: int


def allocate(amount_minor: int, items: list[tuple[str, int, int | None]]):
    """最大余数法分摊。

    items 为 (ticket, ticket_version, basis_scaled) 三元组，basis_scaled 为 None
    表示该票基数未知。返回 (rows, missing_tickets)；存在缺失或基数合计为 0 时
    rows 为空，missing_tickets 给出无法分派的货票。
    """
    if not items:
        return [], []
    missing = sorted(t for t, _v, b in items if b is None)
    if missing:
        return [], missing
    total = sum(b for _t, _v, b in items)
    if total <= 0:
        # 基数全部为 0 时不存在精确比例，按不可分派处理，而不是均摊。
        return [], sorted(t for t, _v, _b in items)

    abs_amount = abs(amount_minor)
    floors: dict[str, int] = {}
    rems: dict[str, int] = {}
    for ticket, _ver, basis in items:
        product = abs_amount * basis
        floors[ticket] = product // total
        rems[ticket] = product % total

    leftover = abs_amount - sum(floors.values())
    # 余数降序，余数相同按 ticket 字典序——与输入顺序无关，结果稳定。
    order = sorted(items, key=lambda x: (-rems[x[0]], x[0]))
    winners = {ticket for ticket, _v, _b in order[:leftover]}
    rank = {ticket: i + 1 for i, (ticket, _v, _b) in enumerate(order)}

    sign = -1 if amount_minor < 0 else 1
    rows = []
    for ticket, version, basis in sorted(items, key=lambda x: x[0]):
        got = ticket in winners
        allocated = (floors[ticket] + (1 if got else 0)) * sign
        rows.append(
            AllocationRow(
                ticket=ticket,
                ticket_version=version,
                basis_value_scaled=basis,
                basis_total_scaled=total,
                basis_num=basis,
                basis_den=total,
                floor_minor=floors[ticket] * sign,
                remainder_scaled=rems[ticket],
                remainder_rank=rank[ticket],
                got_remainder=got,
                allocated_minor=allocated,
            )
        )
    return rows, []
