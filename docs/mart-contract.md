# 回收运输集市契约（Mart Contract）

本文件说明 `mart/` 产出的物理表、裁决规则与查询入口。原始资料契约见
[input-contract.md](input-contract.md)，本文件不改变其中任何业务约定。

## 1. 技术形态与版本模型

- 物理表为 Parquet（zstd），处理与裁决用纯 Python 标准库 + pyarrow，查询用 DuckDB。
- 一次构建产出一个**不可变发布**：`mart_store/releases/<version>/`，每表一个 Parquet，
  配套 `manifest.json` 记录行数、字节数与 sha256；`registry.json` 登记全部发布，
  `claim.json` 记录使用者领取的固定版本。
- 发布一旦写入即拒绝覆盖；后续资料只能产生新发布。旧发布的 Parquet 字节不变，
  因此旧分摊永远不会与新基数混用。
- 金额一律为币种最小单位整数（`*_minor`，CNY/USD 均为分）；重量以
  `weight_scaled = 吨 × 1000` 的整数参与运算，原始字符串保留在 `weight_text`。
  全程不使用浮点。

```bash
python -m mart build --version release-seed-0000          # 只含 seed
python -m mart build --all-actions --version release-actions-0004
python -m mart claim   --version release-actions-0004     # 领取固定版本
python -m mart verify                                      # 哈希核对 + 真实表对账
python -m mart versions
```

## 2. 物理表目录（9 张表，各自独立 grain）

| 表 | grain | 作用 |
| --- | --- | --- |
| `cargo_ticket_history` | 货票 × 版本 | 货票 SCD2 版本链，`valid_from_step/valid_to_step` 标注适用动作区间，`is_current` 标当前态 |
| `cargo_return` | 退运记录 | 独立返程运输量；不修改、不抹掉原运次与原分摊基数 |
| `charge_revision` | 费用行 × 修订 | 全部修订保留；`is_effective` 标当前有效，被替代修订留痕但不参与当前分摊 |
| `credit_note` | 红冲 id | 合法红冲 `accepted=true`；被拒红冲以 `accepted=false` 保留并同步进待核实 |
| `quarantine_event` | 待核实事件 | 超额红冲、跨币种替代、无依据引用等，只挂起问题实体，不改写其他有效费用 |
| `allocation_bridge` | 有效费用修订 × 范围内货票 | 分摊桥表，保留精确基数、整数下界、余数与余数裁决次序 |
| `unallocated_charge` | 有效费用修订 | 暂时分不下去的金额整笔挂账，独立可查，不均摊给已到仓的票 |
| `charge_reconciliation` | 费用行 × 修订 × 币种 | 毛额−红冲=净额=已分摊+待分摊的对账总账 |
| `trip_transport` | 运次 | 运次真实运输量，只按有效货票求和，与费用行零连接 |

### 待核实原因码

| reason_code | 触发条件 | 处理 |
| --- | --- | --- |
| `over_credit` | 同币种累计红冲超过该修订可冲金额 | 该红冲整条拒绝，已接受红冲与其他费用行不受影响 |
| `currency_mismatch` | 替代修订或红冲与目标修订币种不一致 | 挂起；原修订保持有效，两币种不互相替代 |
| `ref_missing` | supersedes/红冲/退运引用的行、修订或货票不存在 | 挂起引用方 |
| `scope_ticket_missing` | 费用声明的票不属于本运次或不存在 | 费用有效但整笔进待分摊 |
| `basis_missing` | 声明基数（weight/pieces）在范围内某票缺失 | 整笔待分摊；**缺失不是 0，不排除未知票后重新归一** |
| `basis_total_zero` | 范围内基数显式合计为 0，无精确比例 | 整笔待分摊 |
| `dup_revision` / `return_ticket_missing` | 修订身份重复 / 退运票运次不符 | 挂起 |

## 3. 分摊规则（与 input-contract 对齐）

1. 分摊目标 = 费用行毛额 − 同币种合法红冲累计，按修订的 `basis` 声明取基数。
2. `tickets` 为空表示运次全部**当前有效**货票；非空则只针对所列票。
3. 最大余数法全程整数：先按 `abs(净额)×基数 // 基数合计` 取下界，余分按余数
   降序分派；余数相同按 `ticket` 字典序（稳定货票顺序），再还原正负号。
   桥表保留 `floor_minor / remainder_scaled / remainder_rank / got_remainder`，
   每一分都可解释，不依赖浮点误差。
4. 红冲可以只撤销一条费用的一部分（R1=30/100）；超额部分（R2=90 使累计 120>100）
   整条进待核实，净额不被冲成负数。
5. 退运只增加 `cargo_return` 与运次净重量列，原运次重量、原分摊基数不变；
   货票载重错误只能由 cargo `replace` 更正，费用变化只能由修订/红冲声明。

## 4. 可加性规则（哪些重量与金额可以相加）

- **金额**：同一 `currency` 内的 minor 可任意相加；不同币种没有换算依据，
  **禁止加成一个金额**。受控入口遇到多币种聚合会抛 `CrossCurrencyError`，
  正确做法是按 `currency` 分组分别列示。
- **重量**：`weight_known=true` 的运次/货票才可相加；`weight_scaled IS NULL`
  表示未知，不是 0，求和时整票/整运次排除并在结果中保留未知标记。
- **件数**：同重量规则，看 `pieces_known`。
- **费用与运输量禁止明细连接**：桥表是“费用行×货票”粒度，直接连货票会把一票重量
  重复成费用行数。视图 `v_trip_cost_and_volume` 与
  `v_*_by_station_material_trip` 均先各自聚合到共同粒度再 FULL OUTER JOIN。
  反例自检：T1 有 3 条桥表行，运次重量仍是 3.500 吨而不是 10.500 吨。

## 5. 查询入口（DuckDB 视图，见 mart/views.sql）

| 视图/入口 | 回答的问题 |
| --- | --- |
| `v_cargo_current` / `v_trip_transport` | 按货票/运次看真实运输量 |
| `v_cargo_return` | 退运量独立查询 |
| `v_charge_effective` / `v_credit_note` | 结算行与红冲裁决 |
| `v_allocation_bridge` / `v_allocation_by_material` | 按物料看分摊费用（含精确基数） |
| `v_allocation_by_station` | 按站点看分摊费用 |
| `v_cost_by_station_material_trip` / `v_volume_by_station_material_trip` | 站点×物料×运次组合入口（费用侧/运输量侧分开） |
| `v_trip_cost_and_volume` | 同一查询内并置费用与运次重量，聚合后连接，不放大 |
| `v_unallocated` / `v_quarantine` | 待分摊金额与待核实事件 |
| `v_charge_reconciliation` | 费用行对账总账 |
| `v_audit_chain` | 从结算行沿桥表逐票回查，含基数与余数裁决 |

CLI 查询：

```bash
python -m mart query material-cost --currency CNY
python -m mart query trip-volume
python -m mart query trip-combined
python -m mart query audit-chain --line C1
python -m mart query reconcile
python -m mart query unallocated
python -m mart query quarantine
python -m mart query station-material-trip
python -m mart sql "SELECT ... FROM v_audit_chain WHERE ..."   # 任意只读 SQL
```

独立核对（不经过构建内存结果，只查 Parquet）：

```bash
python -m mart verify      # 清单哈希 + 净额恒等式 + 桥表汇总一致性
python -m mart query reconcile
```

`tests/test_mart.py` 的全部断言都在临时目录中构建真实 Parquet 后通过 DuckDB
执行，覆盖：局部红冲与超额红冲、货票更正后的基数切换、跨币种替代挂起、
退运不减费、未知基数整笔挂账、同余数字典序分派、跨币种守卫与发布字节确定性。
