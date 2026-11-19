# 混载回收运输的费用分摊桥表

本项目提供合成回收业务原件及物理检查入口。当前没有入仓、仓库发布或产品查询能力；检查通过只表示输入能读取，不表示动作已经处理。

使用 Python 3.11，依赖版本固定在 requirements.txt。安装命令为 python -m pip install -r requirements.txt。运行 python -m materials inputs 校验清单、摘要、Parquet行数并实际使用DuckDB读取；运行 python -m unittest discover -s tests -v 检查独立JSON值、原件损坏及业务材料形状。

可执行 docker build -t recycling-inputs .，再执行 docker run --rm recycling-inputs；测试命令为 docker run --rm recycling-inputs python -m unittest discover -s tests -v。环境不依赖外部业务站点。资料均为本地合成，不含真实个人、企业数据或凭据。

inputs/seed.json与同名Parquet是相同输入的两种载体；inputs/actions.json提供尚未应用的后续资料。docs/input-contract.md解释字段及原有业务约定。materials.physical只校验物理文件，不裁决身份、金额、匹配或有效版本。materials.build_inputs仅重建输入Parquet和物理清单，通常无需运行；不要把其产物误当未来业务结果。

## 回收运输集市（mart 包）

mart/ 在原始资料之上构建可维护、可查询的费用分摊集市：Python 负责裁决与整数分摊，DuckDB 负责查询，物理表为不可变 Parquet 发布。表目录、裁决原因码、可加性规则与查询视图见 docs/mart-contract.md。

```bash
python -m mart build --version release-seed-0000            # 只发布 seed
python -m mart build --all-actions --version release-actions-0004
python -m mart claim --version release-actions-0004         # 领取固定集市版本
python -m mart verify                                       # 哈希核对+真实表对账
python -m mart query reconcile                              # 独立核对各费用行
python -m mart query material-cost --currency CNY           # 按物料看分摊费用
python -m mart query trip-volume                            # 按运次看真实运输量
python -m mart query trip-combined                          # 同查询并置，先聚合后连接
python -m mart sql "SELECT * FROM v_audit_chain WHERE line='C1'"
```

关键约定：金额为最小货币单位整数，重量按千分之一吨缩放为整数，分摊用最大余数法、同余数按货票字典序；基数缺失整笔挂入待分摊而不是当 0；超额红冲、跨币种替代、无依据引用进待核实且不改写其他费用；不同币种无换算依据不得加成；退运独立记录，不抹掉原运次。产品输出默认位于 mart_store/（已在 .gitignore 忽略），可用 --store 指定目录；发布不可变，新资料只产生新版本。

产品输出使用独立目录；原始资料保留供回查。针对业务的检查见 tests/test_mart.py：全部断言在临时目录构建真实 Parquet 后用 DuckDB 查询执行，覆盖局部冲销、货票更正、跨币种守卫与版本不可变；已有输入检查不替代产品验收。
