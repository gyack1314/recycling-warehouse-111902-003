-- 集市查询视图。参数 :release_dir 由 query.py 绑定为已领取发布的物理目录。
-- 所有视图直接读取 Parquet 真实表；视图只读、不落地，发布内容保持不可变。

-- 货票当前态（运输量查询入口，grain=货票）
CREATE OR REPLACE VIEW v_cargo_current AS
SELECT ticket, ticket_version, trip, material, station,
       weight_text, weight_scaled, pieces, active
FROM read_parquet(:release_dir || '/cargo_ticket_history.parquet')
WHERE is_current AND active;

-- 运次运输量（grain=运次；重量只来自有效货票，与费用行无连接）
CREATE OR REPLACE VIEW v_trip_transport AS
SELECT * FROM read_parquet(:release_dir || '/trip_transport.parquet');

-- 退运独立可查，不抹掉原运次
CREATE OR REPLACE VIEW v_cargo_return AS
SELECT * FROM read_parquet(:release_dir || '/cargo_return.parquet');

-- 有效费用修订（结算行入口，grain=费用行×修订）
CREATE OR REPLACE VIEW v_charge_effective AS
SELECT line, revision, trip, tickets, basis, currency, minor, supersedes, source_step
FROM read_parquet(:release_dir || '/charge_revision.parquet')
WHERE is_effective;

-- 红冲及裁决状态
CREATE OR REPLACE VIEW v_credit_note AS
SELECT * FROM read_parquet(:release_dir || '/credit_note.parquet');

-- 待核实事件
CREATE OR REPLACE VIEW v_quarantine AS
SELECT * FROM read_parquet(:release_dir || '/quarantine_event.parquet');

-- 分摊桥表（grain=费用修订×货票；保留精确基数与余数裁决）
CREATE OR REPLACE VIEW v_allocation_bridge AS
SELECT * FROM read_parquet(:release_dir || '/allocation_bridge.parquet');

-- 待分摊金额（独立可查，不允许均摊给已到仓的票）
CREATE OR REPLACE VIEW v_unallocated AS
SELECT * FROM read_parquet(:release_dir || '/unallocated_charge.parquet');

-- 费用行对账（沿分摊行回查结算行的总账）
CREATE OR REPLACE VIEW v_charge_reconciliation AS
SELECT * FROM read_parquet(:release_dir || '/charge_reconciliation.parquet');

-- 按物料的分摊费用：先在桥表内聚合，不与货票明细做明细连接
CREATE OR REPLACE VIEW v_allocation_by_material AS
SELECT material, currency,
       count(DISTINCT line || '/' || revision) AS charge_lines,
       count(DISTINCT ticket)                  AS tickets,
       sum(allocated_minor)                    AS allocated_minor
FROM read_parquet(:release_dir || '/allocation_bridge.parquet')
GROUP BY 1, 2;

-- 按站点的分摊费用
CREATE OR REPLACE VIEW v_allocation_by_station AS
SELECT station, currency,
       count(DISTINCT line || '/' || revision) AS charge_lines,
       sum(allocated_minor)                    AS allocated_minor
FROM read_parquet(:release_dir || '/allocation_bridge.parquet')
GROUP BY 1, 2;

-- 站点×物料×运次组合查询入口（费用侧，先聚合到该粒度）
CREATE OR REPLACE VIEW v_cost_by_station_material_trip AS
SELECT station, material, trip, currency,
       sum(allocated_minor) AS allocated_minor
FROM read_parquet(:release_dir || '/allocation_bridge.parquet')
GROUP BY 1, 2, 3, 4;

-- 站点×物料×运次组合查询入口（运输量侧，独立从货票聚合）
CREATE OR REPLACE VIEW v_volume_by_station_material_trip AS
SELECT station, material, trip,
       count(*) AS ticket_count,
       CASE WHEN bool_and(weight_scaled IS NOT NULL)
            THEN sum(weight_scaled) END AS weight_scaled,
       CASE WHEN bool_and(pieces IS NOT NULL)
            THEN sum(pieces) END       AS pieces
FROM read_parquet(:release_dir || '/cargo_ticket_history.parquet')
WHERE is_current AND active
GROUP BY 1, 2, 3;

-- 同查询内并置费用与运输量：两侧各自先聚合到运次粒度再 FULL OUTER JOIN，
-- 杜绝费用行与货票明细直接连接导致的行数/重量放大。
-- 所有运次都保留：重量未知或费用整笔待分摊时对应列为 NULL，而不是静默消失。
CREATE OR REPLACE VIEW v_trip_cost_and_volume AS
WITH cost AS (
    SELECT trip, currency, sum(allocated_minor) AS allocated_minor
    FROM read_parquet(:release_dir || '/allocation_bridge.parquet')
    GROUP BY 1, 2
),
vol AS (
    SELECT trip, weight_known, weight_scaled
    FROM read_parquet(:release_dir || '/trip_transport.parquet')
)
SELECT coalesce(cost.trip, vol.trip) AS trip,
       cost.currency,
       cost.allocated_minor,
       vol.weight_known,
       vol.weight_scaled
FROM cost FULL OUTER JOIN vol ON cost.trip = vol.trip;

-- 结算行 -> 分摊行回查链
CREATE OR REPLACE VIEW v_audit_chain AS
SELECT r.line, r.revision, r.currency,
       r.gross_minor, r.credited_minor, r.net_minor,
       r.allocated_minor AS recon_allocated_minor,
       r.unallocated_minor,
       b.ticket, b.ticket_version, b.basis,
       b.basis_value_scaled, b.basis_total_scaled,
       b.basis_num, b.basis_den,
       b.floor_minor, b.remainder_scaled, b.remainder_rank,
       b.got_remainder, b.allocated_minor
FROM read_parquet(:release_dir || '/charge_reconciliation.parquet') r
LEFT JOIN read_parquet(:release_dir || '/allocation_bridge.parquet') b
       ON r.line = b.line AND r.revision = b.revision
WHERE r.is_effective;
