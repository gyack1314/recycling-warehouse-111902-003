"""发布构建：管道行集 -> 不可变 Parquet 发布目录 + 清单哈希 + 领取登记。

目录布局：
  mart_store/releases/<version>/<table>.parquet
  mart_store/releases/<version>/manifest.json
  mart_store/registry.json
  mart_store/claim.json         （使用者领取的固定版本）
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .model import SCHEMAS, TABLE_NAMES
from .pipeline import run_pipeline


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _file_digest(path: Path) -> dict:
    data = path.read_bytes()
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def default_version(step_cutoff: int | None) -> str:
    if step_cutoff is None:
        return "release-seed-0000"
    return f"release-actions-{step_cutoff:04d}"


def build_release(inputs_dir: str | Path, store_dir: str | Path,
                  version: str | None = None,
                  step_cutoff: int | None = None) -> dict:
    inputs_dir = Path(inputs_dir).resolve()
    store_dir = Path(store_dir).resolve()
    seed = json.loads((inputs_dir / "seed.json").read_text(encoding="utf-8"))
    actions = json.loads((inputs_dir / "actions.json").read_text(encoding="utf-8"))
    version = version or default_version(step_cutoff)

    release_dir = store_dir / "releases" / version
    if release_dir.exists():
        raise FileExistsError(f"发布版本已存在且不可变: {version}")
    release_dir.mkdir(parents=True)

    tables = run_pipeline(seed, actions, step_cutoff)
    table_manifests = []
    for name in TABLE_NAMES:
        rows = tables[name]
        table = pa.Table.from_pylist(rows, schema=SCHEMAS[name])
        target = release_dir / f"{name}.parquet"
        pq.write_table(table, target, row_group_size=max(1, len(rows)), compression="zstd")
        digest = _file_digest(target)
        table_manifests.append({
            "table": name, "rows": len(rows),
            "bytes": digest["bytes"], "sha256": digest["sha256"],
        })

    manifest = {
        "version": version,
        "step_cutoff": step_cutoff,
        "inputs": {
            "seed.json": _file_digest(inputs_dir / "seed.json"),
            "actions.json": _file_digest(inputs_dir / "actions.json"),
        },
        "tables": table_manifests,
    }
    (release_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    registry_path = store_dir / "registry.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8")) if registry_path.exists() else {"releases": []}
    if any(r["version"] == version for r in registry["releases"]):
        raise FileExistsError(f"登记中已存在版本: {version}")
    registry["releases"].append({
        "version": version,
        "path": str(release_dir.relative_to(store_dir)),
        "step_cutoff": step_cutoff,
        "manifest_sha256": _sha256(release_dir / "manifest.json"),
        "table_count": len(table_manifests),
    })
    registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def claim_version(store_dir: str | Path, version: str) -> dict:
    store_dir = Path(store_dir).resolve()
    registry = json.loads((store_dir / "registry.json").read_text(encoding="utf-8"))
    if not any(r["version"] == version for r in registry["releases"]):
        raise KeyError(f"未登记的发布版本，无法领取: {version}")
    claim = {"version": version}
    (store_dir / "claim.json").write_text(
        json.dumps(claim, ensure_ascii=False, indent=2), encoding="utf-8")
    return claim


def claimed_version(store_dir: str | Path) -> str:
    claim_path = Path(store_dir).resolve() / "claim.json"
    if not claim_path.exists():
        raise FileNotFoundError("尚无领取版本，请先运行 claim")
    return json.loads(claim_path.read_text(encoding="utf-8"))["version"]


def verify_release(store_dir: str | Path, version: str | None = None) -> dict:
    """核对发布物理完整性，并在真实 Parquet 上跑对账查询。"""
    import duckdb

    store_dir = Path(store_dir).resolve()
    version = version or claimed_version(store_dir)
    release_dir = store_dir / "releases" / version
    manifest = json.loads((release_dir / "manifest.json").read_text(encoding="utf-8"))
    file_checks = []
    for entry in manifest["tables"]:
        p = release_dir / f"{entry['table']}.parquet"
        digest = _file_digest(p)
        ok = digest["sha256"] == entry["sha256"] and digest["bytes"] == entry["bytes"]
        if not ok:
            raise ValueError(f"物理文件与清单不符: {entry['table']}")
        file_checks.append({"table": entry["table"], "rows": entry["rows"], "ok": True})

    con = duckdb.connect()
    con.execute(f"CREATE VIEW release_root AS SELECT '{release_dir.as_posix()}' AS root")
    # 直接读真实表做对账，而不是信任管道内存结果。
    bad = con.execute(
        f"""
        select line, revision, currency, gross_minor, credited_minor,
               net_minor, allocated_minor, unallocated_minor
        from read_parquet(?)
        where not (
            (not is_effective)
            or (allocated_minor + unallocated_minor = net_minor
                and gross_minor - credited_minor = net_minor)
        )
        """,
        [str(release_dir / "charge_reconciliation.parquet")],
    ).fetchall()
    if bad:
        raise ValueError(f"费用行对账不平: {bad}")

    bridge_bad = con.execute(
        """
        select r.line, r.revision
        from (
            select line, revision, sum(allocated_minor) as allocated
            from read_parquet(?) group by 1, 2
        ) r
        join read_parquet(?) c using (line, revision)
        where r.allocated <> c.allocated_minor
        """,
        [str(release_dir / "allocation_bridge.parquet"),
         str(release_dir / "charge_reconciliation.parquet")],
    ).fetchall()
    if bridge_bad:
        raise ValueError(f"桥表汇总与对账表不一致: {bridge_bad}")
    return {"version": version, "files": file_checks,
            "reconciliation_rows": len(file_checks), "balanced": True}
