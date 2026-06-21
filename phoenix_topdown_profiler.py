#!/usr/bin/env python3
"""
End-to-end Phoenix CPU + CMN + DMC profiler using topdown-tool.

v14 fixes/refinements:
- Adds dedicated DMC-only FE/BE slots instead of attaching all DMC groups to every CPU+CMN pass.
- Uses topdown-tool --dmc-metric-group, split by logical Phoenix FE/BE groups to improve DMC coverage and avoid counter pressure.
- Keeps DMC tool-native metrics, but renames DMC FE RDAT CBusy methodology rows back to dmc_fe_rdata_cbusy{0..3}_pct names.
- Treats DMC rates/percentages as already normalized by DMC_CYCLES and does NOT scale them by a DMC clock.
- Keeps only README.txt, timeline_unified.csv, and timeline_long_raw_devices.csv in merged/.
- Converts only CMN *_rate metrics to percent of CMN clock using --cmn-clock-ghz.

Typical collection:
  rm -rf ./phoenix_out
  python3 phoenix_topdown_profiler.py \
    --frames 1 \
    --core 4 \
    --cmn-indices 0,1 \
    --workload "taskset -c 4 /root/bw_mem_init_once -P 1 -N 1000 -W 2 1024m rd"

Re-parse existing data only:
  python3 phoenix_topdown_profiler.py --parse-only --out phoenix_out
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

STOP = False


def _sig_handler(*_: object) -> None:
    global STOP
    STOP = True


signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


REQUIRED_CMN_METRICS: List[str] = [
    "cmn_hns_txdat_rate",
    "cmn_xp_txdat_eastport_rate",
    "cmn_xp_txdat_westport_rate",
    "cmn_xp_txdat_northport_rate",
    "cmn_xp_txdat_southport_rate",
    "cmn_bw_rnf",
    "cmn_bw_rni",
    "cmn_bw_rnd",
    "cmn_bw_snf",
    "cmn_bw_ccg_c2c_requestor",
    "cmn_bw_ccg_c2c_destination",
    "cmn_hns_pocq_avg_occupancy",
    "cmn_hns_memcntrl_retry_ratio",
    "cmn_hns_pocq_retry_ratio",
    "cmn_hns_memcntrl_cbusy_req_throttle_rate",
    "cmn_ccg_rxreq_rate",
    "cmn_ccg_txreq_rate",
    "cmn_ccg_rxdat_rate",
    "cmn_ccg_txdat_rate",
    "cmn_ccg_rxsnp_rate",
    "cmn_ccg_txsnp_rate",
    "cmn_ccg_rxrsp_rate",
    "cmn_ccg_txrsp_rate",
    "cmn_ccg_rxrsp_retry_rate",
    "cmn_ccg_txrsp_retry_rate",
    "cmn_ccg_64byte_rxreq_rate",
    "cmn_ccg_64byte_txreq_rate",
]


DEFAULT_DMC_GROUPS: List[str] = [
    # DMC Phoenix FE groups exposed by topdown-tool.
    "IF_BW_Analysis",
    "IF_BW_Stack_Analysis",
    "IF_Command_Mix",
    "IF_Databeat_Utilization",
    "IF_Load_Analysis",
    "IF_Retry_Analysis",
    "IF_Zero_Credits",
    "Queue_Effectiveness",
    # DMC Phoenix BE groups exposed by topdown-tool.
    "DMC_Queue_Effectiveness",
    "DMC_DRAM_BW_Stack",
    "DMC_DRAM_BW_Stack_Analysis",
    "DMC_DRAM_Command_Mix",
    "DMC_DRAM_Cycle_Effectiveness",
    "DMC_DRAM_Low_Power",
    "DMC_Stage1_Analysis",
]

# Official Arm DMC-Phoenix telemetry metrics recovered from topdown-tool wide DMC CSVs.
# The raw CSV columns exported by some topdown-tool versions are anonymous
# frontend0..frontendN / backend0..backendN columns.  Keep only the columns
# that map to real metrics in the DMC-Phoenix telemetry specification.
DMC_FE_FRONTEND_INDEX_TO_METRIC: Dict[int, str] = {
    # CHI_Command_Mix, 8 metrics
    0: "dmc_fe_cleansharedpersist_rate",
    1: "dmc_fe_prefetchtgt_rate",
    2: "dmc_fe_readnosnp_rate",
    3: "dmc_fe_readnosnpsep_rate",
    4: "dmc_fe_writenosnp_pcmosep_rate",
    5: "dmc_fe_writenosnpfull_rate",
    6: "dmc_fe_writenosnpptl_rate",
    7: "dmc_fe_writenosnpzero_rate",
    # CHI_Traffic_Level, 17 metrics
    8: "dmc_fe_rdat_rate",
    9: "dmc_fe_rdata_cbusy0_rate",
    10: "dmc_fe_rdata_cbusy1_rate",
    11: "dmc_fe_rdata_cbusy2_rate",
    12: "dmc_fe_rdata_cbusy3_rate",
    13: "dmc_fe_rdata_cbusy4_rate",
    14: "dmc_fe_rdata_cbusy5_rate",
    15: "dmc_fe_rdata_cbusy6_rate",
    16: "dmc_fe_rdata_cbusy7_rate",
    17: "dmc_fe_read_data_stall",
    18: "dmc_fe_read_request_util",
    19: "dmc_fe_read_retry_util",
    20: "dmc_fe_req_rate",
    21: "dmc_fe_request_stall",
    22: "dmc_fe_response_stall",
    23: "dmc_fe_write_request_util",
    24: "dmc_fe_write_retry_util",
}

DMC_BE_BACKEND_INDEX_TO_METRIC: Dict[int, str] = {
    0: "dmc_be_command_queue_occupancy",
    1: "dmc_be_read_cmdq_alloc",
    2: "dmc_be_read_cmdq_dealloc",
    3: "dmc_be_write_cmdq_alloc",
    4: "dmc_be_write_cmdq_dealloc",
}

DMC_OFFICIAL_METRICS = set(DMC_FE_FRONTEND_INDEX_TO_METRIC.values()) | set(DMC_BE_BACKEND_INDEX_TO_METRIC.values())

# topdown-tool exposes DMC FE RDAT CBusy using methodology names rather than
# the raw telemetry names.  Rename those rows back to CBusy bucket names so the
# output aligns with the CPU->CMN->DMC methodology and is comparable with
# cmn_hns_cbusy*_pct.
DMC_TOOL_NATIVE_RENAMES: Dict[str, str] = {
    "chi_rd_subordinate_not_busy_percentage": "dmc_fe_rdata_cbusy0_pct",
    "chi_rd_subordinate_optimally_busy_percentage": "dmc_fe_rdata_cbusy1_pct",
    "chi_rd_subordinate_quite_busy_percentage": "dmc_fe_rdata_cbusy2_pct",
    "chi_rd_subordinate_very_busy_percentage": "dmc_fe_rdata_cbusy3_pct",

    "chi_rd_subordinate_not_busy_percentage_singlecore_active": "dmc_fe_rdata_cbusy0_singlecore_active_pct",
    "chi_rd_subordinate_optimally_busy_percentage_singlecore_active": "dmc_fe_rdata_cbusy1_singlecore_active_pct",
    "chi_rd_subordinate_quite_busy_percentage_singlecore_active": "dmc_fe_rdata_cbusy2_singlecore_active_pct",
    "chi_rd_subordinate_very_busy_percentage_singlecore_active": "dmc_fe_rdata_cbusy3_singlecore_active_pct",

    "chi_rd_subordinate_not_busy_percentage_multicores_active": "dmc_fe_rdata_cbusy0_multicores_active_pct",
    "chi_rd_subordinate_optimally_busy_percentage_multicores_active": "dmc_fe_rdata_cbusy1_multicores_active_pct",
    "chi_rd_subordinate_quite_busy_percentage_multicores_active": "dmc_fe_rdata_cbusy2_multicores_active_pct",
    "chi_rd_subordinate_very_busy_percentage_multicores_active": "dmc_fe_rdata_cbusy3_multicores_active_pct",

    # Tool-native zero-credit rows map directly to the Phoenix FE stall names.
    "chi_request_zerocredits": "dmc_fe_request_stall_pct",
    "chi_response_zerocredits": "dmc_fe_response_stall_pct",
    "chi_read_data_zerocredits": "dmc_fe_read_data_stall_pct",

    # Retry methodology rows.
    "chi_retry_percentage": "dmc_fe_retry_pct",
    "chi_rd_retry_percentage": "dmc_fe_read_retry_pct",
    "chi_wr_retry_percentage": "dmc_fe_write_retry_pct",
}


DISPLAY_TO_CANONICAL: Dict[str, str] = {
    "bandwidth metrics for rnf": "cmn_bw_rnf",
    "bandwidth metrics for rni": "cmn_bw_rni",
    "bandwidth metrics for rnd": "cmn_bw_rnd",
    "bandwidth metrics for ccg-c2c": "cmn_bw_ccg_c2c_requestor",
    "bandwidth metrics for ccg-c2c destination": "cmn_bw_ccg_c2c_destination",
    "bandwidth metrics for dram": "cmn_bw_snf",
    "bandwidth metrics for slc": "cmn_bw_slc",
    "bandwidth metrics for cmn": "cmn_bw_total",
    "bandwidth metrics for io": "cmn_bw_io",
    "bandwidth metrics for peer cpu cache": "cmn_bw_peer_cpu_cache",
    "bandwidth metrics for ccg-cxl": "cmn_bw_ccg_cxl",
    "instructions per cycle": "ipc",
}

CPU_PATTERNS = [
    r"^ipc$",
    r"^frontend",
    r"^bad_speculation$",
    r"^retiring$",
    r"^load_",
    r"^store_",
    r"^l1d_",
    r"^l2_",
    r"^ll_",
    r"^itlb_",
    r"^dtlb_",
    r"stalled_cycles",
    r"^memory_bound$",
    r"^core_bound$",
]
CMN_PATTERNS = [r"^cmn_", r"^bandwidth_metrics_for_", r"^hns_", r"^hnf_", r"^xp_", r"^ccg_"]
DMC_FE_PATTERNS = [r"^dmc_fe_", r"^chi_if_", r"phoenix_fe", r"^frontend\d*_value$"]
DMC_BE_PATTERNS = [r"^dmc_be_", r"phoenix_be", r"dram", r"queue", r"^backend\d*_value$"]


@dataclass(frozen=True)
class PassPlan:
    name: str
    probe_mode: str
    cpu_groups: List[str]
    cmn_groups: List[str]
    dmc_enabled: bool
    description: str
    dmc_groups: Optional[List[str]] = None


def build_pass_plan(extra_groups: List[str]) -> List[PassPlan]:
    """Build collection schedule.

    v11 keeps CPU+CMN passes focused and moves DMC into dedicated DMC-only
    passes.  This avoids passing all FE/BE DMC groups into every combined
    pass, which can hide BE metrics or pressure the PMU resource scheduler.
    """
    passes: List[PassPlan] = [
        PassPlan("slot00_bandwidth_a", "cmn_only", [], ["CMN_Requestor_Bandwidth", "CMN_Completer_Bandwidth"], False, "trusted CMN-only bandwidth pass A"),
        PassPlan("slot01_hns_txdat", "combined", ["Topdown_L1", "General", "Cycle_Accounting"], ["HNS_TXDAT_BW_Stack"], False, "CPU top-level + HNS TXDAT BW stack"),
        PassPlan("slot02_hns_occ_ratio", "combined", ["Topdown_Backend"], ["HNS_Analysis_Occupancy", "HNS_Analysis_Ratio"], False, "CPU backend + HNS occupancy/ratio"),
        PassPlan("slot03_hns_pocq_eff", "combined", ["Miss_Ratio", "ITLB_Effectiveness", "DTLB_Effectiveness", "Operation_Mix"], ["HNS_POCQ_Effectiveness"], False, "CPU miss/TLB/mix + HNS POCQ effectiveness"),
        PassPlan("slot04_ccg_ingress", "combined", ["Topdown_L1"], ["CCG_CHI_Ingress_Traffic"], False, "CPU anchor + CCG CHI ingress traffic"),
        PassPlan("slot05_ccg_egress", "combined", ["Topdown_Backend"], ["CCG_CHI_Egress_Traffic"], False, "CPU backend + CCG CHI egress traffic"),
        PassPlan("slot06_ccg_trk", "combined", ["Miss_Ratio", "ITLB_Effectiveness", "DTLB_Effectiveness", "Operation_Mix"], ["CCG_TRK_Effectiveness"], False, "CPU miss/TLB/mix + CCG tracker effectiveness"),
        PassPlan("slot07_xp_tx_traffic", "combined", ["Topdown_L1", "General"], ["XP_TX_Traffic"], False, "CPU anchor + XP TX traffic"),
        PassPlan("slot08_bandwidth_b", "cmn_only", [], ["CMN_Requestor_Bandwidth", "CMN_Completer_Bandwidth"], False, "trusted CMN-only bandwidth pass B"),

        # Dedicated DMC Phoenix FE passes.
        PassPlan("slot09_dmc_fe_bw_cmd", "dmc_only", [], [], True, "DMC FE bandwidth + command mix", ["IF_BW_Analysis", "IF_BW_Stack_Analysis", "IF_Command_Mix"]),
        PassPlan("slot10_dmc_fe_databeat_load", "dmc_only", [], [], True, "DMC FE databeat utilization + load analysis", ["IF_Databeat_Utilization", "IF_Load_Analysis"]),
        PassPlan("slot11_dmc_fe_retry_credit_queue", "dmc_only", [], [], True, "DMC FE retry + zero credits + queue effectiveness", ["IF_Retry_Analysis", "IF_Zero_Credits", "Queue_Effectiveness"]),

        # Dedicated DMC Phoenix BE passes.
        PassPlan("slot12_dmc_be_queue_bw", "dmc_only", [], [], True, "DMC BE queue effectiveness + DRAM bandwidth stack", ["DMC_Queue_Effectiveness", "DMC_DRAM_BW_Stack", "DMC_DRAM_BW_Stack_Analysis"]),
        PassPlan("slot13_dmc_be_cmd_cycle", "dmc_only", [], [], True, "DMC BE DRAM command mix + cycle effectiveness", ["DMC_DRAM_Command_Mix", "DMC_DRAM_Cycle_Effectiveness"]),
        PassPlan("slot14_dmc_be_low_power_stage1", "dmc_only", [], [], True, "DMC BE low power + stage1 analysis", ["DMC_DRAM_Low_Power", "DMC_Stage1_Analysis"]),
    ]
    for idx, grp in enumerate(extra_groups):
        passes.append(PassPlan(f"slot_extra_{idx:02d}", "combined", ["Topdown_L1"], [grp], False, f"user extra CMN group: {grp}"))
    return passes


def ensure_dirs(root: Path) -> Dict[str, Path]:
    out = {
        "root": root,
        "logs": root / "logs",
        "meta": root / "meta",
        "csv": root / "csv",
        "reports": root / "reports",
        "merged": root / "merged",
    }
    for p in out.values():
        p.mkdir(parents=True, exist_ok=True)
    return out


def split_csv_arg(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def canonicalize_metric_name(name: object) -> str:
    s = str(name).strip().lower()
    if s in DISPLAY_TO_CANONICAL:
        return DISPLAY_TO_CANONICAL[s]
    s = re.sub(r"\s+", "_", s)
    s = s.replace("%", "percent")
    s = s.replace("-", "_")
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_unit(unit: object) -> str:
    u = str(unit).strip()
    if not u or u.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", u)


def source_hint(source_csv: str) -> Tuple[str, Optional[str]]:
    s = source_csv.lower().replace("\\", "/")
    name = Path(s).name
    if "/dmc/" in s or name.startswith("dmc_"):
        if "frontend" in name or "phoenix_fe" in s:
            return "DMC", "FE"
        if "backend" in name or "phoenix_be" in s:
            return "DMC", "BE"
        return "DMC", None
    if "/cmn/" in s or "cmn" in name:
        return "CMN", None
    if "/cpu/" in s or "core" in name or "cpu" in name or "neoverse" in name:
        return "CPU", None
    if "/logs/" in s or name.endswith(".out"):
        return "WORKLOAD", None
    return "OTHER", None


def matches_any(name: str, patterns: Iterable[str]) -> bool:
    return any(re.search(p, name, flags=re.IGNORECASE) for p in patterns)


def map_dmc_anonymous_metric(raw_metric: str, source_csv: str) -> Optional[str]:
    """Map anonymous topdown DMC wide CSV columns to official DMC-Phoenix metrics.

    Some topdown-tool builds emit DMC CSVs as frontend0..frontend24 and
    backend0..backend4 instead of the metric names from the DMC-Phoenix telemetry
    specification.  This function restores those official names.  Unmapped
    frontend/backend indexes are intentionally left anonymous and later dropped.
    """
    raw = canonicalize_metric_name(raw_metric)
    domain, sub = source_hint(source_csv)
    # Accept raw names before or after an earlier dmc_fe_/dmc_be_ prefix.
    mfe = re.match(r"^(?:dmc_fe_)?frontend(\d+)(?:_value)?$", raw)
    if domain == "DMC" and sub == "FE" and mfe:
        return DMC_FE_FRONTEND_INDEX_TO_METRIC.get(int(mfe.group(1)))
    mbe = re.match(r"^(?:dmc_be_)?backend(\d+)(?:_value)?$", raw)
    if domain == "DMC" and sub == "BE" and mbe:
        return DMC_BE_BACKEND_INDEX_TO_METRIC.get(int(mbe.group(1)))
    return None


def normalize_metric_for_source(metric: str, source_csv: str) -> str:
    """Canonicalize metric names without inventing DMC names.

    v13 rule:
    - If a DMC CSV row already has a real metric name (dmc_*, chi_*, dram_*), keep it.
    - Only map anonymous frontendN/backendN columns when there is no row metric.
    - Do not prefix real BE metrics as dmc_be_dmc_*; the DMC subdomain column already carries FE/BE.
    """
    mapped = map_dmc_anonymous_metric(metric, source_csv)
    if mapped:
        return mapped
    raw = canonicalize_metric_name(metric)
    domain, _sub = source_hint(source_csv)
    if domain == "DMC":
        if raw in DMC_TOOL_NATIVE_RENAMES:
            return DMC_TOOL_NATIVE_RENAMES[raw]
        # The raw telemetry-spec names use *_rate even though the DMC spec/tool
        # define them as normalized percentages.  Normalize the CBusy names in
        # the user-facing output, but keep other dmc_* names unchanged.
        m = re.match(r"^dmc_fe_rdata_cbusy([0-7])_rate$", raw)
        if m:
            return f"dmc_fe_rdata_cbusy{m.group(1)}_pct"
        if raw.startswith(("dmc_", "chi_", "dram_")):
            return raw
        return raw
    if domain == "WORKLOAD" and not raw.startswith("workload_"):
        return f"workload_{raw}"
    return raw

def classify_domain(metric: str, source_csv: str) -> Tuple[str, Optional[str]]:
    """Classify using source path first. This fixes dmc_backend metrics named backend*."""
    domain, sub = source_hint(source_csv)
    if domain != "OTHER":
        return domain, sub
    m = str(metric).lower()
    if matches_any(m, CMN_PATTERNS):
        return "CMN", None
    if matches_any(m, DMC_FE_PATTERNS):
        return "DMC", "FE"
    if matches_any(m, DMC_BE_PATTERNS):
        return "DMC", "BE"
    if matches_any(m, CPU_PATTERNS):
        return "CPU", None
    if m.startswith("workload_"):
        return "WORKLOAD", None
    return "OTHER", None


def infer_metric_semantics(metric: str, unit: str = "", domain: str = "") -> Tuple[str, str]:
    m = str(metric).lower()
    u = normalize_unit(unit)
    ul = u.lower()
    inferred_unit = u
    if not inferred_unit:
        if "gbytes_per_second" in m or "gbps" in m or "gb_s" in m:
            inferred_unit = "GBytes per second"
        elif "mbytes_per_second" in m or "mbps" in m or "mb_s" in m:
            inferred_unit = "MBytes per second"
        elif "bytes_per_second" in m or "bandwidth" in m or m.endswith("_bw"):
            inferred_unit = "bytes per second"
        elif "bytes_per_cycle" in m:
            inferred_unit = "bytes per cycle"
        elif "latency" in m and "ns" in m:
            inferred_unit = "ns"
        elif "per_cycle" in m or m == "ipc":
            inferred_unit = "per cycle"
        elif "percentage" in m or "percent" in m or "ratio" in m:
            inferred_unit = "%"
        elif "occupancy" in m:
            inferred_unit = "occupancy"
        elif "rate" in m:
            inferred_unit = "rate"

    # DMC topdown CSVs are already metric-level results. For percent/utilization/
    # occupancy metrics, the correct cross-device aggregation is mean. For alloc/
    # dealloc counters, system-level activity is sum. Do not scale DMC by clock.
    if str(domain).upper() == "DMC" or m.startswith(("dmc_", "chi_", "dram_")):
        if m.endswith(("_alloc", "_dealloc")) or m.endswith(("_alloc_cycles", "_dealloc_cycles")):
            return "sum", "count"
        if "bytes_per_cycle" in m:
            return "sum", "bytes per cycle"
        if ul in {"percent", "%"} or "percentage" in m or "percent" in m:
            return "mean", "%"
        if "occupancy" in m:
            return "mean", "occupancy"
        if any(tok in m for tok in ["retry", "zerocredits", "zero_credits", "stall", "busy", "util"]):
            return "mean", "%"
        if m.endswith("_rate"):
            # topdown-tool DMC rates are formulas normalized by DMC/CHI cycles.
            # Treat as percentages unless the explicit unit says otherwise.
            return "mean", "%" if not inferred_unit or inferred_unit == "rate" else inferred_unit
        return "mean", inferred_unit or u or ""

    sum_patterns = [
        r"^cmn_bw_",
        r"^cmn_.*_rate$",
        r"^xp_.*_rate$",
        r"^ccg_.*_rate$",
        r"^workload_.*(bw|bandwidth|gbps|mbps)",
    ]
    mean_patterns = [
        r"^ipc$",
        r".*percent.*",
        r".*ratio.*",
        r".*occupancy.*",
        r".*bound.*",
        r".*stalled_cycles.*",
        r".*hit_ratio.*",
        r".*miss_ratio.*",
        r".*per_cycle.*",
        r".*latency.*",
    ]
    agg = "mean"
    if any(re.search(p, m, flags=re.IGNORECASE) for p in sum_patterns):
        agg = "sum"
    elif any(re.search(p, m, flags=re.IGNORECASE) for p in mean_patterns):
        agg = "mean"
    else:
        iu = inferred_unit.lower()
        if iu in {"gbytes per second", "mbytes per second", "bytes per second", "rate"}:
            agg = "sum"
        elif iu in {"%", "percent", "per cycle", "occupancy", "ns"}:
            agg = "mean"
        elif domain == "WORKLOAD":
            agg = "mean"
    return agg, inferred_unit


def is_cmn_clock_rate_metric(domain: str, metric: str, effective_unit: str = "") -> bool:
    """True for CMN event-rate metrics that should be normalized to CMN clock.

    These are not bandwidth metrics. They are event occurrence rates such as
    cmn_hns_cbusy0_rate, hns retry rates, txreq/rxreq rates, etc.
    """
    if str(domain).upper() != "CMN":
        return False
    m = str(metric).lower()
    u = normalize_unit(effective_unit).lower()
    if "bandwidth" in m or m.startswith("cmn_bw_") or m.endswith("_bw"):
        return False
    return m.endswith("_rate") or "_rate_" in m or u == "rate"


def add_cmn_clock_scaled_columns(df, cmn_clock_hz: float):
    """Add CMN clock-normalized columns for CMN rate metrics.

    Raw topdown CMN *_rate values are treated as events/second. For a
    Phoenix CMN clock of 2 GHz:
      events_per_1000_cmn_cycles = value / 2e9 * 1000
      percent_of_cmn_clock      = value / 2e9 * 100
    Non-CMN-rate rows keep NA in the scaled columns.
    """
    pd = import_pandas()
    out = df.copy()
    if out.empty:
        return out
    try:
        clock_hz = float(cmn_clock_hz)
    except Exception:
        clock_hz = 2_000_000_000.0
    if clock_hz <= 0:
        clock_hz = 2_000_000_000.0
    mask = out.apply(lambda r: is_cmn_clock_rate_metric(r.get("domain", ""), r.get("metric", ""), r.get("effective_unit", "")), axis=1)
    out["cmn_clock_hz"] = pd.NA
    out["value_per_cmn_cycle"] = pd.NA
    out["value_per_1000_cmn_cycles"] = pd.NA
    out["value_percent_of_cmn_clock"] = pd.NA
    out["cmn_rate_scaled_unit"] = pd.NA
    if mask.any():
        out.loc[mask, "cmn_clock_hz"] = clock_hz
        out.loc[mask, "value_per_cmn_cycle"] = out.loc[mask, "value"] / clock_hz
        out.loc[mask, "value_per_1000_cmn_cycles"] = out.loc[mask, "value"] / clock_hz * 1000.0
        out.loc[mask, "value_percent_of_cmn_clock"] = out.loc[mask, "value"] / clock_hz * 100.0
        out.loc[mask, "cmn_rate_scaled_unit"] = "% of CMN clock"
    return out

def build_command(
    tool_bin: str,
    hwplatform: str,
    slot_pass: PassPlan,
    csv_dir: Path,
    cmn_indices: str,
    core: Optional[str],
    workload: List[str],
    extra_tool_args: Optional[List[str]],
    dmc_groups: Optional[List[str]] = None,
) -> List[str]:
    base = [tool_bin, "--hwplatform", hwplatform]
    if slot_pass.probe_mode == "cmn_only":
        cmd = base + [
            "--probe", "CMN",
            "--csv-output-path", str(csv_dir),
            "--cmn-generate-csv", "metrics",
            "--cmn-metric-groups", ",".join(slot_pass.cmn_groups),
            "--cmn-indices", cmn_indices,
        ]
    elif slot_pass.probe_mode == "dmc_only":
        effective_dmc_groups = slot_pass.dmc_groups if slot_pass.dmc_groups is not None else dmc_groups
        cmd = base + [
            "--probe", "DMC",
            "--csv-output-path", str(csv_dir),
            "--dmc-generate-csv", "metrics",
        ]
        if effective_dmc_groups:
            cmd.extend(["--dmc-metric-group", ",".join(effective_dmc_groups)])
    else:
        cmd = base + [
            "--probe", "CPU,CMN",
            "--csv-output-path", str(csv_dir),
            "--cpu-generate-csv", "metrics",
            "--cpu-metric-group", ",".join(slot_pass.cpu_groups),
            "--cmn-generate-csv", "metrics",
            "--cmn-metric-groups", ",".join(slot_pass.cmn_groups),
            "--cmn-indices", cmn_indices,
        ]
        if core:
            cmd.extend(["--core", core])
    if extra_tool_args:
        cmd.extend(extra_tool_args)
    cmd.append("--")
    cmd.extend(workload)
    return cmd


def run_one(cmd: List[str], stdout_path: Path, stderr_path: Path, dry_run: bool = False) -> Tuple[int, float, float]:
    start = time.time()
    if dry_run:
        stdout_path.write_text(shlex.join(cmd) + "\n", encoding="utf-8")
        stderr_path.write_text("[DRY-RUN] command not executed\n", encoding="utf-8")
        return 0, start, time.time()
    with stdout_path.open("w", encoding="utf-8") as out_f, stderr_path.open("w", encoding="utf-8") as err_f:
        rc = subprocess.run(cmd, stdout=out_f, stderr=err_f, text=True, check=False).returncode
    return rc, start, time.time()


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def discover_metric_csvs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.rglob("*.csv") if p.name.endswith("_metrics.csv")])


def read_metric_names_from_csv(csv_path: Path) -> Set[str]:
    found: Set[str] = set()
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                lower = {c.strip().lower(): c for c in reader.fieldnames}
                metric_col = lower.get("metric")
                if metric_col is not None:
                    for row in reader:
                        val = row.get(metric_col, "")
                        if val:
                            found.add(normalize_metric_for_source(val, str(csv_path)))
                    return found
    except Exception:
        pass
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            for row in rows[1:]:
                if row and row[0]:
                    found.add(normalize_metric_for_source(row[0], str(csv_path)))
    except Exception:
        pass
    return found


def collect_seen_cmn_metrics(csv_root: Path) -> Set[str]:
    seen: Set[str] = set()
    for p in discover_metric_csvs(csv_root):
        domain, _sub = source_hint(str(p))
        if domain == "CMN":
            seen |= read_metric_names_from_csv(p)
    return seen


def write_coverage_report(report_path: Path, seen: Set[str], required: List[str]) -> List[str]:
    missing = sorted(set(required) - seen)
    write_json(report_path, {"required_cmn_metrics": required, "seen_cmn_metrics": sorted(seen), "missing_cmn_metrics": missing})
    return missing


def check_tool_exists(tool: str) -> None:
    if os.path.sep in tool:
        if not Path(tool).exists():
            raise SystemExit(f"[ERROR] topdown-tool not found: {tool}")
    elif shutil.which(tool) is None:
        raise SystemExit(f"[ERROR] topdown-tool not found in PATH: {tool}")


def print_pass_plan(passes: List[PassPlan]) -> None:
    print("Phoenix pass plan:")
    for idx, p in enumerate(passes):
        print(f"\nSlot {idx}:")
        print(f"  Name        : {p.name}")
        print(f"  Mode        : {p.probe_mode}")
        print(f"  Description : {p.description}")
        print(f"  CPU groups  : {', '.join(p.cpu_groups) if p.cpu_groups else '(none)'}")
        print(f"  CMN groups  : {', '.join(p.cmn_groups) if p.cmn_groups else '(none)'}")
        print(f"  DMC enabled : {p.dmc_enabled}")


def import_pandas():
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:
        raise SystemExit(f"[ERROR] merge requires pandas. Install with: python3 -m pip install pandas. Details: {exc}")
    return pd


def load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def list_meta_files(root: Path) -> List[Path]:
    meta_dir = root / "meta"
    return sorted(meta_dir.glob("*.json")) if meta_dir.exists() else []


def find_csv_root_for_meta(meta: Dict, root: Path) -> Optional[Path]:
    frame, slot = meta.get("frame"), meta.get("slot")
    if frame is None or slot is None:
        return None
    prefixes = [f"frame{int(frame):04d}_slot{int(slot)}", f"frame{int(frame):06d}_slot{int(slot)}"]
    csv_root = root / "csv"
    if not csv_root.exists():
        return None
    matches = [p for p in csv_root.iterdir() if p.is_dir() and any(p.name.startswith(prefix) for prefix in prefixes)]
    return sorted(matches)[-1] if matches else None


def is_metadata_like_column(col: str) -> bool:
    c = canonicalize_metric_name(col)
    return c in {"unit", "units", "node", "nodeid", "node_id", "device", "instance", "index", "socket", "channel", "timestamp", "time", "description"}


def normalize_csv(csv_path: Path):
    pd = import_pandas()
    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["metric", "raw_metric", "value", "unit", "device_label"])

    lower_cols = {c: c.strip().lower() for c in df.columns}
    metric_col = value_col = unit_col = None
    for orig, low in lower_cols.items():
        if low in {"metric", "metrics", "name", "event", "counter"} and metric_col is None:
            metric_col = orig
        elif low in {"value", "values", "metric value", "metric_value"} and value_col is None:
            value_col = orig
        elif low in {"unit", "units"} and unit_col is None:
            unit_col = orig

    # DMC topdown metrics are row-oriented plus per-device columns:
    #   time,stage,level,group,metric,units,aggregate,average,std-dev,backend12-value,...
    # Use the row metric name and device columns. Do NOT melt aggregate/average/stage
    # into metric names. This is the root fix for wrong % and missing BE stats.
    domain, sub = source_hint(str(csv_path))
    if domain == "DMC" and metric_col is not None:
        device_cols = []
        for c in df.columns:
            cc = canonicalize_metric_name(c)
            if re.match(r"^(frontend|backend)\d+_value$", cc) or re.match(r"^(frontend|backend)\d+$", cc):
                device_cols.append(c)
        rows = []
        if device_cols:
            for _, r in df.iterrows():
                raw_m = r.get(metric_col, "")
                metric = normalize_metric_for_source(raw_m, str(csv_path))
                unit = normalize_unit(r.get(unit_col, "")) if unit_col is not None else ""
                for dc in device_cols:
                    val = pd.to_numeric(r.get(dc), errors="coerce")
                    if pd.isna(val):
                        continue
                    rows.append({
                        "metric": metric,
                        "raw_metric": str(raw_m),
                        "value": float(val),
                        "unit": unit,
                        "device_label": canonicalize_metric_name(dc).replace("_value", ""),
                    })
            return pd.DataFrame(rows).reset_index(drop=True)
        # If no per-device columns exist, use the topdown-tool average for
        # normalized metrics and aggregate for obvious count metrics.
        if "average" in lower_cols.values() or "aggregate" in lower_cols.values():
            avg_col = next((c for c,l in lower_cols.items() if l == "average"), None)
            agg_col = next((c for c,l in lower_cols.items() if l == "aggregate"), None)
            rows = []
            for _, r in df.iterrows():
                raw_m = r.get(metric_col, "")
                metric = normalize_metric_for_source(raw_m, str(csv_path))
                unit = normalize_unit(r.get(unit_col, "")) if unit_col is not None else ""
                agg_kind, _u = infer_metric_semantics(metric, unit, "DMC")
                chosen_col = agg_col if agg_kind == "sum" and agg_col is not None else avg_col or agg_col
                if chosen_col is None:
                    continue
                val = pd.to_numeric(r.get(chosen_col), errors="coerce")
                if pd.isna(val):
                    continue
                rows.append({"metric": metric, "raw_metric": str(raw_m), "value": float(val), "unit": unit, "device_label": str(chosen_col)})
            return pd.DataFrame(rows).reset_index(drop=True)

    if metric_col is not None and value_col is not None:
        raw_metric = df[metric_col].astype(str)
        out = pd.DataFrame({
            "metric": raw_metric.map(lambda x: normalize_metric_for_source(x, str(csv_path))),
            "raw_metric": raw_metric,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "unit": df[unit_col].astype(str).map(normalize_unit) if unit_col is not None else "",
        })
        return out.dropna(subset=["value"]).reset_index(drop=True)

    # Wide non-DMC CSVs: non-numeric identifier columns plus numeric metric columns.
    numeric_cols = []
    for c in df.columns:
        if is_metadata_like_column(c):
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().sum() > 0:
            numeric_cols.append(c)
    if numeric_cols:
        melted = df[numeric_cols].melt(var_name="raw_metric", value_name="value")
        melted["metric"] = melted["raw_metric"].astype(str).map(lambda x: normalize_metric_for_source(x, str(csv_path)))
        melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
        melted["unit"] = ""
        return melted.dropna(subset=["value"]).reset_index(drop=True)

    object_cols = [c for c in df.columns if df[c].dtype == object]
    if object_cols:
        maybe_metric = object_cols[0]
        numeric_col = None
        for c in df.columns:
            if c == maybe_metric or is_metadata_like_column(c):
                continue
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > 0:
                numeric_col = c
                break
        if numeric_col is not None:
            raw_metric = df[maybe_metric].astype(str)
            out = pd.DataFrame({
                "metric": raw_metric.map(lambda x: normalize_metric_for_source(x, str(csv_path))),
                "raw_metric": raw_metric,
                "value": pd.to_numeric(df[numeric_col], errors="coerce"),
                "unit": "",
            })
            return out.dropna(subset=["value"]).reset_index(drop=True)

    raise ValueError(f"Unknown CSV schema for {csv_path}; columns={list(df.columns)}")


def read_metric_csv(csv_path: Path, meta: Dict):
    pd = import_pandas()
    df = normalize_csv(csv_path)
    if df.empty:
        return df
    df = df.copy()
    df["frame"] = int(meta.get("frame")) if meta.get("frame") is not None else pd.NA
    df["slot"] = int(meta.get("slot")) if meta.get("slot") is not None else pd.NA
    df["slot_name"] = meta.get("name", "")
    df["source_csv"] = str(csv_path)
    dom = df.apply(lambda r: classify_domain(r["metric"], str(csv_path)), axis=1)
    df["domain"] = [x[0] for x in dom]
    df["dmc_subdomain"] = [x[1] for x in dom]
    semantics = df.apply(lambda r: infer_metric_semantics(r["metric"], r["unit"], r["domain"]), axis=1)
    df["aggregation_kind"] = [x[0] for x in semantics]
    df["inferred_unit"] = [x[1] for x in semantics]
    df["effective_unit"] = df.apply(lambda r: normalize_unit(r["unit"]) or r["inferred_unit"], axis=1)
    if "device_label" not in df.columns:
        df["device_label"] = csv_path.stem.replace("_metrics", "")
    else:
        df["device_label"] = df["device_label"].fillna(csv_path.stem.replace("_metrics", ""))
    return df


def parse_number(text: str) -> Optional[float]:
    try:
        return float(text.replace(",", ""))
    except Exception:
        return None


def unit_to_metric_suffix(unit: str) -> Tuple[str, str, float]:
    u = unit.strip().lower().replace("/", "ps")
    u = u.replace(" ", "")
    if u in {"gbps", "gb/s", "gbytesps", "gibps", "gib/s"}:
        return "gbps", "GBytes per second", 1.0
    if u in {"mbps", "mb/s", "mbytesps", "mibps", "mib/s"}:
        return "mbps", "MBytes per second", 1.0
    if u in {"kbps", "kb/s", "kbytesps", "kibps", "kib/s"}:
        return "kbps", "KBytes per second", 1.0
    if u in {"bps", "b/s", "bytesps"}:
        return "bytes_per_second", "bytes per second", 1.0
    if u in {"ns", "nsec"}:
        return "ns", "ns", 1.0
    if u in {"us", "usec"}:
        return "us", "us", 1.0
    if u in {"ms", "msec"}:
        return "ms", "ms", 1.0
    return canonicalize_metric_name(unit), unit, 1.0


def parse_workload_stdout(stdout_path: Path, meta: Dict):
    pd = import_pandas()
    if not stdout_path.exists():
        return pd.DataFrame(columns=["metric", "raw_metric", "value", "unit"])
    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    rows: List[Dict[str, object]] = []

    # Match lines like: read bandwidth: 123.4 GB/s, rd 123 GB/s, 123 MB/s, latency 88 ns.
    for line_no, line in enumerate(text.splitlines(), 1):
        lower = line.lower()
        if not any(k in lower for k in ["bw", "bandwidth", "mb/s", "gb/s", "gbps", "mbps", "lat", "ns"]):
            continue
        patterns = [
            (r"(?P<name>read|rd|write|wr|copy|total|bandwidth|bw)[^0-9+\-]*(?P<val>[+\-]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)\s*(?P<unit>g(?:i)?b/s|m(?:i)?b/s|k(?:i)?b/s|gbps|mbps|kbps|bytes/s|b/s)", "bandwidth"),
            (r"(?P<name>latency|lat)[^0-9+\-]*(?P<val>[+\-]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)\s*(?P<unit>ns|us|ms)", "latency"),
            (r"(?P<val>[+\-]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)\s*(?P<unit>g(?:i)?b/s|m(?:i)?b/s|k(?:i)?b/s|gbps|mbps|kbps|bytes/s|b/s)", "bandwidth"),
        ]
        for pat, kind in patterns:
            for match in re.finditer(pat, line, flags=re.IGNORECASE):
                val = parse_number(match.group("val"))
                if val is None:
                    continue
                unit_text = match.group("unit")
                suffix, unit_norm, scale = unit_to_metric_suffix(unit_text)
                name = match.groupdict().get("name") or kind
                name = {"rd": "read", "wr": "write", "bw": "bandwidth", "lat": "latency"}.get(name.lower(), name.lower())
                metric = f"workload_{canonicalize_metric_name(name)}_{suffix}"
                rows.append({
                    "metric": metric,
                    "raw_metric": line.strip(),
                    "value": val * scale,
                    "unit": unit_norm,
                    "frame": int(meta.get("frame")) if meta.get("frame") is not None else pd.NA,
                    "slot": int(meta.get("slot")) if meta.get("slot") is not None else pd.NA,
                    "slot_name": meta.get("name", ""),
                    "source_csv": str(stdout_path),
                    "domain": "WORKLOAD",
                    "dmc_subdomain": None,
                    "aggregation_kind": "mean",
                    "inferred_unit": unit_norm,
                    "effective_unit": unit_norm,
                    "device_label": "workload_stdout",
                    "line_no": line_no,
                })
    if not rows:
        return pd.DataFrame(columns=["metric", "raw_metric", "value", "unit", "frame", "slot", "slot_name", "source_csv", "domain", "dmc_subdomain", "aggregation_kind", "inferred_unit", "effective_unit", "device_label", "line_no"])
    return pd.DataFrame(rows).drop_duplicates(subset=["frame", "slot", "metric", "value", "source_csv", "line_no"]).reset_index(drop=True)




def is_generated_placeholder_metric(metric: str) -> bool:
    """Return True for anonymous/generated metrics that have no official name.

    v8 keeps official DMC-Phoenix metrics recovered from frontendN/backendN,
    but still drops unmapped anonymous columns and generic stats fields.
    """
    m = str(metric or "").strip().lower()
    # Keep all real tool-native DMC metric names. Drop only generated columns
    # and summary-stat pseudo metrics such as aggregate/average/stage/level.
    if m.startswith(("dmc_", "chi_", "dram_")) and not re.match(r"^dmc_(?:fe|be)_(?:aggregate|average|std_dev|stage|level)$", m):
        return False
    patterns = [
        r"^aggregate$",
        r"^average$",
        r"^std_dev$",
        r"^std_dev$",
        r"^stage$",
        r"^level$",
        r"^dmc_fe_frontend\d+_value$",
        r"^dmc_be_backend\d+_value$",
        r"^frontend\d+_value$",
        r"^backend\d+_value$",
        r"^dmc_fe_frontend\d+$",
        r"^dmc_be_backend\d+$",
        r"^frontend\d+$",
        r"^backend\d+$",
        r"^dmc_(?:fe|be)_(?:aggregate|average|std_dev|stage|level)$",
    ]
    return any(re.match(p, m) for p in patterns)

def split_named_and_placeholder_metrics(df):
    if df.empty or "metric" not in df.columns:
        return df, df.iloc[0:0].copy()
    mask = df["metric"].map(is_generated_placeholder_metric)
    return df.loc[~mask].copy(), df.loc[mask].copy()


def finalize_preferred_output(df_unified, cmn_clock_hz: float):
    """Return the compact user-facing table.

    For CMN *_rate metrics, preferred_value is converted to percent of CMN
    clock using --cmn-clock-ghz. For all other metrics, preferred_value is the
    aggregation-aware value chosen by aggregate_per_slot_metric().

    Conversion:
      percent_of_cmn_clock = raw_rate_events_per_second / cmn_clock_hz * 100
    The debug CSV keeps the equivalent events-per-1000-CMN-cycles value.
    """
    out = df_unified.copy()
    cmn_rate_mask = out.apply(lambda r: is_cmn_clock_rate_metric(r.get("domain", ""), r.get("metric", ""), r.get("effective_unit", "")), axis=1)
    if cmn_rate_mask.any():
        clock_hz = float(cmn_clock_hz or 2_000_000_000.0)
        if clock_hz <= 0:
            clock_hz = 2_000_000_000.0
        out.loc[cmn_rate_mask, "preferred_value"] = out.loc[cmn_rate_mask, "preferred_value"] / clock_hz * 100.0
        out.loc[cmn_rate_mask, "effective_unit"] = "% of CMN clock"
    cols = ["frame", "slot", "slot_name", "domain", "dmc_subdomain", "metric", "effective_unit", "preferred_value"]
    return out[cols].sort_values(["frame", "slot", "domain", "dmc_subdomain", "metric"], na_position="last").reset_index(drop=True)

def apply_filters(df, include_regex: Optional[str], exclude_regex: Optional[str]):
    out = df
    if include_regex:
        out = out[out["metric"].str.contains(include_regex, regex=True, na=False)]
    if exclude_regex:
        out = out[~out["metric"].str.contains(exclude_regex, regex=True, na=False)]
    return out


def aggregate_per_slot_metric(df_raw, cmn_clock_hz: float = 2_000_000_000.0):
    key_cols = ["frame", "slot", "slot_name", "domain", "dmc_subdomain", "metric", "aggregation_kind", "effective_unit"]
    agg = df_raw.groupby(key_cols, dropna=False).agg(
        value_mean_across_devices=("value", "mean"),
        value_sum_across_devices=("value", "sum"),
        sample_count=("value", "count"),
        device_count=("device_label", "nunique"),
        source_count=("source_csv", "nunique"),
    ).reset_index()
    agg["preferred_value"] = agg.apply(lambda r: r["value_sum_across_devices"] if r["aggregation_kind"] == "sum" else r["value_mean_across_devices"], axis=1)

    # CMN rate normalization to the configured CMN clock.  For Phoenix the
    # default is 2 GHz, so scaled value is events per 1000 CMN cycles.
    pd = import_pandas()
    try:
        clock_hz = float(cmn_clock_hz)
    except Exception:
        clock_hz = 2_000_000_000.0
    if clock_hz <= 0:
        clock_hz = 2_000_000_000.0
    cmn_rate_mask = agg.apply(lambda r: is_cmn_clock_rate_metric(r.get("domain", ""), r.get("metric", ""), r.get("effective_unit", "")), axis=1)
    agg["cmn_clock_hz"] = pd.NA
    agg["preferred_value_per_cmn_cycle"] = pd.NA
    agg["preferred_value_per_1000_cmn_cycles"] = pd.NA
    agg["preferred_value_percent_of_cmn_clock"] = pd.NA
    agg["cmn_rate_scaled_unit"] = pd.NA
    if cmn_rate_mask.any():
        agg.loc[cmn_rate_mask, "cmn_clock_hz"] = clock_hz
        agg.loc[cmn_rate_mask, "preferred_value_per_cmn_cycle"] = agg.loc[cmn_rate_mask, "preferred_value"] / clock_hz
        agg.loc[cmn_rate_mask, "preferred_value_per_1000_cmn_cycles"] = agg.loc[cmn_rate_mask, "preferred_value"] / clock_hz * 1000.0
        agg.loc[cmn_rate_mask, "preferred_value_percent_of_cmn_clock"] = agg.loc[cmn_rate_mask, "preferred_value"] / clock_hz * 100.0
        agg.loc[cmn_rate_mask, "cmn_rate_scaled_unit"] = "% of CMN clock"
    return agg


def write_inventory(df_raw, merged_dir: Path) -> None:
    # Intentionally no-op in v7. Keep merged/ user-facing and minimal.
    return


def cleanup_merged_dir(merged_dir: Path) -> None:
    keep = {"timeline_unified.csv", "timeline_long_raw_devices.csv", "README.txt"}
    for p in merged_dir.iterdir():
        if p.is_file() and p.name not in keep:
            try:
                p.unlink()
            except Exception:
                pass

def merge_outputs(root: Path, include_regex: Optional[str], exclude_regex: Optional[str], cmn_clock_hz: float = 2_000_000_000.0) -> Tuple[Path, List[str]]:
    pd = import_pandas()
    merged_dir = root / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    meta_files = list_meta_files(root)
    if not meta_files:
        raise SystemExit(f"[ERROR] no meta files found under {root / 'meta'}")

    warnings: List[str] = []
    parts = []
    schema_rows: List[Dict[str, object]] = []
    for meta_path in meta_files:
        meta = load_json(meta_path)
        slot_csv_root = find_csv_root_for_meta(meta, root)
        if slot_csv_root is None or not slot_csv_root.exists():
            warnings.append(f"[WARN] no csv root for {meta_path}")
        else:
            csv_files = discover_metric_csvs(slot_csv_root)
            if not csv_files:
                warnings.append(f"[WARN] no *_metrics.csv files under {slot_csv_root}")
            for csv_path in csv_files:
                try:
                    cols = list(pd.read_csv(csv_path, nrows=0).columns)
                    schema_rows.append({"frame": meta.get("frame"), "slot": meta.get("slot"), "source_csv": str(csv_path), "columns": "|".join(cols), "column_count": len(cols)})
                    part = read_metric_csv(csv_path, meta)
                    if not part.empty:
                        parts.append(part)
                except Exception as exc:
                    warnings.append(f"[WARN] skipping {csv_path}: {exc}")
        stdout_path = Path(str(meta.get("stdout", "")))
        if stdout_path.exists():
            try:
                wpart = parse_workload_stdout(stdout_path, meta)
                if not wpart.empty:
                    parts.append(wpart)
            except Exception as exc:
                warnings.append(f"[WARN] skipping workload stdout {stdout_path}: {exc}")


    if not parts:
        for w in warnings[:50]:
            print(w, file=sys.stderr)
        raise SystemExit("[ERROR] no readable metric files or workload stdout metrics found")

    df_raw_all = pd.concat(parts, ignore_index=True)
    df_raw_all = apply_filters(df_raw_all, include_regex, exclude_regex)

    df_raw, df_dropped = split_named_and_placeholder_metrics(df_raw_all)
    if not df_dropped.empty:
        warnings.append(f"[WARN] dropped {len(df_dropped)} rows with generated placeholder metric names")

    if df_raw.empty:
        for w in warnings[:50]:
            print(w, file=sys.stderr)
        raise SystemExit("[ERROR] no named metrics remain after dropping generated placeholder metric names")

    df_raw = df_raw.sort_values(["frame", "slot", "domain", "dmc_subdomain", "metric", "device_label"], na_position="last").reset_index(drop=True)
    df_unified_full = aggregate_per_slot_metric(df_raw, cmn_clock_hz=cmn_clock_hz)
    df_unified = finalize_preferred_output(df_unified_full, cmn_clock_hz=cmn_clock_hz)

    # Keep raw details for debug, but the main unified output is intentionally compact.
    df_raw.to_csv(merged_dir / "timeline_long_raw_devices.csv", index=False)
    df_unified.to_csv(merged_dir / "timeline_unified.csv", index=False)

    unknown = df_raw[df_raw["domain"].eq("OTHER")]
    if not unknown.empty:
        warnings.append(f"[WARN] {len(unknown)} rows classified as OTHER")

    with (merged_dir / "README.txt").open("w", encoding="utf-8") as f:
        f.write("Generated files:\n")
        f.write("- timeline_unified.csv          : per-slot aggregate table with preferred_value\n")
        f.write("- timeline_long_raw_devices.csv : raw normalized per-device rows\n")
        f.write("- README.txt                    : this file\n\n")
        f.write("Use preferred_value by default. It equals sum across devices for bandwidth-like metrics and mean across devices for ratio/occupancy/IPC/latency metrics.\n")
        f.write("CMN *_rate metrics are converted to % of CMN clock in timeline_unified.csv using --cmn-clock-ghz.\n")
        f.write("DMC metrics from topdown-tool are already normalized by DMC_CYCLES where applicable; they are not scaled by a DMC clock.\n")
        f.write("DMC FE/BE metrics are prefixed with dmc_fe_ or dmc_be_ to avoid confusion with CPU backend metrics.\n")

    if warnings:
        for w in warnings:
            print(w, file=sys.stderr)

    cleanup_merged_dir(merged_dir)
    return merged_dir, warnings


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="End-to-end Phoenix CPU + CMN + DMC profiler")
    p.add_argument("--tool", default="topdown-tool", help="Path to topdown-tool binary")
    p.add_argument("--hwplatform", default="phoenix", help="topdown-tool hw platform argument; default: phoenix")
    p.add_argument("--out", default="./phoenix_out", help="Output directory")
    p.add_argument("--frames", type=int, default=1, help="Number of full pass cycles to run")
    p.add_argument("--cmn-indices", default="0,1", help="Comma-separated CMN indices")
    p.add_argument("--cmn-clock-ghz", type=float, default=2.0, help="CMN clock in GHz used to convert CMN *_rate metrics to percent of CMN clock; default: 2.0")
    p.add_argument("--dmc-clock-ghz", type=float, default=None, help="Optional DMC clock in GHz for documentation only. DMC topdown metrics are already normalized, so this is not applied to percentages.")
    p.add_argument("--core", default=None, help="CPU core list for combined passes, e.g. 4 or 0-7")
    p.add_argument("--extra-cmn-groups", default="", help="Comma-separated extra CMN groups")
    p.add_argument("--dmc-groups", default="", help="Override DMC groups for DMC-only slots if a slot has no built-in group list. Default uses v11 per-slot Phoenix FE/BE groups.")
    p.add_argument("--tool-extra-args", default="", help='Extra args passed to topdown-tool, e.g. "--log-level INFO"')
    p.add_argument("--workload", required=False, help='Quoted workload command, e.g. "taskset -c 4 ./a.out"')
    p.add_argument("--include-regex", default=None, help="Metric include regex for merged output")
    p.add_argument("--exclude-regex", default=None, help="Metric exclude regex for merged output")
    p.add_argument("--run-only", action="store_true", help="Collect only; skip merge")
    p.add_argument("--merge-only", action="store_true", help="Merge existing output only; skip collection")
    p.add_argument("--parse-only", action="store_true", help="Alias for --merge-only; parse/merge existing output only; skip collection")
    p.add_argument("--no-coverage-fail", action="store_true", help="Do not return non-zero if required CMN coverage is incomplete")
    p.add_argument("--continue-on-slot-error", action="store_true", help="Continue remaining slots when a topdown-tool pass fails")
    p.add_argument("--dry-run", action="store_true", help="Write commands/meta only; do not execute topdown-tool")
    p.add_argument("--print-pass-plan", action="store_true", help="Print pass plan and exit")
    return p.parse_args()


def run_collection(args: argparse.Namespace, pass_plan: List[PassPlan], out_dirs: Dict[str, Path]) -> int:
    if not args.dry_run:
        check_tool_exists(args.tool)
    if not args.workload:
        raise SystemExit("[ERROR] --workload is required unless --merge-only, --parse-only, or --print-pass-plan is used")
    if args.frames < 1:
        raise SystemExit("[ERROR] --frames must be >= 1")
    workload = shlex.split(args.workload)
    if not workload:
        raise SystemExit("[ERROR] --workload parsed to an empty command")
    extra_tool_args = shlex.split(args.tool_extra_args) if args.tool_extra_args else None
    dmc_groups = split_csv_arg(args.dmc_groups) if getattr(args, "dmc_groups", "") else []
    all_seen_cmn_metrics: Set[str] = set()
    failed_slots: List[Dict[str, object]] = []

    print(f"[INFO] tool: {args.tool}", file=sys.stderr)
    print(f"[INFO] hwplatform: {args.hwplatform}", file=sys.stderr)
    print(f"[INFO] output: {out_dirs['root']}", file=sys.stderr)
    print(f"[INFO] cmn indices: {args.cmn_indices}", file=sys.stderr)
    print(f"[INFO] frames: {args.frames}", file=sys.stderr)
    print(f"[INFO] slots per frame: {len(pass_plan)}", file=sys.stderr)
    print(f"[INFO] core: {args.core if args.core else 'default'}", file=sys.stderr)
    print(f"[INFO] workload: {shlex.join(workload)}", file=sys.stderr)
    if dmc_groups:
        print(f"[INFO] global dmc groups override: {','.join(dmc_groups)}", file=sys.stderr)
    else:
        print("[INFO] dmc groups: v11 per-slot FE/BE group schedule", file=sys.stderr)

    for frame in range(args.frames):
        if STOP:
            break
        for slot, slot_pass in enumerate(pass_plan):
            if STOP:
                break
            tag = time.strftime("%Y%m%dT%H%M%S")
            slot_id = f"frame{frame:04d}_slot{slot}"
            csv_dir = out_dirs["csv"] / f"{slot_id}_{tag}"
            csv_dir.mkdir(parents=True, exist_ok=True)
            stdout_path = out_dirs["logs"] / f"{slot_id}.out"
            stderr_path = out_dirs["logs"] / f"{slot_id}.err"
            meta_path = out_dirs["meta"] / f"{slot_id}.json"
            cmd = build_command(args.tool, args.hwplatform, slot_pass, csv_dir, args.cmn_indices, args.core, workload, extra_tool_args, dmc_groups=dmc_groups)
            rc, start, end = run_one(cmd, stdout_path, stderr_path, args.dry_run)
            seen_this_slot = collect_seen_cmn_metrics(csv_dir)
            all_seen_cmn_metrics |= seen_this_slot
            meta = {
                "frame": frame,
                "slot": slot,
                "name": slot_pass.name,
                "probe_mode": slot_pass.probe_mode,
                "description": slot_pass.description,
                "cpu_groups": slot_pass.cpu_groups,
                "cmn_groups": slot_pass.cmn_groups,
                "dmc_enabled": slot_pass.dmc_enabled,
                "dmc_groups": (slot_pass.dmc_groups if slot_pass.dmc_groups is not None else dmc_groups) if slot_pass.dmc_enabled else [],
                "hwplatform": args.hwplatform,
                "cmn_indices": args.cmn_indices,
                "cmd": cmd,
                "rc": rc,
                "start": start,
                "end": end,
                "elapsed_s": end - start,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "csv_root": str(csv_dir),
                "seen_cmn_metrics_this_slot": sorted(seen_this_slot),
            }
            write_json(meta_path, meta)
            dmc_group_note = ",".join((slot_pass.dmc_groups if slot_pass.dmc_groups is not None else dmc_groups) or [])
            print(f"[frame {frame} slot {slot}] mode={slot_pass.probe_mode} rc={rc} cmn_groups={','.join(slot_pass.cmn_groups)} dmc_groups={dmc_group_note}", flush=True)
            if rc != 0:
                failed_slots.append({"frame": frame, "slot": slot, "name": slot_pass.name, "rc": rc, "stderr": str(stderr_path)})
                if not args.continue_on_slot_error:
                    print(f"[ERROR] slot failed; see {stderr_path}", file=sys.stderr)
                    write_json(out_dirs["reports"] / "failed_slots.json", {"failed_slots": failed_slots})
                    return rc or 1

    missing = write_coverage_report(out_dirs["reports"] / "cmn_coverage_report.json", all_seen_cmn_metrics, REQUIRED_CMN_METRICS)
    write_json(out_dirs["reports"] / "failed_slots.json", {"failed_slots": failed_slots})
    required_set = set(REQUIRED_CMN_METRICS)
    print(f"[INFO] seen required CMN metrics: {len(required_set & all_seen_cmn_metrics)}/{len(required_set)}", file=sys.stderr)
    if missing:
        print("[ERROR] CMN coverage incomplete", file=sys.stderr)
        for m in missing:
            print(f"  - {m}", file=sys.stderr)
        print(f"[INFO] coverage report: {out_dirs['reports'] / 'cmn_coverage_report.json'}", file=sys.stderr)
        if not args.no_coverage_fail:
            return 1
    else:
        print(f"[OK] full required CMN coverage achieved. report: {out_dirs['reports'] / 'cmn_coverage_report.json'}", file=sys.stderr)
    if failed_slots:
        print(f"[WARN] {len(failed_slots)} slot(s) failed. See {out_dirs['reports'] / 'failed_slots.json'}", file=sys.stderr)
    return 0


def main() -> int:
    args = parse_args()
    if args.parse_only:
        args.merge_only = True
    if args.run_only and args.merge_only:
        raise SystemExit("[ERROR] --run-only cannot be combined with --merge-only/--parse-only")
    extra_groups = split_csv_arg(args.extra_cmn_groups)
    pass_plan = build_pass_plan(extra_groups)
    if args.print_pass_plan:
        print_pass_plan(pass_plan)
        return 0
    out_dirs = ensure_dirs(Path(args.out))
    rc = 0
    if not args.merge_only:
        rc = run_collection(args, pass_plan, out_dirs)
    if not args.run_only and rc == 0:
        merged_dir, warnings = merge_outputs(Path(args.out), args.include_regex, args.exclude_regex, args.cmn_clock_ghz * 1e9)
        print(f"[OK] wrote merged outputs under {merged_dir}")
        if warnings:
            print(f"[WARN] merge completed with {len(warnings)} warning(s)", file=sys.stderr)
    elif not args.run_only and rc != 0:
        print("[INFO] skipping merge because collection returned non-zero", file=sys.stderr)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
