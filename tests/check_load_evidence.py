#!/usr/bin/env python3
"""Fail closed on missing/failed load evidence; never infer production capacity."""
import argparse
import json
import math
from pathlib import Path

PROFILES = {"capacity", "soak", "cluster_load", "cluster_fault_soak"}


def number(record, key, minimum=0, integer=False):
    value = record.get(key)
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < minimum
            or (integer and not isinstance(value, int))):
        raise ValueError(f"invalid or missing {key}")
    return value


def validate(record):
    if record.get("passed") is not True:
        raise ValueError("profile did not finish successfully")
    completed = number(record, "completed", minimum=1, integer=True)
    if number(record, "elapsed_seconds") <= 0:
        raise ValueError("no measured observation window")
    if completed != number(record, "accepted", minimum=1, integer=True):
        raise ValueError("accepted/completed mismatch")
    if record["profile"].startswith("cluster_"):
        if number(record, "healthy_errors", integer=True) != 0:
            raise ValueError("healthy request error budget exceeded")
        rto = number(record, "failover_to_new_completion_ms")
        if number(record, "rto_limit_ms") != 30000 or not 0 < rto <= 30000:
            raise ValueError("RTO missing or outside original 30000ms budget")
        if number(record, "convergence_limit_ms") != 15000:
            raise ValueError("original convergence budget changed")
    else:
        if number(record, "errors", integer=True) != 0:
            raise ValueError("failed operation")
        if number(record, "measurement_seconds") <= 0 or number(record, "verified_log_bytes") <= 0:
            raise ValueError("missing measurement or verified logs")
        api = record.get("api_latency_ms", {})
        if number(api, "p99_upper_bound") <= 0:
            raise ValueError("missing API latency samples")
        if record["profile"] == "soak" and number(record, "cycles", integer=True) < 2:
            raise ValueError("insufficient restart/maintenance cycles")


def load_records(directory, not_before=None):
    records = []
    for path in sorted(directory.rglob("*.json")):
        record = json.loads(path.read_text())
        if not isinstance(record, dict):
            raise ValueError(f"{path.name}: evidence must be an object")
        if record.get("profile") not in PROFILES:
            continue
        try:
            validate(record)
            if not_before is not None and path.stat().st_mtime < not_before:
                raise ValueError("stale evidence predates this run")
        except (ValueError, TypeError) as error:
            raise ValueError(f"{path}: {error}") from error
        records.append((path.relative_to(directory), record))
    return records


def validate_matrix(records, repeat, selected_stages=None):
    # Separate stages cannot borrow evidence from full regression or an old run.
    for path, record in records:
        if not record.get("platform", "").startswith("linux/"):
            raise ValueError(f"{path}: matrix evidence is not Linux")
    stages = {
        "regression": {"capacity": (1, True), "soak": (1, True), "cluster_load": (1, None)},
        "cluster": {"cluster_load": (repeat, None)},
        "capacity-race": {"capacity": (1, True)},
        "capacity-plain": {"capacity": (1, False)},
        "daemon-soak": {"soak": (1, True)},
        "cluster-soak": {"cluster_fault_soak": (None, None)},
    }
    selected = set(stages) if selected_stages is None else set(selected_stages)
    if not selected or selected - stages.keys():
        raise ValueError("unknown or empty matrix stage selection")
    for stage, profiles in stages.items():
        if stage not in selected:
            continue
        rows = [(path, record) for path, record in records if len(path.parts) == 2 and path.parts[0] == stage]
        for profile, (count, race) in profiles.items():
            matches = [(path, record) for path, record in rows if record["profile"] == profile]
            if (count is None and not matches) or (count is not None and len(matches) != count):
                raise ValueError(f"{stage}: expected {count or 'at least one'} {profile} records, got {len(matches)}")
            for path, record in matches:
                if race is not None and record.get("race_enabled") is not race:
                    raise ValueError(f"{path}: incorrect or missing race instrumentation")
        unexpected = [str(path) for path, record in rows if record["profile"] not in profiles]
        if unexpected:
            raise ValueError(f"{stage}: unexpected profile evidence {unexpected}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--require-profiles", nargs="+", default=[])
    parser.add_argument("--matrix-repeat", type=int)
    parser.add_argument("--stages", nargs="+", help="validate selected shards; omit for the complete matrix")
    parser.add_argument("--not-before", type=float, help="earliest permitted file mtime, Unix seconds")
    args = parser.parse_args()
    if args.matrix_repeat is not None and (not 1 <= args.matrix_repeat <= 9 or args.not_before is None):
        parser.error("--matrix-repeat requires 1..9 and --not-before for this run")
    if args.stages is not None and args.matrix_repeat is None:
        parser.error("--stages requires --matrix-repeat")
    if args.not_before is not None and not math.isfinite(args.not_before):
        parser.error("--not-before must be finite")
    try:
        records = load_records(args.directory, args.not_before)
        found = {record["profile"] for _, record in records}
        missing = set(args.require_profiles) - found
        if missing or not records:
            raise ValueError(f"missing profile evidence: {sorted(missing) or 'all'}")
        if args.matrix_repeat is not None:
            validate_matrix(records, args.matrix_repeat, args.stages)
    except (ValueError, TypeError, OSError) as error:
        raise SystemExit(str(error)) from error
    print(f"Validated {len(records)} measured profiles; no maximum-capacity or long-soak claim inferred.")


if __name__ == "__main__":
    main()
