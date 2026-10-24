import importlib.util
from pathlib import Path
import unittest
import tempfile
import json
import os
import time

spec = importlib.util.spec_from_file_location("load_evidence", Path(__file__).with_name("check_load_evidence.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LoadEvidenceTest(unittest.TestCase):
    def capacity(self):
        return {"profile": "capacity", "passed": True, "accepted": 8, "completed": 8,
                "errors": 0, "elapsed_seconds": 4, "measurement_seconds": 3,
                "verified_log_bytes": 2048, "api_latency_ms": {"p99_upper_bound": 64}}

    def test_complete_capacity_profile(self):
        module.validate(self.capacity())

    def test_incomplete_or_lost_work_is_rejected(self):
        for change in ({"passed": False}, {"completed": 7}, {"errors": 1},
                       {"api_latency_ms": {}}, {"measurement_seconds": 0}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.validate(dict(self.capacity(), **change))

    def test_cluster_enforces_original_budget_and_healthy_errors(self):
        record = {"profile": "cluster_load", "passed": True, "completed": 18,
                  "elapsed_seconds": 30, "healthy_errors": 0, "accepted": 18,
                  "failover_to_new_completion_ms": 14000, "rto_limit_ms": 30000,
                  "convergence_limit_ms": 15000}
        module.validate(record)
        for change in ({"healthy_errors": 1}, {"failover_to_new_completion_ms": 31000},
                       {"failover_to_new_completion_ms": float("nan")},
                       {"convergence_limit_ms": 30000}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.validate(dict(record, **change))

    def test_nonfinite_numbers_and_bool_counts_rejected(self):
        for change in ({"elapsed_seconds": float("nan")},
                       {"measurement_seconds": float("inf")},
                       {"api_latency_ms": {"p99_upper_bound": float("nan")}},
                       {"completed": True}, {"accepted": True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.validate(dict(self.capacity(), **change))

    def cluster(self):
        return {"profile": "cluster_load", "passed": True, "accepted": 18, "completed": 18,
                "elapsed_seconds": 30, "healthy_errors": 0,
                "failover_to_new_completion_ms": 14000, "rto_limit_ms": 30000,
                "convergence_limit_ms": 15000, "platform": "linux/arm64"}

    def matrix(self):
        cap = dict(self.capacity(), platform="linux/arm64", race_enabled=True)
        soak = dict(cap, profile="soak", cycles=2)
        entries = [("regression/capacity.json", cap), ("regression/soak.json", soak),
                   ("regression/cluster.json", self.cluster()),
                   ("cluster/one.json", self.cluster()), ("cluster/two.json", self.cluster()),
                   ("capacity-race/capacity.json", cap),
                   ("capacity-plain/capacity.json", dict(cap, race_enabled=False)),
                   ("daemon-soak/soak.json", soak),
                   ("cluster-soak/one.json", dict(self.cluster(), profile="cluster_fault_soak"))]
        return [(Path(path), record) for path, record in entries]

    def test_matrix_requires_every_stage_and_repeat(self):
        module.validate_matrix(self.matrix(), 2)
        for stage in ("cluster", "capacity-race", "capacity-plain", "daemon-soak", "cluster-soak"):
            records = [(p, r) for p, r in self.matrix() if p.parts[0] != stage]
            with self.subTest(stage=stage), self.assertRaises(ValueError):
                module.validate_matrix(records, 2)
        with self.assertRaises(ValueError):
            module.validate_matrix(self.matrix(), 3)

    def test_matrix_rejects_wrong_instrumentation_platform_and_nested_borrowing(self):
        for replacement in ({"race_enabled": False}, {"platform": "darwin/arm64"}):
            records = [(p, dict(r, **replacement) if p.parts[0] == "capacity-race" else r)
                       for p, r in self.matrix()]
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                module.validate_matrix(records, 2)
        records = [(Path("old") / p if p.parts[0] == "capacity-plain" else p, r)
                   for p, r in self.matrix()]
        with self.assertRaises(ValueError):
            module.validate_matrix(records, 2)

    def test_cluster_rejects_lost_work_and_inflated_budget(self):
        for change in ({"accepted": 19}, {"rto_limit_ms": 9999999}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.validate(dict(self.cluster(), **change))

    def test_stale_results_cannot_fill_stage(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "capacity.json"
            path.write_text(json.dumps(self.capacity()))
            os.utime(path, (1, 1))
            with self.assertRaises(ValueError):
                module.load_records(Path(folder), time.time())


if __name__ == "__main__":
    unittest.main()
