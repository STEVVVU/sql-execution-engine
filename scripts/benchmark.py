"""Release CLI benchmark. No third-party Python dependencies or cloud services."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
QUERIES = {
    "scan_sum": "select sum(value) as total,count(*) as n from facts",
    "selective": "select count(*) as n,sum(value) as total from facts where value>=990",
    "grouped": "select category,count(*) as n,sum(value) as total from facts group by category order by category",
    "topk": "select id,value from facts order by value desc,id limit 100",
    "dimension_join": "select d.category,count(*) as n,sum(f.value) as total from facts f join dimension d on f.category=d.category group by d.category order by d.category",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--sizes", default="1000,100000,1000000")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release" / ("ee2.exe" if os.name == "nt" else "ee2"))
    args = parser.parse_args()
    sizes = [int(n) for n in args.sizes.split(",")]
    if not sizes or any(n < 1 or n > 1000000 for n in sizes):
        parser.error("sizes must be 1..1000000")
    if args.repeats < 5 and not args.smoke:
        parser.error("comparisons require at least five repeats")
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        raise RuntimeError("10 GiB disk reserve required")
    args.output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    manifest = {
        "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "dirty": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "binary_sha256": digest(binary), "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "cpu": Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else platform.processor(),
        "load": os.getloadavg() if hasattr(os, "getloadavg") else None,
        "config": vars(args) | {"output": str(args.output), "binary": str(binary)}, "queries": QUERIES,
        "policy": "Fresh CLI per sample. Wall includes startup, CSV decode, planning, execution and consumed CSV output. OS cache warm after unmeasured warmup; no spill. Custom workers 1/4 vs default DataFusion with 4 target partitions, batch 8192. Fixtures streamed to disk outside timing.",
        "samples": [], "passed": False,
    }
    try:
        with tempfile.TemporaryDirectory(prefix="vectorloom-bench-") as scratch:
            work = Path(scratch)
            for size in sizes:
                facts, dim = work / "facts.csv", work / "dimension.csv"
                with facts.open("w", newline="") as out:
                    writer = csv.writer(out)
                    writer.writerow(["id", "category", "value"])
                    writer.writerows((i, i % 100, (i * 7919) % 1000) for i in range(size))
                dim.write_text("category\n" + "".join(f"{i}\n" for i in range(100)))
                fixture = {"rows": size, "facts_sha256": digest(facts), "bytes": facts.stat().st_size, "dimension_rows": 100}
                for name, sql in QUERIES.items():
                    expected = None
                    for repeat in range(-1, args.repeats):
                        sides = ["custom1", "custom4", "datafusion4"]
                        if repeat % 2:
                            sides.reverse()
                        for side in sides:
                            if args.profile and (repeat != 0 or name != "dimension_join" or side != "custom4"):
                                continue
                            label = f"{size}-{name}-{repeat}-{side}"
                            output = args.output / f"{label}.csv"
                            stderr = args.output / f"{label}.stderr"
                            usage = args.output / f"{label}.resource.json"
                            command = [str(binary), "--csv", f"facts={facts}", "--csv", f"dimension={dim}", "--batch-size", "8192", "--workers", "1" if side == "custom1" else "4", "--sql", sql]
                            if side == "datafusion4":
                                command.append("--reference-tuned")
                            measured = command
                            if os.name != "nt":
                                measured = ["/usr/bin/time", "-f", '{"user_s":%U,"sys_s":%S,"peak_rss_kib":%M,"read_blocks":%I,"write_blocks":%O}', "-o", str(usage), *command]
                            if args.profile:
                                measured = ["strace", "-f", "-c", "-o", str(args.output / f"{label}.strace"), *measured]
                            started = time.perf_counter()
                            result = subprocess.run(measured, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=240)
                            wall = time.perf_counter() - started
                            assert len(result.stdout) <= 1024 * 1024 and len(result.stderr) <= 1024 * 1024, "bounded analytical output exceeded 1 MiB"
                            output.write_bytes(result.stdout)
                            stderr.write_bytes(result.stderr)
                            assert result.returncode == 0, f"failed {label}, inspect stderr"
                            checksum = digest(output)
                            if expected is None:
                                expected = checksum
                            assert checksum == expected, f"differential mismatch {label}"
                            metrics = json.loads(stderr.read_text().splitlines()[-1])
                            sample = {"size": size, "query": name, "side": side, "repeat": repeat, "warmup": repeat == -1, "wall_s": wall, "rows": metrics["rows"], "sha256": checksum, "command": command, "fixture": fixture, "returncode": result.returncode, "resource": json.loads(usage.read_text()) if usage.exists() else None}
                            manifest["samples"].append(sample)
                            (args.output / "result.json").write_text(json.dumps(manifest, indent=2))
                            print(label, flush=True)
            summary = []
            for size in sizes:
                for name in QUERIES:
                    for side in ["custom1", "custom4", "datafusion4"]:
                        samples = [s for s in manifest["samples"] if s["size"] == size and s["query"] == name and s["side"] == side and not s["warmup"]]
                        if samples:
                            values = [s["wall_s"] for s in samples]
                            summary.append({"size": size, "query": name, "side": side, "n": len(values), "median_s": statistics.median(values), "min_s": min(values), "max_s": max(values), "max_rss_kib": max((s["resource"] or {}).get("peak_rss_kib", 0) for s in samples)})
            manifest["summary"] = summary
            manifest["passed"] = True
    finally:
        manifest["scratch_removed"] = True
        (args.output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
