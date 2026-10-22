"""Frozen CLI fixture/oracle glue; Hyperfine owns measurement and native exports."""
import argparse
import csv
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile

from benchmark import ROOT, digest

BASELINE = "1f73fcafb08c869f03c1b94cb922222c3c352543"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--baseline-archive", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        raise RuntimeError("10 GiB disk reserve required")
    frozen = json.loads((ROOT / "scripts/frozen-benchmark.json").read_text())
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"baseline": args.baseline_binary.resolve(), "candidate": args.candidate_binary.resolve()}
    version = subprocess.check_output(["hyperfine", "--version"], text=True).strip()
    assert version == "hyperfine 2.0.0", version
    manifest = {
        "baseline_sha": BASELINE,
        "candidate_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "binary_sha256": {name: digest(binary) for name, binary in binaries.items()},
        "hyperfine_version": version,
        "baseline_archive_sha256": digest(args.baseline_archive),
        "source_sha256": {build: {name: digest(source / name) for name in ["src/engine.rs", "Cargo.toml", "Cargo.lock"]} for build, source in [("baseline", args.baseline_source), ("candidate", ROOT)]},
        "build_identity": {
            "rustc": subprocess.check_output(["rustc", "+1.99.1", "-Vv"], text=True),
            "cargo": subprocess.check_output(["cargo", "+1.99.1", "-V"], text=True).strip(),
            "common_arguments": ["+1.99.1", "build", "--release", "--locked", "--jobs", "2"],
            "environment": {name: value for name, value in os.environ.items() if name in ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTUP_TOOLCHAIN", "CARGO_INCREMENTAL"] or name.startswith("CARGO_PROFILE_RELEASE_")},
            "target_policy": "Independent baseline target directory; matching Cargo manifests/lockfiles and shared compiler/environment on the same host.",
        },
        "frozen_oracle_sha256": digest(ROOT / "scripts/frozen-benchmark.json"),
        "policy": "Same-runner release builds. Original full matrix: 3 sizes, 5 queries, custom1/custom4/tuned DataFusion4 on both builds. One excluded warmup then five reversed-order rounds. Hyperfine --runs 1 native schema2 exports; no custom timer or statistics. A shell exec, GNU time, file output and CLI startup/decode/plan/execution are included equally on both sides. OS cache warm; no spill. All CSV outputs checked against original campaign digests.",
        "samples": [], "passed": False, "scratch_removed": False,
    }
    try:
        assert len(set(manifest["binary_sha256"].values())) == 2, "baseline and candidate binaries are identical; use independent build targets"
        for name in ["Cargo.toml", "Cargo.lock"]:
            assert manifest["source_sha256"]["baseline"][name] == manifest["source_sha256"]["candidate"][name], f"unmatched dependency/profile input: {name}"
        assert manifest["source_sha256"]["baseline"]["src/engine.rs"] != manifest["source_sha256"]["candidate"]["src/engine.rs"], "engine sources do not contain the intended change"
        with tempfile.TemporaryDirectory(prefix="vectorloom-compare-") as scratch:
            work = Path(scratch)
            for size, fixture in frozen["cases"].items():
                facts, dim = work / "facts.csv", work / "dimension.csv"
                with facts.open("w", newline="") as out:
                    writer = csv.writer(out)
                    writer.writerow(["id", "category", "value"])
                    writer.writerows((i, i % 100, (i * 7919) % 1000) for i in range(int(size)))
                dim.write_text("category\n" + "".join(f"{i}\n" for i in range(100)))
                assert digest(facts) == fixture["facts_sha256"], "frozen fixture changed"
                for query, sql in frozen["queries"].items():
                    for repeat in range(-1, 5):
                        sides = [(build, side) for side in ["custom1", "custom4", "datafusion4"] for build in binaries]
                        if repeat % 2:
                            sides.reverse()
                        for build, side in sides:
                            label = f"{size}-{query}-{repeat}-{build}-{side}"
                            output = args.output / f"{label}.csv"
                            stderr = args.output / f"{label}.stderr"
                            usage = args.output / f"{label}.resource.json"
                            native = args.output / f"{label}.hyperfine.json"
                            command = [str(binaries[build]), "--csv", f"facts={facts}", "--csv", f"dimension={dim}", "--batch-size", "8192", "--workers", "1" if side == "custom1" else "4", "--sql", sql]
                            if side == "datafusion4":
                                command.append("--reference-tuned")
                            resource_format = '{"user_s":%U,"sys_s":%S,"peak_rss_kib":%M,"read_blocks":%I,"write_blocks":%O}'
                            shell = "exec " + shlex.join(["/usr/bin/time", "-f", resource_format, "-o", str(usage), *command]) + " > " + shlex.quote(str(output)) + " 2> " + shlex.quote(str(stderr))
                            # Direct exec avoids Hyperfine shell calibration; the explicit wrapper is identical for both builds.
                            measured = ["hyperfine", "--shell=none", "--runs", "1", "--warmup", "0", "--style", "none", "--metrics", "time_wall_clock", "--export-json", str(native), shlex.join(["sh", "-c", shell])]
                            result = subprocess.run(measured, cwd=ROOT, capture_output=True, timeout=240)
                            (args.output / f"{label}.hyperfine.log").write_bytes(result.stdout + result.stderr)
                            sample = {"size": int(size), "query": query, "build": build, "side": side, "repeat": repeat, "warmup": repeat == -1, "command": command, "hyperfine_command": measured, "exit": result.returncode, "native": native.name}
                            manifest["samples"].append(sample)
                            assert result.returncode == 0, f"Hyperfine failed: {label}"
                            assert output.stat().st_size <= 1024**2 and stderr.stat().st_size <= 1024**2, "output cap"
                            sample["csv_sha256"] = digest(output)
                            assert sample["csv_sha256"] == fixture["outputs"][query], f"original result mismatch: {label}"
                            exported = json.loads(native.read_text())
                            assert exported["schema_version"] == 2 and len(exported["results"]) == 1
                            measurements = exported["results"][0]["measurements"]
                            assert len(measurements) == 1 and measurements[0]["exit_code"] == 0
                            assert exported["primary_metric"] == "time_wall_clock"
                            sample["resource"] = json.loads(usage.read_text())
                            (args.output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")
                            print(label, flush=True)
            assert len(manifest["samples"]) == 540
            manifest["passed"] = True
        manifest["scratch_removed"] = True
    finally:
        (args.output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
