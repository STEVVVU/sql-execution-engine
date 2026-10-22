"""Separate Callgrind diagnosis of the frozen 100k-row dimension join."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

from benchmark import QUERIES, ROOT

FACTS_SHA256 = "43957c70963b20d63471fc2c21713552d0062580ed8d765596bf3e9d4fbf468c"
RESULT_SHA256 = "e645c4ed1153beb934d1636714f4095036d6be6d3beb9975c041af5500ec3139"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("Callgrind diagnosis requires Linux")
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        raise RuntimeError("10 GiB disk reserve required")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    binary = ROOT / "target/release/ee2"
    result = {"source_sha":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),"binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),"platform":platform.platform(),"cpu_count":os.cpu_count(),"command_version":subprocess.check_output(["valgrind","--version"],text=True).strip(),"query":QUERIES["dimension_join"],"rows":100000,"batch_size":8192,"facts_sha256":FACTS_SHA256,"expected_csv_sha256":RESULT_SHA256,"samples":[],"passed":False,"policy":"Diagnostic instruction profile only, excluded from wall-time comparisons. Release optimization with line debug info. Same frozen query/data as campaign39729583363. All three sides consume and verify complete CSV output."}
    try:
        with tempfile.TemporaryDirectory(prefix="vectorloom-profile-") as scratch:
            work = Path(scratch)
            facts, dimension = work / "facts.csv", work / "dimension.csv"
            with facts.open("w",newline="") as output:
                writer = csv.writer(output)
                writer.writerow(["id","category","value"])
                writer.writerows((i,i%100,(i*7919)%1000) for i in range(100000))
            dimension.write_text("category\n"+"".join(f"{i}\n" for i in range(100)))
            assert hashlib.sha256(facts.read_bytes()).hexdigest() == FACTS_SHA256, "profile workload differs from measured baseline"
            for side,workers in [("custom1",1),("custom4",4),("datafusion4",4)]:
                callgrind = args.output / f"{side}.callgrind"
                command = ["valgrind","--tool=callgrind","--quiet","--error-exitcode=99",f"--callgrind-out-file={callgrind}","--dump-instr=yes","--collect-jumps=yes",str(binary),"--csv",f"facts={facts}","--csv",f"dimension={dimension}","--batch-size","8192","--workers",str(workers),"--sql",QUERIES["dimension_join"]]
                if side == "datafusion4":
                    command.append("--reference-tuned")
                completed = subprocess.run(command,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=240)
                (args.output/f"{side}.csv").write_bytes(completed.stdout)
                (args.output/f"{side}.stderr").write_bytes(completed.stderr)
                checksum = hashlib.sha256(completed.stdout).hexdigest()
                sample = {"side":side,"workers":workers,"command":command,"exit":completed.returncode,"csv_sha256":checksum}
                result["samples"].append(sample)
                assert completed.returncode == 0, f"profile failed for {side}"
                assert checksum == RESULT_SHA256, f"profile output differs from measured baseline for {side}"
                for inclusive,automatic,label in [("yes","no","inclusive"),("no","yes","source")]:
                    annotation = subprocess.run(["callgrind_annotate",f"--inclusive={inclusive}",f"--auto={automatic}","--tree=both","--threshold=95",str(callgrind)],capture_output=True,timeout=60,check=True)
                    (args.output/f"{side}.{label}.txt").write_bytes(annotation.stdout)
                    (args.output/f"{side}.{label}.stderr").write_bytes(annotation.stderr)
                print(f"PASS {side}: frozen fixture and result digests match",flush=True)
            result["passed"] = True
    finally:
        (args.output/"result.json").write_text(json.dumps(result,indent=2)+"\n")


if __name__ == "__main__":
    main()
