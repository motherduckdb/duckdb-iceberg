#!/usr/bin/env python3
"""Run the admitted SQL files serially, each with a fresh loopback catalog."""

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

from mock_rest_catalog.server import MockServer
from mock_rest_catalog.config import load_config


ROOT = Path(__file__).resolve().parents[1]
BASE = Path("test/sql/local/catalog_test_config_setup/catalog_agnostic")
SELECTION = ROOT / "test/mock_rest_catalog/selection.json"
SMOKE = ROOT / "test/mock_rest_catalog/smoke.test"


def run_process(command, environment, timeout, output_path, input_text=None):
    with output_path.open("w") as output:
        with subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
        ) as process:
            try:
                process.communicate(input=input_text, timeout=timeout)
            except BaseException:
                process.kill()
                process.wait()
                raise
    return process.returncode


def result_status(output, name):
    events = [json.loads(match) for match in re.findall(r"\[TEST_EVENT\] (\{[^\n]*\})", output)]
    ends = [event for event in events if event["event"] == "end"]
    if len(ends) != 1 or ends[0]["name"] != name:
        raise RuntimeError(f"Expected exactly one terminal test event for {name}; got {ends}")
    event = ends[0]
    if event["status"] == "skip-requirement" or event.get("skip-mode", 0):
        return "skipped"
    if event["status"] != "ok" or event["fails"] or not event["passes"]:
        return "failed"
    return "executed"


def run_case(binary, name, environment, timeout, input_text=None):
    directory = Path(tempfile.mkdtemp(prefix="iceberg-mock-"))
    success = False
    try:
        with MockServer(directory) as server:
            # The socket is already bound. A bounded real HTTP exchange checks readiness.
            with build_opener(ProxyHandler({})).open(server.uri + "/v1/config", timeout=5) as response:
                if response.status != 200:
                    raise RuntimeError("Mock catalog failed readiness")
            config, default_uri = load_config()
            config["on_init"] = config["on_init"].replace(default_uri, server.uri)
            config_path = directory / "config.json"
            config_path.write_text(json.dumps(config, indent=2))
            output_path = directory / "unittest.log"
            command = [
                str(binary),
                "--test-config",
                str(config_path),
                "--emit-test-events",
                "--stdin" if input_text is not None else name,
            ]
            code = run_process(command, environment, timeout, output_path, input_text)
            status = result_status(output_path.read_text(), name)
            if code:
                status = "failed"
            if server.unsupported:
                status = "unsupported"
            if server.errors:
                (directory / "server-errors.log").write_text("\n".join(server.errors))
                status = "failed"
            success = status == "executed"
            print(f"{status}: {name}", flush=True)
            if not success:
                print(output_path.read_text()[-8000:], flush=True)
            return status
    except Exception as error:
        print(f"failed: {name}: {error}", flush=True)
        (directory / "runner-error.log").write_text(f"{type(error).__name__}: {error}\n")
        return "failed"
    finally:
        if success:
            shutil.rmtree(directory)
        else:
            print(f"Diagnostics retained: {directory}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unittest-binary", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=120, help="Seconds per SQL file")
    parser.add_argument("tests", nargs="*", help="Exact paths relative to catalog_agnostic; defaults to selection.json")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    binary = args.unittest_binary.resolve()
    selection = json.loads(SELECTION.read_text())
    tests = args.tests or list(selection)
    if len(set(tests)) != len(tests) or any(test not in selection for test in tests):
        parser.error("Tests must be unique exact entries in test/mock_rest_catalog/selection.json")
    environment = os.environ.copy()
    # A developer's catalog support flags must not turn unsupported features on.
    for key in list(environment):
        if key.endswith("_SUPPORT") or key in ("CATALOG_TEST_CONFIG_SETUP", "SCAN_PLANNING_MODE"):
            del environment[key]
    environment["NO_PROXY"] = environment["no_proxy"] = "127.0.0.1,localhost"
    names = []
    # Resolve both extension-relative and absolute registrations, without running duplicates.
    for test in tests:
        relative = str(BASE / test)
        found = subprocess.run(
            [str(binary), "*" + relative, "--list-test-names-only"],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
        candidates = found.stdout.splitlines()
        if found.returncode not in (0, len(candidates)):
            raise RuntimeError(f"Test discovery failed: {found.stdout}\n{found.stderr}")
        if relative in candidates:
            names.append(relative)
        elif str(ROOT / relative) in candidates:
            names.append(str(ROOT / relative))
        else:
            raise RuntimeError(f"Selected test is not registered: {relative}")
    counts = dict.fromkeys(("executed", "skipped", "failed", "unsupported"), 0)
    # Verify attachment, publication and a fresh connection before the selected files.
    status = run_case(binary, "<stdin>", environment, args.timeout, SMOKE.read_text())
    if status != "executed":
        print("Attachment/end-to-end smoke failed; selection was not run.")
        return 1
    for name in names:
        counts[run_case(binary, name, environment, args.timeout)] += 1
    print("SQL selection: " + ", ".join(f"{key}={value}" for key, value in counts.items()))
    return int(counts["executed"] != len(names))


if __name__ == "__main__":
    raise SystemExit(main())
