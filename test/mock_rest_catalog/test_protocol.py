"""Independent contract tests. Run with python3 -m unittest discover -s test/mock_rest_catalog."""

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from mock_rest_catalog.model import Catalog, CatalogError
from mock_rest_catalog.server import ENDPOINTS, MockServer
from run_mock_catalog_tests import result_status, run_process


SCHEMA = {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}]}


class ModelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.catalog = Catalog(Path(self.temp.name) / "warehouse")
        self.catalog.create_namespace({"namespace": ["default"]})
        self.key = (("default",), "table")

    def stage(self):
        return self.catalog.create(("default",), {"name": "table", "schema": SCHEMA, "stage-create": True})["metadata"]

    def create_commit(self, metadata, extra=()):
        return self.catalog.commit(
            self.key,
            {
                "requirements": [{"type": "assert-create"}],
                "updates": [{"action": "assign-uuid", "uuid": metadata["table-uuid"]}, *extra],
            },
        )

    def assert_error(self, code, function, *args):
        with self.assertRaises(CatalogError) as error:
            function(*args)
        self.assertEqual(error.exception.code, code)

    def test_staged_invisibility_and_competing_creates(self):
        abandoned = self.stage()
        winner = self.stage()
        self.assertNotEqual(abandoned["location"], winner["location"])
        self.assertEqual(self.catalog.tables, {})
        self.assert_error(404, self.catalog.load, self.key)
        result = self.create_commit(winner)
        self.assertEqual(result["metadata"]["table-uuid"], winner["table-uuid"])
        self.assert_error(409, self.create_commit, abandoned)
        self.assertEqual(self.catalog.load(self.key), result)

    def test_legacy_create_spec_matches_commit_spec(self):
        metadata = self.catalog.create(
            ("default",),
            {
                "name": "table",
                "schema": SCHEMA,
                "stage-create": True,
                "partition-spec": {"type": "struct", "spec-id": 0, "fields": []},
            },
        )["metadata"]
        result = self.create_commit(metadata, [{"action": "add-spec", "spec": {"spec-id": 0, "fields": []}}])
        self.assertEqual(result["metadata"]["partition-specs"], [{"spec-id": 0, "fields": []}])
        self.assertTrue((Path(metadata["location"]) / "metadata").is_dir())

    def test_malformed_create_and_update_do_not_publish(self):
        staged = self.stage()
        self.assert_error(
            400,
            self.create_commit,
            staged,
            [
                {"action": "set-properties", "updates": {"should-not-publish": "yes"}},
                {"action": "set-current-schema", "schema-id": 123},
            ],
        )
        self.assertEqual(self.catalog.tables, {})
        self.assertEqual(list(Path(staged["location"]).rglob("*.metadata.json")), [])
        old = self.create_commit(staged)
        self.assert_error(
            501,
            self.catalog.commit,
            self.key,
            {
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"bad": "yes"}}, {"action": "not-implemented"}],
            },
        )
        self.assertEqual(self.catalog.load(self.key), old)
        self.assertEqual(json.loads(Path(old["metadata-location"]).read_text()), old["metadata"])

    def test_requirement_conflicts_preserve_state(self):
        old = self.create_commit(self.stage())
        requirements = [
            {"type": "assert-table-uuid", "uuid": "wrong"},
            {"type": "assert-current-schema-id", "current-schema-id": 99},
            {"type": "assert-last-assigned-field-id", "last-assigned-field-id": 99},
            {"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 99},
            {"type": "assert-default-spec-id", "default-spec-id": 99},
            {"type": "assert-default-sort-order-id", "default-sort-order-id": 99},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 123},
        ]
        for requirement in requirements:
            with self.subTest(requirement=requirement):
                self.assert_error(
                    409,
                    self.catalog.commit,
                    self.key,
                    {
                        "requirements": [requirement],
                        "updates": [{"action": "set-properties", "updates": {"bad": "yes"}}],
                    },
                )
                self.assertEqual(self.catalog.load(self.key), old)
        self.assert_error(
            501, self.catalog.commit, self.key, {"requirements": [{"type": "assert-unknown"}], "updates": []}
        )

    def test_metadata_versions_and_snapshot_numbers_are_exact(self):
        old = self.create_commit(self.stage())
        identifier = 9007199254740993
        manifest = str(Path(old["metadata"]["location"]) / "metadata" / "manifest-list.avro")
        # Protocol tests don't fabricate Avro: DuckDB's smoke test validates actual files.
        snapshot = {
            "snapshot-id": identifier,
            "sequence-number": 1,
            "schema-id": 0,
            "timestamp-ms": 1700000000000,
            "manifest-list": manifest,
            "summary": {"operation": "append", "added-records": "2"},
        }
        result = self.catalog.commit(
            self.key,
            {
                "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}],
                "updates": [
                    {"action": "add-snapshot", "snapshot": snapshot},
                    {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": identifier},
                ],
            },
        )
        self.assertEqual(result["metadata"]["snapshots"], [snapshot])
        self.assertEqual(result["metadata"]["current-snapshot-id"], identifier)
        self.assertEqual(result["metadata"]["last-sequence-number"], 1)
        self.assertNotEqual(old["metadata-location"], result["metadata-location"])
        self.assertEqual(json.loads(Path(old["metadata-location"]).read_text()), old["metadata"])
        self.assertEqual(json.loads(Path(result["metadata-location"]).read_text()), result["metadata"])
        self.assert_error(
            409,
            self.catalog.commit,
            self.key,
            {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}], "updates": []},
        )

    def test_last_added_ids_are_scoped_to_commit(self):
        self.create_commit(self.stage())
        schema = copy.deepcopy(SCHEMA)
        schema["schema-id"] = 2
        result = self.catalog.commit(
            self.key,
            {
                "requirements": [],
                "updates": [
                    {"action": "add-schema", "schema": schema},
                    {"action": "set-current-schema", "schema-id": -1},
                    {
                        "action": "add-spec",
                        "spec": {
                            "spec-id": 3,
                            "fields": [{"name": "id", "source-id": 1, "field-id": 1000, "transform": "identity"}],
                        },
                    },
                    {"action": "set-default-spec", "spec-id": -1},
                    {
                        "action": "add-sort-order",
                        "sort-order": {
                            "order-id": 4,
                            "fields": [
                                {
                                    "source-id": 1,
                                    "transform": "identity",
                                    "direction": "asc",
                                    "null-order": "nulls-first",
                                }
                            ],
                        },
                    },
                    {"action": "set-default-sort-order", "sort-order-id": -1},
                ],
            },
        )
        self.assertEqual(result["metadata"]["current-schema-id"], 2)
        self.assertEqual(result["metadata"]["default-spec-id"], 3)
        self.assertEqual(result["metadata"]["last-partition-id"], 1000)
        self.assertEqual(result["metadata"]["default-sort-order-id"], 4)
        self.assert_error(
            400,
            self.catalog.commit,
            self.key,
            {"requirements": [], "updates": [{"action": "set-current-schema", "schema-id": -1}]},
        )

    def test_uuid_cannot_be_committed_under_another_name(self):
        staged = self.stage()
        self.assert_error(
            400,
            self.catalog.commit,
            (("default",), "other"),
            {
                "requirements": [{"type": "assert-create"}],
                "updates": [{"action": "assign-uuid", "uuid": staged["table-uuid"]}],
            },
        )
        self.assertEqual(self.catalog.tables, {})

    def test_namespace_missing_existing_and_nonempty(self):
        self.assert_error(404, self.catalog.namespace, ("missing",))
        self.assert_error(409, self.catalog.create_namespace, {"namespace": ["default"]})
        self.create_commit(self.stage())
        self.assert_error(409, self.catalog.drop_namespace, ("default",))
        self.assertIn(self.key, self.catalog.tables)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.server = MockServer(Path(self.temp.name))
        self.server.__enter__()
        self.addCleanup(self.server.__exit__)
        self.opener = build_opener(ProxyHandler({}))

    def request(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.server.uri + path, data=data, method=method)
        try:
            response = self.opener.open(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            content = response.read()
            return response.status, json.loads(content) if content else None

    def test_endpoints_unknown_operations_and_bad_json(self):
        status, config = self.request("GET", "/v1/config?warehouse=ignored")
        self.assertEqual(status, 200)
        self.assertEqual(config["endpoints"], ENDPOINTS)
        self.assertFalse(any("views" in e or "transactions" in e or e.startswith("HEAD") for e in ENDPOINTS))
        status, body = self.request("POST", "/v1/transactions/commit", {})
        self.assertEqual(status, 501)
        self.assertEqual(body["error"]["code"], 501)
        self.assertEqual(len(self.server.unsupported), 1)
        self.assertEqual(self.request("POST", "/v1/namespaces", [1])[0], 400)
        self.assertEqual(self.server.errors, [])
        journal = [json.loads(line) for line in self.server.journal.read_text().splitlines()]
        self.assertEqual([row["status"] for row in journal], [200, 501, 400])

    def test_encoded_names_and_unregister_preserve_files(self):
        namespace = ["Mixed / %", "child"]
        table = "Table / % +"
        ns_path = "/v1/namespaces/" + quote("\x1f".join(namespace), safe="")
        table_path = ns_path + "/tables/" + quote(table, safe="")
        self.assertEqual(self.request("POST", "/v1/namespaces", {"namespace": namespace})[0], 200)
        status, created = self.request(
            "POST", ns_path + "/tables", {"name": table, "schema": SCHEMA, "stage-create": False}
        )
        self.assertEqual(status, 200)
        self.assertEqual(self.request("GET", table_path), (200, created))
        self.assertEqual(
            self.request("GET", ns_path + "/tables")[1]["identifiers"], [{"namespace": namespace, "name": table}]
        )
        self.assertEqual(self.request("DELETE", table_path + "?purgeRequested=false")[0], 204)
        self.assertTrue(Path(created["metadata-location"]).exists())
        self.assertEqual(self.request("GET", table_path)[0], 404)
        self.assertEqual(self.request("DELETE", ns_path)[0], 204)

    def test_competing_http_commits_have_one_winner(self):
        # Concurrent requests are intentional within this one isolated temporary catalog.
        from concurrent.futures import ThreadPoolExecutor

        self.request("POST", "/v1/namespaces", {"namespace": ["default"]})
        self.request(
            "POST", "/v1/namespaces/default/tables", {"name": "table", "schema": SCHEMA, "stage-create": False}
        )
        path = "/v1/namespaces/default/tables/table"

        def append(identifier):
            return self.request(
                "POST",
                path,
                {
                    "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}],
                    "updates": [
                        {
                            "action": "add-snapshot",
                            "snapshot": {
                                "snapshot-id": identifier,
                                "sequence-number": 1,
                                "schema-id": 0,
                                "timestamp-ms": 1700000000000,
                                "manifest-list": "/unused-protocol-only.avro",
                                "summary": {"operation": "append"},
                            },
                        },
                        {"action": "set-snapshot-ref", "snapshot-id": identifier, "ref-name": "main", "type": "branch"},
                    ],
                },
            )[0]

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(append, [123, 456]))
        self.assertEqual(sorted(statuses), [200, 409])
        metadata = self.request("GET", path)[1]["metadata"]
        self.assertEqual(len(metadata["snapshots"]), 1)
        self.assertEqual(metadata["current-snapshot-id"], metadata["snapshots"][0]["snapshot-id"])


class RunnerTests(unittest.TestCase):
    def test_no_match_skip_and_zero_assertions_cannot_pass(self):
        with self.assertRaises(RuntimeError):
            result_status("No test cases matched", "a")
        event = {"event": "end", "name": "a", "status": "skip-requirement", "passes": 0, "fails": 0}
        self.assertEqual(result_status("[TEST_EVENT] " + json.dumps(event), "a"), "skipped")
        event["status"] = "ok"
        self.assertEqual(result_status("[TEST_EVENT] " + json.dumps(event), "a"), "failed")
        event["passes"] = 2
        self.assertEqual(result_status("[TEST_EVENT] " + json.dumps(event), "a"), "executed")

    def test_timeout_reaps_child_and_retains_output(self):
        import os
        import subprocess

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "child.log"
            with self.assertRaises(subprocess.TimeoutExpired):
                run_process(
                    [sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)"],
                    os.environ.copy(),
                    0.5,
                    output,
                )
            self.assertIn("started", output.read_text())


if __name__ == "__main__":
    unittest.main()
