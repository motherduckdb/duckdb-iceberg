"""The deliberately small v2 catalog contract used by the mock SQL suite."""

import copy
import json
import threading
import time
import uuid
from pathlib import Path


class CatalogError(Exception):
    def __init__(self, code, kind, message):
        super().__init__(message)
        self.code = code
        self.kind = kind


def invalid(message):
    raise CatalogError(400, "ValidationException", message)


def unsupported(message):
    raise CatalogError(501, "UnsupportedOperationException", message)


def namespace_parts(value):
    if not isinstance(value, list) or not value or any(not isinstance(part, str) or not part for part in value):
        invalid("Namespace must contain nonempty strings")
    return tuple(value)


def string_properties(value):
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
    ):
        invalid("Properties must be a string-to-string map")
    return copy.deepcopy(value)


def field_ids(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in ("id", "element-id", "key-id", "value-id"):
                yield child
            else:
                yield from field_ids(child)
    elif isinstance(value, list):
        for child in value:
            yield from field_ids(child)


class Catalog:
    def __init__(self, warehouse):
        self.warehouse = Path(warehouse).resolve()
        self.warehouse.mkdir(parents=True)
        self.lock = threading.RLock()
        self.namespaces = {}
        self.tables = {}
        # A UUID identifies a staged attempt, including competing creates of the same name.
        self.staged = {}

    def namespace(self, namespace):
        if namespace not in self.namespaces:
            raise CatalogError(404, "NoSuchNamespaceException", f"Namespace does not exist: {namespace}")
        return {"namespace": list(namespace), "properties": copy.deepcopy(self.namespaces[namespace])}

    def create_namespace(self, body):
        namespace = namespace_parts(body["namespace"])
        properties = string_properties(body.get("properties", {}))
        if namespace in self.namespaces:
            raise CatalogError(409, "AlreadyExistsException", "Namespace already exists")
        # Tables use unique UUID directories below this default warehouse root.
        properties.setdefault("location", str(self.warehouse))
        self.namespaces[namespace] = properties
        return self.namespace(namespace)

    def update_namespace_properties(self, namespace, body):
        properties = self.namespace(namespace)["properties"]
        updates = string_properties(body.get("updates", {}))
        removals = body.get("removals", [])
        if not isinstance(removals, list) or any(not isinstance(key, str) for key in removals):
            invalid("Property removals must be a list of strings")
        if len(set(removals)) != len(removals):
            invalid("Property removals must be unique")
        if set(removals).intersection(updates):
            raise CatalogError(422, "UnprocessableEntityException", "A property cannot be updated and removed together")
        removed = [key for key in removals if key in properties]
        missing = [key for key in removals if key not in properties]
        for key in removed:
            del properties[key]
        properties.update(updates)
        self.namespaces[namespace] = properties
        return {"updated": list(updates), "removed": removed, "missing": missing}

    def drop_namespace(self, namespace):
        self.namespace(namespace)
        if any(key[0] == namespace for key in self.tables) or any(
            other[: len(namespace)] == namespace and other != namespace for other in self.namespaces
        ):
            raise CatalogError(409, "NamespaceNotEmptyException", "Namespace is not empty")
        del self.namespaces[namespace]

    def load(self, key):
        self.namespace(key[0])
        if key not in self.tables:
            raise CatalogError(404, "NoSuchTableException", f"Table does not exist: {key[1]}")
        return copy.deepcopy(self.tables[key])

    def rename(self, body):
        identifiers = []
        for field in ("source", "destination"):
            identifier = body[field]
            namespace = namespace_parts(identifier["namespace"])
            name = identifier["name"]
            if not isinstance(name, str) or not name:
                invalid("Table name must be a nonempty string")
            identifiers.append((namespace, name))
        source, destination = identifiers
        if source not in self.tables:
            raise CatalogError(404, "NoSuchTableException", f"Table does not exist: {source[1]}")
        self.namespace(destination[0])
        if destination in self.tables:
            raise CatalogError(409, "AlreadyExistsException", f"Table already exists: {destination[1]}")
        # Move only the published identifier. UUID, location, snapshots and metadata
        # file stay unchanged; staged creates remain bound to their original names.
        self.tables[destination] = self.tables.pop(source)

    def create(self, namespace, body):
        self.namespace(namespace)
        key = namespace, body["name"]
        if not isinstance(key[1], str) or not key[1]:
            invalid("Table name must be a nonempty string")
        if key in self.tables:
            raise CatalogError(409, "AlreadyExistsException", "Table already exists")
        properties = copy.deepcopy(body.get("properties", {}))
        version = int(properties.pop("format-version", "2"))
        if version != 2:
            unsupported("The mock implements only format-version 2")
        if body.get("location"):
            unsupported("Explicit table locations are outside the mock's temporary warehouse contract")
        table_uuid = str(uuid.uuid4())
        location = self.warehouse / table_uuid
        location.mkdir()
        (location / "metadata").mkdir()
        schema = copy.deepcopy(body["schema"])
        schema.setdefault("schema-id", 0)
        spec = copy.deepcopy(body.get("partition-spec") or {"spec-id": 0, "fields": []})
        # The client's create request includes a legacy "type": "struct" member.
        spec.pop("type", None)
        spec.setdefault("spec-id", 0)
        order = copy.deepcopy(body.get("write-order") or {"order-id": 0, "fields": []})
        order.setdefault("order-id", 1 if order["fields"] else 0)
        metadata = {
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": str(location),
            "last-updated-ms": int(time.time() * 1000),
            "last-column-id": max(field_ids(schema), default=0),
            "schemas": [schema],
            "current-schema-id": schema["schema-id"],
            "partition-specs": [spec],
            "default-spec-id": spec["spec-id"],
            "last-partition-id": max((f["field-id"] for f in spec["fields"]), default=999),
            "sort-orders": [order],
            "default-sort-order-id": order["order-id"],
            "properties": properties,
            "snapshots": [],
            "refs": {},
            "last-sequence-number": 0,
            "snapshot-log": [],
            "metadata-log": [],
        }
        if body.get("stage-create", False):
            self.staged[table_uuid] = (key, metadata)
            return {"metadata": copy.deepcopy(metadata), "config": {}}
        return self.publish(key, metadata)

    def publish(self, key, metadata):
        schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
        current_fields = set(field_ids(schema))
        for collection, id_key, current_key in (
            ("partition-specs", "spec-id", "default-spec-id"),
            ("sort-orders", "order-id", "default-sort-order-id"),
        ):
            layout = next(item for item in metadata[collection] if item[id_key] == metadata[current_key])
            for field in layout["fields"]:
                # A void partition field may remain after its source column is dropped.
                if field["transform"] != "void" and field["source-id"] not in current_fields:
                    invalid(f"Current {collection} reference missing schema field {field['source-id']}")
        metadata["last-updated-ms"] = time.time_ns() // 1_000_000
        previous = self.tables.get(key)
        if previous:
            # Each entry describes the *previous* file and its own update time.
            # Keep metadata-only versions as well as versions that add snapshots.
            metadata["metadata-log"].append(
                {
                    "metadata-file": previous["metadata-location"],
                    "timestamp-ms": previous["metadata"]["last-updated-ms"],
                }
            )
        directory = Path(metadata["location"]) / "metadata"
        directory.mkdir(exist_ok=True)
        path = directory / f"{uuid.uuid4()}.metadata.json"
        # No published state changes until the complete immutable file is closed.
        with path.open("x") as output:
            json.dump(metadata, output)
        result = {"metadata": metadata, "metadata-location": str(path), "config": {}}
        self.tables[key] = result
        return copy.deepcopy(result)

    def check_requirement(self, metadata, requirement):
        kind = requirement["type"]
        fields = {
            "assert-table-uuid": ("table-uuid", "uuid"),
            "assert-current-schema-id": ("current-schema-id", "current-schema-id"),
            "assert-last-assigned-field-id": ("last-column-id", "last-assigned-field-id"),
            "assert-last-assigned-partition-id": ("last-partition-id", "last-assigned-partition-id"),
            "assert-default-spec-id": ("default-spec-id", "default-spec-id"),
            "assert-default-sort-order-id": ("default-sort-order-id", "default-sort-order-id"),
        }
        if kind == "assert-create":
            valid = metadata is None
        elif kind == "assert-ref-snapshot-id":
            actual = metadata["refs"].get(requirement["ref"], {}).get("snapshot-id") if metadata else None
            valid = metadata is not None and actual == requirement.get("snapshot-id")
        elif kind in fields:
            field, argument = fields[kind]
            valid = metadata is not None and metadata[field] == requirement[argument]
        else:
            unsupported(f"Unknown requirement: {kind}")
        if not valid:
            raise CatalogError(409, "CommitFailedException", f"Requirement failed: {kind}")

    def commit(self, key, body):
        self.namespace(key[0])
        requirements, updates = body["requirements"], body["updates"]
        old = self.tables.get(key)
        for requirement in requirements:
            self.check_requirement(old["metadata"] if old else None, requirement)
        if old:
            candidate = copy.deepcopy(old["metadata"])
        else:
            if not any(r["type"] == "assert-create" for r in requirements):
                raise CatalogError(404, "NoSuchTableException", "Create commit requires assert-create")
            identifiers = [u["uuid"] for u in updates if u["action"] == "assign-uuid"]
            if len(identifiers) != 1 or identifiers[0] not in self.staged:
                invalid("Create commit must identify a staged UUID")
            staged_key, staged_metadata = self.staged[identifiers[0]]
            if staged_key != key:
                invalid("Staged UUID belongs to another table")
            candidate = copy.deepcopy(staged_metadata)
        last_added = {}
        for update in updates:
            self.apply(candidate, update, last_added)
        result = self.publish(key, candidate)
        self.staged.pop(candidate["table-uuid"], None)
        return result

    def apply(self, metadata, update, last_added):
        action = update["action"]
        additions = {
            "add-schema": ("schema", "schemas", "schema-id"),
            "add-spec": ("spec", "partition-specs", "spec-id"),
            "add-sort-order": ("sort-order", "sort-orders", "order-id"),
        }
        selections = {
            "set-current-schema": ("schema-id", "current-schema-id", "schemas", "schema-id"),
            "set-default-spec": ("spec-id", "default-spec-id", "partition-specs", "spec-id"),
            "set-default-sort-order": ("sort-order-id", "default-sort-order-id", "sort-orders", "order-id"),
        }
        if action == "assign-uuid":
            if update["uuid"] != metadata["table-uuid"]:
                invalid("Cannot replace table UUID")
        elif action == "upgrade-format-version":
            if update["format-version"] != 2:
                unsupported("The mock implements only format-version 2")
        elif action == "set-location":
            if update["location"] != metadata["location"]:
                unsupported("Relocating tables is not implemented")
        elif action in additions:
            argument, collection, id_key = additions[action]
            item = copy.deepcopy(update[argument])
            identifier = item[id_key]
            if not isinstance(identifier, int) or identifier < 0:
                invalid(f"Invalid {id_key}")
            existing = next((v for v in metadata[collection] if v[id_key] == identifier), None)
            if existing is not None and existing != item:
                invalid(f"Conflicting {id_key}: {identifier}")
            if existing is None:
                metadata[collection].append(item)
            last_added[collection] = identifier
            if action == "add-schema":
                metadata["last-column-id"] = max(
                    metadata["last-column-id"], max(field_ids(item), default=0), update.get("last-column-id", 0)
                )
            elif action == "add-spec":
                metadata["last-partition-id"] = max(
                    metadata["last-partition-id"], max((f["field-id"] for f in item["fields"]), default=999)
                )
        elif action in selections:
            argument, target, collection, id_key = selections[action]
            identifier = update[argument]
            if identifier == -1:
                if collection not in last_added:
                    invalid(f"No last added {collection} in this commit")
                identifier = last_added[collection]
            if not any(v[id_key] == identifier for v in metadata[collection]):
                invalid(f"Unknown {id_key}: {identifier}")
            metadata[target] = identifier
        elif action == "set-properties":
            if "format-version" in update["updates"]:
                invalid("Use upgrade-format-version instead of a property update")
            metadata["properties"].update(update["updates"])
        elif action == "remove-properties":
            for name in update["removals"]:
                metadata["properties"].pop(name, None)
        elif action == "add-snapshot":
            snapshot = copy.deepcopy(update["snapshot"])
            identifier = snapshot["snapshot-id"]
            if any(s["snapshot-id"] == identifier for s in metadata["snapshots"]):
                invalid("Snapshot ID already exists")
            if snapshot["sequence-number"] <= metadata["last-sequence-number"]:
                invalid("Snapshot sequence number must increase")
            if snapshot.get("parent-snapshot-id") is not None and not any(
                s["snapshot-id"] == snapshot["parent-snapshot-id"] for s in metadata["snapshots"]
            ):
                invalid("Unknown parent snapshot")
            if not any(s["schema-id"] == snapshot["schema-id"] for s in metadata["schemas"]):
                invalid("Unknown snapshot schema")
            # DuckDB writes the data/delete files and manifest list for every operation.
            # The catalog publishes their metadata identically, preserving the summary.
            if snapshot["summary"]["operation"] not in ("append", "replace", "overwrite", "delete"):
                invalid("Unknown snapshot operation")
            for field in ("manifest-list", "timestamp-ms"):
                if field not in snapshot:
                    invalid(f"Missing snapshot {field}")
            metadata["snapshots"].append(snapshot)
            metadata["last-sequence-number"] = snapshot["sequence-number"]
            last_added.setdefault("snapshots", set()).add(identifier)
        elif action == "set-snapshot-ref":
            identifier = update["snapshot-id"]
            if not any(s["snapshot-id"] == identifier for s in metadata["snapshots"]):
                invalid("Unknown snapshot reference")
            if update["ref-name"] != "main" or update["type"] != "branch":
                unsupported("Only the main branch is implemented")
            metadata["refs"]["main"] = {k: v for k, v in update.items() if k not in ("action", "ref-name")}
            if metadata.get("current-snapshot-id") != identifier:
                # New snapshots are addressable at their own timestamps. Moving main
                # back to an existing snapshot is a new history event, not a rewrite
                # of the snapshot's creation time.
                timestamp = time.time_ns() // 1_000_000
                if identifier in last_added.get("snapshots", set()):
                    timestamp = next(s["timestamp-ms"] for s in metadata["snapshots"] if s["snapshot-id"] == identifier)
                metadata["snapshot-log"].append({"snapshot-id": identifier, "timestamp-ms": timestamp})
                metadata["current-snapshot-id"] = identifier
        else:
            unsupported(f"Unknown update action: {action}")
