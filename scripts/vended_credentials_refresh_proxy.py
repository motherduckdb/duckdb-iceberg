"""mitmproxy addon for deterministic Iceberg vended credential refresh tests.

Run with:

    mitmdump --mode regular@19133 -s scripts/vended_credentials_refresh_proxy.py

The addon rewrites selected fixture catalog table responses to vend controlled
S3 credentials, then intercepts MinIO requests to reject stale credentials and
serve deterministic local Parquet objects. All state is kept in this process so
the sqllogictest can exercise HTTPFS refresh paths without waiting for a real
STS expiry.
"""

import datetime
import hashlib
import hmac
import json
import re
import urllib.parse
from pathlib import Path

from mitmproxy import http


CATALOG_HOST = "127.0.0.1"
CATALOG_PORT = 8181
S3_HOST = "127.0.0.1"
S3_PORT = 9000

TABLE_SOURCE = "table_unpartitioned"
INIT_TABLE = "vended_init_refresh"
RANGE_TABLE = "vended_range_refresh"
SAME_BAD_TABLE = "vended_same_bad_refresh"
RANGE_FAIL_TABLE = "vended_range_fail_refresh"

OLD_INIT_KEY = "OLD_INIT_KEY"
NEW_INIT_KEY = "NEW_INIT_KEY"
OLD_RANGE_KEY = "OLD_RANGE_KEY"
NEW_RANGE_KEY = "NEW_RANGE_KEY"
OLD_SAME_BAD_KEY = "OLD_SAME_BAD_KEY"
OLD_RANGE_FAIL_KEY = "OLD_RANGE_FAIL_KEY"
NEW_RANGE_FAIL_KEY = "NEW_RANGE_FAIL_KEY"

UPSTREAM_S3_KEY = "admin"
UPSTREAM_S3_SECRET = "password"
UPSTREAM_S3_REGION = "us-east-1"

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_TINY_PARQUET_SOURCE = (
    SCRIPT_DIR.parent
    / "data/persistent/expression_filter/data/00000-0-1406cdaa-c3e4-4e6d-a22b-d85e4a813169-00001.parquet"
)
LOCAL_RANGE_PARQUET_SOURCE = (
    SCRIPT_DIR.parent
    / "data/persistent/iceberg/lineitem_iceberg_gz/data/00000-2-371a340c-ded5-4e85-aa49-9c788d6f21cd-00001.parquet"
)
LOCAL_PARQUET_OBJECTS = {
    "/warehouse/vended_credentials_refresh/init/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/range/data.parquet": LOCAL_RANGE_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/same_bad/data.parquet": LOCAL_TINY_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/range_fail/data.parquet": LOCAL_RANGE_PARQUET_SOURCE,
}


class VendedCredentialRefreshAddon:
    def __init__(self):
        self.refresh_unlocked = {
            INIT_TABLE: False,
            RANGE_TABLE: False,
            SAME_BAD_TABLE: False,
            RANGE_FAIL_TABLE: False,
        }

    def request(self, flow: http.HTTPFlow):
        if self._is_catalog_request(flow):
            self._handle_catalog_request(flow)
            return
        if self._is_s3_request(flow):
            self._handle_s3_request(flow)

    def response(self, flow: http.HTTPFlow):
        table = flow.metadata.get("vended_table")
        if not table or not flow.response or flow.response.status_code >= 300:
            return

        response = json.loads(flow.response.content.decode())
        response["storage-credentials"] = [self._credentials_for_table(table)]
        flow.response.text = json.dumps(response)

    @staticmethod
    def _is_catalog_request(flow: http.HTTPFlow):
        return flow.request.host == CATALOG_HOST and flow.request.port == CATALOG_PORT

    @staticmethod
    def _is_s3_request(flow: http.HTTPFlow):
        return flow.request.host == S3_HOST and flow.request.port == S3_PORT

    def _handle_catalog_request(self, flow: http.HTTPFlow):
        parsed_path = urllib.parse.urlparse(flow.request.path)
        path = parsed_path.path
        query = f"?{parsed_path.query}" if parsed_path.query else ""

        table_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)", path)
        credentials_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)/credentials", path)

        if table_match and table_match.group(1) in self.refresh_unlocked:
            table = table_match.group(1)
            flow.metadata["vended_table"] = table
            flow.request.path = f"/v1/namespaces/default/tables/{TABLE_SOURCE}{query}"
            return

        if credentials_match and credentials_match.group(1) in self.refresh_unlocked:
            table = credentials_match.group(1)
            body = json.dumps({"storage-credentials": [self._credentials_for_table(table)]}).encode()
            flow.response = http.Response.make(200, body, {"Content-Type": "application/json"})

    def _handle_s3_request(self, flow: http.HTTPFlow):
        key_id = self._extract_access_key(flow.request.headers)
        has_range = flow.request.headers.get("Range") is not None

        if key_id == OLD_INIT_KEY:
            self.refresh_unlocked[INIT_TABLE] = True
            self._forbidden(flow, "stale init credentials")
            return
        if key_id == OLD_RANGE_KEY and flow.request.method == "GET" and has_range:
            self.refresh_unlocked[RANGE_TABLE] = True
            self._forbidden(flow, "stale range credentials")
            return
        if key_id == OLD_SAME_BAD_KEY:
            self.refresh_unlocked[SAME_BAD_TABLE] = True
            self._forbidden(flow, "same bad credentials")
            return
        if key_id == OLD_RANGE_FAIL_KEY and flow.request.method == "GET" and has_range:
            self.refresh_unlocked[RANGE_FAIL_TABLE] = True
            self._forbidden(flow, "stale range-fail credentials")
            return
        if key_id == NEW_RANGE_FAIL_KEY:
            self._forbidden(flow, "refreshed range-fail credentials")
            return
        if key_id not in {NEW_INIT_KEY, OLD_RANGE_KEY, NEW_RANGE_KEY, OLD_RANGE_FAIL_KEY}:
            self._forbidden(flow, "unknown test credentials")
            return

        local_object = LOCAL_PARQUET_OBJECTS.get(urllib.parse.urlparse(flow.request.path).path)
        if local_object:
            self._serve_local_object(flow, local_object)
            return

        self._resign_s3_request(flow)

    def _credentials_for_table(self, table):
        key_id = self._key_for_table(table)
        return {
            "prefix": self._credentials_prefix(table),
            "config": {
                "s3.access-key-id": key_id,
                "s3.secret-access-key": f"{key_id}_SECRET",
                "s3.region": "us-east-1",
                "s3.endpoint": f"http://{S3_HOST}:{S3_PORT}",
                "s3.path-style-access": "true",
            },
        }

    def _key_for_table(self, table):
        if table == INIT_TABLE:
            return NEW_INIT_KEY if self.refresh_unlocked[table] else OLD_INIT_KEY
        if table == RANGE_TABLE:
            return NEW_RANGE_KEY if self.refresh_unlocked[table] else OLD_RANGE_KEY
        if table == SAME_BAD_TABLE:
            return OLD_SAME_BAD_KEY
        if table == RANGE_FAIL_TABLE:
            return NEW_RANGE_FAIL_KEY if self.refresh_unlocked[table] else OLD_RANGE_FAIL_KEY
        raise ValueError(f"unexpected table: {table}")

    @staticmethod
    def _credentials_prefix(table):
        if table == INIT_TABLE:
            return "s3://warehouse/vended_credentials_refresh/init"
        if table == RANGE_TABLE:
            return "s3://warehouse/vended_credentials_refresh/range"
        if table == SAME_BAD_TABLE:
            return "s3://warehouse/vended_credentials_refresh/same_bad"
        if table == RANGE_FAIL_TABLE:
            return "s3://warehouse/vended_credentials_refresh/range_fail"
        raise ValueError(f"unexpected table: {table}")

    @staticmethod
    def _extract_access_key(headers):
        auth = headers.get("Authorization", "")
        match = re.search(r"Credential=([^/]+)/", auth)
        return match.group(1) if match else ""

    @staticmethod
    def _forbidden(flow: http.HTTPFlow, message):
        body = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error><Code>AccessDenied</Code>"
            f"<Message>{message}</Message></Error>"
        ).encode()
        flow.response = http.Response.make(
            403,
            body,
            {"Content-Type": "application/xml", "Content-Length": str(len(body))},
        )

    def _serve_local_object(self, flow: http.HTTPFlow, file_path: Path):
        if not file_path.exists():
            body = f"Missing local fixture object: {file_path}".encode()
            flow.response = http.Response.make(
                404,
                body,
                {"Content-Type": "text/plain", "Content-Length": str(len(body))},
            )
            return

        file_size = file_path.stat().st_size
        try:
            byte_range = self._parse_range_header(flow.request.headers.get("Range"), file_size)
        except ValueError:
            flow.response = http.Response.make(
                416,
                b"",
                {"Content-Range": f"bytes */{file_size}", "Content-Length": "0"},
            )
            return

        headers = {"Accept-Ranges": "bytes", "Content-Type": "application/octet-stream"}
        if byte_range:
            start, end = byte_range
            content_length = end - start + 1
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            headers["Content-Length"] = str(content_length)
            with file_path.open("rb") as f:
                f.seek(start)
                body = f.read(content_length)
            flow.response = http.Response.make(206, body, headers)
            return

        headers["Content-Length"] = str(file_size)
        body = file_path.read_bytes()
        flow.response = http.Response.make(200, body, headers)

    @staticmethod
    def _parse_range_header(range_header, file_size):
        if not range_header:
            return None
        if not range_header.startswith("bytes="):
            raise ValueError("unsupported range unit")
        range_spec = range_header[len("bytes=") :]
        if "," in range_spec:
            raise ValueError("multiple ranges are not supported")
        start_text, separator, end_text = range_spec.partition("-")
        if separator != "-":
            raise ValueError("invalid range")
        if not start_text:
            suffix_size = int(end_text)
            if suffix_size <= 0:
                raise ValueError("invalid suffix range")
            start = max(file_size - suffix_size, 0)
            end = file_size - 1
        else:
            start = int(start_text)
            end = int(end_text) if end_text else file_size - 1
        if start < 0 or end < start or start >= file_size:
            raise ValueError("range outside file")
        return start, min(end, file_size - 1)

    def _resign_s3_request(self, flow: http.HTTPFlow):
        range_header = flow.request.headers.get("Range")
        flow.request.headers.clear()
        flow.request.headers.update(self._signed_s3_headers(flow.request.method, flow.request.path, range_header))
        flow.request.headers["Host"] = f"{S3_HOST}:{S3_PORT}"

    @staticmethod
    def _signed_s3_headers(method, path, range_header):
        now = datetime.datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = "UNSIGNED-PAYLOAD"
        host_header = f"{S3_HOST}:{S3_PORT}"
        parsed_path = urllib.parse.urlparse(path)
        canonical_uri = urllib.parse.quote(parsed_path.path or "/", safe="/-_.~")
        canonical_query = VendedCredentialRefreshAddon._canonical_query_string(parsed_path.query)
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_headers = (
            f"host:{host_header}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{UPSTREAM_S3_REGION}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        signature = hmac.new(
            VendedCredentialRefreshAddon._signing_key(UPSTREAM_S3_SECRET, date_stamp, UPSTREAM_S3_REGION),
            string_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={UPSTREAM_S3_KEY}/{credential_scope}, "
                f"SignedHeaders={signed_headers}, "
                f"Signature={signature}"
            ),
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if range_header:
            headers["Range"] = range_header
        return headers

    @staticmethod
    def _canonical_query_string(query):
        values = urllib.parse.parse_qsl(query, keep_blank_values=True)
        encoded = [
            (
                urllib.parse.quote(key, safe="-_.~"),
                urllib.parse.quote(value, safe="-_.~"),
            )
            for key, value in values
        ]
        return "&".join(f"{key}={value}" for key, value in sorted(encoded))

    @staticmethod
    def _hmac_sha256(key, message):
        return hmac.new(key, message.encode(), hashlib.sha256).digest()

    @staticmethod
    def _signing_key(secret, date_stamp, region):
        date_key = VendedCredentialRefreshAddon._hmac_sha256(("AWS4" + secret).encode(), date_stamp)
        region_key = VendedCredentialRefreshAddon._hmac_sha256(date_key, region)
        service_key = VendedCredentialRefreshAddon._hmac_sha256(region_key, "s3")
        return VendedCredentialRefreshAddon._hmac_sha256(service_key, "aws4_request")


addons = [VendedCredentialRefreshAddon()]
