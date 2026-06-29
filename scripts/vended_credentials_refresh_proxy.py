#!/usr/bin/env python3
"""Local proxy for deterministic Iceberg vended credential refresh tests.

The catalog proxy reuses the fixture catalog's table metadata for
default.table_unpartitioned, but vends controlled S3 credentials. The S3 proxy
injects 403 responses for stale credentials and serves local fixture Parquet
objects, so DuckDB exercises its secret refresh paths without waiting for a real
STS expiry.
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import re
import signal
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


TABLE_SOURCE = "table_unpartitioned"
INIT_TABLE = "vended_init_refresh"
RANGE_TABLE = "vended_range_refresh"

OLD_INIT_KEY = "OLD_INIT_KEY"
NEW_INIT_KEY = "NEW_INIT_KEY"
OLD_RANGE_KEY = "OLD_RANGE_KEY"
NEW_RANGE_KEY = "NEW_RANGE_KEY"

UPSTREAM_S3_KEY = "admin"
UPSTREAM_S3_SECRET = "password"
UPSTREAM_S3_REGION = "us-east-1"
SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_PARQUET_SOURCE = (
    SCRIPT_DIR.parent
    / "data/persistent/expression_filter/data/00000-0-1406cdaa-c3e4-4e6d-a22b-d85e4a813169-00001.parquet"
)
LOCAL_PARQUET_OBJECTS = {
    "/warehouse/vended_credentials_refresh/init/data.parquet": LOCAL_PARQUET_SOURCE,
    "/warehouse/vended_credentials_refresh/range/data.parquet": LOCAL_PARQUET_SOURCE,
}


class ProxyState:
    def __init__(self, catalog_upstream, s3_upstream, s3_endpoint, verbose=False):
        self.catalog_upstream = urllib.parse.urlparse(catalog_upstream)
        self.s3_upstream = urllib.parse.urlparse(s3_upstream)
        self.s3_endpoint = s3_endpoint
        self.verbose = verbose
        self.lock = threading.Lock()
        self.refresh_unlocked = {INIT_TABLE: False, RANGE_TABLE: False}

    def key_for_table(self, table):
        with self.lock:
            refresh_unlocked = self.refresh_unlocked[table]
        if table == INIT_TABLE:
            return NEW_INIT_KEY if refresh_unlocked else OLD_INIT_KEY
        if table == RANGE_TABLE:
            return NEW_RANGE_KEY if refresh_unlocked else OLD_RANGE_KEY
        raise ValueError(f"unexpected table: {table}")

    def unlock_refresh(self, table):
        with self.lock:
            self.refresh_unlocked[table] = True

    def log(self, message):
        if self.verbose:
            print(message, flush=True)


def split_host_port(parsed_url, default_port):
    return parsed_url.hostname, parsed_url.port or default_port


def forward_request(parsed_upstream, method, path, headers, body=None, strip_auth=True):
    default_port = 443 if parsed_upstream.scheme == "https" else 80
    host, port = split_host_port(parsed_upstream, default_port)
    conn_cls = http.client.HTTPSConnection if parsed_upstream.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(host, port, timeout=30)

    upstream_path = path
    if parsed_upstream.path and parsed_upstream.path != "/":
        upstream_path = parsed_upstream.path.rstrip("/") + path

    forward_headers = {}
    for key, value in headers.items():
        lower_key = key.lower()
        if lower_key in {"connection", "host"}:
            continue
        if strip_auth and lower_key in {"authorization", "x-amz-security-token", "x-amz-content-sha256", "x-amz-date"}:
            continue
        forward_headers[key] = value
    forward_headers["Host"] = host if port in (80, 443) else f"{host}:{port}"

    conn.request(method, upstream_path, body=body, headers=forward_headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return response.status, response.reason, response.getheaders(), data


def hmac_sha256(key, message):
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def signing_key(secret, date_stamp, region):
    date_key = hmac_sha256(("AWS4" + secret).encode(), date_stamp)
    region_key = hmac_sha256(date_key, region)
    service_key = hmac_sha256(region_key, "s3")
    return hmac_sha256(service_key, "aws4_request")


def canonical_query_string(query):
    values = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = [
        (
            urllib.parse.quote(key, safe="-_.~"),
            urllib.parse.quote(value, safe="-_.~"),
        )
        for key, value in values
    ]
    return "&".join(f"{key}={value}" for key, value in sorted(encoded))


def signed_s3_headers(parsed_upstream, method, path, range_header):
    default_port = 443 if parsed_upstream.scheme == "https" else 80
    host, port = split_host_port(parsed_upstream, default_port)
    host_header = host if port in (80, 443) else f"{host}:{port}"
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    parsed_path = urllib.parse.urlparse(path)
    canonical_uri = urllib.parse.quote(parsed_path.path or "/", safe="/-_.~")
    canonical_query = canonical_query_string(parsed_path.query)
    payload_hash = "UNSIGNED-PAYLOAD"
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
        signing_key(UPSTREAM_S3_SECRET, date_stamp, UPSTREAM_S3_REGION),
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


def send_response(handler, status, reason, headers, body=b""):
    handler.send_response(status, reason)
    skipped = {"connection", "transfer-encoding"}
    has_length = False
    for key, value in headers:
        lower_key = key.lower()
        if lower_key in skipped:
            continue
        if lower_key == "content-length":
            has_length = True
        handler.send_header(key, value)
    if not has_length:
        handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    if handler.command != "HEAD" and body:
        handler.wfile.write(body)


def forbidden(handler, message):
    body = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>AccessDenied</Code>"
        f"<Message>{message}</Message></Error>"
    ).encode()
    send_response(
        handler,
        403,
        "Forbidden",
        [("Content-Type", "application/xml"), ("Content-Length", str(len(body)))],
        body,
    )


def not_found(handler, message):
    body = message.encode()
    send_response(
        handler,
        404,
        "Not Found",
        [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))],
        body,
    )


def range_not_satisfiable(handler, file_size):
    send_response(
        handler,
        416,
        "Requested Range Not Satisfiable",
        [("Content-Range", f"bytes */{file_size}"), ("Content-Length", "0")],
    )


def extract_access_key(headers):
    auth = headers.get("Authorization", "")
    match = re.search(r"Credential=([^/]+)/", auth)
    return match.group(1) if match else ""


def credentials_prefix(table):
    if table == INIT_TABLE:
        return "s3://warehouse/vended_credentials_refresh/init"
    if table == RANGE_TABLE:
        return "s3://warehouse/vended_credentials_refresh/range"
    raise ValueError(f"unexpected table: {table}")


def credentials_for_table(state, table):
    key_id = state.key_for_table(table)
    return {
        "prefix": credentials_prefix(table),
        "config": {
            "s3.access-key-id": key_id,
            "s3.secret-access-key": f"{key_id}_SECRET",
            "s3.region": "us-east-1",
            "s3.endpoint": f"http://{state.s3_endpoint}",
            "s3.path-style-access": "true",
        },
    }


def rewrite_table_response(state, table, data):
    response = json.loads(data.decode())
    response["storage-credentials"] = [credentials_for_table(state, table)]
    return json.dumps(response).encode()


def credentials_response(state, table):
    return json.dumps({"storage-credentials": [credentials_for_table(state, table)]}).encode()


def parse_range_header(range_header, file_size):
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


def serve_local_object(handler, file_path):
    if not file_path.exists():
        not_found(handler, f"Missing local fixture object: {file_path}")
        return

    file_size = file_path.stat().st_size
    try:
        byte_range = parse_range_header(handler.headers.get("Range"), file_size)
    except ValueError:
        range_not_satisfiable(handler, file_size)
        return

    headers = [("Accept-Ranges", "bytes"), ("Content-Type", "application/octet-stream")]
    if byte_range:
        start, end = byte_range
        content_length = end - start + 1
        headers.append(("Content-Range", f"bytes {start}-{end}/{file_size}"))
        headers.append(("Content-Length", str(content_length)))
        handler.send_response(206, "Partial Content")
        for key, value in headers:
            handler.send_header(key, value)
        handler.end_headers()
        if handler.command != "HEAD":
            with file_path.open("rb") as f:
                f.seek(start)
                handler.wfile.write(f.read(content_length))
        return

    headers.append(("Content-Length", str(file_size)))
    handler.send_response(200, "OK")
    for key, value in headers:
        handler.send_header(key, value)
    handler.end_headers()
    if handler.command != "HEAD":
        with file_path.open("rb") as f:
            handler.wfile.write(f.read())


def make_catalog_handler(state):
    class CatalogProxyHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path
            query = f"?{parsed_path.query}" if parsed_path.query else ""

            table_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)", path)
            credentials_match = re.fullmatch(r"/v1/namespaces/default/tables/([^/]+)/credentials", path)

            if table_match and table_match.group(1) in {INIT_TABLE, RANGE_TABLE}:
                table = table_match.group(1)
                upstream_path = f"/v1/namespaces/default/tables/{TABLE_SOURCE}{query}"
                state.log(f"catalog table {table} -> {upstream_path}")
                status, reason, headers, data = forward_request(
                    state.catalog_upstream, "GET", upstream_path, self.headers
                )
                if status < 300:
                    data = rewrite_table_response(state, table, data)
                    headers = [(k, v) for k, v in headers if k.lower() != "content-length"]
                    headers.append(("Content-Length", str(len(data))))
                state.log(f"catalog table {table} status {status}")
                send_response(self, status, reason, headers, data)
                return

            if credentials_match and credentials_match.group(1) in {INIT_TABLE, RANGE_TABLE}:
                table = credentials_match.group(1)
                state.log(f"catalog credentials {table}")
                data = credentials_response(state, table)
                send_response(
                    self,
                    200,
                    "OK",
                    [("Content-Type", "application/json"), ("Content-Length", str(len(data)))],
                    data,
                )
                return

            status, reason, headers, data = forward_request(state.catalog_upstream, "GET", self.path, self.headers)
            send_response(self, status, reason, headers, data)

        def do_HEAD(self):
            status, reason, headers, data = forward_request(state.catalog_upstream, "HEAD", self.path, self.headers)
            send_response(self, status, reason, headers, data)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else None
            status, reason, headers, data = forward_request(
                state.catalog_upstream, "POST", self.path, self.headers, body
            )
            send_response(self, status, reason, headers, data)

        def log_message(self, fmt, *args):
            return

    return CatalogProxyHandler


def make_s3_handler(state):
    class S3ProxyHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.handle_s3()

        def do_GET(self):
            self.handle_s3()

        def handle_s3(self):
            key_id = extract_access_key(self.headers)
            has_range = self.headers.get("Range") is not None
            state.log(f"s3 {self.command} {self.path} key={key_id} range={has_range}")

            if key_id == OLD_INIT_KEY:
                state.unlock_refresh(INIT_TABLE)
                state.log("s3 reject OLD_INIT_KEY")
                forbidden(self, "stale init credentials")
                return
            if key_id == OLD_RANGE_KEY and self.command == "GET" and has_range:
                state.unlock_refresh(RANGE_TABLE)
                state.log("s3 reject OLD_RANGE_KEY ranged GET")
                forbidden(self, "stale range credentials")
                return
            if key_id not in {NEW_INIT_KEY, OLD_RANGE_KEY, NEW_RANGE_KEY}:
                state.log("s3 reject unknown credentials")
                forbidden(self, "unknown test credentials")
                return

            parsed_path = urllib.parse.urlparse(self.path)
            local_object = LOCAL_PARQUET_OBJECTS.get(parsed_path.path)
            if local_object:
                state.log(f"s3 local object {local_object}")
                serve_local_object(self, local_object)
                return

            signed_headers = signed_s3_headers(
                state.s3_upstream,
                self.command,
                self.path,
                self.headers.get("Range"),
            )
            status, reason, headers, data = forward_request(
                state.s3_upstream,
                self.command,
                self.path,
                signed_headers,
                strip_auth=False,
            )
            state.log(f"s3 upstream status {status}")
            send_response(self, status, reason, headers, data)

        def log_message(self, fmt, *args):
            return

    return S3ProxyHandler


def run_server(server):
    server.serve_forever(poll_interval=0.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-port", type=int, default=19131)
    parser.add_argument("--s3-port", type=int, default=19132)
    parser.add_argument("--catalog-upstream", default="http://127.0.0.1:8181")
    parser.add_argument("--s3-upstream", default="http://127.0.0.1:9000")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    state = ProxyState(
        catalog_upstream=args.catalog_upstream,
        s3_upstream=args.s3_upstream,
        s3_endpoint=f"127.0.0.1:{args.s3_port}",
        verbose=args.verbose,
    )
    catalog_server = ThreadingHTTPServer(("127.0.0.1", args.catalog_port), make_catalog_handler(state))
    s3_server = ThreadingHTTPServer(("127.0.0.1", args.s3_port), make_s3_handler(state))

    stop = threading.Event()

    def handle_signal(signum, frame):
        stop.set()
        catalog_server.shutdown()
        s3_server.shutdown()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    threading.Thread(target=run_server, args=(catalog_server,), daemon=True).start()
    threading.Thread(target=run_server, args=(s3_server,), daemon=True).start()
    print(f"Catalog proxy listening on http://127.0.0.1:{args.catalog_port}")
    print(f"S3 proxy listening on http://127.0.0.1:{args.s3_port}")
    print("Set VENDED_CREDENTIAL_REFRESH_PROXY_AVAILABLE=1 to run the sqllogictest")
    while not stop.wait(1):
        pass


if __name__ == "__main__":
    main()
