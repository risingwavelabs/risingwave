#!/usr/bin/env python3

import argparse
import hashlib
import hmac
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, quote, urlsplit


class State:
    def __init__(self, access_key, secret_key, region, service):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service
        self.docs = {}
        self.signed_requests = []


def _sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _normalize_header(value):
    return " ".join(value.strip().split())


def _canonical_query(query):
    pairs = parse_qsl(query, keep_blank_values=True)
    encoded = [
        (quote(k, safe="-_.~"), quote(v, safe="-_.~"))
        for k, v in pairs
    ]
    return "&".join(f"{k}={v}" for k, v in sorted(encoded))


def _parse_auth_header(header):
    if not header or not header.startswith("AWS4-HMAC-SHA256 "):
        raise ValueError("missing AWS4-HMAC-SHA256 authorization")
    attrs = {}
    for item in header[len("AWS4-HMAC-SHA256 "):].split(","):
        key, value = item.strip().split("=", 1)
        attrs[key] = value
    return attrs


def _verify_sigv4(handler, body):
    state = handler.server.state
    parsed = urlsplit(handler.path)
    auth = _parse_auth_header(handler.headers.get("authorization"))
    credential = auth.get("Credential", "").split("/")
    if len(credential) != 5:
        raise ValueError("invalid credential scope")
    access_key, date, region, service, terminal = credential
    if access_key != state.access_key:
        raise ValueError("unexpected access key")
    if region != state.region:
        raise ValueError(f"unexpected region: {region}")
    if service != state.service:
        raise ValueError(f"unexpected service: {service}")
    if terminal != "aws4_request":
        raise ValueError("invalid credential terminal")

    signed_headers = auth.get("SignedHeaders", "").split(";")
    if not signed_headers:
        raise ValueError("missing signed headers")
    canonical_headers = []
    for name in signed_headers:
        value = handler.headers.get(name)
        if value is None:
            raise ValueError(f"missing signed header: {name}")
        canonical_headers.append(f"{name}:{_normalize_header(value)}\n")

    payload_hash = handler.headers.get("x-amz-content-sha256")
    if not payload_hash:
        payload_hash = hashlib.sha256(body).hexdigest()

    canonical_request = "\n".join([
        handler.command,
        quote(parsed.path or "/", safe="/-_.~"),
        _canonical_query(parsed.query),
        "".join(canonical_headers),
        ";".join(signed_headers),
        payload_hash,
    ])
    hashed_request = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    amz_date = handler.headers.get("x-amz-date")
    if not amz_date:
        raise ValueError("missing x-amz-date")
    scope = f"{date}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        scope,
        hashed_request,
    ])

    date_key = _sign(("AWS4" + state.secret_key).encode("utf-8"), date)
    region_key = _sign(date_key, region)
    service_key = _sign(region_key, service)
    signing_key = _sign(service_key, "aws4_request")
    expected = hmac.new(
        signing_key,
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    actual = auth.get("Signature")
    if actual != expected:
        raise ValueError("signature mismatch")
    state.signed_requests.append({
        "method": handler.command,
        "path": parsed.path,
        "region": region,
        "service": service,
    })


def _bulk_items(body):
    lines = [line for line in body.decode("utf-8").splitlines() if line.strip()]
    i = 0
    while i < len(lines):
        meta = json.loads(lines[i])
        i += 1
        op, attrs = next(iter(meta.items()))
        source = None
        if op != "delete":
            source = json.loads(lines[i])
            i += 1
        yield op, attrs, source


class Handler(BaseHTTPRequestHandler):
    server_version = "MockOpenSearchSigV4/0.1"

    def log_message(self, fmt, *args):
        return

    def _body(self):
        length = int(self.headers.get("content-length", "0") or "0")
        return self.rfile.read(length) if length else b""

    def _json(self, status, obj):
        payload = json.dumps(obj, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _require_sigv4(self, body):
        try:
            _verify_sigv4(self, body)
            return True
        except Exception as exc:
            self._json(403, {"error": str(exc)})
            return False

    def do_HEAD(self):
        if self.path == "/":
            if self._require_sigv4(b""):
                self.send_response(200)
                self.send_header("content-length", "0")
                self.end_headers()
            return
        self._json(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/__stats":
            docs = {
                index: {doc_id: doc for doc_id, doc in sorted(index_docs.items())}
                for index, index_docs in sorted(self.server.state.docs.items())
            }
            self._json(200, {
                "docs": docs,
                "signed_requests": self.server.state.signed_requests,
            })
            return

        body = b""
        if not self._require_sigv4(body):
            return
        path = urlsplit(self.path).path
        count_match = re.fullmatch(r"/([^/]+)/_count", path)
        search_match = re.fullmatch(r"/([^/]+)/_search", path)
        if count_match:
            index = count_match.group(1)
            self._json(200, {"count": len(self.server.state.docs.get(index, {}))})
        elif search_match:
            index = search_match.group(1)
            hits = [
                {"_index": index, "_id": doc_id, "_source": doc}
                for doc_id, doc in self.server.state.docs.get(index, {}).items()
            ]
            self._json(200, {"hits": {"hits": hits}})
        elif path == "/":
            self._json(200, {"version": {"distribution": "opensearch"}})
        else:
            self._json(404, {"error": "not found"})

    def do_DELETE(self):
        body = self._body()
        if not self._require_sigv4(body):
            return
        path = urlsplit(self.path).path
        index = path.strip("/")
        if index:
            self.server.state.docs.pop(index, None)
        self._json(200, {"acknowledged": True})

    def do_POST(self):
        body = self._body()
        if not self._require_sigv4(body):
            return
        if urlsplit(self.path).path != "/_bulk":
            self._json(404, {"error": "not found"})
            return

        items = []
        for op, attrs, source in _bulk_items(body):
            index = attrs.get("_index")
            doc_id = attrs.get("_id")
            if op == "update":
                doc = source.get("doc", source)
                self.server.state.docs.setdefault(index, {})[doc_id] = doc
                status = 200
            elif op == "delete":
                self.server.state.docs.setdefault(index, {}).pop(doc_id, None)
                status = 200
            else:
                status = 400
            items.append({op: {"_index": index, "_id": doc_id, "status": status}})
        self._json(200, {"errors": False, "items": items})


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, handler, state):
        super().__init__(server_address, handler)
        self.state = state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19200)
    parser.add_argument("--access-key", default="AKIDEXAMPLE")
    parser.add_argument(
        "--secret-key",
        default="wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--service", default="es")
    args = parser.parse_args()

    state = State(args.access_key, args.secret_key, args.region, args.service)
    server = Server((args.host, args.port), Handler, state)
    print(f"mock opensearch sigv4 listening on {args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
