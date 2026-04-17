#!/usr/bin/env python3
"""
Mock HTTP server for testing the RisingWave HTTP sink.

Usage:
    python3 http_sink_mock_server.py <body_output_file> <port> [<header_output_file>]

Each POST request body is appended as a line to the body output file.
If a header output file is given, received headers are appended as a JSON line per request.
Responds 200 OK to every POST, 400 to anything else.
"""

import json
import signal
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def main():
    body_output_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/http_sink_test_body.txt"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 18081
    header_output_file = sys.argv[3] if len(sys.argv) > 3 else None

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8")
            with open(body_output_file, "a") as f:
                f.write(body + "\n")
            if header_output_file is not None:
                headers = {k.lower(): v for k, v in self.headers.items()}
                with open(header_output_file, "a") as f:
                    f.write(json.dumps(headers) + "\n")
            self.send_response(200)
            self.end_headers()

        def do_GET(self):
            # Health check endpoint
            self.send_response(200)
            self.end_headers()

        def log_message(self, format, *args):
            pass  # suppress request logs

    server = HTTPServer(("", port), Handler)

    def handle_sigterm(signum, frame):
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    server.serve_forever()


if __name__ == "__main__":
    main()
