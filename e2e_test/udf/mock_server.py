import http.server
import sys

class MockHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTPServer mock request handler"""

    def do_GET(self):  # pylint: disable=invalid-name
        """Handle GET requests"""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"results": [{"idx": 1}, {"idx": 2}]}')

    def log_request(self, code=None, size=None):
        """Don't log anything"""

def main() -> int:
    """Echo the input arguments to standard output"""

    httpd = http.server.HTTPServer( ("127.0.0.1", 4101), MockHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    sys.exit(main())