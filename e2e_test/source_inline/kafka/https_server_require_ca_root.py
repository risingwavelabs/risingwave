import http.server, ssl, socketserver, sys

# A simple handler that always returns 200 OK
class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'SSL Handshake Successful!')

# Create server
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain('./secrets/server.crt', './secrets/server.key')

port = int(sys.argv[1])

with socketserver.TCPServer(('0.0.0.0', port), Handler) as httpd:
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    print('Serving HTTPS on port {}...'.format(port))
    httpd.serve_forever()
