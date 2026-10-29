"""Disposable loopback proxy: bounded streaming bodies and no request diagnostics."""
import http.client
import http.server
import pathlib
import ssl
import sys
import threading

root = pathlib.Path(sys.argv[1])
lock = threading.Lock()
downloads = 0
limit = 128 * 1024 * 1024


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def forward(self):
        self.connection.settimeout(30)
        upstream = http.client.HTTPConnection("127.0.0.1", int((root / "upstream").read_text()), timeout=30)
        try:
            upstream.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for name, value in self.headers.items():
                if name.lower() not in ("host", "connection"):
                    upstream.putheader(name, value)
            upstream.putheader("Host", "127.0.0.1")
            upstream.endheaders()
            total = 0
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                while True:
                    line = self.rfile.readline(1024)
                    size = int(line.split(b";", 1)[0], 16)
                    total += size
                    if total > limit or size < 0:
                        raise ValueError("fixture request too large")
                    upstream.send(line)
                    if size == 0:
                        trailer_bytes = 0
                        while True:
                            trailer = self.rfile.readline(65536)
                            trailer_bytes += len(trailer)
                            if not trailer or trailer_bytes > 65536:
                                raise ValueError("invalid fixture trailers")
                            upstream.send(trailer)
                            if trailer == b"\r\n":
                                break
                        break
                    self.copy_request(upstream, size + 2)
            else:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 <= size <= limit:
                    raise ValueError("fixture request too large")
                self.copy_request(upstream, size)
            response = upstream.getresponse()
            if self.command == "GET" and self.path.startswith("/transfer/xorb/") and response.status in (200, 206):
                global downloads
                with lock:
                    downloads += 1
                    (root / "xorb-downloads.pending").write_text(str(downloads))
                    (root / "xorb-downloads.pending").replace(root / "xorb-downloads")
            self.send_response(response.status)
            for name, value in response.getheaders():
                if name.lower() not in ("connection", "transfer-encoding"):
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                while block := response.read(65536):
                    self.wfile.write(block)
        except (OSError, ValueError, http.client.HTTPException):
            pass
        finally:
            self.close_connection = True
            upstream.close()

    def copy_request(self, upstream, remaining):
        while remaining:
            block = self.rfile.read(min(65536, remaining))
            if not block:
                raise ValueError("incomplete fixture body")
            upstream.send(block)
            remaining -= len(block)

    do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = forward


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
server.daemon_threads = True
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(root / "server-cert.pem", root / "server-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
(root / "port.pending").write_text(str(server.server_port))
(root / "port.pending").replace(root / "port")
server.serve_forever()
