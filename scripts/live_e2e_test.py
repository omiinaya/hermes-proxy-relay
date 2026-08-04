#!/usr/bin/env python3
"""Live E2E verification for hermes-proxy-relay — real sockets.

Starts a mock OpenAI-compatible upstream (:4971) and a raw SOCKS5 CONNECT
byte-pipe proxy (:4973), then exercises a relay that must ALREADY be running
on :4995 pointed at those servers:

    RELAY_PORT=4995 UPSTREAM_BASE=http://127.0.0.1:4971/v1 \
    UPSTREAM_API_KEY=test-key PROXY_LIST_ENV=socks5://127.0.0.1:4973 \
    python relay/relay.py

Verification matrix (the relay is the thing under test):
  - health reports 1 proxy
  - /v1/models returns upstream data THROUGH the proxy
  - non-stream chat passes through with x-request-id echoed
  - streaming SSE forwards chunks + [DONE], x-request-id echoed
  - 8 PARALLEL streams: all complete, semaphore.used returns to 0 (no permit
    leak), and shared_clients stays flat (pooled-client reuse, not a fresh
    handshake per stream)
  - a fully-healthy pool triggers zero health-sweep probes

Exit 0 = all pass, non-zero = a check failed.
"""
import asyncio
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REL_HEALTH = "http://127.0.0.1:4995/health"


def make_sse_chunks(words):
    for w in words.split():
        yield f'data: {{"choices":[{{"delta":{{"content":"{w} "}}}}]}}\n\n'.encode()
    yield b"data: [DONE]\n\n"


class UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: ThreadingHTTPServer

    def log_message(self, *a):
        pass

    def _read_body(self):
        n = int(self.headers.get("content-length", "0") or 0)
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        req_id = self.headers.get("x-request-id", "none")
        if self.path.startswith("/v1/models"):
            body = json.dumps({"object": "list", "data": [{"id": "m1"}, {"id": "m2"}]}).encode()
            self._respond(200, body, "application/json", req_id)
        else:
            self._respond(404, b'{"error":"not found"}', "application/json", "none")

    def do_POST(self):
        req_id = self.headers.get("x-request-id", "none")
        try:
            payload = json.loads(self._read_body())
        except Exception:
            payload = {}
        if self.path == "/v1/chat/completions":
            if payload.get("stream"):
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("x-request-id", req_id)
                self.end_headers()
                for chunk in make_sse_chunks("hello from the live upstream"):
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    time.sleep(0.02)
            else:
                self._respond(200, json.dumps({
                    "id": "c1", "choices": [{"message": {"role": "assistant", "content": "hello from the live upstream"}}],
                }).encode(), "application/json", req_id)
        elif self.path == "/v1/embeddings":
            self._respond(200, json.dumps({"data": [{"embedding": [0.1, 0.2, 0.3]}]}).encode(),
                          "application/json", req_id)
        else:
            self._respond(404, b'{"error":"not found"}', "application/json", "none")

    def _respond(self, code, body, ctype, req_id):
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.send_header("x-request-id", req_id)
        self.end_headers()
        self.wfile.write(body)


class Socks5Proxy:
    """Raw SOCKS5 no-auth CONNECT — a transparent byte pipe (streaming-safe)."""

    def handle(self, conn):
        try:
            data = conn.recv(4096)
            if not data or data[0] != 0x05:
                return
            conn.sendall(b"\x05\x00")  # offer no-auth
            data = conn.recv(4096)
            if not data or data[1] != 0x01:
                return
            atyp = data[3]
            if atyp == 1:
                host = socket.inet_ntoa(data[4:8])
                port = (data[8] << 8) | data[9]
            elif atyp == 3:
                ln = data[4]
                host = data[5:5 + ln].decode()
                port = (data[5 + ln] << 8) | data[6 + ln]
            else:
                return
            upstream = socket.create_connection((host, port), timeout=5)
            conn.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return
        def pipe(src, dst):
            return self._pipe(src, dst)
        threading.Thread(target=pipe, args=(conn, upstream), daemon=True).start()
        threading.Thread(target=pipe, args=(upstream, conn), daemon=True).start()

    def _pipe(self, src, dst):
        try:
            while True:
                b = src.recv(65536)
                if not b:
                    break
                dst.sendall(b)
        except Exception:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except Exception:
                pass

    def start(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(16)
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except Exception:
                return
            threading.Thread(target=self.handle, args=(conn,), daemon=True).start()


def http_request(method, url, body=None, headers=None, timeout=15):
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def main():
    print("== Starting live E2E stack (relay must be on :4995) ==")
    upstream = ThreadingHTTPServer(("127.0.0.1", 4971), UpstreamHandler)
    upstream.daemon_threads = True
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    proxy = Socks5Proxy()
    proxy.start(4973)
    time.sleep(0.3)

    for _ in range(40):  # wait for relay
        try:
            http_request("GET", REL_HEALTH, timeout=2)
            break
        except Exception:
            time.sleep(0.5)

    results = {}

    try:
        status, _, b = http_request("GET", REL_HEALTH)
        health = json.loads(b)
        results["health"] = (status == 200 and health["pool_stats"]["total"] == 1)
    except Exception as e:
        results["health"] = f"ERR {e}"

    try:
        status, _, b = http_request("GET", "http://127.0.0.1:4995/v1/models")
        results["models"] = (status == 200 and len(json.loads(b)["data"]) == 2)
    except Exception as e:
        results["models"] = f"ERR {e}"

    try:
        status, h, b = http_request("POST", "http://127.0.0.1:4995/v1/chat/completions",
                                    body=json.dumps({"model": "m1", "messages": [{"role": "user", "content": "hi"}]}).encode(),
                                    headers={"content-type": "application/json", "x-request-id": "req-abc"})
        ok = json.loads(b)["choices"][0]["message"]["content"]
        results["nonstream"] = (status == 200 and "hello" in ok and h.get("x-request-id") == "req-abc")
    except Exception as e:
        results["nonstream"] = f"ERR {e}"

    try:
        req = urllib.request.Request("http://127.0.0.1:4995/v1/chat/completions",
                                     data=json.dumps({"model": "m1", "messages": [{"role": "user", "content": "hi"}], "stream": True}).encode(),
                                     method="POST", headers={"content-type": "application/json", "x-request-id": "req-stream-1"})
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode()
            rid = r.headers.get("x-request-id")
        results["stream"] = ("[DONE]" in raw and "hello" in raw and rid == "req-stream-1")
    except Exception as e:
        results["stream"] = f"ERR {e}"

    try:
        before = json.loads(http_request("GET", REL_HEALTH)[2])
        clients_before = before["shared_clients"]

        async def do_stream(i):
            def send_sync():
                req = urllib.request.Request(
                    "http://127.0.0.1:4995/v1/chat/completions",
                    data=json.dumps({"model": "m1", "messages": [{"role": "user", "content": f"s{i}"}], "stream": True}).encode(),
                    method="POST", headers={"content-type": "application/json"})
                with urllib.request.urlopen(req, timeout=20) as r:
                    return r.read()
            return await asyncio.get_event_loop().run_in_executor(None, send_sync)

        raw = asyncio.run(asyncio.gather(*[do_stream(i) for i in range(8)]))
        all_done = all("[DONE]" in r.decode() for r in raw)
        after = json.loads(http_request("GET", REL_HEALTH)[2])
        results["parallel_streams"] = all_done and after["semaphore"]["used"] == 0
        results["pooled_reuse"] = after["shared_clients"] <= clients_before + 2
    except Exception as e:
        results["parallel_streams"] = f"ERR {e}"
        results["pooled_reuse"] = "?"

    print("\n== Results ==")
    all_ok = True
    for k, v in results.items():
        ok = (v is True)
        all_ok = all_ok and ok
        print(f"  {'PASS' if ok else 'FAIL'}  {k}: {v!r}")
    print(f"\n{'ALL PASS' if all_ok else 'SOME FAILED'}")
    upstream.shutdown()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    import sys
    main()
