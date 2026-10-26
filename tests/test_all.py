#!/usr/bin/env python3
"""End-to-end tests for the Distributed File Storage project.

Run from the project folder AFTER compiling server, client and gateway:

    python3 tests/test_all.py

It starts its own server (port 18080) and web gateway (port 18000) with a
temporary storage folder, runs every test, prints PASS / FAIL, and shuts the
processes down again. Uses only the Python standard library.
"""
import hashlib
import http.client
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_PORT = 18080
GATEWAY_PORT = 18000
STORAGE = tempfile.mkdtemp(prefix="dfs_test_storage_")
WORK = tempfile.mkdtemp(prefix="dfs_test_work_")  # working directory for the CLI client test


def sha(data):
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Raw protocol client (speaks the length-prefixed protocol directly)
# ---------------------------------------------------------------------------
def frame(text):
    data = text.encode() if isinstance(text, str) else text
    return struct.pack(">I", len(data)) + data


class Proto:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=30)
        self.welcome = self.recv_msg()

    def recv_exact(self, n):
        chunks, left = [], n
        while left:
            part = self.s.recv(min(left, 1 << 20))
            if not part:
                raise ConnectionError("server closed the connection")
            chunks.append(part)
            left -= len(part)
        return b"".join(chunks)

    def recv_msg(self):
        (n,) = struct.unpack(">I", self.recv_exact(4))
        return self.recv_exact(n).decode()

    def send_msg(self, text):
        self.s.sendall(frame(text))

    def cmd(self, text):
        self.send_msg(text)
        return self.recv_msg()

    def upload(self, name, data):
        reply = self.cmd("UPLOAD %s %d" % (name, len(data)))
        if reply != "OK READY":
            return reply
        self.s.sendall(data)
        return self.recv_msg()

    def download(self, name):
        reply = self.cmd("DOWNLOAD " + name)
        if not reply.startswith("OK "):
            return reply, None
        return reply, self.recv_exact(int(reply[3:]))

    def close(self):
        self.s.close()


def http_request(method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", GATEWAY_PORT, timeout=60)
    conn.request(method, path, body=body)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def run_threads(targets):
    errors = []

    def wrap(fn):
        def inner():
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))
        return inner

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    return errors


# ---------------------------------------------------------------------------
# Protocol / server tests
# ---------------------------------------------------------------------------
def test_welcome_and_help():
    c = Proto()
    assert c.welcome.startswith("OK"), c.welcome
    reply = c.cmd("HELP")
    assert reply.startswith("OK") and "UPLOAD" in reply and "DELETE" in reply, reply
    c.close()


def test_framing_message_sent_one_byte_at_a_time():
    c = Proto()
    for b in struct.pack(">I", 4) + b"LIST":
        c.s.sendall(bytes([b]))
        time.sleep(0.05)
    reply = c.recv_msg()
    assert reply.startswith("OK"), reply
    c.close()


def test_framing_two_commands_in_one_packet():
    c = Proto()
    c.s.sendall(frame("HELP") + frame("LIST"))
    r1, r2 = c.recv_msg(), c.recv_msg()
    assert r1.startswith("OK") and r2.startswith("OK"), (r1, r2)
    c.close()


def test_invalid_and_unknown_commands():
    c = Proto()
    assert c.cmd("FLY away").startswith("ERR BAD_REQUEST")
    assert c.cmd("").startswith("ERR BAD_REQUEST")
    assert c.cmd("UPLOAD onlyname").startswith("ERR BAD_REQUEST")
    assert c.cmd("UPLOAD a.txt abc").startswith("ERR BAD_REQUEST")
    assert c.cmd("UPLOAD a.txt -5").startswith("ERR BAD_REQUEST")
    assert c.cmd("LIST extra").startswith("ERR BAD_REQUEST")
    assert c.cmd("LIST").startswith("OK"), "connection must stay usable after errors"
    c.close()


def test_path_traversal_and_bad_names_rejected():
    c = Proto()
    bad = ["../evil.txt", "..", ".", "/etc/passwd", "a/b.txt", ".hidden", "sub\\x.txt", "%2e%2e%2fx", "a" * 101]
    for name in bad:
        for command in ("UPLOAD %s 3" % name, "DOWNLOAD " + name, "DELETE " + name):
            reply = c.cmd(command)
            assert reply.startswith("ERR BAD_REQUEST"), (command, reply)
    outside = os.path.join(os.path.dirname(STORAGE), "evil.txt")
    assert not os.path.exists(outside), "a file escaped the storage directory!"
    assert c.cmd("LIST").startswith("OK")
    c.close()


def test_binary_roundtrip_all_byte_values():
    data = bytes(range(256)) * 4096 + os.urandom(100003)  # ~1.1 MB, contains NUL, CR, LF, 0xFF ...
    c = Proto()
    assert c.upload("bin.dat", data).startswith("OK Stored")
    header, got = c.download("bin.dat")
    assert header == "OK %d" % len(data), header
    assert sha(got) == sha(data), "downloaded bytes differ from uploaded bytes"
    assert ("bin.dat %d" % len(data)) in c.cmd("LIST")
    assert c.cmd("DELETE bin.dat").startswith("OK Deleted")
    c.close()


def test_empty_file_and_overwrite():
    c = Proto()
    assert c.upload("empty.txt", b"").startswith("OK Stored")
    header, got = c.download("empty.txt")
    assert header == "OK 0" and got == b"", (header, got)
    assert c.upload("ow.txt", b"version one").startswith("OK Stored")
    assert c.upload("ow.txt", b"version two!").startswith("OK Stored")
    assert c.download("ow.txt")[1] == b"version two!"
    c.cmd("DELETE empty.txt")
    c.cmd("DELETE ow.txt")
    c.close()


def test_missing_file_and_too_large():
    c = Proto()
    assert c.cmd("DOWNLOAD nosuchfile.txt").startswith("ERR NOT_FOUND")
    assert c.cmd("DELETE nosuchfile.txt").startswith("ERR NOT_FOUND")
    assert c.cmd("UPLOAD huge.bin %d" % (100 * 1024 * 1024 + 1)).startswith("ERR TOO_LARGE")
    assert c.cmd("LIST").startswith("OK")
    c.close()


def test_client_disconnect_mid_upload_leaves_no_debris():
    c = Proto()
    assert c.cmd("UPLOAD half.bin 1000000") == "OK READY"
    c.s.sendall(b"x" * 1000)
    c.close()
    time.sleep(1.0)
    names = os.listdir(STORAGE)
    assert not [n for n in names if n.startswith(".tmp-")], names
    assert "half.bin" not in names, names
    c2 = Proto()
    assert c2.cmd("LIST").startswith("OK"), "server must survive a client that disconnects mid-upload"
    c2.close()


def test_client_reset_mid_download_releases_lock():
    data = os.urandom(30 * 1024 * 1024)
    c = Proto()
    assert c.upload("reset.bin", data).startswith("OK Stored")
    c.send_msg("DOWNLOAD reset.bin")
    assert c.recv_msg().startswith("OK ")
    c.recv_exact(100 * 1024)
    c.s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    c.s.close()  # sends a TCP reset
    d = Proto()
    t0 = time.time()
    assert d.cmd("DELETE reset.bin").startswith("OK Deleted")
    assert time.time() - t0 < 5, "DELETE was blocked by a dead download"
    d.close()


def test_many_clients_connected_at_once_and_working():
    clients = [Proto() for _ in range(20)]  # 20 simultaneous connections
    for i, c in enumerate(clients):
        assert c.welcome.startswith("OK")
    results = []

    def work(i, c):
        def run():
            data = os.urandom(200000 + i)
            assert c.upload("many%d.bin" % i, data).startswith("OK Stored")
            assert c.download("many%d.bin" % i)[1] == data
            results.append(i)
        return run

    errors = run_threads([work(i, c) for i, c in enumerate(clients)])
    assert not errors, errors[:3]
    assert len(results) == 20
    for i, c in enumerate(clients):
        c.cmd("DELETE many%d.bin" % i)
        c.close()


def test_concurrent_uploads_of_different_files():
    expected = {}

    def worker(i):
        def run():
            data = os.urandom(1_000_000 + i * 1000)
            expected[i] = sha(data)
            c = Proto()
            assert c.upload("par%d.bin" % i, data).startswith("OK Stored")
            c.close()
        return run

    errors = run_threads([worker(i) for i in range(12)])
    assert not errors, errors[:3]
    c = Proto()
    for i in range(12):
        header, got = c.download("par%d.bin" % i)
        assert got is not None and sha(got) == expected[i], "file par%d.bin is corrupt" % i
        c.cmd("DELETE par%d.bin" % i)
    c.close()


def test_conflicting_uploads_to_same_file_never_mix():
    size = 2 * 1024 * 1024
    seed = Proto()
    assert seed.upload("conflict.bin", b"A" * size).startswith("OK Stored")
    seed.close()
    stop = threading.Event()
    problems = []

    def writer(i):
        def run():
            c = Proto()
            reply = c.upload("conflict.bin", bytes([66 + i]) * size)
            if not reply.startswith("OK Stored"):
                problems.append(reply)
            c.close()
        return run

    def reader():
        c = Proto()
        while not stop.is_set():
            header, data = c.download("conflict.bin")
            if data is None:
                problems.append(header)
                return
            if len(data) != size or data != data[:1] * size:
                problems.append("reader saw MIXED or partial content")
                return
        c.close()

    reader_threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in reader_threads:
        t.start()
    errors = run_threads([writer(i) for i in range(8)])
    stop.set()
    for t in reader_threads:
        t.join(60)
    assert not errors and not problems, (errors[:3], problems[:3])
    final = Proto().download("conflict.bin")[1]
    assert len(final) == size and final == final[:1] * size, "final file is not uniform"
    d = Proto()
    d.cmd("DELETE conflict.bin")
    d.close()


def test_delete_waits_for_running_download():
    data = os.urandom(60 * 1024 * 1024)
    c1 = Proto()
    assert c1.upload("lock.bin", data).startswith("OK Stored")
    c1.send_msg("DOWNLOAD lock.bin")
    header = c1.recv_msg()
    size = int(header[3:])
    got = c1.recv_exact(1024 * 1024)  # read only 1 MB: the server blocks while sending the rest

    c2 = Proto()
    result = {}

    def deleter():
        result["reply"] = c2.cmd("DELETE lock.bin")
        result["time"] = time.time()

    th = threading.Thread(target=deleter)
    th.start()
    time.sleep(1.5)
    assert th.is_alive(), "DELETE returned while a download was still running (no exclusive lock!)"
    got += c1.recv_exact(size - len(got))
    finished = time.time()
    th.join(30)
    assert result["reply"].startswith("OK Deleted"), result
    assert result["time"] >= finished - 0.5, "DELETE completed before the download did"
    assert sha(got) == sha(data), "the download was damaged by the concurrent DELETE"
    assert c2.cmd("DOWNLOAD lock.bin").startswith("ERR NOT_FOUND")
    c1.close()
    c2.close()


def test_unrelated_files_are_not_blocked_by_a_slow_download():
    data = os.urandom(60 * 1024 * 1024)
    c1 = Proto()
    assert c1.upload("slow.bin", data).startswith("OK Stored")
    c1.send_msg("DOWNLOAD slow.bin")
    size = int(c1.recv_msg()[3:])
    got = c1.recv_exact(1024 * 1024)  # stall: the server thread for c1 is now blocked in send()
    c2 = Proto()
    t0 = time.time()
    assert c2.upload("other.txt", b"quick").startswith("OK Stored")
    assert c2.download("other.txt")[1] == b"quick"
    assert c2.cmd("DELETE other.txt").startswith("OK Deleted")
    assert c2.cmd("LIST").startswith("OK")
    assert time.time() - t0 < 3, "an unrelated file was blocked by someone else's slow download"
    got += c1.recv_exact(size - len(got))
    assert sha(got) == sha(data)
    c1.cmd("DELETE slow.bin")
    c1.close()
    c2.close()


# ---------------------------------------------------------------------------
# Web gateway tests
# ---------------------------------------------------------------------------
def test_gateway_serves_the_page():
    status, body = http_request("GET", "/")
    assert status == 200 and b"Distributed File Storage" in body, status


def test_gateway_upload_list_download_delete():
    status, body = http_request("PUT", "/api/files/web1.txt", b"hello from the browser")
    assert status == 201, (status, body)
    status, body = http_request("GET", "/api/files")
    assert status == 200 and b'"web1.txt"' in body, body
    status, body = http_request("GET", "/api/files/web1.txt")
    assert status == 200 and body == b"hello from the browser", (status, body)
    status, body = http_request("DELETE", "/api/files/web1.txt")
    assert status == 200, (status, body)
    status, body = http_request("GET", "/api/files/web1.txt")
    assert status == 404, (status, body)
    status, body = http_request("DELETE", "/api/files/web1.txt")
    assert status == 404, (status, body)


def test_gateway_rejects_bad_names():
    for path in ["/api/files/..%2Fevil.txt", "/api/files/.hidden", "/api/files/a%20b.txt",
                 "/api/files/a/b.txt", "/api/files/" + "x" * 101]:
        status, _ = http_request("PUT", path, b"data")
        assert status == 400, (path, status)
        status, _ = http_request("GET", path)
        assert status == 400, (path, status)
        status, _ = http_request("DELETE", path)
        assert status == 400, (path, status)
    assert not os.path.exists(os.path.join(os.path.dirname(STORAGE), "evil.txt"))


def test_gateway_binary_upload_download_5mb():
    data = os.urandom(5 * 1024 * 1024) + bytes(range(256))
    status, _ = http_request("PUT", "/api/files/web-big.bin", data)
    assert status == 201
    status, body = http_request("GET", "/api/files/web-big.bin")
    assert status == 200 and sha(body) == sha(data)
    http_request("DELETE", "/api/files/web-big.bin")


def test_gateway_rejects_oversized_upload():
    conn = http.client.HTTPConnection("127.0.0.1", GATEWAY_PORT, timeout=30)
    conn.putrequest("PUT", "/api/files/toobig.bin")
    conn.putheader("Content-Length", str(200 * 1024 * 1024))
    conn.endheaders()  # headers only; the gateway must answer without waiting for 200 MB
    resp = conn.getresponse()
    assert resp.status == 413, resp.status
    conn.close()


def test_gateway_routes_and_methods():
    assert http_request("GET", "/nothing-here")[0] == 404
    assert http_request("POST", "/api/files")[0] == 405
    assert http_request("PATCH", "/api/files/x.txt")[0] == 405


def test_gateway_concurrent_uploads():
    expected = {}

    def worker(i):
        def run():
            data = os.urandom(800_000 + i * 777)
            expected[i] = sha(data)
            status, body = http_request("PUT", "/api/files/gw%d.bin" % i, data)
            assert status == 201, (status, body)
        return run

    errors = run_threads([worker(i) for i in range(16)])
    assert not errors, errors[:3]
    status, body = http_request("GET", "/api/files")
    for i in range(16):
        assert ('"gw%d.bin"' % i).encode() in body
        status, got = http_request("GET", "/api/files/gw%d.bin" % i)
        assert status == 200 and sha(got) == expected[i], "gw%d.bin corrupted" % i
        http_request("DELETE", "/api/files/gw%d.bin" % i)


def test_gateway_reports_storage_server_down():
    gw = subprocess.Popen([os.path.join(ROOT, "gateway"), "18001", "127.0.0.1", "18999"],
                          cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_port(18001)
        conn = http.client.HTTPConnection("127.0.0.1", 18001, timeout=15)
        conn.request("GET", "/api/files")
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 502, resp.status
        conn.close()
    finally:
        gw.send_signal(signal.SIGINT)
        gw.wait(10)


# ---------------------------------------------------------------------------
# Command-line client test
# ---------------------------------------------------------------------------
def test_cli_client_session():
    content = os.urandom(150000)
    with open(os.path.join(WORK, "hello_cli.bin"), "wb") as f:
        f.write(content)
    script = "\n".join([
        "LIST", "UPLOAD hello_cli.bin", "LIST", "DOWNLOAD hello_cli.bin", "DELETE hello_cli.bin",
        "DELETE hello_cli.bin", "DELETE ../etc/passwd", "DOWNLOAD ../x", "FLY", "HELP", "QUIT", ""])
    proc = subprocess.run([os.path.join(ROOT, "client"), "127.0.0.1", str(SERVER_PORT)], input=script.encode(),
                          cwd=WORK, capture_output=True, timeout=60)
    out = proc.stdout.decode()
    assert proc.returncode == 0, (proc.returncode, out)
    for expected in ["Connected to server", "Stored hello_cli.bin", "hello_cli.bin  (150000 bytes)",
                     "Downloaded hello_cli.bin (150000 bytes)", "Deleted hello_cli.bin", "File not found",
                     "Invalid filename", "invalid file name", "Unknown command", "Server says:"]:
        assert expected in out, (expected, out)
    with open(os.path.join(WORK, "downloads", "hello_cli.bin"), "rb") as f:
        assert f.read() == content, "CLI download differs from the original"


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------
def wait_for_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("port %d did not open" % port)


def main():
    for binary in ("server", "client", "gateway"):
        if not os.path.exists(os.path.join(ROOT, binary)):
            print("Missing ./%s - compile first (see README / the compile commands)." % binary)
            return 2

    server_log = open(os.path.join(WORK, "server_stdout.txt"), "w")
    gateway_log = open(os.path.join(WORK, "gateway_stdout.txt"), "w")
    server = subprocess.Popen([os.path.join(ROOT, "server"), str(SERVER_PORT), STORAGE],
                              cwd=ROOT, stdout=server_log, stderr=subprocess.STDOUT)
    gateway = subprocess.Popen([os.path.join(ROOT, "gateway"), str(GATEWAY_PORT), "127.0.0.1", str(SERVER_PORT)],
                               cwd=ROOT, stdout=gateway_log, stderr=subprocess.STDOUT)
    failures = 0
    try:
        wait_for_port(SERVER_PORT)
        wait_for_port(GATEWAY_PORT)
        tests = [(n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)]
        for name, fn in tests:
            t0 = time.time()
            try:
                fn()
                print("PASS  %-62s %5.1fs" % (name, time.time() - t0))
            except Exception:  # noqa: BLE001
                failures += 1
                print("FAIL  %s" % name)
                traceback.print_exc(limit=3)
        print("Server still running after all tests:", "yes" if server.poll() is None else "NO (crashed!)")
        if server.poll() is not None:
            failures += 1
    finally:
        # Ctrl+C-style shutdown must be clean and quick
        for name, proc in (("gateway", gateway), ("server", server)):
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
            try:
                code = proc.wait(timeout=10)
                ok = code == 0
            except subprocess.TimeoutExpired:
                proc.kill()
                ok = False
            print("%s %-62s" % ("PASS " if ok else "FAIL ", "graceful_shutdown_" + name))
            if not ok:
                failures += 1
        leftovers = [n for n in os.listdir(STORAGE) if n.startswith(".tmp-")]
        if leftovers:
            print("FAIL  temporary upload files left behind:", leftovers)
            failures += 1
        shutil.rmtree(STORAGE, ignore_errors=True)
        if failures:
            print("Server/gateway output is kept in", WORK)
        else:
            shutil.rmtree(WORK, ignore_errors=True)
    print("\nRESULT:", "ALL TESTS PASSED" if failures == 0 else "%d FAILURE(S)" % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
