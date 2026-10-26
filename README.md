# Distributed File Storage System with Concurrent Client Access

A C++17 client-server file storage application for Ubuntu / WSL2. Many clients (terminal or web browser)
connect at the same time to one storage server and upload, list, download and delete files.
It demonstrates **multithreading, synchronization, file management and TCP networking**.

**Team:** Kamal (networking and integration), Chandru (file management), Santhosh (concurrency and testing)

> **Honest scope:** this version has ONE central storage server. Replication, multiple storage nodes and
> fault tolerance are *future work* (see the roadmap at the end). They are not implemented.

---

## 1. Directory tree

```
DistributedFileStorage/
├── common.h          protocol framing, filename validation, logger, StorageClient (shared by all programs)
├── server.cpp        storage server: socket setup, accept loop, one thread per client, locks, file operations
├── client.cpp        command-line client
├── gateway.cpp       HTTP gateway: lets a web browser talk to the storage server
├── web/index.html    the browser page (upload, list, download, delete, concurrency demo)
├── tests/test_all.py 24 automated end-to-end tests
├── storage/          all stored files live here (created automatically)
├── logs/server.log   server log (created automatically)
├── test_files/       sample files for demos
├── README.md
└── REVIEW_GUIDE.md   demo script, stop/restart steps, viva questions
```

## 2. Install, compile, test

```bash
sudo apt update
sudo apt install build-essential gdb -y
cd ~/DistributedFileStorage
g++ -std=c++17 -Wall -Wextra -pthread -o server  server.cpp
g++ -std=c++17 -Wall -Wextra -pthread -o client  client.cpp
g++ -std=c++17 -Wall -Wextra -pthread -o gateway gateway.cpp
python3 tests/test_all.py        # expect: RESULT: ALL TESTS PASSED
```

## 3. Run

Use one terminal per program, all inside `~/DistributedFileStorage`.

| Terminal | Command | Purpose |
|---|---|---|
| 1 | `./server 8080` | storage server |
| 2 | `./gateway 8000` | web gateway, then open <http://localhost:8000> |
| 3 | `./client 127.0.0.1 8080` | command-line client (start several for several clients) |

**Stop** any of them with `Ctrl+C` (they shut down cleanly). **Start again** by pressing the Up arrow, then Enter.

Optional arguments: `./server [port] [storage_dir]`, `./gateway [http_port] [storage_host] [storage_port] [bind_addr]`.

## 4. Commands

| Command | Meaning |
|---|---|
| `HELP` | show help |
| `LIST` | list stored files with sizes |
| `UPLOAD <local file>` | upload a file from this computer |
| `DOWNLOAD <name>` | download into `./downloads/` |
| `DELETE <name>` | delete a stored file |
| `QUIT` | disconnect |

Filenames: letters, digits, `.`, `_`, `-` only; at most 100 characters; must not start with `.`.
Files up to 100 MB.

## 5. Architecture

```
 Browser (web/index.html)
        │  HTTP
        ▼
 ┌──────────────┐   our TCP protocol   ┌────────────────────────────────────────┐
 │  gateway     │ ───────────────────► │  storage server                         │
 │ (1 thread    │                      │  main thread: accept() loop             │
 │ per request) │                      │  ├─ client thread 1 ─┐                  │
 └──────────────┘                      │  ├─ client thread 2 ─┼─► per-file locks │──► storage/
                                       │  └─ client thread N ─┘   (shared_mutex) │
 Terminal client ───────────────────►  └────────────────────────────────────────┘
        our TCP protocol
```

### Modules

| Module | What it does |
|---|---|
| `Socket` class (common.h) | RAII wrapper: the file descriptor is closed automatically, even on errors |
| `send_all` / `recv_all` | loops that move exactly N bytes (TCP may deliver partial data) |
| `send_message` / `recv_message` | length-prefixed framing: 4-byte length + payload |
| `is_valid_filename` | the only gate for names: blocks `/`, `..`, hidden files, spaces |
| `Logger` | one mutex so log lines from different threads never mix |
| `StorageClient` | the client side of the protocol, shared by `client` and `gateway` |
| `FileLocks` (server) | one `std::shared_mutex` per filename |
| `ClientRegistry` (server) | counts clients (max 64), wakes all of them at shutdown |
| `handle_upload/download/delete/list` | the file operations |
| `gateway.cpp` | translates HTTP requests to protocol requests |

## 6. Socket lifecycle (server)

`socket()` → `setsockopt(SO_REUSEADDR)` → `bind()` → `listen()` → loop { `poll()` → `accept()` → start a `std::thread` for that client }.
Each client thread loops on `recv_message()` until the client disconnects; the socket is closed automatically (RAII).

## 7. Protocol

Every message is `[4-byte big-endian length][payload]`. Requests and replies are text; file data is raw bytes whose
count is announced beforehand, so file contents can never be mistaken for a command.

```
HELP                    -> OK <text>
LIST                    -> OK <count>\n<name> <size>\n...
UPLOAD <name> <size>    -> OK READY        then client sends exactly <size> bytes -> OK Stored ...
DOWNLOAD <name>         -> OK <size>       then server sends exactly <size> bytes
DELETE <name>           -> OK Deleted ...
errors                  -> ERR <BAD_REQUEST | NOT_FOUND | TOO_LARGE | BUSY | INTERNAL> <text>
```

## 8. Synchronization (the heart of the OS part)

**Problem:** two clients touching the same file at the same time (a race condition) could corrupt it, or one could
delete a file while another is still downloading it.

**Solution:** one reader/writer lock (`std::shared_mutex`) per filename.

| Operation | Lock | Why |
|---|---|---|
| DOWNLOAD | **shared** (many at once) | readers do not disturb each other |
| DELETE | **exclusive** (one at a time) | waits until running downloads of that file finish |
| UPLOAD, final commit | **exclusive**, held only for one `rename()` | atomic replace |
| LIST | none | only reads directory metadata |

**Why slow clients do not block others:**
1. An upload is received into a private temporary file (`.tmp-...`) while **no lock is held**.
2. Only at the end does it take the exclusive lock for a single atomic `rename()`. Readers see either the complete old file or the complete new file, never half a file.
3. Locks are per filename, so operations on different files never wait for each other. The lock map itself is protected by a plain `std::mutex` held only for the lookup.
4. Timeouts stop a stalled client from holding a thread (and a lock) forever.

## 9. Error handling

Bad commands, bad filenames, missing files, oversized files, full server, client disconnects in the middle of an
upload or download, and failed `accept()` calls are all handled without stopping the server. A half-finished upload
leaves no file behind (the temp file is deleted automatically). `SIGPIPE` is disabled, `Ctrl+C` shuts down cleanly.

## 10. Test plan (automated: `python3 tests/test_all.py`)

| Area | Tests |
|---|---|
| Protocol | welcome/help; message sent one byte at a time; two commands in one packet; invalid commands |
| Security | path traversal and bad names rejected for UPLOAD, DOWNLOAD and DELETE |
| Files | binary round trip (all 256 byte values), empty file, overwrite, missing file, too-large file |
| Robustness | client disconnects mid-upload; client reset mid-download frees the lock |
| Concurrency | 20 clients connected at once; 12 parallel uploads; 8 writers + 4 readers on the same file never see mixed data |
| Locking | DELETE waits for a running download; an unrelated file is not blocked by a slow download |
| Gateway | page served, full upload/list/download/delete, bad names, 5 MB binary, oversized, 16 parallel uploads, storage server down (502) |
| CLI | scripted session with upload, list, download, delete, errors |

The suite also passed under ThreadSanitizer and AddressSanitizer/UBSan (no data races, no memory errors).

## 11. Implemented vs future work

| Implemented and tested | Not implemented (future work) |
|---|---|
| TCP server/client, framing | Multiple storage nodes |
| LIST, HELP, UPLOAD, DOWNLOAD, DELETE | Replication, failover |
| Binary-safe transfer, filename validation | User login / permissions |
| One thread per client, per-file shared/exclusive locks | Integrity checksums |
| Logging, timeouts, clean shutdown | Encryption (TLS) |
| Web gateway and browser page | File versioning, quotas |
| 24 automated tests | Folders (storage is flat) |

## 12. Known limitations

- One central server: if it stops, nothing is available.
- No authentication: anyone who can reach the ports can use the system. The gateway listens on localhost only unless started with `0.0.0.0`.
- Uploading an existing name overwrites it (last writer wins, never a mix).
- Browsers allow about 6 parallel connections per site; use several tabs or terminals for more simultaneous clients.
- One thread per client is simple but does not scale to thousands of clients.

## 13. Troubleshooting

| Symptom | Fix |
|---|---|
| `g++: command not found` | `sudo apt install build-essential -y` |
| `bind(): Address already in use` | the program is already running in another terminal: stop it with `Ctrl+C`, or `pkill -x server` (and `pkill -x gateway`) |
| `Text file busy` when compiling | the old program is still running: stop it first |
| `connect(): Connection refused` | the server is not running or the port differs |
| Page shows "Storage server unreachable" | start `./server 8080` first, then the gateway |
| `localhost:8000` does not open | start `./gateway 8000 127.0.0.1 8080 0.0.0.0`, run `hostname -I`, open `http://<that-ip>:8000` |
| Upload says "Invalid filename" | rename the file: letters, digits, `.`, `_`, `-` only |
| `cd: No such file or directory` | you are in the wrong terminal (PowerShell vs Ubuntu); run `wsl ~` first |

## 14. Roadmap

1. Multiple storage nodes with replication and failover (makes it truly "distributed").
2. SHA-256 checksum stored and verified on download.
3. Simple login (username/password) and per-user file ownership.
4. Thread pool instead of one thread per client.
5. TLS encryption.
