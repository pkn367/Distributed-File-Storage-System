# Review Guide: presenting to the professor

## A. Before the review (5 minutes)

```bash
cd ~/DistributedFileStorage
pkill -x server; pkill -x gateway; pkill -x client      # clear anything left over from earlier runs
rm -f storage/* logs/server.log                          # clean slate
python3 tests/test_all.py                                # confirm: RESULT: ALL TESTS PASSED
```

Prepare **four Ubuntu tabs** (use the ⌄ arrow next to `+`, choose Ubuntu, or run `wsl ~` in PowerShell):

| Tab | Name it | Command (type it, do not run it yet) |
|---|---|---|
| 1 | SERVER | `cd ~/DistributedFileStorage && ./server 8080` |
| 2 | GATEWAY | `cd ~/DistributedFileStorage && ./gateway 8000` |
| 3 | CLIENT 1 | `cd ~/DistributedFileStorage && ./client 127.0.0.1 8080` |
| 4 | CLIENT 2 | `cd ~/DistributedFileStorage && ./client 127.0.0.1 8080` |

Prepare two small test files in `test_files/` (names without spaces):
```bash
echo "Quarterly report for the OS lab" > test_files/report.txt
head -c 3000000 /dev/urandom > test_files/photo.bin
```

Open `http://localhost:8000` in the browser but do not click anything yet.

## B. How to stop and start again in front of the professor

| Action | How |
|---|---|
| Stop server / gateway / client | click that tab, press **Ctrl+C** (client: type `quit`) |
| Start again | press the **Up arrow** (it recalls the last command) then **Enter** |
| Something says "Address already in use" | the program is still running somewhere: `pkill -x server` (or `pkill -x gateway`), then start again |
| Reset the stored files | `rm -f storage/*`, or click **Delete demo files** on the page |
| Watch the log live | `tail -f logs/server.log` in a spare tab |

**A good moment to show the restart:** after the demo, stop the server with Ctrl+C. The server prints
`Shutting down` and `Server stopped` (clean shutdown). Start it again: the files you uploaded are still listed,
because they are stored on disk in `storage/`.

## C. Demo script (about 8 minutes)

1. **Start** the server (tab 1), then the gateway (tab 2). Say: *"The server listens on port 8080 and creates one thread per client. The gateway lets a browser talk to it."*
2. **CLIENT 1** (tab 3): run `UPLOAD test_files/report.txt`, then `LIST`. Point at the SERVER tab: `Client 1: UPLOAD report.txt ...` and `uploaded report.txt`.
3. **CLIENT 2** (tab 4): run `LIST` (sees `report.txt` uploaded by client 1), `DOWNLOAD report.txt`, then `cat downloads/report.txt` in another tab. Say: *"Two clients are connected at the same time and neither disconnected the other."*
4. **Browser:** drop 3 files on the page. Say: *"The browser opens one connection per file, so these three uploads run concurrently."* Show the server log with several clients.
5. Click **Run concurrent upload test** (8 clients). Say: *"Eight clients upload at the same moment, all succeed."*
6. Click **Same-file conflict test**. Say: *"Eight clients write different content to the SAME file at once. The page downloads it and checks that it is one client's complete content, never a mix. That is the race condition we prevent."* Expect `PASS`.
7. **Security:** in a client run `DELETE ../secret`. Expect `Error: Invalid filename. [BAD_REQUEST]`. Say: *"Path traversal is blocked by filename validation."*
8. **Tests:** run `python3 tests/test_all.py`, show `ALL TESTS PASSED`.
9. **Stop** the server with Ctrl+C, start again, `LIST`: files are still there.

Do not turn **Auto-refresh** on during the demo unless you want to show live updates from another tab: it adds a `LIST` line every 5 seconds.

## D. How to explain it (read this until you can say it without notes)

**One sentence:** *"It is a file storage server where many clients, from a terminal or a browser, upload, list, download and delete files at the same time, and we use threads and locks so concurrent access never corrupts a file."*

**Why is it an OS project?** It uses threads (concurrency), mutex and shared/exclusive locks (synchronization), file I/O and atomic rename (file management), and sockets and timeouts (resource management).

**Flow of one upload:**
1. The client sends `UPLOAD report.txt 44`.
2. The server validates the name and size, creates a temp file, replies `OK READY`.
3. The client sends exactly 44 bytes; the server writes them to the temp file (no lock held).
4. The server takes the exclusive lock for that filename, does an atomic `rename()` to the real name, releases the lock.
5. The server replies `OK Stored`.

**Who does what (all three should be able to explain all of it):**
- Kamal: `common.h` framing, socket lifecycle in `server.cpp`, `gateway.cpp`, running the demo.
- Chandru: `handle_upload / download / delete / list`, `is_valid_filename`, storage directory.
- Santhosh: client threads, `FileLocks`, `Logger`, `tests/test_all.py`.

## E. Likely viva questions

**1. Why TCP and not UDP?** TCP is reliable and ordered, so file bytes arrive complete and in order. UDP could lose or reorder packets.

**2. Why do you need message lengths? Does one `recv()` return one whole message?** No. TCP is a byte stream, so `recv()` can return part of a message or two messages together. We send a 4-byte length first and loop until exactly that many bytes arrive (`recv_all`). The tests send a message one byte at a time and two messages in one packet.

**3. Why multithreading?** One thread per client means a slow client cannot block the others. The main thread only accepts connections.

**4. What is a race condition? Show one in your project.** The result depends on timing. Example: two uploads write the same file at once and the bytes get mixed, or a delete happens while a download is reading. Our conflict test runs 8 writers and 4 readers on one file and checks nobody ever sees mixed data.

**5. What synchronization do you use and why not one global lock?** A `std::shared_mutex` per filename. Many downloads share the lock; delete and the upload commit need it exclusively. A global lock would make every client wait for every other client, including during slow network transfers.

**6. Why a temporary file and `rename()`?** The upload takes as long as the network needs. Writing straight into the real file would require holding a lock the whole time, or readers could see half a file. With a temp file nobody waits during the transfer, and `rename()` is atomic, so readers see the old file or the new file, never a partial one.

**7. What happens if a client disconnects in the middle of an upload?** `recv` returns 0 or an error, the server logs a warning, the temp file is deleted automatically (RAII), the lock was never held, and the server keeps running. There is a test for it.

**8. What is a deadlock? Can your server deadlock?** Threads waiting for each other forever. Each operation takes at most one file lock and never holds the map mutex while waiting for a file lock, so there is no circular wait.

**9. How do you prevent path traversal?** `is_valid_filename` accepts only letters, digits, `.`, `_`, `-`, rejects a leading `.`, so `/` and `..` are impossible. It is checked on the server for every command, so a custom client cannot bypass it.

**10. Why does the browser need a gateway?** Browsers can only speak HTTP, not our custom TCP protocol. The gateway converts each HTTP request into one protocol connection, so each browser upload is a real concurrent client of the server.

**11. Is this really "distributed"?** Honest answer: right now it is a client-server system with concurrent clients and one storage server. The next step is several storage nodes with replication and failover; that is on our roadmap and not implemented yet.

**12. How did you test it?** 24 automated tests (protocol, security, binary files, disconnects, concurrency, locking, gateway, CLI), plus runs under ThreadSanitizer and AddressSanitizer with no reports.

**13. What are the limits?** One server, no login, no encryption, flat storage, 100 MB per file, 64 clients at once.

**14. What would you improve?** Replication across nodes, checksums, authentication, a thread pool, TLS.
