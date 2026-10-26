// server.cpp - Distributed File Storage server
//
// Socket lifecycle : socket() -> setsockopt() -> bind() -> listen() -> accept() loop
// Concurrency      : one std::thread per connected client
// Synchronization  : one std::shared_mutex per file name (see FileLocks below)
// Storage          : every file lives in one dedicated directory (default ./storage)
//
// Usage: ./server [port] [storage_dir]

#include "common.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/stat.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <thread>

namespace fs = std::filesystem;

constexpr size_t MAX_CLIENTS = 64;       // refuse connections beyond this many threads
constexpr int IO_TIMEOUT_SECONDS = 30;   // stalled transfers are abandoned after this long
constexpr size_t MAX_LIST_ENTRIES = 5000;

static std::atomic<bool> g_running{true};
static void on_signal(int) { g_running = false; }  // Ctrl+C -> clean shutdown

// ---------------------------------------------------------------------------
// FileLocks: one reader/writer lock PER FILE NAME.
//   * shared lock    (many at once) : DOWNLOAD reads the file
//   * exclusive lock (only one)     : DELETE, and the final "commit" of UPLOAD
// The map itself is protected by an ordinary std::mutex that is held only for
// the instant of the lookup, never during file or network work.
// Operations on DIFFERENT file names never wait for each other.
// ---------------------------------------------------------------------------
class FileLocks {
public:
    std::shared_ptr<std::shared_mutex> get(const std::string& name) {
        std::lock_guard<std::mutex> guard(map_mutex_);
        auto& slot = locks_[name];
        if (!slot) slot = std::make_shared<std::shared_mutex>();
        return slot;
    }

private:
    std::mutex map_mutex_;
    std::map<std::string, std::shared_ptr<std::shared_mutex>> locks_;
};

// ---------------------------------------------------------------------------
// ClientRegistry: remembers connected clients so that (a) we can refuse new
// ones when the server is full and (b) on shutdown we can wake every client
// thread and wait until all of them have finished.
// ---------------------------------------------------------------------------
class ClientRegistry {
public:
    bool add(int id, int fd, size_t max_clients) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fds_.size() >= max_clients) return false;
        fds_[id] = fd;
        return true;
    }

    void remove(int id) {
        std::lock_guard<std::mutex> lock(mutex_);
        fds_.erase(id);
        cv_.notify_all();  // notify while holding the lock (safe during shutdown)
    }

    void shutdown_all() {  // makes blocked recv()/send() calls return
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : fds_) ::shutdown(entry.second, SHUT_RDWR);
    }

    void wait_until_empty() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return fds_.empty(); });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::map<int, int> fds_;  // client id -> socket fd
};

struct ServerContext {
    explicit ServerContext(std::string dir) : storage_dir(std::move(dir)) {}

    std::string storage_dir;
    Logger log;
    FileLocks locks;
    ClientRegistry clients;
    std::atomic<unsigned long> temp_counter{0};

    std::string path_for(const std::string& name) const { return storage_dir + "/" + name; }
};

// Deletes the temporary upload file unless the upload was committed (RAII).
struct TempFile {
    std::string path;
    bool keep = false;
    ~TempFile() {
        if (!keep && !path.empty()) ::unlink(path.c_str());
    }
};

static bool send_err(int fd, const std::string& code, const std::string& text) {
    return send_message(fd, "ERR " + code + " " + text);
}

static std::string who(int id) { return "Client " + std::to_string(id); }

static const char* HELP_TEXT =
    "Commands:\n"
    "  HELP                   show this help\n"
    "  LIST                   list stored files\n"
    "  UPLOAD <name> <size>   upload <size> bytes (send them after 'OK READY')\n"
    "  DOWNLOAD <name>        download a file\n"
    "  DELETE <name>          delete a file\n"
    "Filenames: letters, digits, '.', '_' and '-' only; max 100 characters; must not start with '.'";

// ---------------------------------------------------------------------------
// Command handlers. Each returns true if the connection can continue, false if
// it must be closed (an I/O error leaves the byte stream out of sync).
// ---------------------------------------------------------------------------

static bool handle_list(ServerContext& ctx, int fd, int id) {
    std::vector<std::pair<std::string, uint64_t>> files;
    std::error_code ec;
    fs::directory_iterator it(ctx.storage_dir, ec);
    fs::directory_iterator end;
    if (ec) {
        ctx.log.log("ERROR", who(id) + ": cannot read storage directory: " + ec.message());
        return send_err(fd, "INTERNAL", "Cannot read the storage directory");
    }
    while (!ec && it != end && files.size() < MAX_LIST_ENTRIES) {
        std::string name = it->path().filename().string();
        std::error_code e1, e2;
        // is_valid_filename() also hides ".tmp-*" files of uploads in progress
        if (is_valid_filename(name) && it->is_regular_file(e1) && !e1) {
            uint64_t size = it->file_size(e2);
            if (!e2) files.emplace_back(name, size);  // e2 set => file vanished meanwhile
        }
        it.increment(ec);
    }
    std::sort(files.begin(), files.end());

    std::string reply = "OK " + std::to_string(files.size()) + "\n";
    for (const auto& f : files) reply += f.first + " " + std::to_string(f.second) + "\n";
    ctx.log.log("INFO", who(id) + ": LIST (" + std::to_string(files.size()) + " files)");
    return send_message(fd, reply);
}

static bool handle_upload(ServerContext& ctx, int fd, int id, const std::string& name, uint64_t size) {
    if (!is_valid_filename(name))
        return send_err(fd, "BAD_REQUEST",
                        "Invalid filename. Use only letters, digits, '.', '_' and '-' "
                        "(max 100 characters, must not start with '.').");
    if (size > MAX_FILE_SIZE) return send_err(fd, "TOO_LARGE", "File is larger than the 100 MB limit.");

    // Phase 1: receive the data into a private temporary file. NO file lock is
    // held here, so a slow client never blocks anybody else (not even readers
    // of the same file name).
    TempFile tmp;
    std::string tmp_path = ctx.storage_dir + "/.tmp-" + std::to_string(::getpid()) + "-" +
                           std::to_string(++ctx.temp_counter);
    FileHandle out(::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644));
    if (!out.valid()) {
        ctx.log.log("ERROR", who(id) + ": cannot create temporary file: " + std::strerror(errno));
        return send_err(fd, "INTERNAL", "Server could not create a temporary file.");
    }
    tmp.path = tmp_path;

    if (!send_message(fd, "OK READY")) return false;

    set_socket_timeout(fd, SO_RCVTIMEO, IO_TIMEOUT_SECONDS);
    std::vector<char> buf(CHUNK_SIZE);
    uint64_t remaining = size;
    while (remaining > 0) {
        size_t want = static_cast<size_t>(std::min<uint64_t>(remaining, buf.size()));
        ssize_t n = ::recv(fd, buf.data(), want, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {  // client vanished, reset or timed out: the temp file is removed by ~TempFile
            ctx.log.log("WARN", who(id) + ": upload of " + name + " aborted after " +
                                    std::to_string(size - remaining) + "/" + std::to_string(size) + " bytes");
            return false;
        }
        if (!write_all_fd(out.get(), buf.data(), static_cast<size_t>(n))) {
            ctx.log.log("ERROR", who(id) + ": disk write failed: " + std::strerror(errno));
            send_err(fd, "INTERNAL", "Disk write failed.");
            return false;
        }
        remaining -= static_cast<uint64_t>(n);
    }
    set_socket_timeout(fd, SO_RCVTIMEO, 0);
    out.reset();  // close the temp file before renaming it

    // Phase 2: commit. rename() is atomic, so readers see either the complete
    // old file or the complete new file, never a half-written one. The
    // exclusive lock is held only for this instant; if a download of the same
    // file is running, the commit waits for it to finish.
    {
        auto file_lock = ctx.locks.get(name);
        std::unique_lock<std::shared_mutex> lock(*file_lock, std::try_to_lock);
        if (!lock.owns_lock()) {
            ctx.log.log("INFO", who(id) + ": waiting for exclusive lock on " + name);
            lock.lock();
        }
        if (::rename(tmp.path.c_str(), ctx.path_for(name).c_str()) < 0) {
            ctx.log.log("ERROR", who(id) + ": rename failed: " + std::strerror(errno));
            return send_err(fd, "INTERNAL", "Could not store the file.");
        }
        tmp.keep = true;  // the temp file no longer exists under its old name
    }

    ctx.log.log("INFO", who(id) + ": uploaded " + name + " (" + std::to_string(size) + " bytes)");
    return send_message(fd, "OK Stored " + name + " (" + std::to_string(size) + " bytes)");
}

static bool handle_download(ServerContext& ctx, int fd, int id, const std::string& name) {
    if (!is_valid_filename(name)) return send_err(fd, "BAD_REQUEST", "Invalid filename.");

    // Shared lock: many clients may download the same file at once, but DELETE
    // and the commit step of an UPLOAD must wait until we are done. Only this
    // one file name is affected; everything else keeps running.
    auto file_lock = ctx.locks.get(name);
    std::shared_lock<std::shared_mutex> lock(*file_lock, std::try_to_lock);
    if (!lock.owns_lock()) {
        ctx.log.log("INFO", who(id) + ": waiting for shared lock on " + name);
        lock.lock();
    }

    FileHandle in(::open(ctx.path_for(name).c_str(), O_RDONLY));
    int open_errno = errno;
    if (!in.valid()) {
        if (open_errno == ENOENT) return send_err(fd, "NOT_FOUND", "File not found: " + name);
        ctx.log.log("ERROR", who(id) + ": cannot open " + name + ": " + std::strerror(open_errno));
        return send_err(fd, "INTERNAL", "Cannot open the file.");
    }
    struct stat st{};
    if (::fstat(in.get(), &st) < 0 || !S_ISREG(st.st_mode))
        return send_err(fd, "NOT_FOUND", "File not found: " + name);
    uint64_t size = static_cast<uint64_t>(st.st_size);

    if (!send_message(fd, "OK " + std::to_string(size))) return false;

    std::vector<char> buf(CHUNK_SIZE);
    uint64_t sent = 0;
    while (sent < size) {
        size_t want = static_cast<size_t>(std::min<uint64_t>(size - sent, buf.size()));
        ssize_t n = ::read(in.get(), buf.data(), want);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            ctx.log.log("ERROR", who(id) + ": read error on " + name);
            return false;
        }
        if (!send_all(fd, buf.data(), static_cast<size_t>(n))) {
            ctx.log.log("WARN", who(id) + ": download of " + name + " aborted after " + std::to_string(sent) + "/" +
                                    std::to_string(size) + " bytes");
            return false;
        }
        sent += static_cast<uint64_t>(n);
    }
    ctx.log.log("INFO", who(id) + ": downloaded " + name + " (" + std::to_string(size) + " bytes)");
    return true;
}

static bool handle_delete(ServerContext& ctx, int fd, int id, const std::string& name) {
    if (!is_valid_filename(name)) return send_err(fd, "BAD_REQUEST", "Invalid filename.");

    auto file_lock = ctx.locks.get(name);
    std::unique_lock<std::shared_mutex> lock(*file_lock, std::try_to_lock);
    if (!lock.owns_lock()) {
        ctx.log.log("INFO", who(id) + ": waiting for exclusive lock on " + name);
        lock.lock();  // waits for running downloads of this file to finish
    }
    if (::unlink(ctx.path_for(name).c_str()) < 0) {
        if (errno == ENOENT) return send_err(fd, "NOT_FOUND", "File not found: " + name);
        ctx.log.log("ERROR", who(id) + ": cannot delete " + name + ": " + std::strerror(errno));
        return send_err(fd, "INTERNAL", "Cannot delete the file.");
    }
    ctx.log.log("INFO", who(id) + ": deleted " + name);
    return send_message(fd, "OK Deleted " + name);
}

// ---------------------------------------------------------------------------
// One client session: runs in its own thread until the client disconnects.
// ---------------------------------------------------------------------------
static void run_session(ServerContext& ctx, int fd, int id) {
    if (!send_message(fd, "OK Welcome to the Distributed File Storage server. Type HELP for commands.")) {
        ctx.log.log("WARN", who(id) + ": could not send welcome message");
        return;
    }

    std::string line;
    while (g_running && recv_message(fd, line)) {
        std::istringstream in(line);
        std::string cmd, arg1, arg2, extra;
        in >> cmd >> arg1 >> arg2 >> extra;
        cmd = to_upper(cmd);
        ctx.log.log("INFO", who(id) + ": " + printable(line));

        bool keep = true;
        if (cmd.empty()) {
            keep = send_err(fd, "BAD_REQUEST", "Empty command. Type HELP.");
        } else if (cmd == "HELP" && arg1.empty()) {
            keep = send_message(fd, std::string("OK ") + HELP_TEXT);
        } else if (cmd == "LIST" && arg1.empty()) {
            keep = handle_list(ctx, fd, id);
        } else if (cmd == "UPLOAD") {
            uint64_t size = 0;
            if (arg1.empty() || arg2.empty() || !extra.empty() || !parse_u64(arg2, size))
                keep = send_err(fd, "BAD_REQUEST", "Usage: UPLOAD <name> <size-in-bytes>");
            else
                keep = handle_upload(ctx, fd, id, arg1, size);
        } else if (cmd == "DOWNLOAD") {
            if (arg1.empty() || !arg2.empty()) keep = send_err(fd, "BAD_REQUEST", "Usage: DOWNLOAD <name>");
            else keep = handle_download(ctx, fd, id, arg1);
        } else if (cmd == "DELETE") {
            if (arg1.empty() || !arg2.empty()) keep = send_err(fd, "BAD_REQUEST", "Usage: DELETE <name>");
            else keep = handle_delete(ctx, fd, id, arg1);
        } else {
            keep = send_err(fd, "BAD_REQUEST", "Unknown or malformed command: " + printable(cmd, 30) + ". Type HELP.");
        }
        if (!keep) break;
    }
    ctx.log.log("INFO", who(id) + " disconnected");
}

static bool parse_port(const char* text, int& port) {
    char* end = nullptr;
    long value = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || value < 1 || value > 65535) return false;
    port = static_cast<int>(value);
    return true;
}

// Remove ".tmp-*" files left behind by a previous crash.
static void remove_stale_temp_files(ServerContext& ctx) {
    std::error_code ec;
    fs::directory_iterator it(ctx.storage_dir, ec);
    fs::directory_iterator end;
    while (!ec && it != end) {
        std::string name = it->path().filename().string();
        if (name.rfind(".tmp-", 0) == 0) {
            std::error_code rm_ec;
            fs::remove(it->path(), rm_ec);
            ctx.log.log("INFO", "Removed stale temporary file " + name);
        }
        it.increment(ec);
    }
}

int main(int argc, char* argv[]) {
    int port = DEFAULT_PORT;
    std::string storage_dir = "storage";
    if (argc > 3) {
        std::cerr << "Usage: " << argv[0] << " [port] [storage_dir]\n";
        return 1;
    }
    if (argc >= 2 && !parse_port(argv[1], port)) {
        std::cerr << "Invalid port: " << argv[1] << " (use 1-65535)\n";
        return 1;
    }
    if (argc == 3) storage_dir = argv[2];

    ServerContext ctx(storage_dir);
    std::error_code ec;
    fs::create_directories(storage_dir, ec);
    if (ec) {
        std::cerr << "Cannot create storage directory '" << storage_dir << "': " << ec.message() << "\n";
        return 1;
    }
    fs::create_directories("logs", ec);
    if (!ctx.log.open_file("logs/server.log"))
        std::cerr << "Warning: cannot open logs/server.log, logging to the console only\n";

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    std::signal(SIGPIPE, SIG_IGN);

    remove_stale_temp_files(ctx);

    Socket listener(::socket(AF_INET, SOCK_STREAM, 0));
    if (!listener.valid()) {
        ctx.log.log("ERROR", std::string("socket(): ") + std::strerror(errno));
        return 1;
    }
    int opt = 1;  // allow quick restarts without "Address already in use"
    if (::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        ctx.log.log("ERROR", std::string("setsockopt(): ") + std::strerror(errno));
        return 1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ctx.log.log("ERROR", std::string("bind() on port ") + std::to_string(port) + ": " + std::strerror(errno));
        return 1;
    }
    if (::listen(listener.get(), 64) < 0) {
        ctx.log.log("ERROR", std::string("listen(): ") + std::strerror(errno));
        return 1;
    }
    ctx.log.log("INFO", "Server listening on port " + std::to_string(port) + " (storage directory: " + storage_dir + ")");

    int next_client_id = 1;
    while (g_running) {
        // poll() with a timeout lets the loop notice Ctrl+C within half a second
        pollfd pfd{};
        pfd.fd = listener.get();
        pfd.events = POLLIN;
        int ready = ::poll(&pfd, 1, 500);
        if (ready <= 0) continue;  // timeout or EINTR

        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int fd = ::accept(listener.get(), reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (fd < 0) {
            if (errno != EINTR) {
                ctx.log.log("ERROR", std::string("accept(): ") + std::strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(100));  // e.g. out of descriptors
            }
            continue;  // one failed accept must never stop the server
        }
        Socket client(fd);  // closed automatically if we do not hand it to a thread

        char ip[INET_ADDRSTRLEN] = "?";
        ::inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        int id = next_client_id++;
        ctx.log.log("INFO", who(id) + " connected from " + ip + ":" + std::to_string(ntohs(client_addr.sin_port)));

        set_socket_timeout(fd, SO_SNDTIMEO, IO_TIMEOUT_SECONDS);  // a client that stops reading cannot hold a thread forever
        if (!ctx.clients.add(id, fd, MAX_CLIENTS)) {
            ctx.log.log("WARN", who(id) + " refused: server is full");
            send_err(fd, "BUSY", "Server is full, try again later.");
            continue;
        }

        try {
            // The Socket is moved into the thread, so it is closed when the thread ends.
            std::thread([&ctx, id, sock = std::move(client)]() mutable {
                run_session(ctx, sock.get(), id);
                ctx.clients.remove(id);  // unregister BEFORE the socket closes (see shutdown_all)
            }).detach();
        } catch (const std::system_error& e) {
            ctx.clients.remove(id);
            ctx.log.log("ERROR", std::string("Cannot start a client thread: ") + e.what());
        }
    }

    ctx.log.log("INFO", "Shutting down: waking connected clients");
    ctx.clients.shutdown_all();
    ctx.clients.wait_until_empty();
    ctx.log.log("INFO", "Server stopped");
    return 0;
}
