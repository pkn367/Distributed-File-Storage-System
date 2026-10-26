// common.h - protocol constants and helpers shared by server, client and gateway
//
// ---------------------------------------------------------------------------
// FRAMING
//   Every message is  [4-byte big-endian length][payload bytes].
//   TCP is a byte stream: one recv() may return half a message, or two
//   messages glued together. So we always loop until exactly the announced
//   number of bytes has arrived (send_all / recv_all below).
//
// PROTOCOL
//   The client sends one framed text command; the server answers with one
//   framed text reply that starts with "OK" or "ERR <CODE> <text>".
//
//   HELP                   -> OK <help text>
//   LIST                   -> OK <count>\n<name> <size>\n<name> <size>...
//   UPLOAD <name> <size>   -> OK READY          (or ERR ...; nothing else follows)
//        client then sends exactly <size> raw bytes
//                          -> OK Stored ...     (or ERR ...)
//   DOWNLOAD <name>        -> OK <size>, then exactly <size> raw bytes (or ERR ...)
//   DELETE <name>          -> OK Deleted ...    (or ERR ...)
//
//   Raw file bytes are never parsed as commands: both sides know the exact
//   byte count in advance, so file data cannot be confused with a command.
// ---------------------------------------------------------------------------

#ifndef COMMON_H
#define COMMON_H

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

constexpr int DEFAULT_PORT = 8080;
constexpr uint32_t MAX_MESSAGE_SIZE = 1024 * 1024;            // limit for text messages (1 MiB)
constexpr uint64_t MAX_FILE_SIZE = 100ULL * 1024 * 1024;      // limit for one file (100 MiB)
constexpr size_t CHUNK_SIZE = 64 * 1024;                      // file data is moved in 64 KiB pieces
constexpr size_t MAX_FILENAME_LEN = 100;

// ===========================================================================
// RAII wrapper for a file descriptor: closed automatically when the object
// goes out of scope (even on early return or error). Works for sockets AND
// regular files, so we use it for both.
// ===========================================================================
class Socket {
public:
    Socket() = default;
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket() { close_fd(); }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close_fd();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int get() const { return fd_; }
    bool valid() const { return fd_ >= 0; }
    void reset() { close_fd(); }

private:
    void close_fd() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    int fd_ = -1;
};

using FileHandle = Socket;  // same idea, clearer name when it holds a file

// ===========================================================================
// Low-level send/receive loops
// ===========================================================================

// Send exactly len bytes. MSG_NOSIGNAL: a vanished peer gives an error code
// instead of killing the process with SIGPIPE.
inline bool send_all(int fd, const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    while (len > 0) {
        ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;  // interrupted by a signal: retry
            return false;
        }
        if (n == 0) return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Receive exactly len bytes (false if the peer closes or an error occurs first).
inline bool recv_all(int fd, void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    while (len > 0) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // orderly shutdown by peer
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// One framed text message.
inline bool send_message(int fd, const std::string& msg) {
    if (msg.size() > MAX_MESSAGE_SIZE) return false;
    uint32_t len_net = htonl(static_cast<uint32_t>(msg.size()));
    if (!send_all(fd, &len_net, sizeof(len_net))) return false;
    return msg.empty() || send_all(fd, msg.data(), msg.size());
}

inline bool recv_message(int fd, std::string& out) {
    uint32_t len_net = 0;
    if (!recv_all(fd, &len_net, sizeof(len_net))) return false;
    uint32_t len = ntohl(len_net);
    if (len > MAX_MESSAGE_SIZE) return false;  // reject absurd lengths
    out.assign(len, '\0');
    return len == 0 || recv_all(fd, &out[0], len);
}

// SO_RCVTIMEO / SO_SNDTIMEO: stop a stalled peer from blocking a thread forever.
// seconds == 0 means "no timeout".
inline bool set_socket_timeout(int fd, int option, int seconds) {
    timeval tv{};
    tv.tv_sec = seconds;
    tv.tv_usec = 0;
    return ::setsockopt(fd, SOL_SOCKET, option, &tv, sizeof(tv)) == 0;
}

// ===========================================================================
// Plain file I/O loops (read()/write() can also return less than asked)
// ===========================================================================
inline bool write_all_fd(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = ::write(fd, data, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Read exactly len bytes from a file (false on error or if the file is shorter).
inline bool read_full_fd(int fd, char* buf, size_t len) {
    while (len > 0) {
        ssize_t n = ::read(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        buf += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// ===========================================================================
// Validation helpers
// ===========================================================================

// A stored filename may contain ONLY letters, digits, '.', '_' and '-'.
// No '/', no spaces, no '..' tricks, and it must not start with '.'
// (so hidden files, "." and ".." are impossible, and temp files that start
// with ".tmp-" can never collide with a real name).
inline bool is_valid_filename(const std::string& name) {
    if (name.empty() || name.size() > MAX_FILENAME_LEN) return false;
    if (name[0] == '.') return false;
    for (unsigned char c : name) {
        bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                  (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return false;
    }
    return true;
}

// Strict unsigned decimal parser (digits only, no sign, no spaces, no overflow).
inline bool parse_u64(const std::string& s, uint64_t& out) {
    if (s.empty() || s.size() > 19) return false;  // 19 digits always fit in 64 bits
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<uint64_t>(c - '0');
    }
    out = v;
    return true;
}

inline std::string to_upper(std::string s) {
    for (char& c : s)
        if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
    return s;
}

// Make arbitrary client text safe to write into a log line (no control
// characters, so nobody can forge log lines) and limit its length.
inline std::string printable(const std::string& s, size_t max_len = 200) {
    std::string out;
    for (unsigned char c : s) {
        if (out.size() >= max_len) {
            out += "...";
            break;
        }
        out += (c >= 32 && c < 127) ? static_cast<char>(c) : '?';
    }
    return out;
}

// ===========================================================================
// Thread-safe logger: one mutex makes sure lines from different threads
// never get mixed up. Writes to the console and (optionally) to a file.
// ===========================================================================
class Logger {
public:
    bool open_file(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        file_.open(path, std::ios::app);
        return file_.is_open();
    }

    void log(const std::string& level, const std::string& msg) {
        char ts[32];
        std::time_t now = std::time(nullptr);
        std::tm tm_buf{};
        localtime_r(&now, &tm_buf);
        std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

        std::lock_guard<std::mutex> lock(mutex_);
        std::string line = std::string(ts) + " [" + level + "] " + msg;
        std::cout << line << std::endl;  // endl flushes: lines appear immediately
        if (file_.is_open()) file_ << line << std::endl;
    }

private:
    std::mutex mutex_;
    std::ofstream file_;
};

// ===========================================================================
// Client side of the storage protocol (used by the CLI client AND the web
// gateway, so the protocol is implemented only once).
// ===========================================================================
struct Reply {
    enum Status { OK, SERVER_ERROR, IO_ERROR };
    Status status = IO_ERROR;
    std::string code;     // SERVER_ERROR only, e.g. NOT_FOUND, BAD_REQUEST, TOO_LARGE
    std::string message;  // text after "OK " / after the error code
    bool ok() const { return status == OK; }
};

inline Reply make_io_error(const std::string& message) {
    Reply r;
    r.status = Reply::IO_ERROR;
    r.message = message;
    return r;
}

inline Reply parse_reply(const std::string& text) {
    Reply r;
    if (text == "OK" || text.rfind("OK ", 0) == 0) {
        r.status = Reply::OK;
        r.message = text.size() > 3 ? text.substr(3) : "";
    } else if (text.rfind("ERR ", 0) == 0) {
        r.status = Reply::SERVER_ERROR;
        std::string rest = text.substr(4);
        size_t sp = rest.find(' ');
        r.code = rest.substr(0, sp);
        r.message = (sp == std::string::npos) ? "" : rest.substr(sp + 1);
    } else {
        r = make_io_error("Malformed reply from server");
    }
    return r;
}

class StorageClient {
public:
    using FillFn = std::function<bool(char*, size_t)>;          // supplies bytes to upload
    using SizeFn = std::function<bool(uint64_t)>;               // told the file size first
    using SinkFn = std::function<bool(const char*, size_t)>;    // receives downloaded bytes

    // Connect and read the server's welcome message.
    Reply connect_to(const std::string& host, const std::string& port) {
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        int rc = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &res);
        if (rc != 0) return make_io_error("Cannot resolve " + host + ": " + gai_strerror(rc));

        std::string last_error = "no usable address";
        for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
            Socket candidate(::socket(p->ai_family, p->ai_socktype, p->ai_protocol));
            if (!candidate.valid()) {
                last_error = std::strerror(errno);
                continue;
            }
            if (::connect(candidate.get(), p->ai_addr, p->ai_addrlen) == 0) {
                sock_ = std::move(candidate);
                break;
            }
            last_error = std::strerror(errno);
        }
        ::freeaddrinfo(res);
        if (!sock_.valid()) return make_io_error("Cannot connect to " + host + ":" + port + " (" + last_error + ")");

        set_socket_timeout(sock_.get(), SO_RCVTIMEO, 60);
        set_socket_timeout(sock_.get(), SO_SNDTIMEO, 60);

        std::string welcome;
        if (!recv_message(sock_.get(), welcome)) return io_failure("Server closed the connection");
        Reply r = parse_reply(welcome);
        if (!r.ok()) sock_.reset();
        return r;
    }

    bool connected() const { return sock_.valid(); }

    // Simple request -> single reply (HELP, LIST, DELETE).
    Reply command(const std::string& text) {
        if (!sock_.valid()) return make_io_error("Not connected");
        if (!send_message(sock_.get(), text)) return io_failure("Failed to send request");
        std::string reply;
        if (!recv_message(sock_.get(), reply)) return io_failure("Connection lost while waiting for the reply");
        return parse_reply(reply);
    }

    // Two-phase upload: ask, wait for "OK READY", send exactly `size` bytes, read result.
    // A SERVER_ERROR in the first phase leaves the connection usable.
    Reply upload(const std::string& name, uint64_t size, const FillFn& fill) {
        Reply r = command("UPLOAD " + name + " " + std::to_string(size));
        if (!r.ok()) return r;
        if (r.message != "READY") return io_failure("Unexpected reply to UPLOAD: " + r.message);

        std::vector<char> buf(CHUNK_SIZE);
        uint64_t remaining = size;
        while (remaining > 0) {
            size_t n = static_cast<size_t>(std::min<uint64_t>(remaining, buf.size()));
            if (!fill(buf.data(), n)) return io_failure("Could not read the data to upload");
            if (!send_all(sock_.get(), buf.data(), n)) return io_failure("Connection lost while uploading");
            remaining -= n;
        }
        std::string reply;
        if (!recv_message(sock_.get(), reply)) return io_failure("Connection lost after upload");
        return parse_reply(reply);
    }

    // Download: on_size(size) is called once before any data; sink(...) gets each chunk.
    Reply download(const std::string& name, const SizeFn& on_size, const SinkFn& sink) {
        Reply r = command("DOWNLOAD " + name);
        if (!r.ok()) return r;
        uint64_t size = 0;
        if (!parse_u64(r.message, size)) return io_failure("Bad size in DOWNLOAD reply");
        if (!on_size(size)) return io_failure("Download cancelled");

        std::vector<char> buf(CHUNK_SIZE);
        uint64_t remaining = size;
        while (remaining > 0) {
            size_t n = static_cast<size_t>(std::min<uint64_t>(remaining, buf.size()));
            if (!recv_all(sock_.get(), buf.data(), n)) return io_failure("Connection lost while downloading");
            if (!sink(buf.data(), n)) return io_failure("Could not store the downloaded data");
            remaining -= n;
        }
        Reply done;
        done.status = Reply::OK;
        done.message = "Downloaded " + std::to_string(size) + " bytes";
        return done;
    }

private:
    // After an I/O error the byte stream is out of sync, so the connection is dropped.
    Reply io_failure(const std::string& msg) {
        sock_.reset();
        return make_io_error(msg);
    }

    Socket sock_;
};

#endif  // COMMON_H
