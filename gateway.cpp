// gateway.cpp - HTTP web gateway
//
//   Browser  --HTTP-->  gateway (this program)  --our TCP protocol-->  storage server
//
// Browsers cannot speak our custom protocol, so this small program:
//   * serves the web page  (web/index.html)
//   * turns each HTTP request into one storage-server connection:
//
//       GET    /api/files        -> LIST
//       PUT    /api/files/<name> -> UPLOAD   (request body = file bytes)
//       GET    /api/files/<name> -> DOWNLOAD
//       DELETE /api/files/<name> -> DELETE
//
// Every HTTP connection is handled in its own std::thread, and every request
// opens its own connection to the storage server - so several browser tabs
// (or several uploads in one tab) really are several concurrent clients.
//
// Usage: ./gateway [http_port=8000] [storage_host=127.0.0.1] [storage_port=8080] [bind_addr=127.0.0.1]
// Run it from the project folder (it reads web/index.html).

#include "common.h"

#include <netinet/in.h>
#include <poll.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <map>
#include <sstream>
#include <thread>

constexpr size_t MAX_HEADER_BYTES = 16 * 1024;
constexpr int HTTP_TIMEOUT_SECONDS = 30;
constexpr int MAX_CONNECTIONS = 256;

static std::atomic<bool> g_running{true};
static std::atomic<int> g_active{0};
static Logger g_log;
static void on_signal(int) { g_running = false; }

struct Config {
    std::string storage_host = "127.0.0.1";
    std::string storage_port = "8080";
};

// ---------------------------------------------------------------------------
// Minimal HTTP/1.1 plumbing
// ---------------------------------------------------------------------------

// Reads the request head, then lets us read the body. Bytes that arrived
// together with the head are kept in pending_ and handed out first.
class HttpConn {
public:
    explicit HttpConn(int fd) : fd_(fd) {}

    bool read_head(std::string& head) {
        char tmp[4096];
        while (true) {
            size_t pos = pending_.find("\r\n\r\n");
            if (pos != std::string::npos) {
                head = pending_.substr(0, pos);
                pending_.erase(0, pos + 4);
                return true;
            }
            if (pending_.size() > MAX_HEADER_BYTES) return false;
            ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            pending_.append(tmp, static_cast<size_t>(n));
        }
    }

    bool read_body(char* out, size_t n) {
        size_t take = std::min(n, pending_.size());
        if (take > 0) {
            std::memcpy(out, pending_.data(), take);
            pending_.erase(0, take);
        }
        if (take == n) return true;
        return recv_all(fd_, out + take, n - take);
    }

    // Read and throw away `n` body bytes (so the browser can read our error
    // reply instead of seeing a connection reset).
    void drain(uint64_t n) {
        std::vector<char> buf(CHUNK_SIZE);
        n = std::min<uint64_t>(n, MAX_FILE_SIZE);
        while (n > 0) {
            size_t take = static_cast<size_t>(std::min<uint64_t>(n, buf.size()));
            if (!read_body(buf.data(), take)) return;
            n -= take;
        }
    }

private:
    int fd_;
    std::string pending_;
};

struct Request {
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;  // keys lower-case
};

static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

static std::string to_lower(std::string s) {
    for (char& c : s)
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return s;
}

static bool parse_request(const std::string& head, Request& req) {
    std::istringstream in(head);
    std::string line;
    if (!std::getline(in, line)) return false;
    std::istringstream request_line(trim(line));
    std::string version;
    if (!(request_line >> req.method >> req.path >> version)) return false;
    if (version.rfind("HTTP/1.", 0) != 0) return false;
    size_t q = req.path.find('?');
    if (q != std::string::npos) req.path.erase(q);  // ignore the query string
    while (std::getline(in, line)) {
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        req.headers[to_lower(trim(line.substr(0, colon)))] = trim(line.substr(colon + 1));
    }
    return true;
}

static const char* reason_for(int status) {
    switch (status) {
        case 200: return "OK";
        case 204: return "No Content";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 411: return "Length Required";
        case 413: return "Payload Too Large";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        default: return "Internal Server Error";
    }
}

static bool send_http(int fd, int status, const std::string& content_type, const std::string& body) {
    std::string head = "HTTP/1.1 " + std::to_string(status) + " " + reason_for(status) +
                       "\r\nContent-Type: " + content_type + "\r\nContent-Length: " + std::to_string(body.size()) +
                       "\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
    return send_all(fd, head.data(), head.size()) && (body.empty() || send_all(fd, body.data(), body.size()));
}

static std::string json_escape(const std::string& s) {
    std::string out;
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 32) {
            out += ' ';
        } else {
            out += static_cast<char>(c);
        }
    }
    return out;
}

static void send_json(int fd, int status, const std::string& json) {
    send_http(fd, status, "application/json", json);
}

static void send_json_error(int fd, int status, const std::string& message) {
    send_json(fd, status, "{\"ok\":false,\"error\":\"" + json_escape(message) + "\"}");
}

// Translate a storage-server reply into an HTTP status code.
static int http_status_for(const Reply& r) {
    if (r.status == Reply::IO_ERROR) return 502;
    if (r.code == "BAD_REQUEST") return 400;
    if (r.code == "NOT_FOUND") return 404;
    if (r.code == "TOO_LARGE") return 413;
    if (r.code == "BUSY") return 503;
    return 500;
}

// ---------------------------------------------------------------------------
// Route handlers. Each returns the HTTP status it sent (for the log line).
// ---------------------------------------------------------------------------

static int handle_index(int fd) {
    std::ifstream page("web/index.html", std::ios::binary);
    if (!page) {
        send_json_error(fd, 500, "web/index.html not found - start the gateway from the project folder");
        return 500;
    }
    std::string html((std::istreambuf_iterator<char>(page)), std::istreambuf_iterator<char>());
    send_http(fd, 200, "text/html; charset=utf-8", html);
    return 200;
}

static int handle_list(int fd, const Config& cfg) {
    StorageClient sc;
    Reply r = sc.connect_to(cfg.storage_host, cfg.storage_port);
    if (!r.ok()) {
        send_json_error(fd, 502, "Storage server unavailable: " + r.message);
        return 502;
    }
    r = sc.command("LIST");
    if (!r.ok()) {
        int status = http_status_for(r);
        send_json_error(fd, status, r.message);
        return status;
    }
    std::istringstream in(r.message);
    std::string line;
    std::getline(in, line);  // file count
    std::string json = "{\"ok\":true,\"files\":[";
    bool first = true;
    while (std::getline(in, line)) {
        std::istringstream ls(line);
        std::string name, size_text;
        uint64_t size = 0;
        ls >> name >> size_text;
        if (!is_valid_filename(name) || !parse_u64(size_text, size)) continue;  // names are safe to embed in JSON
        if (!first) json += ",";
        first = false;
        json += "{\"name\":\"" + name + "\",\"size\":" + size_text + "}";
    }
    json += "]}";
    send_json(fd, 200, json);
    return 200;
}

static int handle_download(int fd, const std::string& name, const Config& cfg) {
    StorageClient sc;
    Reply r = sc.connect_to(cfg.storage_host, cfg.storage_port);
    if (!r.ok()) {
        send_json_error(fd, 502, "Storage server unavailable: " + r.message);
        return 502;
    }
    bool headers_sent = false;
    r = sc.download(
        name,
        [&](uint64_t size) {
            std::string head = "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: " +
                               std::to_string(size) + "\r\nContent-Disposition: attachment; filename=\"" + name +
                               "\"\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
            headers_sent = send_all(fd, head.data(), head.size());
            return headers_sent;
        },
        [&](const char* data, size_t n) { return send_all(fd, data, n); });
    if (r.ok()) return 200;
    if (!headers_sent) {
        int status = http_status_for(r);
        send_json_error(fd, status, r.message);
        return status;
    }
    return 500;  // failed half-way: the browser sees a truncated download
}

static int handle_upload(HttpConn& conn, int fd, const Request& req, const std::string& name, const Config& cfg) {
    auto it = req.headers.find("content-length");
    if (it == req.headers.end()) {
        send_json_error(fd, 411, "Content-Length header is required");
        return 411;
    }
    uint64_t length = 0;
    if (!parse_u64(it->second, length)) {
        send_json_error(fd, 400, "Invalid Content-Length");
        return 400;
    }
    if (length > MAX_FILE_SIZE) {
        send_json_error(fd, 413, "File is larger than the 100 MB limit");
        return 413;
    }
    auto expect = req.headers.find("expect");
    if (expect != req.headers.end() && to_lower(expect->second) == "100-continue") {
        const std::string cont = "HTTP/1.1 100 Continue\r\n\r\n";
        send_all(fd, cont.data(), cont.size());
    }

    StorageClient sc;
    Reply r = sc.connect_to(cfg.storage_host, cfg.storage_port);
    if (!r.ok()) {
        send_json_error(fd, 502, "Storage server unavailable: " + r.message);
        conn.drain(length);
        return 502;
    }

    // Stream the HTTP body straight into the storage server, one chunk at a
    // time (the whole file is never held in memory).
    uint64_t consumed = 0;
    r = sc.upload(name, length, [&](char* buf, size_t n) {
        if (!conn.read_body(buf, n)) return false;
        consumed += n;
        return true;
    });
    if (r.ok()) {
        send_json(fd, 201,
                  "{\"ok\":true,\"name\":\"" + name + "\",\"size\":" + std::to_string(length) + ",\"message\":\"" +
                      json_escape(r.message) + "\"}");
        return 201;
    }
    int status = http_status_for(r);
    send_json_error(fd, status, r.message);
    if (r.status == Reply::SERVER_ERROR) conn.drain(length - consumed);  // storage refused before reading the body
    return status;
}

static int handle_delete(int fd, const std::string& name, const Config& cfg) {
    StorageClient sc;
    Reply r = sc.connect_to(cfg.storage_host, cfg.storage_port);
    if (!r.ok()) {
        send_json_error(fd, 502, "Storage server unavailable: " + r.message);
        return 502;
    }
    r = sc.command("DELETE " + name);
    if (!r.ok()) {
        int status = http_status_for(r);
        send_json_error(fd, status, r.message);
        return status;
    }
    send_json(fd, 200, "{\"ok\":true,\"message\":\"" + json_escape(r.message) + "\"}");
    return 200;
}

// ---------------------------------------------------------------------------
// One HTTP connection = one thread
// ---------------------------------------------------------------------------
static void handle_connection(Socket sock, Config cfg) {
    int fd = sock.get();
    set_socket_timeout(fd, SO_RCVTIMEO, HTTP_TIMEOUT_SECONDS);
    set_socket_timeout(fd, SO_SNDTIMEO, HTTP_TIMEOUT_SECONDS);

    HttpConn conn(fd);
    std::string head;
    if (!conn.read_head(head)) return;  // client left or sent garbage

    Request req;
    if (!parse_request(head, req)) {
        send_json_error(fd, 400, "Malformed HTTP request");
        return;
    }

    const std::string prefix = "/api/files/";
    int status = 404;
    if (req.path == "/favicon.ico") {  // browsers ask for this automatically; answer quietly
        send_http(fd, 204, "image/x-icon", "");
        return;
    }
    if (req.path == "/" || req.path == "/index.html") {
        status = (req.method == "GET") ? handle_index(fd) : 405;
        if (status == 405) send_json_error(fd, 405, "Method not allowed");
    } else if (req.path == "/api/files") {
        if (req.method == "GET") {
            status = handle_list(fd, cfg);
        } else {
            status = 405;
            send_json_error(fd, 405, "Method not allowed");
        }
    } else if (req.path.rfind(prefix, 0) == 0) {
        std::string name = req.path.substr(prefix.size());
        if (!is_valid_filename(name)) {
            status = 400;
            send_json_error(fd, 400,
                            "Invalid filename. Use only letters, digits, '.', '_' and '-' "
                            "(max 100 characters, must not start with '.').");
            // Not draining here: for a bad name we simply close the connection.
        } else if (req.method == "GET") {
            status = handle_download(fd, name, cfg);
        } else if (req.method == "PUT" || req.method == "POST") {
            status = handle_upload(conn, fd, req, name, cfg);
        } else if (req.method == "DELETE") {
            status = handle_delete(fd, name, cfg);
        } else {
            status = 405;
            send_json_error(fd, 405, "Method not allowed");
        }
    } else {
        send_json_error(fd, 404, "Not found");
    }
    g_log.log("INFO", req.method + " " + printable(req.path, 120) + " -> " + std::to_string(status));
}

static bool parse_port(const char* text, int& port) {
    char* end = nullptr;
    long value = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || value < 1 || value > 65535) return false;
    port = static_cast<int>(value);
    return true;
}

int main(int argc, char* argv[]) {
    int http_port = 8000;
    int storage_port = 8080;
    Config cfg;
    std::string bind_addr = "127.0.0.1";  // localhost only; pass 0.0.0.0 to allow other computers

    if (argc > 5) {
        std::cerr << "Usage: " << argv[0] << " [http_port] [storage_host] [storage_port] [bind_addr]\n";
        return 1;
    }
    if (argc >= 2 && !parse_port(argv[1], http_port)) {
        std::cerr << "Invalid HTTP port: " << argv[1] << "\n";
        return 1;
    }
    if (argc >= 3) cfg.storage_host = argv[2];
    if (argc >= 4) {
        if (!parse_port(argv[3], storage_port)) {
            std::cerr << "Invalid storage port: " << argv[3] << "\n";
            return 1;
        }
    }
    cfg.storage_port = std::to_string(storage_port);
    if (argc >= 5) bind_addr = argv[4];

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    std::signal(SIGPIPE, SIG_IGN);

    Socket listener(::socket(AF_INET, SOCK_STREAM, 0));
    if (!listener.valid()) {
        std::cerr << "socket(): " << std::strerror(errno) << "\n";
        return 1;
    }
    int opt = 1;
    ::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(http_port));
    if (::inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "Invalid bind address: " << bind_addr << "\n";
        return 1;
    }
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "bind() on " << bind_addr << ":" << http_port << ": " << std::strerror(errno) << "\n";
        return 1;
    }
    if (::listen(listener.get(), 128) < 0) {
        std::cerr << "listen(): " << std::strerror(errno) << "\n";
        return 1;
    }
    g_log.log("INFO", "Web gateway listening on http://" + bind_addr + ":" + std::to_string(http_port) +
                          "  (storage server " + cfg.storage_host + ":" + cfg.storage_port + ")");

    while (g_running) {
        pollfd pfd{};
        pfd.fd = listener.get();
        pfd.events = POLLIN;
        if (::poll(&pfd, 1, 500) <= 0) continue;

        int fd = ::accept(listener.get(), nullptr, nullptr);
        if (fd < 0) {
            if (errno != EINTR) std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        Socket client(fd);
        if (g_active.load() >= MAX_CONNECTIONS) {
            send_json_error(fd, 503, "Gateway is busy");
            continue;
        }
        ++g_active;
        try {
            std::thread([sock = std::move(client), cfg]() mutable {
                handle_connection(std::move(sock), cfg);
                --g_active;
            }).detach();
        } catch (const std::system_error& e) {
            --g_active;
            g_log.log("ERROR", std::string("Cannot start a thread: ") + e.what());
        }
    }

    g_log.log("INFO", "Gateway shutting down");
    for (int i = 0; i < 100 && g_active.load() > 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return 0;
}
