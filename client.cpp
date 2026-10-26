// client.cpp - interactive command-line client
// Usage: ./client <server-ip> <port>
// Commands: HELP, LIST, UPLOAD <local file>, DOWNLOAD <name>, DELETE <name>, QUIT

#include "common.h"

#include <filesystem>
#include <iostream>
#include <sstream>

namespace fs = std::filesystem;

static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

static void print_usage() {
    std::cout << "Client commands:\n"
                 "  HELP                  show this help\n"
                 "  LIST                  list files stored on the server\n"
                 "  UPLOAD <local file>   upload a file from this computer\n"
                 "  DOWNLOAD <name>       download a stored file into ./downloads/\n"
                 "  DELETE <name>         delete a stored file on the server\n"
                 "  QUIT                  disconnect\n";
}

// Prints the outcome of a request. Returns false if the connection is broken.
static bool report(const Reply& r) {
    if (r.ok()) {
        std::cout << r.message << "\n";
        return true;
    }
    if (r.status == Reply::SERVER_ERROR) {
        std::cout << "Error: " << r.message << " [" << r.code << "]\n";
        return true;
    }
    std::cout << "Connection problem: " << r.message << "\n";
    return false;
}

static bool do_list(StorageClient& sc) {
    Reply r = sc.command("LIST");
    if (!r.ok()) return report(r);
    std::istringstream in(r.message);
    std::string line;
    std::getline(in, line);  // first line: the file count
    std::vector<std::string> rows;
    while (std::getline(in, line))
        if (!line.empty()) rows.push_back(line);
    if (rows.empty()) {
        std::cout << "No files stored on the server.\n";
        return true;
    }
    std::cout << "Files on server (" << rows.size() << "):\n";
    for (const auto& row : rows) {
        std::istringstream ls(row);
        std::string name, size;
        ls >> name >> size;
        std::cout << "  " << name << "  (" << size << " bytes)\n";
    }
    return true;
}

static bool do_upload(StorageClient& sc, const std::string& path) {
    FileHandle in(::open(path.c_str(), O_RDONLY));
    if (!in.valid()) {
        std::cout << "Error: cannot open '" << path << "': " << std::strerror(errno) << "\n";
        return true;
    }
    struct stat st{};
    if (::fstat(in.get(), &st) < 0 || !S_ISREG(st.st_mode)) {
        std::cout << "Error: '" << path << "' is not a regular file\n";
        return true;
    }
    std::string name = fs::path(path).filename().string();
    if (!is_valid_filename(name)) {
        std::cout << "Error: the file name must use only letters, digits, '.', '_' and '-' (no spaces), "
                     "and must not start with '.'. Rename the file first.\n";
        return true;
    }
    uint64_t size = static_cast<uint64_t>(st.st_size);
    std::cout << "Uploading " << name << " (" << size << " bytes)...\n";
    Reply r = sc.upload(name, size, [&](char* buf, size_t n) { return read_full_fd(in.get(), buf, n); });
    return report(r);
}

static bool do_download(StorageClient& sc, const std::string& name) {
    // Check the name locally too: it decides where we write on THIS computer.
    if (!is_valid_filename(name)) {
        std::cout << "Error: invalid file name (letters, digits, '.', '_' and '-' only)\n";
        return true;
    }
    std::error_code ec;
    fs::create_directories("downloads", ec);
    std::string out_path = "downloads/" + name;
    FileHandle out;
    bool created = false;
    uint64_t total = 0;
    Reply r = sc.download(
        name,
        [&](uint64_t size) {
            total = size;
            out = FileHandle(::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644));
            created = out.valid();
            return created;
        },
        [&](const char* data, size_t n) { return write_all_fd(out.get(), data, n); });
    out.reset();
    if (!r.ok()) {
        if (created) ::unlink(out_path.c_str());  // never keep a partial download
        return report(r);
    }
    std::cout << "Downloaded " << name << " (" << total << " bytes) -> " << out_path << "\n";
    return true;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <server-ip> <port>\n";
        return 1;
    }

    StorageClient sc;
    Reply hello = sc.connect_to(argv[1], argv[2]);
    if (!hello.ok()) {
        std::cerr << "Could not connect: " << hello.message << "\n";
        return 1;
    }
    std::cout << "Connected to server\n" << hello.message << "\n";

    std::string line;
    bool alive = true;
    while (alive) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) break;  // Ctrl+D / end of input
        line = trim(line);
        if (line.empty()) continue;

        size_t sp = line.find_first_of(" \t");
        std::string cmd = to_upper(line.substr(0, sp));
        std::string arg = (sp == std::string::npos) ? "" : trim(line.substr(sp + 1));

        if (cmd == "QUIT" || cmd == "EXIT") {
            break;
        } else if (cmd == "HELP") {
            print_usage();
            std::cout << "\nServer says:\n";
            alive = report(sc.command("HELP"));
        } else if (cmd == "LIST") {
            alive = do_list(sc);
        } else if (cmd == "UPLOAD") {
            if (arg.empty()) std::cout << "Usage: UPLOAD <local file>\n";
            else alive = do_upload(sc, arg);
        } else if (cmd == "DOWNLOAD") {
            if (arg.empty()) std::cout << "Usage: DOWNLOAD <name>\n";
            else alive = do_download(sc, arg);
        } else if (cmd == "DELETE") {
            // Sent as typed so the SERVER performs the filename validation.
            if (arg.empty()) std::cout << "Usage: DELETE <name>\n";
            else alive = report(sc.command("DELETE " + arg));
        } else {
            std::cout << "Unknown command '" << cmd << "'. Type HELP.\n";
        }
    }
    return alive ? 0 : 1;
}
