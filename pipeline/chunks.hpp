#pragma once
#include "input.hpp"
#include <cstring>
#include <memory>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace shopping {
class MappedFile {
public:
    const char* data = nullptr;
    size_t size = 0;
    explicit MappedFile(const fs::path& path) {
        size = fs::file_size(path);
        if (!size) return;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open " + path.string());
        void* mapping = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (mapping == MAP_FAILED) throw std::runtime_error("Cannot map " + path.string());
        data = static_cast<const char*>(mapping);
    }
    ~MappedFile() { if (data) munmap(const_cast<char*>(data), size); }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
};

template<class F> void parallel(size_t count, int workers, F work) {
    std::atomic<size_t> next{0};
    std::mutex mutex;
    std::exception_ptr error;
    auto run = [&](size_t worker) {
        try {
            while (true) {
                const size_t task = next.fetch_add(1);
                if (task >= count) break;
                work(task, worker);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex);
            if (!error) error = std::current_exception();
            next.store(count);
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < workers; ++i) threads.emplace_back(run, i);
    for (auto& thread : threads) thread.join();
    if (error) std::rethrow_exception(error);
}

struct Chunk { const char* begin; const char* end; bool click; };

struct MappedInputs {
    std::vector<std::unique_ptr<MappedFile>> mappings;
    std::vector<Chunk> chunks;
    explicit MappedInputs(const fs::path& data) {
        constexpr size_t CHUNK_BYTES = 8 * 1024 * 1024;
        for (bool click : {false, true}) {
            for (const auto& path : files(data / (click ? "clicks" : "events"))) {
                auto file = std::make_unique<MappedFile>(path);
                if (file->size) {
                    const char* limit = file->data + file->size;
                    const char* header = static_cast<const char*>(std::memchr(file->data, '\n', file->size));
                    const char* begin = header ? header + 1 : limit;
                    while (begin < limit) {
                        const size_t remaining = limit - begin;
                        const char* boundary = limit;
                        if (remaining > CHUNK_BYTES) {
                            const char* candidate = begin + CHUNK_BYTES;
                            const char* lf = static_cast<const char*>(std::memchr(candidate, '\n', limit - candidate));
                            if (lf) boundary = lf + 1;
                        }
                        chunks.push_back({begin, boundary, click});
                        begin = boundary;
                    }
                }
                mappings.push_back(std::move(file));
            }
        }
    }
};
} // namespace shopping
