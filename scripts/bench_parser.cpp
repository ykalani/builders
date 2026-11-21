#include "../pipeline/chunks.hpp"
#include "../pipeline/decode.hpp"

// The Python wrapper passes the scorer's manifest boundaries. The timer covers
// the row scan and checksum only, after mapping input and loading dimensions.
int main(int argc, char** argv) {
    if (argc != 5) throw std::runtime_error("Expected DATA_DIR WINDOW_START WINDOW_END NCPU");
    const fs::path data = argv[1];
    const int workers = std::stoi(argv[4]);
    i64 start, end;
    if (workers < 1 || !timestamp(argv[2], start) || !timestamp(argv[3], end))
        throw std::runtime_error("Invalid bounds or worker count");

    std::unordered_map<std::string, std::uint32_t> codes{{"organic", 0}, {"unknown", 1}};
    std::unordered_map<std::string, std::uint32_t> product_storage, campaign_storage;
    read(data / "products.csv", 2, [&](auto& f) {
        product_storage.emplace(f[0], codes.try_emplace(std::string(f[1]), codes.size()).first->second);
    });
    read(data / "campaigns.csv", 2, [&](auto& f) {
        campaign_storage.emplace(f[0], codes.try_emplace(std::string(f[0]), codes.size()).first->second);
    });
    CodeMap products, campaigns;
    for (const auto& [id, code] : product_storage) products.emplace(id, code);
    for (const auto& [id, code] : campaign_storage) campaigns.emplace(id, code);

    shopping::MappedInputs inputs(data);
    size_t event_bytes = 0;
    for (const auto& chunk : inputs.chunks)
        if (!chunk.click) event_bytes += chunk.end - chunk.begin;
    const size_t slot_mask = (size_t{1} << (event_bytes > 3ULL * 1024 * 1024 * 1024 ? 26 : 25)) - 1;
    struct Result { std::uint64_t checksum = 0, events = 0, clicks = 0, invalid_events = 0; };
    std::vector<Result> chunks(inputs.chunks.size());
    const auto began = std::chrono::steady_clock::now();
    shopping::parallel(inputs.chunks.size(), workers, [&](size_t task, size_t) {
        const auto& chunk = inputs.chunks[task];
        std::vector<std::string_view> fields;
        Result result;
        for (const char* line = chunk.begin; line < chunk.end;) {
            const char* lf = static_cast<const char*>(std::memchr(line, '\n', chunk.end - line));
            const char* end_line = lf ? lf : chunk.end;
            std::string_view record(line, end_line - line);
            line = lf ? lf + 1 : end_line;
            Parsed row;
            if (!decode_row(record, chunk.click, start, end, products, campaigns, fields, row)) continue;
            const auto user = encode_id(row.user);
            const auto event = chunk.click ? std::hash<std::string_view>{}(row.id) : event_slot(row.id, slot_mask);
            result.checksum += (user ? user : std::hash<std::string_view>{}(row.user)) ^ event ^
                std::uint64_t(row.ts) ^ std::uint64_t(row.ingest) ^ row.code ^ row.revenue;
            if (chunk.click) ++result.clicks;
            else if (row.user.empty()) ++result.invalid_events;
            else ++result.events;
        }
        chunks[task] = result;
    });
    const auto elapsed = std::chrono::steady_clock::now() - began;
    Result total;
    for (const auto& chunk : chunks) {
        total.checksum += chunk.checksum;
        total.events += chunk.events;
        total.clicks += chunk.clicks;
        total.invalid_events += chunk.invalid_events;
    }
    std::cout << "seconds=" << std::chrono::duration<double>(elapsed).count() << " checksum=" << total.checksum
              << " valid_event_copies=" << total.events << " valid_click_copies=" << total.clicks
              << " invalid_event_candidates=" << total.invalid_events << '\n';
}
