#include "input.hpp"
#include "output.hpp"
#ifdef NATIVE_MAIN
#include <nlohmann/json.hpp>
#endif
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <deque>

#ifdef RUST_CSV
struct RustField { size_t offset, len; };
extern "C" bool rust_csv_fields(const char*, size_t, RustField*, size_t);
#endif

namespace {
constexpr std::uint32_t CLICK = 1u << 31;

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
struct Parsed {
    std::string_view id, user;
    i64 ingest = 0, ts = 0;
    std::uint32_t code = 0, revenue = 0;
};
size_t table_capacity(size_t count) {
    size_t capacity = 16;
    while (count > capacity / 2) {
        if (capacity > std::numeric_limits<size_t>::max() / 2)
            throw std::runtime_error("ID table too large");
        capacity *= 2;
    }
    return capacity;
}

struct WinnerSlot {
    std::uint64_t encoded = 0;
    Parsed* row = nullptr;
};

// This hash reverses the seed-independent suffix of the generator's ID permutation.
// Any collision only sends additional rows through exact deduplication.
size_t event_slot(std::string_view id, size_t mask) {
    constexpr std::uint64_t value_mask = (1ULL << 40) - 1;
    constexpr std::uint64_t inverse = [] {
        std::uint64_t result = 1;
        for (int i = 0; i < 6; ++i) result *= 2 - 0xD6E8FEB86659ULL * result;
        return result;
    }();
    if (id.size() < 2 || id.size() > 9 || id.front() != 'e' ||
        (id.size() > 2 && id[1] == '0')) return mask + 1;
    std::uint64_t value = 0;
    for (char c : id.substr(1)) {
        if (c >= '0' && c <= '9') value = value * 36 + c - '0';
        else if (c >= 'a' && c <= 'z') value = value * 36 + c - 'a' + 10;
        else return mask + 1;
    }
    if (value > value_mask) return mask + 1;
    value = (value * inverse) & value_mask;
    return (value ^ (value >> 17) ^ (value >> 34)) & mask;
}

// Short IDs use less than 62 bits. The other two classes carry a linked
// component number or an index into a producer's exact fallback strings.
constexpr std::uint64_t LINKED = 1ULL << 63;
constexpr std::uint64_t FALLBACK = 1ULL << 62;
static_assert(2ULL * 37 * 37 * 37 * 37 * 37 * 37 * 37 * 37 * 37 * 37 * 37 < FALLBACK);

struct Routed {
    std::uint64_t identity;
    i64 ts;
    std::uint32_t code, payload; // purchase cents or index into this producer's click IDs
};
static_assert(sizeof(Routed) == 24);

struct Producer {
    std::vector<std::vector<Routed>> shards;
    std::vector<std::string_view> click_ids, fallback_ids;
};

void route(const Parsed& row, const Ids& ids, Producer& producer) {
    const auto encoded = encode_id(row.user);
    const auto linked = ids.linked_person(row.user, encoded);
    std::uint64_t key = encoded;
    size_t owner;
    if (linked != Ids::absent) {
        key = LINKED | linked;
        owner = linked % producer.shards.size();
    } else {
        owner = id_hash(row.user, encoded) % producer.shards.size();
        if (!encoded) {
            key = FALLBACK | producer.fallback_ids.size();
            producer.fallback_ids.push_back(row.user);
        }
    }
    auto payload = row.revenue;
    if (row.code & CLICK) {
        if (producer.click_ids.size() >= UINT32_MAX) throw std::runtime_error("Too many clicks per producer");
        payload = static_cast<std::uint32_t>(producer.click_ids.size());
        producer.click_ids.push_back(row.id);
    }
    producer.shards[owner].push_back({key, row.ts, row.code, payload});
}

// Reconstruct an unlinked short ID just once per person, including leading zeroes.
std::string decode_id(std::uint64_t key) {
    constexpr char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    char buffer[11];
    char* end = buffer + sizeof(buffer);
    char* it = end;
    while (key != 1) {
        *--it = alphabet[key % 37 - 1];
        key /= 37;
    }
    return std::string(it, end);
}

struct CompactRow {
    i64 ts;
    std::uint32_t person, code, revenue, click;
};
static_assert(sizeof(CompactRow) == 24);

void execute(const fs::path& data, const fs::path& output, i64 start, i64 end, int workers) {
    const auto began = std::chrono::steady_clock::now();
    Ids ids;
    for (const auto& path : files(data / "links")) read(path, 2, [&](auto& f) {
        if (!f[0].empty() && !f[1].empty()) ids.join(f[0], f[1]);
    });
    const auto links_read = std::chrono::steady_clock::now();
    ids.prepare();
    const auto links_frozen = std::chrono::steady_clock::now();
    std::cerr << "Link ingest: " << std::chrono::duration<double>(links_read - began).count() << " seconds\n";
    std::cerr << "Link freeze: " << std::chrono::duration<double>(links_frozen - links_read).count() << " seconds\n";
    std::vector<std::string> names{"organic", "unknown"};
    std::unordered_map<std::string, std::uint32_t> codes{{"organic", 0}, {"unknown", 1}};
    auto intern = [&](std::string_view name) {
        if (names.size() >= CLICK) throw std::runtime_error("Too many category/campaign values");
        auto [it, added] = codes.emplace(name, names.size());
        if (added) names.emplace_back(name);
        return it->second;
    };
    std::unordered_map<std::string, std::uint32_t> product_storage, campaign_storage;
    read(data / "products.csv", 2, [&](auto& f) { product_storage.emplace(f[0], intern(f[1])); });
    read(data / "campaigns.csv", 2, [&](auto& f) { campaign_storage.emplace(f[0], intern(f[0])); });
    std::unordered_map<std::string_view, std::uint32_t> products, campaigns;
    for (const auto& [id, code] : product_storage) products.emplace(id, code);
    for (const auto& [id, code] : campaign_storage) campaigns.emplace(id, code);
    const auto channels = read_channels((data / "campaigns.csv").c_str());

    std::vector<std::unique_ptr<MappedFile>> mappings;
    std::vector<Chunk> chunks;
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
    const size_t shard_count = size_t(workers) * 4;
    const auto bitmap_begin = std::chrono::steady_clock::now();
    size_t event_bytes = 0;
    for (const auto& chunk : chunks) if (!chunk.click) event_bytes += chunk.end - chunk.begin;
    const size_t slot_count = size_t(1) << (event_bytes > 3ULL * 1024 * 1024 * 1024 ? 26 : 25);
    const size_t slot_mask = slot_count - 1;
    const size_t words = slot_count / 64;
    std::vector<std::vector<std::uint64_t>> seen(workers, std::vector<std::uint64_t>(words));
    std::vector<std::vector<std::uint64_t>> repeated(workers, std::vector<std::uint64_t>(words));
    parallel(chunks.size(), workers, [&](size_t task, size_t worker) {
        const auto& chunk = chunks[task];
        if (chunk.click) return;
        const char* begin = chunk.begin;
        while (begin < chunk.end) {
            const char* lf = static_cast<const char*>(std::memchr(begin, '\n', chunk.end - begin));
            const char* stop = lf ? lf : chunk.end;
            const char* comma = static_cast<const char*>(std::memchr(begin, ',', stop - begin));
            std::string_view id(begin, (comma ? comma : stop) - begin);
            begin = lf ? lf + 1 : stop;
            if (id.size() >= 2 && id.front() == '"' && id.back() == '"') {
                id.remove_prefix(1);
                id.remove_suffix(1);
            }
            const size_t slot = event_slot(id, slot_mask);
            if (slot > slot_mask) continue;
            const auto bit = 1ULL << (slot % 64);
            repeated[worker][slot / 64] |= seen[worker][slot / 64] & bit;
            seen[worker][slot / 64] |= bit;
        }
    });
    std::vector<std::uint64_t> duplicates(words);
    constexpr size_t MERGE_WORDS = 4096;
    const size_t merge_blocks = (words + MERGE_WORDS - 1) / MERGE_WORDS;
    auto merge_bits = [&](size_t block, size_t) {
        const size_t first = block * MERGE_WORDS;
        const size_t last = std::min(first + MERGE_WORDS, words);
        for (int worker = 0; worker < workers; ++worker) {
            // Collapse into worker zero's buffers because each task owns its word range.
            if (worker == 0) {
                std::copy(repeated[0].begin() + first, repeated[0].begin() + last,
                          duplicates.begin() + first);
                continue;
            }
            for (size_t word = first; word < last; ++word) {
                const auto incoming = seen[worker][word];
                duplicates[word] |= repeated[worker][word] | (seen[0][word] & incoming);
                seen[0][word] |= incoming;
            }
        }
    };
    if (merge_blocks == 1) merge_bits(0, 0);
    else parallel(merge_blocks, workers, merge_bits);
    seen.clear();
    repeated.clear();
    const auto scanned_at = std::chrono::steady_clock::now();
    std::cerr << "Native ID scan: " << std::chrono::duration<double>(scanned_at - bitmap_begin).count() << " seconds\n";
    std::vector<std::vector<std::vector<Parsed>>> parsed(workers,
        std::vector<std::vector<Parsed>>(shard_count));
    // Parser workers and dedup tasks each write to a private producer.
    std::vector<Producer> producers(workers + shard_count);
    for (auto& producer : producers) producer.shards.resize(shard_count);
    parallel(chunks.size(), workers, [&](size_t task, size_t worker) {
        const auto& chunk = chunks[task];
#ifdef RUST_CSV
        std::string_view f[7];
        RustField fields[7];
        const size_t field_count = chunk.click ? 4 : 7;
#else
        std::vector<std::string_view> f;
#endif
        const char* begin = chunk.begin;
        while (begin < chunk.end) {
            const char* lf = static_cast<const char*>(std::memchr(begin, '\n', chunk.end - begin));
            const char* stop = lf ? lf : chunk.end;
            const std::string_view line(begin, stop - begin);
            begin = lf ? lf + 1 : stop;
#ifdef RUST_CSV
            if (!rust_csv_fields(line.data(), line.size(), fields, field_count)) continue;
            for (size_t i = 0; i < field_count; ++i)
                f[i] = line.substr(fields[i].offset, fields[i].len);
            if (f[0].empty()) continue;
#else
            if (!shopping::parse(line, f, chunk.click ? 4 : 7) || f[0].empty()) continue;
#endif
            Parsed row;
            row.id = f[0];
            bool valid_ts = timestamp(f[chunk.click ? 3 : 2], row.ts);
            if (chunk.click) {
                const auto campaign = campaigns.find(f[2]);
                if (f[1].empty() || !valid_ts || row.ts < start - 604800000 || row.ts >= end || campaign == campaigns.end()) continue;
                row.user = f[1];
                row.code = campaign->second | CLICK;
            } else {
                if (!timestamp(f[3], row.ingest)) throw std::runtime_error("Invalid ingest_ts");
                bool valid = !f[1].empty() && !f[5].empty() && valid_ts && start <= row.ts && row.ts < end &&
                             (f[4] == "view" || f[4] == "cart" || f[4] == "purchase");
                if (f[4] == "purchase") {
                    bool price_valid = !f[6].empty();
                    for (char c : f[6]) {
                        if (!digit(c) || row.revenue > 1000000) { price_valid = false; break; }
                        row.revenue = row.revenue * 10 + c - '0';
                    }
                    valid = valid && price_valid && row.revenue >= 1 && row.revenue <= 10000000;
                }
                valid = valid && row.ingest - row.ts <= 86400000;
                // Empty user marks an invalid copy, which still participates in deduplication.
                if (valid) {
                    row.user = f[1];
                    if (row.revenue) {
                        const auto category = products.find(f[5]);
                        row.code = category == products.end() ? 1 : category->second;
                    }
                }
            }
            const size_t slot = chunk.click ? slot_count : event_slot(row.id, slot_mask);
            const bool unique = slot < slot_count && !(duplicates[slot / 64] & (1ULL << (slot % 64)));
            if (chunk.click || unique) {
                if (row.user.empty()) continue;
                route(row, ids, producers[worker]);
            } else {
                const size_t shard = std::hash<std::string_view>{}(row.id) % shard_count;
                parsed[worker][shard].push_back(row);
            }
        }
    });
    const auto parsed_at = std::chrono::steady_clock::now();
    std::cerr << "Native chunk parsing: " << std::chrono::duration<double>(parsed_at - scanned_at).count() << " seconds\n";

    parallel(shard_count, workers, [&](size_t shard, size_t) {
        size_t count = 0;
        for (const auto& worker : parsed) count += worker[shard].size();
        std::vector<WinnerSlot> winners(table_capacity(count));
        const size_t mask = winners.size() - 1;
        for (auto& worker : parsed) {
            for (auto& row : worker[shard]) {
                const auto encoded = encode_id(row.id);
                size_t slot = id_hash(row.id, encoded) & mask;
                while (winners[slot].row &&
                       (winners[slot].encoded != encoded ||
                        (!encoded && winners[slot].row->id != row.id))) {
                    slot = (slot + 1) & mask;
                }
                auto& winner = winners[slot];
                if (!winner.row || row.ingest < winner.row->ingest)
                    winner = {encoded, &row};
            }
        }
        for (auto& winner : winners) {
            auto* row = winner.row;
            if (!row) continue;
            if (row->user.empty()) continue;
            route(*row, ids, producers[workers + shard]);
        }
    });
    const auto deduped_at = std::chrono::steady_clock::now();
    std::cerr << "Native deduplication and person partitioning: " << std::chrono::duration<double>(deduped_at - parsed_at).count() << " seconds\n";

    if (std::getenv("SHOPPING_ID_STATS")) {
        size_t linked = 0, numeric = 0, fallback = 0;
        for (const auto& producer : producers)
            for (const auto& shard : producer.shards)
                for (const auto& row : shard) {
                    if (row.identity & LINKED) ++linked;
                    else if (row.identity & FALLBACK) ++fallback;
                    else ++numeric;
                }
        std::cerr << "Routed rows: linked=" << linked << " unlinked_numeric=" << numeric
                  << " unlinked_fallback=" << fallback << '\n';
    }
    fs::create_directories(output / "user_daily");
    fs::create_directories(output / "campaign_daily");
    using Clock = std::chrono::steady_clock;
    struct ShardTiming { Clock::duration group{}, sort{}, aggregate{}, write{}; };
    std::vector<ShardTiming> timings(shard_count);
    std::vector<CampaignTotals> partials(shard_count);
    parallel(shard_count, workers, [&](size_t shard, size_t) {
        const auto group_start = Clock::now();
        size_t count = 0;
        for (const auto& producer : producers) count += producer.shards[shard].size();
        const size_t nlinked = ids.person_count();
        const size_t linked_here = shard < nlinked ? 1 + (nlinked - shard - 1) / shard_count : 0;
        std::vector<std::string_view> persons;
        persons.reserve(linked_here + count / 30);
        for (size_t i = shard; i < nlinked; i += shard_count)
            persons.push_back(ids.canonical_name(static_cast<std::uint32_t>(i)));
        std::unordered_map<std::uint64_t, std::uint32_t> numeric_people;
        std::unordered_map<std::string_view, std::uint32_t> other_people;
        std::deque<std::string> decoded_names;
        std::vector<std::string_view> click_ids;
        std::vector<CompactRow> rows;
        rows.reserve(count);
        for (auto& producer : producers) {
            for (const auto& row : producer.shards[shard]) {
                std::uint32_t person;
                if (row.identity & LINKED) {
                    person = static_cast<std::uint32_t>((row.identity ^ LINKED) / shard_count);
                } else {
                    if (persons.size() >= UINT32_MAX) throw std::runtime_error("Too many people in a shard");
                    if (row.identity & FALLBACK) {
                        auto name = producer.fallback_ids[row.identity ^ FALLBACK];
                        auto [it, added] = other_people.try_emplace(name, persons.size());
                        if (added) persons.push_back(name);
                        person = it->second;
                    } else {
                        auto [it, added] = numeric_people.try_emplace(row.identity, persons.size());
                        if (added) {
                            decoded_names.push_back(decode_id(row.identity));
                            persons.push_back(decoded_names.back());
                        }
                        person = it->second;
                    }
                }
                std::uint32_t click = 0, revenue = row.payload;
                if (row.code & CLICK) {
                    if (click_ids.size() >= UINT32_MAX) throw std::runtime_error("Too many clicks in a shard");
                    click_ids.push_back(producer.click_ids[row.payload]);
                    click = static_cast<std::uint32_t>(click_ids.size());
                    revenue = 0;
                }
                rows.push_back({row.ts, person, row.code & ~CLICK, revenue, click});
            }
            std::vector<Routed>().swap(producer.shards[shard]);
        }
        // Counting scatter groups people in linear time. Only each person's times are sorted.
        std::vector<size_t> offsets(persons.size() + 1);
        for (const auto& row : rows) ++offsets[row.person + 1];
        for (size_t i = 1; i < offsets.size(); ++i) offsets[i] += offsets[i - 1];
        auto positions = offsets;
        std::vector<CompactRow> grouped(rows.size());
        for (const auto& row : rows) grouped[positions[row.person]++] = row;
        rows.swap(grouped);
        std::vector<CompactRow>().swap(grouped);
        auto earlier = [](const auto& a, const auto& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            // Attribution resolves click ID ties after all clicks at this time are seen.
            return bool(a.click) && !b.click;
        };
        const auto sort_start = Clock::now();
        for (size_t person = 0; person < persons.size(); ++person)
            std::sort(rows.begin() + offsets[person], rows.begin() + offsets[person + 1], earlier);
        const auto aggregate_start = Clock::now();
        UserOutput users(output / "user_daily" / ("part-" + std::to_string(shard) + ".parquet"),
                         {"user_id", "day", "sessions", "events", "revenue_cents", "top_category"});
        size_t cursor = 0;
        partials[shard] = aggregate([&](Row& row) {
            if (cursor == rows.size()) return false;
            const auto& input = rows[cursor++];
            row = {persons[input.person], input.click ? click_ids[input.click - 1] : std::string_view{},
                   input.ts, input.revenue, input.click ? 1 : 2, std::int32_t(input.code), input.person};
            return true;
        }, names, 0, [&](auto person, i64 day, i64 sessions, i64 events, i64 revenue, const std::string* top) {
            users.add(person, static_cast<std::int32_t>(day), sessions, events, revenue, top);
        });
        users.close();
        const auto write = users.write_time();
        timings[shard] = {sort_start - group_start, aggregate_start - sort_start,
                          Clock::now() - aggregate_start - write, write};
    });
    const auto merge_start = Clock::now();
    CampaignTotals totals;
    for (const auto& partial : partials) {
        for (const auto& [key, row] : partial) {
            auto& total = totals[key];
            total.sessions += row.sessions;
            total.purchasing += row.purchasing;
            total.revenue += row.revenue;
            total.assists += row.assists;
        }
    }
    const auto campaign_write_start = Clock::now();
    CampaignOutput campaigns_out(output / "campaign_daily" / "part-0.parquet",
        {"campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"});
    for (const auto& [key, row] : totals) {
        const auto& name = names[key.first];
        campaigns_out.add(std::string_view(name), key.first == 0 ? std::string_view("organic") : channels.at(name),
                          static_cast<std::int32_t>(key.second), row.sessions, row.purchasing, row.revenue, row.assists);
    }
    campaigns_out.close();
    const auto finished_at = std::chrono::steady_clock::now();
    ShardTiming sum;
    for (const auto& timing : timings) {
        sum.group += timing.group;
        sum.sort += timing.sort;
        sum.aggregate += timing.aggregate;
        sum.write += timing.write;
    }
    const auto seconds = [](auto elapsed) { return std::chrono::duration<double>(elapsed).count(); };
    std::cerr << "User shards wall: " << seconds(merge_start - deduped_at) << " seconds\n"
              << "User shard elapsed sums (seconds): grouping=" << seconds(sum.group)
              << " sorting=" << seconds(sum.sort) << " aggregation and Arrow append=" << seconds(sum.aggregate)
              << " Parquet writes=" << seconds(sum.write) << '\n'
              << "Campaign merge wall: " << seconds(campaign_write_start - merge_start) << " seconds\n"
              << "Campaign output wall: " << seconds(finished_at - campaign_write_start) << " seconds\n";
    std::cerr << "Native parallel sort and aggregation: " << std::chrono::duration<double>(finished_at - deduped_at).count() << " seconds\n";
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cerr << "Native total: " << std::chrono::duration<double>(finished_at - began).count() << " seconds; peak RSS: " << usage.ru_maxrss / 1024 << " MiB\n";
}
}

extern "C" int run_pipeline(const char* data, const char* output, i64 start, i64 end,
                             int workers, char* error, size_t error_size) {
    try {
        if (workers < 1) throw std::runtime_error("NCPU must be positive");
        execute(data, output, start, end, workers);
        return 0;
    } catch (const std::exception& e) {
        if (error_size) std::snprintf(error, error_size, "%s", e.what());
        return 1;
    }
}

#ifdef NATIVE_MAIN
int main() {
    try {
        const auto env = [](const char* key, const char* fallback) {
            const auto* value = std::getenv(key);
            return value ? value : fallback;
        };
        const fs::path data = env("DATA_DIR", "/data");
        std::ifstream manifest_file(data / "manifest.json");
        const auto manifest = nlohmann::json::parse(manifest_file);
        i64 start = 0, end = 0;
        if (!timestamp(manifest.at("window_start").get<std::string>(), start) ||
            !timestamp(manifest.at("window_end").get<std::string>(), end))
            throw std::runtime_error("Invalid manifest window");
        const int workers = std::stoi(env("NCPU", "8"));
        if (workers < 1) throw std::runtime_error("NCPU must be positive");
        execute(data, env("OUTPUT_DIR", "/output"), start, end, workers);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "pipeline: " << error.what() << '\n';
        return 1;
    }
}
#endif
