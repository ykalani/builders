#pragma once
#include "input.hpp"

constexpr std::uint32_t CLICK = 1u << 31;
struct Parsed {
    std::string_view id, user;
    i64 ingest = 0, ts = 0;
    std::uint32_t code = 0, revenue = 0;
};
using CodeMap = std::unordered_map<std::string_view, std::uint32_t>;

// This hash reverses the seed-independent suffix of the generator's ID permutation.
// Any collision only sends additional rows through exact deduplication.
inline size_t event_slot(std::string_view id, size_t mask) {
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

// Malformed rows and unusable clicks are dropped immediately. Invalid events
// still carry their ID and ingestion time so an invalid earliest copy wins.
inline bool decode_fields(const std::string_view* f, bool click, i64 start, i64 end,
                       const CodeMap& products, const CodeMap& campaigns,
                       Parsed& row) {
    if (f[0].empty()) return false;
    row = Parsed{};
    row.id = f[0];
    bool valid_ts = timestamp(f[click ? 3 : 2], row.ts);
    if (click) {
        const auto campaign = campaigns.find(f[2]);
        if (f[1].empty() || !valid_ts || row.ts < start - 604800000 || row.ts >= end || campaign == campaigns.end()) return false;
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
    return true;
}

inline bool decode_row(std::string_view line, bool click, i64 start, i64 end,
                       const CodeMap& products, const CodeMap& campaigns,
                       std::vector<std::string_view>& fields, Parsed& row) {
    return shopping::parse(line, fields, click ? 4 : 7) &&
           decode_fields(fields.data(), click, start, end, products, campaigns, row);
}
