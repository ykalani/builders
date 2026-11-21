#pragma once
#include "csv.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using i64 = std::int64_t;
using shopping::read;
using shopping::field;

std::vector<fs::path> files(const fs::path& dir) {
    std::vector<fs::path> result;
    for (const auto& entry : fs::directory_iterator(dir))
        if (entry.is_regular_file() && entry.path().extension() == ".csv") result.push_back(entry.path());
    std::sort(result.begin(), result.end());
    return result;
}

bool digit(char c) { return c >= '0' && c <= '9'; }
int number(std::string_view s, size_t at, size_t length) {
    int value = 0;
    for (size_t i = at; i < at + length; ++i) {
        if (!digit(s[i])) return -1;
        value = value * 10 + s[i] - '0';
    }
    return value;
}

// Gregorian civil date to days since 1970-01-01, including pre-epoch dates.
i64 days(int year, int month, int day) {
    year -= month <= 2;
    int era = year / 400;
    unsigned y = year - era * 400;
    unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    return era * 146097LL + y * 365 + y / 4 - y / 100 + doy - 719468;
}

bool timestamp(std::string_view s, i64& value) {
    if (s.empty()) return false;
    const bool iso = (s.size() == 20 || s.size() == 25) &&
                     s[4] == '-' && s[7] == '-' && s[10] == 'T' && s[13] == ':' && s[16] == ':';
    if (!iso) {
        value = 0;
        // Up to 18 digits always fit in a signed 64-bit value.
        const bool check_overflow = s.size() > 18;
        for (char c : s) {
            if (!digit(c) || (check_overflow && value > (std::numeric_limits<i64>::max() - (c - '0')) / 10))
                return false;
            value = value * 10 + c - '0';
        }
        return true;
    }
    int y = number(s, 0, 4), m = number(s, 5, 2), d = number(s, 8, 2);
    int h = number(s, 11, 2), minute = number(s, 14, 2), second = number(s, 17, 2);
    if (y < 1 || m < 1 || m > 12 || d < 1 || h < 0 || h > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return false;
    static constexpr int lengths[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    if (d > lengths[m] + (m == 2 && leap)) return false;
    int offset = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
    } else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
        int oh = number(s, 20, 2), om = number(s, 23, 2);
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return false;
        offset = (oh * 60 + om) * (s[19] == '+' ? 1 : -1);
    }
    value = ((days(y, m, d) * 24 + h) * 60 + minute - offset) * 60000 + second * 1000;
    return true;
}

inline std::uint64_t encode_id(std::string_view id) {
    if (id.empty() || id.size() > 11) return 0;
    std::uint64_t value = 1;
    for (unsigned char c : id) {
        unsigned digit;
        if (c >= '0' && c <= '9') digit = c - '0' + 1;
        else if (c >= 'a' && c <= 'z') digit = c - 'a' + 11;
        else return 0;
        value = value * 37 + digit;
    }
    return value;
}

inline std::uint64_t id_hash(std::string_view id, std::uint64_t encoded) {
    if (!encoded) return std::hash<std::string_view>{}(id);
    encoded ^= encoded >> 30;
    encoded *= 0xbf58476d1ce4e5b9ULL;
    encoded ^= encoded >> 27;
    encoded *= 0x94d049bb133111ebULL;
    return encoded ^ (encoded >> 31);
}

// During link ingestion each slot holds a union-find node. prepare() replaces it
// with the final person number so event lookups use the same table in place.
class Ids {
public:
    static constexpr std::uint32_t absent = std::numeric_limits<std::uint32_t>::max();

    void join(std::string_view a, std::string_view b) {
        auto x = root(add(a));
        auto y = root(add(b));
        if (x == y) return;
        if (rank[x] < rank[y]) std::swap(x, y);
        parent[y] = x;
        if (rank[x] == rank[y]) ++rank[x];
        if (names[smallest[y]] < names[smallest[x]]) smallest[x] = smallest[y];
    }

    void prepare() {
        std::vector<std::uint32_t> dense(names.size(), absent);
        for (size_t i = 0; i < names.size(); ++i) {
            const auto representative = root(static_cast<std::uint32_t>(i));
            if (dense[representative] == absent) {
                dense[representative] = static_cast<std::uint32_t>(canonical.size());
                canonical.push_back(std::move(names[smallest[representative]]));
            }
            dense[i] = dense[representative];
        }
        for (auto& slot : table) if (slot.key) slot.value = dense[slot.value];
        for (auto& item : fallback) item.second = dense[item.second];
        std::vector<std::string>().swap(names);
        std::vector<std::uint32_t>().swap(parent);
        std::vector<std::uint32_t>().swap(smallest);
        std::vector<std::uint8_t>().swap(rank);
    }

    std::uint32_t linked_person(std::string_view id, std::uint64_t encoded) const {
        if (encoded) {
            const auto& entry = table[position(encoded, table)];
            return entry.key ? entry.value : absent;
        }
        if (fallback.empty()) return absent;
        const auto it = fallback.find(std::string(id));
        return it == fallback.end() ? absent : it->second;
    }

    size_t person_count() const { return canonical.size(); }
    std::string_view canonical_name(std::uint32_t person) const { return canonical[person]; }

    std::string_view person(std::string_view id) const {
        const auto linked = linked_person(id, encode_id(id));
        return linked == absent ? id : canonical_name(linked);
    }

private:
    struct Slot { std::uint64_t key = 0; std::uint32_t value = 0; };
    std::vector<Slot> table = std::vector<Slot>(16);
    size_t used = 0;
    std::unordered_map<std::string, std::uint32_t> fallback;
    std::vector<std::string> names, canonical;
    std::vector<std::uint32_t> parent, smallest;
    std::vector<std::uint8_t> rank;

    static size_t position(std::uint64_t key, const std::vector<Slot>& slots) {
        const auto mask = slots.size() - 1;
        auto i = id_hash({}, key) & mask;
        while (slots[i].key && slots[i].key != key) i = (i + 1) & mask;
        return i;
    }

    std::uint32_t add(std::string_view name) {
        const auto node = static_cast<std::uint32_t>(names.size());
        const auto key = encode_id(name);
        if (key) {
            auto i = position(key, table);
            if (table[i].key) return table[i].value;
            if (used >= table.size() * 2 / 3) {
                std::vector<Slot> grown(table.size() * 2);
                for (const auto& entry : table)
                    if (entry.key) grown[position(entry.key, grown)] = entry;
                table.swap(grown);
                i = position(key, table);
            }
            table[i] = {key, node};
            ++used;
        } else {
            auto [it, inserted] = fallback.try_emplace(std::string(name), node);
            if (!inserted) return it->second;
        }
        if (node == absent) throw std::runtime_error("Too many linked IDs");
        names.emplace_back(name);
        parent.push_back(node);
        smallest.push_back(node);
        rank.push_back(0);
        return node;
    }

    std::uint32_t root(std::uint32_t node) {
        while (parent[node] != node) {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        return node;
    }
};
