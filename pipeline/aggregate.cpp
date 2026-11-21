#include "csv.hpp"
#include "aggregate.hpp"
#include "arrow_c_abi.h"
#include <charconv>
#include <cstdint>
#include <deque>
#include <functional>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <map>
#include <optional>
#include <unordered_map>

namespace fs = std::filesystem;
using i64 = std::int64_t;
using shopping::field;

i64 integer(std::string_view value) {
    i64 result;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size())
        throw std::runtime_error("Invalid integer in sorted input");
    return result;
}

struct Click { i64 ts; std::uint32_t campaign; };
struct LatestClick { i64 ts; std::string id; std::uint32_t campaign; };
struct UserDaily {
    i64 sessions = 0, events = 0, revenue = 0;
};
struct Session {
    bool active = false;
    i64 events = 0, revenue = 0;
    CampaignDaily* winner = nullptr;
};

i64 day_of(i64 ts) {
    i64 day = ts / 86400000;
    return day - (ts % 86400000 < 0);
}

Channels read_channels(const char* path) {
    Channels channels;
    shopping::read(path, 2, [&](auto& f) { channels.emplace(f[0], f[1]); });
    return channels;
}

CampaignTotals aggregate(const std::function<bool(Row&)>& next, const std::vector<std::string>& names,
                         std::uint32_t organic, const UserSink& emit) {
    UserDaily user;
    i64 user_day = 0;
    CampaignTotals campaigns;
    std::deque<Click> clicks;
    // Dense counts and a packed list let expiry and insertion avoid hash lookups.
    std::vector<size_t> counts(names.size()), positions(names.size());
    std::vector<std::uint32_t> active;
    std::vector<i64> category_revenue(names.size());
    std::vector<bool> category_seen(names.size());
    std::vector<std::uint32_t> purchased_categories;
    std::optional<LatestClick> latest;
    std::string person;
    std::uint32_t person_number = UINT32_MAX;
    bool have_person = false, have_previous = false;
    i64 previous = 0;
    Session session;

    auto finish = [&] {
        if (!session.active) return;
        user.events += session.events;
        user.revenue += session.revenue;
        session.winner->purchasing += session.revenue > 0;
        session.winner->revenue += session.revenue;
        session.active = false;
    };

    auto flush_day = [&] {
        if (!user.sessions) return;
        const std::string* top = nullptr;
        i64 best = 0;
        for (const auto category : purchased_categories) {
            const auto revenue = category_revenue[category];
            category_revenue[category] = 0;
            category_seen[category] = false;
            if (!top || revenue > best || (revenue == best && names[category] < *top)) {
                top = &names[category];
                best = revenue;
            }
        }
        emit(person, user_day, user.sessions, user.events, user.revenue, top);
        user.sessions = user.events = user.revenue = 0;
        purchased_categories.clear();
    };

    Row row;
    while (next(row)) {
        if (row.code < 0 || size_t(row.code) >= names.size()) throw std::runtime_error("Invalid value code");
        const i64 ts = row.ts;
        if (!have_person || (row.person_number == UINT32_MAX ? row.person != person : row.person_number != person_number)) {
            finish();
            flush_day();
            person = row.person;
            person_number = row.person_number;
            have_person = true;
            have_previous = false;
            clicks.clear();
            for (auto campaign : active) counts[campaign] = 0;
            active.clear();
            latest.reset();
        }
        if (row.kind == 1) {
            clicks.push_back({ts, std::uint32_t(row.code)});
            const auto campaign = clicks.back().campaign;
            if (counts[campaign]++ == 0) {
                positions[campaign] = active.size();
                active.push_back(campaign);
            }
            if (!latest || ts > latest->ts || (ts == latest->ts && row.id < std::string_view(latest->id)))
                latest = LatestClick{ts, std::string(row.id), std::uint32_t(row.code)};
            continue;
        }
        if (row.kind != 2) throw std::runtime_error("Invalid sorted row kind");
        if (!have_previous || ts - previous > 1800000) {
            finish();
            const i64 cutoff = ts - 604800000;
            while (!clicks.empty() && clicks.front().ts < cutoff) {
                const auto campaign = clicks.front().campaign;
                if (--counts[campaign] == 0) {
                    const auto last = active.back();
                    positions[last] = positions[campaign];
                    active[positions[campaign]] = last;
                    active.pop_back();
                }
                clicks.pop_front();
            }
            // If the newest click expired, every older click has expired too.
            if (latest && latest->ts < cutoff) latest.reset();
            session = Session{};
            session.active = true;
            const i64 day = day_of(ts);
            if (day != user_day) flush_day();
            user_day = day;
            ++user.sessions;
            session.winner = &campaigns[{latest ? latest->campaign : organic, day}];
            ++session.winner->sessions;
            // Attribute immediately so clicks later in this session cannot assist it.
            for (auto campaign : active) ++campaigns[{campaign, day}].assists;
        }
        previous = ts;
        have_previous = true;
        const i64 revenue = row.revenue;
        ++session.events;
        session.revenue += revenue;
        if (revenue) {
            if (!category_seen[row.code]) {
                category_seen[row.code] = true;
                purchased_categories.push_back(std::uint32_t(row.code));
            }
            category_revenue[row.code] += revenue;
        }
    }
    finish();
    flush_day();

    return campaigns;
}

void aggregate_rows(const std::function<bool(Row&)>& next, const std::vector<std::string>& names,
                    std::uint32_t organic, const Channels& channels, const fs::path& temp) {
    std::ofstream user_out(temp / "user_daily.csv", std::ios::binary);
    std::ofstream campaign_out(temp / "campaign_daily.csv", std::ios::binary);
    if (!user_out || !campaign_out) throw std::runtime_error("Cannot open aggregate output");
    user_out << "user_id,day,sessions,events,revenue_cents,top_category\n";
    auto campaigns = aggregate(next, names, organic, [&](auto person, i64 day, i64 sessions,
                                                         i64 events, i64 revenue, const std::string* top) {
        field(user_out, person);
        user_out << ',' << day << ',' << sessions << ',' << events << ',' << revenue << ',';
        if (top) field(user_out, *top);
        user_out << '\n';
    });
    campaign_out << "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
    for (const auto& [key, campaign] : campaigns) {
        field(campaign_out, names[key.first]); campaign_out << ',';
        field(campaign_out, key.first == organic ? "organic" : channels.at(names[key.first]));
        campaign_out << ',' << key.second << ',' << campaign.sessions << ',' << campaign.purchasing << ',' << campaign.revenue << ',' << campaign.assists << '\n';
    }
    user_out.flush(); campaign_out.flush();
    if (!user_out || !campaign_out) throw std::runtime_error("Aggregate output write failed");
}

class ArrowRows {
    ArrowArrayStream* stream;
    ArrowSchema schema{};
    ArrowArray batch{};
    i64 position = 0;
    void check(int code) const {
        if (code) {
            const char* message = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
            throw std::runtime_error(message ? message : "Arrow stream failed");
        }
    }
    template<class T> T number(size_t column, i64 row) const {
        auto* array = batch.children[column];
        return static_cast<const T*>(array->buffers[1])[array->offset + row];
    }
    std::string_view text(size_t column, i64 row) const {
        auto* array = batch.children[column];
        row += array->offset;
        i64 begin, end;
        if (std::strcmp(schema.children[column]->format, "U") == 0) {
            const auto* offsets = static_cast<const i64*>(array->buffers[1]);
            begin = offsets[row]; end = offsets[row + 1];
        } else {
            const auto* offsets = static_cast<const std::int32_t*>(array->buffers[1]);
            begin = offsets[row]; end = offsets[row + 1];
        }
        if (begin == end) return {};
        return {static_cast<const char*>(array->buffers[2]) + begin, size_t(end - begin)};
    }
public:
    explicit ArrowRows(ArrowArrayStream* input) : stream(input) {}
    ~ArrowRows() {
        if (batch.release) batch.release(&batch);
        if (schema.release) schema.release(&schema);
        if (stream->release) stream->release(stream);
    }
    void initialize() {
        check(stream->get_schema(stream, &schema));
        if (std::strcmp(schema.format, "+s") || schema.n_children != 6)
            throw std::runtime_error("Expected six Arrow columns");
        const char* types[] = {"u", "l", "i", "u", "i", "l"};
        for (size_t i = 0; i < 6; ++i) {
            const auto* child = schema.children[i];
            const bool large_text = (i == 0 || i == 3) && std::strcmp(child->format, "U") == 0;
            if (child->dictionary || (std::strcmp(child->format, types[i]) && !large_text))
                throw std::runtime_error("Unexpected Arrow column type");
        }
    }
    bool next(Row& row) {
        while (!batch.release || position == batch.length) {
            if (batch.release) batch.release(&batch);
            batch = ArrowArray{};
            check(stream->get_next(stream, &batch));
            position = 0;
            if (!batch.release) return false;
            if (batch.n_children != 6 || batch.null_count != 0)
                throw std::runtime_error("Unexpected Arrow batch shape");
            for (int i = 0; i < 6; ++i)
                if (batch.children[i]->null_count != 0) throw std::runtime_error("Unexpected null Arrow value");
        }
        const i64 index = batch.offset + position++;
        row.person = text(0, index);
        row.ts = number<i64>(1, index);
        row.kind = number<std::int32_t>(2, index);
        row.id = text(3, index);
        row.code = number<std::int32_t>(4, index);
        row.revenue = number<i64>(5, index);
        return true;
    }
};

extern "C" int aggregate_arrow(ArrowArrayStream* stream, const char* dictionary_path,
                                const char* campaigns_path, const char* output,
                                char* error, size_t error_size) {
    ArrowRows rows(stream);
    try {
        std::vector<std::string> names;
        std::optional<std::uint32_t> organic;
        shopping::read(dictionary_path, 2, [&](auto& f) {
            if (integer(f[0]) != i64(names.size())) throw std::runtime_error("Invalid value dictionary");
            if (f[1] == "organic") organic = names.size();
            names.emplace_back(f[1]);
        });
        if (!organic) throw std::runtime_error("Missing organic value code");
        rows.initialize();
        aggregate_rows([&](Row& row) { return rows.next(row); }, names, *organic,
                       read_channels(campaigns_path), output);
        return 0;
    } catch (const std::exception& e) {
        if (error_size) std::snprintf(error, error_size, "%s", e.what());
        return 1;
    }
}

#ifndef AGGREGATE_LIBRARY
int main(int argc, char** argv) {
    try {
        if (argc != 4) throw std::runtime_error("Usage: aggregate SORTED_CSV CAMPAIGNS_CSV TEMP");
        std::vector<std::string> names{"organic"};
        std::unordered_map<std::string, std::uint32_t> codes{{"organic", 0}};
        std::ifstream input(argv[1], std::ios::binary);
        if (!input) throw std::runtime_error("Cannot open sorted input");
        std::string line;
        std::vector<std::string_view> fields;
        std::getline(input, line);
        aggregate_rows([&](Row& row) {
            if (!std::getline(input, line)) {
                if (input.bad()) throw std::runtime_error("Sorted input read failed");
                return false;
            }
            if (!shopping::parse(line, fields, 6)) throw std::runtime_error("Malformed sorted input");
            auto [code, added] = codes.emplace(fields[4], names.size());
            if (added) names.emplace_back(fields[4]);
            row = {fields[0], fields[3], integer(fields[1]), integer(fields[5]),
                   std::int32_t(integer(fields[2])), std::int32_t(code->second)};
            return true;
        }, names, 0, read_channels(argv[2]), argv[3]);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "aggregate: " << e.what() << '\n';
        return 1;
    }
}

#endif
