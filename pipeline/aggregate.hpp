#pragma once
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
using i64 = std::int64_t;
struct Row {
    std::string_view person, id;
    i64 ts = 0, revenue = 0;
    std::int32_t kind = 0, code = 0;
    std::uint32_t person_number = UINT32_MAX; // External row readers supply names only.
};

using Channels = std::unordered_map<std::string, std::string>;
Channels read_channels(const char* path);
void aggregate_rows(const std::function<bool(Row&)>& next, const std::vector<std::string>& names,
                    std::uint32_t organic, const Channels& channels, const std::filesystem::path& output);

struct CampaignDaily { i64 sessions = 0, purchasing = 0, revenue = 0, assists = 0; };
using CampaignTotals = std::map<std::pair<std::uint32_t, i64>, CampaignDaily>;
using UserSink = std::function<void(std::string_view, i64, i64, i64, i64, const std::string*)>;
CampaignTotals aggregate(const std::function<bool(Row&)>& next, const std::vector<std::string>& names,
                         std::uint32_t organic, const UserSink& emit);
