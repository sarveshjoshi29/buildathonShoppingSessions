#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "dsu.hpp"
#include "parquet_writer.hpp"
#include "parser.hpp"

namespace fs = std::filesystem;
using namespace pipeline;

namespace {

constexpr size_t NUM_SHARDS = 256;
constexpr size_t SHARD_MASK = NUM_SHARDS - 1;
constexpr int64_t DAY_MS = 86400000LL;
constexpr int64_t SESSION_GAP_MS = 1800000LL;      // 30 minutes
constexpr int64_t CLICK_LOOKBACK_MS = 604800000LL; // 7 days

inline int64_t floor_div_day(int64_t ms) {
    int64_t q = ms / DAY_MS;
    int64_t r = ms % DAY_MS;
    if (r < 0) --q;
    return q;
}

// Extract string value for a JSON key from manifest.json
std::string extract_json_string(std::string_view json, std::string_view key) {
    std::string pattern = "\"" + std::string(key) + "\"";
    size_t pos = json.find(pattern);
    if (pos == std::string_view::npos) return {};
    pos = json.find(':', pos + pattern.size());
    if (pos == std::string_view::npos) return {};
    size_t q1 = json.find('"', pos + 1);
    if (q1 == std::string_view::npos) return {};
    size_t q2 = json.find('"', q1 + 1);
    if (q2 == std::string_view::npos) return {};
    return std::string(json.substr(q1 + 1, q2 - q1 - 1));
}

// Fast open-addressed string -> uint32_t lookup map (read-only after build)
struct FlatStringMap {
    std::vector<char> arena;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    std::vector<uint32_t> values;
    std::vector<uint32_t> table;
    uint64_t mask = 0;

    void init(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 2) cap <<= 1;
        if (cap < 256) cap = 256;
        table.assign(cap, 0xFFFFFFFFu);
        mask = cap - 1;
        arena.reserve(expected * 10);
        offsets.reserve(expected);
        lengths.reserve(expected);
        values.reserve(expected);
    }

    std::string_view get_key(uint32_t idx) const {
        return std::string_view(arena.data() + offsets[idx], lengths[idx]);
    }

    void rehash() {
        size_t new_cap = table.size() * 2;
        std::vector<uint32_t> new_table(new_cap, 0xFFFFFFFFu);
        uint64_t new_mask = new_cap - 1;
        for (uint32_t idx : table) {
            if (idx != 0xFFFFFFFFu) {
                uint64_t pos = hash_bytes(get_key(idx)) & new_mask;
                while (new_table[pos] != 0xFFFFFFFFu) pos = (pos + 1) & new_mask;
                new_table[pos] = idx;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    void insert(std::string_view key, uint32_t val) {
        if (offsets.size() * 2 >= table.size()) rehash();
        uint64_t pos = hash_bytes(key) & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) {
                uint32_t new_idx = static_cast<uint32_t>(offsets.size());
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), key.begin(), key.end());
                offsets.push_back(off);
                lengths.push_back(static_cast<uint32_t>(key.size()));
                values.push_back(val);
                table[pos] = new_idx;
                return;
            }
            if (lengths[idx] == key.size() &&
                std::memcmp(arena.data() + offsets[idx], key.data(), key.size()) == 0) {
                values[idx] = val;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    uint32_t lookup(std::string_view key, uint32_t default_val) const {
        if (offsets.empty()) return default_val;
        uint64_t pos = hash_bytes(key) & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) return default_val;
            if (lengths[idx] == key.size() &&
                std::memcmp(arena.data() + offsets[idx], key.data(), key.size()) == 0) {
                return values[idx];
            }
            pos = (pos + 1) & mask;
        }
    }
};

struct RawEventRecord {
    uint64_t event_id_hash;
    int64_t ingest_ts;
    int64_t ts;
    uint32_t event_id_off;
    uint32_t user_off;
    int32_t price_cents; // 0 for view/cart, 1..10M for purchase
    uint16_t event_id_len;
    uint16_t user_len;
    uint16_t cat_idx;
    uint8_t is_valid;
};

struct EventShardBuffer {
    std::vector<char> arena;
    std::vector<RawEventRecord> recs;
};

struct SurvivingEventRecord {
    int64_t ts;
    uint32_t user_off;
    uint32_t user_len;
    int32_t price_cents;
    uint16_t cat_idx;
};

struct PersonEventShardBuffer {
    std::vector<char> arena;
    std::vector<SurvivingEventRecord> recs;
};

struct ValidClickRecord {
    int64_t ts;
    uint32_t user_off;
    uint32_t user_len;
    uint32_t click_id_off;
    uint32_t click_id_len;
    uint32_t campaign_idx;
};

struct PersonClickShardBuffer {
    std::vector<char> arena;
    std::vector<ValidClickRecord> recs;
};

struct CampaignAgg {
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

std::vector<std::string> list_csv_files(const fs::path& dir) {
    std::vector<std::string> files;
    if (!fs::exists(dir) || !fs::is_directory(dir)) return files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

} // namespace

int main(int argc, char** argv) {
    std::string data_dir = "/data";
    std::string out_dir = "/output";
    if (const char* env_d = std::getenv("DATA_DIR")) data_dir = env_d;
    if (const char* env_o = std::getenv("OUTPUT_DIR")) out_dir = env_o;
    if (argc >= 3) {
        data_dir = argv[1];
        out_dir = argv[2];
    }

    unsigned int num_threads = std::thread::hardware_concurrency();
    if (const char* env_ncpu = std::getenv("NCPU")) {
        int n = std::atoi(env_ncpu);
        if (n > 0) num_threads = static_cast<unsigned int>(n);
    }
    if (num_threads == 0) num_threads = 4;

    // 1. Parse manifest.json
    std::string manifest_str = read_file_binary((fs::path(data_dir) / "manifest.json").string());
    int64_t window_start = 0, window_end = 0;
    parse_timestamp(extract_json_string(manifest_str, "window_start"), window_start);
    parse_timestamp(extract_json_string(manifest_str, "window_end"), window_end);
    int64_t click_window_start = window_start - CLICK_LOOKBACK_MS;
    int64_t click_window_end = window_end;

    // 2. Parse products.csv -> sort category names byte-wise ascending
    std::vector<std::pair<std::string, std::string>> raw_products;
    std::vector<std::string> category_names;
    category_names.push_back("unknown");
    {
        std::string prod_buf = read_file_binary((fs::path(data_dir) / "products.csv").string());
        for_each_csv_data_line_in_buffer(prod_buf, [&](std::string_view line) {
            std::array<std::string_view, 2> f;
            if (parse_csv_line<2>(line, f) && !f[0].empty() && !f[1].empty()) {
                raw_products.emplace_back(std::string(f[0]), std::string(f[1]));
                category_names.emplace_back(f[1]);
            }
        });
    }
    std::sort(category_names.begin(), category_names.end());
    category_names.erase(std::unique(category_names.begin(), category_names.end()), category_names.end());

    std::unordered_map<std::string, uint16_t> cat_name_to_idx;
    for (size_t i = 0; i < category_names.size(); ++i) {
        cat_name_to_idx[category_names[i]] = static_cast<uint16_t>(i);
    }
    uint16_t unknown_cat_idx = cat_name_to_idx["unknown"];

    FlatStringMap product_map;
    product_map.init(raw_products.size() + 16);
    for (const auto& kv : raw_products) {
        product_map.insert(kv.first, cat_name_to_idx[kv.second]);
    }
    raw_products.clear();

    // 3. Parse campaigns.csv
    std::vector<std::string> campaign_ids;
    std::vector<std::string> campaign_channels;
    FlatStringMap campaign_map;
    {
        std::string camp_buf = read_file_binary((fs::path(data_dir) / "campaigns.csv").string());
        std::vector<std::pair<std::string, std::string>> raw_camps;
        for_each_csv_data_line_in_buffer(camp_buf, [&](std::string_view line) {
            std::array<std::string_view, 2> f;
            if (parse_csv_line<2>(line, f) && !f[0].empty() && !f[1].empty()) {
                raw_camps.emplace_back(std::string(f[0]), std::string(f[1]));
            }
        });
        campaign_map.init(raw_camps.size() + 16);
        campaign_ids.reserve(raw_camps.size() + 1);
        campaign_channels.reserve(raw_camps.size() + 1);
        for (const auto& kv : raw_camps) {
            uint32_t idx = static_cast<uint32_t>(campaign_ids.size());
            campaign_ids.push_back(kv.first);
            campaign_channels.push_back(kv.second);
            campaign_map.insert(kv.first, idx);
        }
    }
    uint32_t organic_campaign_idx = static_cast<uint32_t>(campaign_ids.size());
    campaign_ids.push_back("organic");
    campaign_channels.push_back("organic");
    size_t num_campaigns = campaign_ids.size(); // includes organic at the end

    // 4. Parse links/*.csv and build single-array DSU
    LinkResolver dsu;
    dsu.init(4096);
    {
        auto link_files = list_csv_files(fs::path(data_dir) / "links");
        for (const auto& lpath : link_files) {
            std::string buf = read_file_binary(lpath);
            for_each_csv_data_line_in_buffer(buf, [&](std::string_view line) {
                std::array<std::string_view, 2> f;
                if (parse_csv_line<2>(line, f) && !f[0].empty() && !f[1].empty()) {
                    dsu.add_link(f[0], f[1]);
                }
            });
        }
        dsu.finalize();
    }

    // 5. Phase A: Read events/*.csv in parallel -> partition by hash(event_id) into NUM_SHARDS
    auto event_files = list_csv_files(fs::path(data_dir) / "events");
    unsigned int active_threads = std::min<unsigned int>(num_threads, std::max<size_t>(1, event_files.size()));
    // Thread-local event shard buffers: [thread_id][ev_shard]
    std::vector<std::vector<EventShardBuffer>> thread_ev_shards(active_threads, std::vector<EventShardBuffer>(NUM_SHARDS));
    {
        std::atomic<size_t> next_file{0};
        std::vector<std::thread> workers;
        workers.reserve(active_threads);
        for (unsigned int t = 0; t < active_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_shards = thread_ev_shards[t];
                while (true) {
                    size_t f_idx = next_file.fetch_add(1, std::memory_order_relaxed);
                    if (f_idx >= event_files.size()) break;
                    std::string buf = read_file_binary(event_files[f_idx]);
                    for_each_csv_data_line_in_buffer(buf, [&](std::string_view line) {
                        std::array<std::string_view, 7> f;
                        // Step 1: Parse CSV & drop malformed or empty event_id
                        if (!parse_csv_line<7>(line, f) || f[0].empty()) return;

                        // ingest_ts is always valid F1
                        int64_t ingest_ts = 0;
                        if (!parse_timestamp(f[3], ingest_ts)) return;

                        // Validate fields for Step 3 & Step 4 (stored via is_valid tombstone flag)
                        bool valid = true;
                        int64_t ts = 0;
                        int32_t price = 0;
                        uint16_t cat_idx = 0;
                        std::string_view canon_user;

                        if (f[1].empty() || f[5].empty()) {
                            valid = false;
                        }
                        if (valid) {
                            if (!parse_timestamp(f[2], ts) || ts < window_start || ts >= window_end) {
                                valid = false;
                            } else if (ingest_ts - ts > DAY_MS) {
                                valid = false; // Step 4: late event (> 24h)
                            }
                        }
                        if (valid) {
                            std::string_view etype = f[4];
                            if (etype == "purchase") {
                                if (!parse_price_cents(f[6], price)) {
                                    valid = false;
                                } else {
                                    cat_idx = static_cast<uint16_t>(product_map.lookup(f[5], unknown_cat_idx));
                                }
                            } else if (etype == "view" || etype == "cart") {
                                price = 0;
                            } else {
                                valid = false;
                            }
                        }
                        if (valid) {
                            canon_user = dsu.resolve(f[1]);
                        }

                        uint64_t h = hash_bytes(f[0]);
                        size_t shard_id = static_cast<size_t>(h & SHARD_MASK);
                        auto& sb = my_shards[shard_id];

                        uint32_t ev_off = static_cast<uint32_t>(sb.arena.size());
                        sb.arena.insert(sb.arena.end(), f[0].begin(), f[0].end());
                        uint32_t u_off = 0;
                        if (valid) {
                            u_off = static_cast<uint32_t>(sb.arena.size());
                            sb.arena.insert(sb.arena.end(), canon_user.begin(), canon_user.end());
                        }

                        sb.recs.push_back(RawEventRecord{
                            h,
                            ingest_ts,
                            ts,
                            ev_off,
                            u_off,
                            price,
                            static_cast<uint16_t>(f[0].size()),
                            static_cast<uint16_t>(canon_user.size()),
                            cat_idx,
                            static_cast<uint8_t>(valid ? 1 : 0)
                        });
                    });
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 6. Phase B: Parallel Deduplication of the 256 event_id shards -> route valid survivors to 256 person shards
    std::vector<std::vector<PersonEventShardBuffer>> thread_person_ev_shards(
        num_threads, std::vector<PersonEventShardBuffer>(NUM_SHARDS));
    {
        std::atomic<size_t> next_shard{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_ev_shards[t];
                struct Slot {
                    uint32_t thread_idx;
                    uint32_t rec_idx;
                };
                std::vector<Slot> table;

                while (true) {
                    size_t s_id = next_shard.fetch_add(1, std::memory_order_relaxed);
                    if (s_id >= NUM_SHARDS) break;

                    size_t total_recs = 0;
                    for (unsigned int rt = 0; rt < active_threads; ++rt) {
                        total_recs += thread_ev_shards[rt][s_id].recs.size();
                    }
                    if (total_recs == 0) continue;

                    size_t cap = 1;
                    while (cap < total_recs * 2) cap <<= 1;
                    if (cap < 64) cap = 64;
                    table.assign(cap, Slot{0xFFFFFFFFu, 0xFFFFFFFFu});
                    uint64_t mask = cap - 1;

                    for (unsigned int rt = 0; rt < active_threads; ++rt) {
                        const auto& sb = thread_ev_shards[rt][s_id];
                        const char* arena_ptr = sb.arena.data();
                        for (uint32_t ri = 0; ri < sb.recs.size(); ++ri) {
                            const auto& rec = sb.recs[ri];
                            std::string_view ev_id(arena_ptr + rec.event_id_off, rec.event_id_len);
                            uint64_t pos = (rec.event_id_hash >> 8) & mask;
                            while (true) {
                                Slot& sl = table[pos];
                                if (sl.thread_idx == 0xFFFFFFFFu) {
                                    sl = Slot{rt, ri};
                                    break;
                                }
                                const auto& ex_sb = thread_ev_shards[sl.thread_idx][s_id];
                                const auto& ex_rec = ex_sb.recs[sl.rec_idx];
                                if (ex_rec.event_id_hash == rec.event_id_hash &&
                                    ex_rec.event_id_len == rec.event_id_len &&
                                    std::memcmp(ex_sb.arena.data() + ex_rec.event_id_off, ev_id.data(), ev_id.size()) == 0) {
                                    // Keep copy with smallest ingest_ts
                                    if (rec.ingest_ts < ex_rec.ingest_ts) {
                                        sl = Slot{rt, ri};
                                    }
                                    break;
                                }
                                pos = (pos + 1) & mask;
                            }
                        }
                    }

                    // Emit valid winners into person shards
                    for (const auto& sl : table) {
                        if (sl.thread_idx == 0xFFFFFFFFu) continue;
                        const auto& sb = thread_ev_shards[sl.thread_idx][s_id];
                        const auto& rec = sb.recs[sl.rec_idx];
                        if (!rec.is_valid) continue; // Step 2 -> Step 3: kept copy was invalid, so event is dropped!

                        std::string_view canon_user(sb.arena.data() + rec.user_off, rec.user_len);
                        size_t p_shard = static_cast<size_t>(hash_bytes(canon_user) & SHARD_MASK);
                        auto& dest = my_out[p_shard];
                        uint32_t u_off = static_cast<uint32_t>(dest.arena.size());
                        dest.arena.insert(dest.arena.end(), canon_user.begin(), canon_user.end());
                        dest.recs.push_back(SurvivingEventRecord{
                            rec.ts,
                            u_off,
                            rec.user_len,
                            rec.price_cents,
                            rec.cat_idx
                        });
                    }

                    // Free raw event shard memory immediately
                    for (unsigned int rt = 0; rt < active_threads; ++rt) {
                        std::vector<char>().swap(thread_ev_shards[rt][s_id].arena);
                        std::vector<RawEventRecord>().swap(thread_ev_shards[rt][s_id].recs);
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }
    thread_ev_shards.clear();

    // 7. Phase C: Read clicks/*.csv in parallel -> validate & route to 256 person shards
    auto click_files = list_csv_files(fs::path(data_dir) / "clicks");
    unsigned int click_threads = std::min<unsigned int>(num_threads, std::max<size_t>(1, click_files.size()));
    std::vector<std::vector<PersonClickShardBuffer>> thread_person_click_shards(
        click_threads, std::vector<PersonClickShardBuffer>(NUM_SHARDS));
    {
        std::atomic<size_t> next_file{0};
        std::vector<std::thread> workers;
        workers.reserve(click_threads);
        for (unsigned int t = 0; t < click_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_click_shards[t];
                while (true) {
                    size_t f_idx = next_file.fetch_add(1, std::memory_order_relaxed);
                    if (f_idx >= click_files.size()) break;
                    std::string buf = read_file_binary(click_files[f_idx]);
                    for_each_csv_data_line_in_buffer(buf, [&](std::string_view line) {
                        std::array<std::string_view, 4> f;
                        // click_id, user_id, campaign_id, ts
                        if (!parse_csv_line<4>(line, f) || f[0].empty() || f[1].empty() || f[2].empty()) return;

                        uint32_t camp_idx = campaign_map.lookup(f[2], 0xFFFFFFFFu);
                        if (camp_idx == 0xFFFFFFFFu) return;

                        int64_t ts = 0;
                        if (!parse_timestamp(f[3], ts) || ts < click_window_start || ts >= click_window_end) return;

                        std::string_view canon_user = dsu.resolve(f[1]);
                        size_t p_shard = static_cast<size_t>(hash_bytes(canon_user) & SHARD_MASK);
                        auto& dest = my_out[p_shard];

                        uint32_t u_off = static_cast<uint32_t>(dest.arena.size());
                        dest.arena.insert(dest.arena.end(), canon_user.begin(), canon_user.end());
                        uint32_t c_off = static_cast<uint32_t>(dest.arena.size());
                        dest.arena.insert(dest.arena.end(), f[0].begin(), f[0].end());

                        dest.recs.push_back(ValidClickRecord{
                            ts,
                            u_off,
                            static_cast<uint32_t>(canon_user.size()),
                            c_off,
                            static_cast<uint32_t>(f[0].size()),
                            camp_idx
                        });
                    });
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 8. Phase D: Parallel Sessionization, Click Attribution & Daily Aggregation across 256 person shards
    struct ThreadOutput {
        std::vector<char> str_arena;
        struct LocalUserRow {
            uint32_t user_off;
            uint32_t user_len;
            int32_t day;
            int64_t sessions;
            int64_t events;
            int64_t revenue_cents;
            uint16_t top_cat_idx;
            bool has_top_cat;
        };
        std::vector<LocalUserRow> user_rows;
        std::unordered_map<uint64_t, CampaignAgg> camp_daily; // key: (uint64_t(camp_idx) << 32) | uint32_t(day)
    };
    std::vector<ThreadOutput> thread_outputs(num_threads);

    {
        std::atomic<size_t> next_p_shard{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        size_t num_cats = category_names.size();

        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t, num_cats]() {
                auto& out = thread_outputs[t];
                std::vector<int64_t> day_cat_rev(num_cats, 0);
                std::vector<int32_t> active_camp_count(num_campaigns, 0);
                std::vector<uint32_t> active_camp_pos(num_campaigns, 0);
                std::vector<uint32_t> active_camps;
                active_camps.reserve(num_campaigns);

                struct CompactEvent {
                    int64_t ts;
                    int32_t price_cents;
                    uint16_t cat_idx;
                };
                struct CompactClick {
                    int64_t ts;
                    uint32_t thread_idx;
                    uint32_t click_id_off;
                    uint32_t click_id_len;
                    uint32_t campaign_idx;
                };

                while (true) {
                    size_t p_shard = next_p_shard.fetch_add(1, std::memory_order_relaxed);
                    if (p_shard >= NUM_SHARDS) break;

                    size_t total_evs = 0;
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        total_evs += thread_person_ev_shards[rt][p_shard].recs.size();
                    }
                    if (total_evs == 0) continue;

                    // Intern distinct persons in this shard that have >= 1 event
                    FlatStringMap person_map;
                    person_map.init(total_evs / 4 + 16);
                    std::vector<uint32_t> ev_person_ids;
                    ev_person_ids.reserve(total_evs);
                    std::vector<uint32_t> person_ev_counts;

                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& sb = thread_person_ev_shards[rt][p_shard];
                        for (const auto& rec : sb.recs) {
                            std::string_view u(sb.arena.data() + rec.user_off, rec.user_len);
                            uint32_t pid = person_map.lookup(u, 0xFFFFFFFFu);
                            if (pid == 0xFFFFFFFFu) {
                                pid = static_cast<uint32_t>(person_ev_counts.size());
                                person_map.insert(u, pid);
                                person_ev_counts.push_back(0);
                            }
                            ev_person_ids.push_back(pid);
                            person_ev_counts[pid]++;
                        }
                    }

                    size_t num_persons = person_ev_counts.size();
                    std::vector<uint32_t> person_ev_offsets(num_persons + 1, 0);
                    for (size_t p = 0; p < num_persons; ++p) {
                        person_ev_offsets[p + 1] = person_ev_offsets[p] + person_ev_counts[p];
                    }
                    std::vector<uint32_t> write_pos = person_ev_offsets;
                    std::vector<CompactEvent> csr_events(total_evs);

                    size_t global_ei = 0;
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& sb = thread_person_ev_shards[rt][p_shard];
                        for (const auto& rec : sb.recs) {
                            uint32_t pid = ev_person_ids[global_ei++];
                            csr_events[write_pos[pid]++] = CompactEvent{rec.ts, rec.price_cents, rec.cat_idx};
                        }
                        // Free event buffer for this shard
                        std::vector<char>().swap(thread_person_ev_shards[rt][p_shard].arena);
                        std::vector<SurvivingEventRecord>().swap(thread_person_ev_shards[rt][p_shard].recs);
                    }
                    ev_person_ids.clear();

                    // Group clicks for active persons in this shard into CSR array
                    std::vector<uint32_t> person_clk_counts(num_persons, 0);
                    struct MatchedClickRef {
                        uint32_t pid;
                        uint32_t rt;
                        uint32_t ri;
                    };
                    std::vector<MatchedClickRef> matched_clicks;
                    for (unsigned int rt = 0; rt < click_threads; ++rt) {
                        const auto& sb = thread_person_click_shards[rt][p_shard];
                        for (uint32_t ri = 0; ri < sb.recs.size(); ++ri) {
                            const auto& rec = sb.recs[ri];
                            std::string_view u(sb.arena.data() + rec.user_off, rec.user_len);
                            uint32_t pid = person_map.lookup(u, 0xFFFFFFFFu);
                            if (pid != 0xFFFFFFFFu) {
                                person_clk_counts[pid]++;
                                matched_clicks.push_back(MatchedClickRef{pid, rt, ri});
                            }
                        }
                    }

                    std::vector<uint32_t> person_clk_offsets(num_persons + 1, 0);
                    for (size_t p = 0; p < num_persons; ++p) {
                        person_clk_offsets[p + 1] = person_clk_offsets[p] + person_clk_counts[p];
                    }
                    std::vector<uint32_t> clk_write_pos = person_clk_offsets;
                    std::vector<CompactClick> csr_clicks(matched_clicks.size());
                    for (const auto& mc : matched_clicks) {
                        const auto& rec = thread_person_click_shards[mc.rt][p_shard].recs[mc.ri];
                        csr_clicks[clk_write_pos[mc.pid]++] = CompactClick{
                            rec.ts, mc.rt, rec.click_id_off, rec.click_id_len, rec.campaign_idx
                        };
                    }
                    matched_clicks.clear();

                    auto get_click_id = [&](const CompactClick& c) -> std::string_view {
                        return std::string_view(
                            thread_person_click_shards[c.thread_idx][p_shard].arena.data() + c.click_id_off,
                            c.click_id_len
                        );
                    };

                    // Process each person in this shard
                    for (size_t p = 0; p < num_persons; ++p) {
                        auto* ev_begin = csr_events.data() + person_ev_offsets[p];
                        auto* ev_end = csr_events.data() + person_ev_offsets[p + 1];
                        size_t n_ev = static_cast<size_t>(ev_end - ev_begin);
                        std::sort(ev_begin, ev_end, [](const CompactEvent& a, const CompactEvent& b) {
                            return a.ts < b.ts;
                        });

                        auto* clk_begin = csr_clicks.data() + person_clk_offsets[p];
                        auto* clk_end = csr_clicks.data() + person_clk_offsets[p + 1];
                        if (clk_begin != clk_end) {
                            // Deduplicate on click_id, then sort by (ts asc, click_id desc)
                            std::sort(clk_begin, clk_end, [&](const CompactClick& a, const CompactClick& b) {
                                return get_click_id(a) < get_click_id(b);
                            });
                            clk_end = std::unique(clk_begin, clk_end, [&](const CompactClick& a, const CompactClick& b) {
                                return get_click_id(a) == get_click_id(b);
                            });
                            std::sort(clk_begin, clk_end, [&](const CompactClick& a, const CompactClick& b) {
                                if (a.ts != b.ts) return a.ts < b.ts;
                                return get_click_id(a) > get_click_id(b);
                            });
                        }
                        size_t n_clk = static_cast<size_t>(clk_end - clk_begin);
                        size_t clk_L = 0, clk_R = 0;

                        std::string_view user_sv = person_map.get_key(static_cast<uint32_t>(p));
                        uint32_t u_out_off = static_cast<uint32_t>(out.str_arena.size());
                        uint32_t u_out_len = static_cast<uint32_t>(user_sv.size());
                        out.str_arena.insert(out.str_arena.end(), user_sv.begin(), user_sv.end());

                        int32_t cur_day = std::numeric_limits<int32_t>::min();
                        int64_t day_sessions = 0;
                        int64_t day_events = 0;
                        int64_t day_revenue = 0;

                        auto flush_user_day = [&]() {
                            if (day_sessions == 0) return;
                            uint16_t top_cat = 0;
                            bool has_top = false;
                            if (day_revenue > 0) {
                                int64_t best_rev = 0;
                                for (size_t c = 0; c < num_cats; ++c) {
                                    if (day_cat_rev[c] > best_rev) {
                                        best_rev = day_cat_rev[c];
                                        top_cat = static_cast<uint16_t>(c);
                                    }
                                    day_cat_rev[c] = 0;
                                }
                                has_top = true;
                            }
                            out.user_rows.push_back(ThreadOutput::LocalUserRow{
                                u_out_off, u_out_len, cur_day, day_sessions, day_events, day_revenue, top_cat, has_top
                            });
                        };

                        size_t i = 0;
                        while (i < n_ev) {
                            int64_t session_start = ev_begin[i].ts;
                            int32_t s_day = static_cast<int32_t>(floor_div_day(session_start));
                            if (s_day != cur_day) {
                                flush_user_day();
                                cur_day = s_day;
                                day_sessions = 0;
                                day_events = 0;
                                day_revenue = 0;
                            }

                            int64_t s_events = 0;
                            int64_t s_rev = 0;
                            size_t j = i;
                            while (j < n_ev && (j == i || ev_begin[j].ts - ev_begin[j - 1].ts <= SESSION_GAP_MS)) {
                                s_events++;
                                if (ev_begin[j].price_cents > 0) {
                                    s_rev += ev_begin[j].price_cents;
                                    day_cat_rev[ev_begin[j].cat_idx] += ev_begin[j].price_cents;
                                }
                                j++;
                            }
                            i = j;

                            day_sessions += 1;
                            day_events += s_events;
                            day_revenue += s_rev;
                            bool s_has_purchase = (s_rev > 0);

                            // Advance sliding window [clk_L, clk_R) for [session_start - 7 days, session_start]
                            while (clk_R < n_clk && clk_begin[clk_R].ts <= session_start) {
                                uint32_t c = clk_begin[clk_R].campaign_idx;
                                if (active_camp_count[c]++ == 0) {
                                    active_camp_pos[c] = static_cast<uint32_t>(active_camps.size());
                                    active_camps.push_back(c);
                                }
                                clk_R++;
                            }
                            int64_t min_click_ts = session_start - CLICK_LOOKBACK_MS;
                            while (clk_L < clk_R && clk_begin[clk_L].ts < min_click_ts) {
                                uint32_t c = clk_begin[clk_L].campaign_idx;
                                if (--active_camp_count[c] == 0) {
                                    uint32_t pos = active_camp_pos[c];
                                    uint32_t last_c = active_camps.back();
                                    active_camps[pos] = last_c;
                                    active_camp_pos[last_c] = pos;
                                    active_camps.pop_back();
                                }
                                clk_L++;
                            }

                            uint32_t credited_camp = (clk_L < clk_R) ? clk_begin[clk_R - 1].campaign_idx : organic_campaign_idx;
                            uint64_t cred_key = (static_cast<uint64_t>(credited_camp) << 32) | static_cast<uint32_t>(s_day);
                            auto& cred_agg = out.camp_daily[cred_key];
                            cred_agg.sessions += 1;
                            if (s_has_purchase) cred_agg.purchasing_sessions += 1;
                            cred_agg.revenue_cents += s_rev;

                            for (uint32_t ac : active_camps) {
                                uint64_t ass_key = (static_cast<uint64_t>(ac) << 32) | static_cast<uint32_t>(s_day);
                                out.camp_daily[ass_key].assisted_sessions += 1;
                            }
                        }

                        flush_user_day();
                        for (uint32_t ac : active_camps) {
                            active_camp_count[ac] = 0;
                        }
                        active_camps.clear();
                    }

                    // Free click buffer for this shard
                    for (unsigned int rt = 0; rt < click_threads; ++rt) {
                        std::vector<char>().swap(thread_person_click_shards[rt][p_shard].arena);
                        std::vector<ValidClickRecord>().swap(thread_person_click_shards[rt][p_shard].recs);
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 9. Assemble final outputs and write Parquet tables
    fs::create_directories(fs::path(out_dir) / "user_daily");
    fs::create_directories(fs::path(out_dir) / "campaign_daily");

    size_t total_user_rows = 0;
    for (const auto& to : thread_outputs) total_user_rows += to.user_rows.size();

    std::vector<parquet::UserDailyRow> final_user_rows;
    final_user_rows.reserve(total_user_rows);
    for (const auto& to : thread_outputs) {
        const char* base = to.str_arena.data();
        for (const auto& r : to.user_rows) {
            final_user_rows.push_back(parquet::UserDailyRow{
                std::string_view(base + r.user_off, r.user_len),
                r.day,
                r.sessions,
                r.events,
                r.revenue_cents,
                r.has_top_cat ? std::string_view(category_names[r.top_cat_idx]) : std::string_view(),
                r.has_top_cat
            });
        }
    }

    std::unordered_map<uint64_t, CampaignAgg> merged_camp;
    for (const auto& to : thread_outputs) {
        for (const auto& kv : to.camp_daily) {
            auto& dst = merged_camp[kv.first];
            dst.sessions += kv.second.sessions;
            dst.purchasing_sessions += kv.second.purchasing_sessions;
            dst.revenue_cents += kv.second.revenue_cents;
            dst.assisted_sessions += kv.second.assisted_sessions;
        }
    }

    std::vector<parquet::CampaignDailyRow> final_camp_rows;
    final_camp_rows.reserve(merged_camp.size());
    for (const auto& kv : merged_camp) {
        uint32_t camp_idx = static_cast<uint32_t>(kv.first >> 32);
        int32_t day = static_cast<int32_t>(static_cast<uint32_t>(kv.first & 0xFFFFFFFFULL));
        const auto& agg = kv.second;
        if (agg.sessions > 0 || agg.assisted_sessions > 0) {
            final_camp_rows.push_back(parquet::CampaignDailyRow{
                campaign_ids[camp_idx],
                campaign_channels[camp_idx],
                day,
                agg.sessions,
                agg.purchasing_sessions,
                agg.revenue_cents,
                agg.assisted_sessions
            });
        }
    }

    parquet::write_user_daily_parquet((fs::path(out_dir) / "user_daily" / "part-0.parquet").string(), final_user_rows);
    parquet::write_campaign_daily_parquet((fs::path(out_dir) / "campaign_daily" / "part-0.parquet").string(), final_camp_rows);

    return 0;
}
