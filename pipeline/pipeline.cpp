// ============================================================================
// TUNABLE PARAMETERS 
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace {
constexpr size_t TUNABLE_ZIPF_CHUNK_BYTES = 1 * 1024 * 1024; // 1 MB \n-aligned chunks for Zipf file load balancing
constexpr size_t TUNABLE_NUM_SHARDS = 256;                   // Must be a power of 2
constexpr unsigned int TUNABLE_DEFAULT_THREADS = 8;
constexpr int64_t TUNABLE_SESSION_GAP_MS = 1800000LL;        // 30 minutes
constexpr int64_t TUNABLE_CLICK_LOOKBACK_MS = 604800000LL;   // 7 days
constexpr size_t TUNABLE_AVG_EVENT_ROW_BYTES = 68;
constexpr size_t TUNABLE_AVG_CLICK_ROW_BYTES = 42;
constexpr size_t TUNABLE_MIN_HASH_CAP = 64;
} // namespace
// ============================================================================

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
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

constexpr size_t SHARD_MASK = TUNABLE_NUM_SHARDS - 1;

inline int64_t floor_div_day(int64_t ms) {
    int64_t q = ms / TUNABLE_DAY_MS;
    int64_t r = ms % TUNABLE_DAY_MS;
    if (r < 0) --q;
    return q;
}

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

struct FlatStringMap {
    std::vector<char> arena;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    std::vector<uint64_t> hashes;
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
        hashes.reserve(expected);
        values.reserve(expected);
    }

    void rehash() {
        size_t new_cap = table.size() * 2;
        std::vector<uint32_t> new_table(new_cap, 0xFFFFFFFFu);
        uint64_t new_mask = new_cap - 1;
        for (uint32_t idx : table) {
            if (idx != 0xFFFFFFFFu) {
                uint64_t pos = hashes[idx] & new_mask;
                while (new_table[pos] != 0xFFFFFFFFu) pos = (pos + 1) & new_mask;
                new_table[pos] = idx;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    void insert(std::string_view key, uint32_t val) {
        if (offsets.size() * 2 >= table.size()) rehash();
        uint64_t h = hash_bytes(key);
        uint64_t pos = h & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) {
                uint32_t new_idx = static_cast<uint32_t>(offsets.size());
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), key.begin(), key.end());
                offsets.push_back(off);
                lengths.push_back(static_cast<uint32_t>(key.size()));
                hashes.push_back(h);
                values.push_back(val);
                table[pos] = new_idx;
                return;
            }
            if (hashes[idx] == h && lengths[idx] == key.size() &&
                std::memcmp(arena.data() + offsets[idx], key.data(), key.size()) == 0) {
                values[idx] = val;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    uint32_t lookup(std::string_view key, uint32_t default_val) const {
        if (offsets.empty()) return default_val;
        uint64_t h = hash_bytes(key);
        uint64_t pos = h & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) return default_val;
            if (hashes[idx] == h && lengths[idx] == key.size() &&
                std::memcmp(arena.data() + offsets[idx], key.data(), key.size()) == 0) {
                return values[idx];
            }
            pos = (pos + 1) & mask;
        }
    }
};

struct FlatViewMap {
    struct Entry {
        const char* ptr;
        uint32_t len;
        uint64_t hash;
    };
    std::vector<Entry> entries;
    std::vector<uint32_t> table;
    uint64_t mask = 0;

    void reset(size_t expected) {
        entries.clear();
        size_t cap = TUNABLE_MIN_HASH_CAP;
        while (cap < expected * 2) cap <<= 1;
        table.assign(cap, 0xFFFFFFFFu);
        mask = cap - 1;
    }

    void rehash() {
        size_t new_cap = table.size() * 2;
        std::vector<uint32_t> new_table(new_cap, 0xFFFFFFFFu);
        uint64_t new_mask = new_cap - 1;
        for (uint32_t idx : table) {
            if (idx != 0xFFFFFFFFu) {
                uint64_t pos = (entries[idx].hash >> 8) & new_mask;
                while (new_table[pos] != 0xFFFFFFFFu) pos = (pos + 1) & new_mask;
                new_table[pos] = idx;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    uint32_t find_or_insert(const char* ptr, uint32_t len, uint64_t h, bool& inserted) {
        if (entries.size() * 2 >= table.size()) rehash();
        uint64_t pos = (h >> 8) & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) {
                uint32_t new_idx = static_cast<uint32_t>(entries.size());
                entries.push_back(Entry{ptr, len, h});
                table[pos] = new_idx;
                inserted = true;
                return new_idx;
            }
            const auto& e = entries[idx];
            if (e.hash == h && e.len == len && (e.ptr == ptr || std::memcmp(e.ptr, ptr, len) == 0)) {
                inserted = false;
                return idx;
            }
            pos = (pos + 1) & mask;
        }
    }

    uint32_t lookup(const char* ptr, uint32_t len, uint64_t h) const {
        if (entries.empty()) return 0xFFFFFFFFu;
        uint64_t pos = (h >> 8) & mask;
        while (true) {
            uint32_t idx = table[pos];
            if (idx == 0xFFFFFFFFu) return 0xFFFFFFFFu;
            const auto& e = entries[idx];
            if (e.hash == h && e.len == len && (e.ptr == ptr || std::memcmp(e.ptr, ptr, len) == 0)) {
                return idx;
            }
            pos = (pos + 1) & mask;
        }
    }
};

struct RawEventRecord {
    uint64_t event_id_hash;
    uint64_t user_hash;
    int64_t ingest_ts;
    int64_t ts;
    const char* event_id_ptr;
    const char* user_ptr;
    int32_t price_cents;
    uint16_t event_id_len;
    uint16_t user_len;
    uint16_t cat_idx;
    uint8_t is_valid;
};

struct SurvivingEventRecord {
    uint64_t user_hash;
    int64_t ts;
    const char* user_ptr;
    int32_t price_cents;
    uint16_t user_len;
    uint16_t cat_idx;
};

struct ValidClickRecord {
    uint64_t user_hash;
    int64_t ts;
    const char* user_ptr;
    const char* click_id_ptr;
    uint32_t campaign_idx;
    uint16_t user_len;
    uint16_t click_id_len;
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

std::vector<MappedFile> open_mapped_files_parallel(const std::vector<std::string>& paths, unsigned int num_threads) {
    std::vector<MappedFile> files(paths.size());
    if (paths.empty()) return files;
    unsigned int nth = std::min<unsigned int>(num_threads, static_cast<unsigned int>(paths.size()));
    std::atomic<size_t> next_idx{0};
    std::vector<std::thread> workers;
    workers.reserve(nth);
    for (unsigned int t = 0; t < nth; ++t) {
        workers.emplace_back([&]() {
            while (true) {
                size_t i = next_idx.fetch_add(1, std::memory_order_relaxed);
                if (i >= paths.size()) break;
                files[i].open(paths[i]);
            }
        });
    }
    for (auto& w : workers) w.join();
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
    if (num_threads == 0) num_threads = TUNABLE_DEFAULT_THREADS;

    // 1. Parse manifest.json
    std::string manifest_str = read_file_binary((fs::path(data_dir) / "manifest.json").string());
    int64_t window_start = 0, window_end = 0;
    parse_timestamp(extract_json_string(manifest_str, "window_start"), window_start);
    parse_timestamp(extract_json_string(manifest_str, "window_end"), window_end);
    int64_t click_window_start = window_start - TUNABLE_CLICK_LOOKBACK_MS;
    int64_t click_window_end = window_end;

    int32_t base_day = static_cast<int32_t>(floor_div_day(window_start)) - 1;
    int32_t max_day = static_cast<int32_t>(floor_div_day(window_end)) + 1;
    size_t num_days = static_cast<size_t>(std::max<int32_t>(1, max_day - base_day + 1));

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
    size_t num_campaigns = campaign_ids.size();

    // 4. Stage 0b: Parallel Single-Pass Links Parsing + Pre-sized Single-Array DSU with L1 Bloom Filter
    LinkResolver dsu;
    {
        auto link_paths = list_csv_files(fs::path(data_dir) / "links");
        auto link_files = open_mapped_files_parallel(link_paths, num_threads);
        auto link_chunks = build_csv_chunks(link_files, TUNABLE_ZIPF_CHUNK_BYTES);

        struct ParsedLink {
            std::string_view a;
            std::string_view b;
            uint64_t ha;
            uint64_t hb;
        };
        unsigned int l_threads = std::min<unsigned int>(num_threads, std::max<size_t>(1, link_chunks.size()));
        std::vector<std::vector<ParsedLink>> thread_links(l_threads);
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(l_threads);
        for (unsigned int t = 0; t < l_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& out_l = thread_links[t];
                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= link_chunks.size()) break;
                    for_each_csv_record_in_slice<2>(link_chunks[c_idx], [&](const std::array<std::string_view, 2>& f) {
                        if (!f[0].empty() && !f[1].empty()) {
                            out_l.push_back(ParsedLink{f[0], f[1], hash_bytes(f[0]), hash_bytes(f[1])});
                        }
                    });
                }
            });
        }
        for (auto& w : workers) w.join();

        size_t total_links = 0;
        for (const auto& tl : thread_links) total_links += tl.size();
        dsu.init(total_links * 2 + 16);
        for (const auto& tl : thread_links) {
            for (const auto& pl : tl) {
                dsu.add_link_prehashed(pl.a, pl.ha, pl.b, pl.hb);
            }
        }
        dsu.finalize();
    }

    // 5. Stage 1: Single-Pass CSV + L1 Calendar Timestamp Parse (DSU deferred to Stage 2)
    auto event_paths = list_csv_files(fs::path(data_dir) / "events");
    auto event_files = open_mapped_files_parallel(event_paths, num_threads);
    auto event_chunks = build_csv_chunks(event_files, TUNABLE_ZIPF_CHUNK_BYTES);

    size_t total_event_bytes = 0;
    for (const auto& mf : event_files) total_event_bytes += mf.data_size;
    size_t est_recs_per_thread_shard =
        (total_event_bytes / TUNABLE_AVG_EVENT_ROW_BYTES) / (num_threads * TUNABLE_NUM_SHARDS) + 16;

    std::vector<std::vector<std::vector<RawEventRecord>>> thread_ev_shards(
        num_threads, std::vector<std::vector<RawEventRecord>>(TUNABLE_NUM_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_shards = thread_ev_shards[t];
                for (auto& s : my_shards) s.reserve(est_recs_per_thread_shard);

                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= event_chunks.size()) break;
                    for_each_csv_record_in_slice<7>(event_chunks[c_idx], [&](const std::array<std::string_view, 7>& f) {
                        if (f[0].empty()) return;

                        int64_t ingest_ts = 0;
                        if (!parse_timestamp_f1(f[3], ingest_ts)) return;

                        bool valid = !f[1].empty() && !f[5].empty();
                        int64_t ts = 0;
                        int32_t price = 0;
                        uint16_t cat_idx = 0;

                        if (valid) {
                            if (!parse_timestamp(f[2], ts) || ts < window_start || ts >= window_end || (ingest_ts - ts > TUNABLE_DAY_MS)) {
                                valid = false;
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

                        std::string_view canon_user;
                        uint64_t user_hash = 0;
                        if (valid) {
                            canon_user = dsu.resolve_with_hash(f[1], user_hash);
                        }

                        uint64_t ev_hash = hash_bytes(f[0]);
                        size_t shard_id = static_cast<size_t>(ev_hash & SHARD_MASK);
                        my_shards[shard_id].push_back(RawEventRecord{
                            ev_hash,
                            user_hash,
                            ingest_ts,
                            ts,
                            f[0].data(),
                            canon_user.data(),
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

    // 6. Stage 2: Parallel Deduplication -> Route surviving winners via precomputed user_hash (zero random DRAM deref)
    std::vector<std::vector<std::vector<SurvivingEventRecord>>> thread_person_ev_shards(
        num_threads, std::vector<std::vector<SurvivingEventRecord>>(TUNABLE_NUM_SHARDS));
    {
        std::atomic<size_t> next_shard{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_ev_shards[t];
                for (auto& s : my_out) s.reserve(est_recs_per_thread_shard);

                struct Slot {
                    uint32_t thread_idx;
                    uint32_t rec_idx;
                };
                std::vector<Slot> table;

                while (true) {
                    size_t s_id = next_shard.fetch_add(1, std::memory_order_relaxed);
                    if (s_id >= TUNABLE_NUM_SHARDS) break;

                    size_t total_recs = 0;
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        total_recs += thread_ev_shards[rt][s_id].size();
                    }
                    if (total_recs == 0) continue;

                    size_t cap = TUNABLE_MIN_HASH_CAP;
                    while (cap < total_recs * 2) cap <<= 1;
                    table.assign(cap, Slot{0xFFFFFFFFu, 0xFFFFFFFFu});
                    uint64_t mask = cap - 1;

                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_ev_shards[rt][s_id];
                        for (uint32_t ri = 0; ri < recs.size(); ++ri) {
                            const auto& rec = recs[ri];
                            uint64_t pos = (rec.event_id_hash >> 8) & mask;
                            while (true) {
                                Slot& sl = table[pos];
                                if (sl.thread_idx == 0xFFFFFFFFu) {
                                    sl = Slot{rt, ri};
                                    break;
                                }
                                const auto& ex_rec = thread_ev_shards[sl.thread_idx][s_id][sl.rec_idx];
                                if (ex_rec.event_id_hash == rec.event_id_hash &&
                                    ex_rec.event_id_len == rec.event_id_len &&
                                    std::memcmp(ex_rec.event_id_ptr, rec.event_id_ptr, rec.event_id_len) == 0) {
                                    if (rec.ingest_ts < ex_rec.ingest_ts) {
                                        sl = Slot{rt, ri};
                                    }
                                    break;
                                }
                                pos = (pos + 1) & mask;
                            }
                        }
                    }

                    for (const auto& sl : table) {
                        if (sl.thread_idx == 0xFFFFFFFFu) continue;
                        const auto& rec = thread_ev_shards[sl.thread_idx][s_id][sl.rec_idx];
                        if (!rec.is_valid) continue;

                        size_t p_shard = static_cast<size_t>(rec.user_hash & SHARD_MASK);
                        my_out[p_shard].push_back(SurvivingEventRecord{
                            rec.user_hash,
                            rec.ts,
                            rec.user_ptr,
                            rec.price_cents,
                            rec.user_len,
                            rec.cat_idx
                        });
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 7. Stage 3: Single-Pass Clicks Parsing -> 256 person shards
    auto click_paths = list_csv_files(fs::path(data_dir) / "clicks");
    auto click_files = open_mapped_files_parallel(click_paths, num_threads);
    auto click_chunks = build_csv_chunks(click_files, TUNABLE_ZIPF_CHUNK_BYTES);

    size_t total_click_bytes = 0;
    for (const auto& mf : click_files) total_click_bytes += mf.data_size;
    size_t est_clicks_per_thread_shard =
        (total_click_bytes / TUNABLE_AVG_CLICK_ROW_BYTES) / (num_threads * TUNABLE_NUM_SHARDS) + 16;

    std::vector<std::vector<std::vector<ValidClickRecord>>> thread_person_click_shards(
        num_threads, std::vector<std::vector<ValidClickRecord>>(TUNABLE_NUM_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_click_shards[t];
                for (auto& s : my_out) s.reserve(est_clicks_per_thread_shard);

                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= click_chunks.size()) break;
                    for_each_csv_record_in_slice<4>(click_chunks[c_idx], [&](const std::array<std::string_view, 4>& f) {
                        if (f[0].empty() || f[1].empty() || f[2].empty()) return;

                        uint32_t camp_idx = campaign_map.lookup(f[2], 0xFFFFFFFFu);
                        if (camp_idx == 0xFFFFFFFFu) return;

                        int64_t ts = 0;
                        if (!parse_timestamp(f[3], ts) || ts < click_window_start || ts >= click_window_end) return;

                        uint64_t user_hash = 0;
                        std::string_view canon_user = dsu.resolve_with_hash(f[1], user_hash);
                        size_t p_shard = static_cast<size_t>(user_hash & SHARD_MASK);

                        my_out[p_shard].push_back(ValidClickRecord{
                            user_hash,
                            ts,
                            canon_user.data(),
                            f[0].data(),
                            camp_idx,
                            static_cast<uint16_t>(canon_user.size()),
                            static_cast<uint16_t>(f[0].size())
                        });
                    });
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 8. Stage 4: Parallel Sessionization & O(1) Sliding-Window Attribution (Single-Sort Clicks + No Munmap Contention)
    struct ThreadOutput {
        std::vector<parquet::UserDailyRow> user_rows;
        std::vector<CampaignAgg> camp_grid;
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
                out.camp_grid.assign(num_campaigns * num_days, CampaignAgg{});

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
                    const char* click_id_ptr;
                    uint32_t campaign_idx;
                    uint16_t click_id_len;
                };
                struct MatchedClickRef {
                    uint32_t pid;
                    uint32_t rt;
                    uint32_t ri;
                };

                FlatViewMap person_map;
                std::vector<uint32_t> ev_person_ids;
                std::vector<uint32_t> person_ev_counts;
                std::vector<uint32_t> person_ev_offsets;
                std::vector<uint32_t> write_pos;
                std::vector<CompactEvent> csr_events;
                std::vector<uint32_t> person_clk_counts;
                std::vector<MatchedClickRef> matched_clicks;
                std::vector<uint32_t> person_clk_offsets;
                std::vector<uint32_t> clk_write_pos;
                std::vector<CompactClick> csr_clicks;

                while (true) {
                    size_t p_shard = next_p_shard.fetch_add(1, std::memory_order_relaxed);
                    if (p_shard >= TUNABLE_NUM_SHARDS) break;

                    size_t total_evs = 0;
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        total_evs += thread_person_ev_shards[rt][p_shard].size();
                    }
                    if (total_evs == 0) continue;

                    person_map.reset(total_evs / 4 + 16);
                    ev_person_ids.clear();
                    ev_person_ids.reserve(total_evs);
                    person_ev_counts.clear();

                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_person_ev_shards[rt][p_shard];
                        for (const auto& rec : recs) {
                            bool inserted = false;
                            uint32_t pid = person_map.find_or_insert(rec.user_ptr, rec.user_len, rec.user_hash, inserted);
                            if (inserted) {
                                person_ev_counts.push_back(0);
                            }
                            ev_person_ids.push_back(pid);
                            person_ev_counts[pid]++;
                        }
                    }

                    size_t num_persons = person_ev_counts.size();
                    person_ev_offsets.resize(num_persons + 1);
                    person_ev_offsets[0] = 0;
                    for (size_t p = 0; p < num_persons; ++p) {
                        person_ev_offsets[p + 1] = person_ev_offsets[p] + person_ev_counts[p];
                    }
                    write_pos = person_ev_offsets;
                    csr_events.resize(total_evs);

                    size_t global_ei = 0;
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_person_ev_shards[rt][p_shard];
                        for (const auto& rec : recs) {
                            uint32_t pid = ev_person_ids[global_ei++];
                            csr_events[write_pos[pid]++] = CompactEvent{rec.ts, rec.price_cents, rec.cat_idx};
                        }
                    }

                    person_clk_counts.assign(num_persons, 0);
                    matched_clicks.clear();
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_person_click_shards[rt][p_shard];
                        for (uint32_t ri = 0; ri < recs.size(); ++ri) {
                            const auto& rec = recs[ri];
                            uint32_t pid = person_map.lookup(rec.user_ptr, rec.user_len, rec.user_hash);
                            if (pid != 0xFFFFFFFFu) {
                                person_clk_counts[pid]++;
                                matched_clicks.push_back(MatchedClickRef{pid, rt, ri});
                            }
                        }
                    }

                    person_clk_offsets.resize(num_persons + 1);
                    person_clk_offsets[0] = 0;
                    for (size_t p = 0; p < num_persons; ++p) {
                        person_clk_offsets[p + 1] = person_clk_offsets[p] + person_clk_counts[p];
                    }
                    clk_write_pos = person_clk_offsets;
                    csr_clicks.resize(matched_clicks.size());
                    for (const auto& mc : matched_clicks) {
                        const auto& rec = thread_person_click_shards[mc.rt][p_shard][mc.ri];
                        csr_clicks[clk_write_pos[mc.pid]++] = CompactClick{
                            rec.ts, rec.click_id_ptr, rec.campaign_idx, rec.click_id_len
                        };
                    }

                    for (size_t p = 0; p < num_persons; ++p) {
                        auto* ev_begin = csr_events.data() + person_ev_offsets[p];
                        auto* ev_end = csr_events.data() + person_ev_offsets[p + 1];
                        size_t n_ev = static_cast<size_t>(ev_end - ev_begin);
                        if (n_ev > 1) {
                            std::sort(ev_begin, ev_end, [](const CompactEvent& a, const CompactEvent& b) {
                                return a.ts < b.ts;
                            });
                        }

                        auto* clk_begin = csr_clicks.data() + person_clk_offsets[p];
                        auto* clk_end = csr_clicks.data() + person_clk_offsets[p + 1];
                        if (clk_end - clk_begin > 1) {
                            // Single sort by (ts asc, click_id desc) places identical click_ids (which have identical ts) adjacent!
                            std::sort(clk_begin, clk_end, [](const CompactClick& a, const CompactClick& b) {
                                if (a.ts != b.ts) return a.ts < b.ts;
                                return std::string_view(a.click_id_ptr, a.click_id_len) >
                                       std::string_view(b.click_id_ptr, b.click_id_len);
                            });
                            clk_end = std::unique(clk_begin, clk_end, [](const CompactClick& a, const CompactClick& b) {
                                return a.ts == b.ts &&
                                       a.click_id_len == b.click_id_len &&
                                       std::memcmp(a.click_id_ptr, b.click_id_ptr, a.click_id_len) == 0;
                            });
                        }
                        size_t n_clk = static_cast<size_t>(clk_end - clk_begin);
                        size_t clk_L = 0, clk_R = 0;

                        const auto& u_entry = person_map.entries[p];
                        std::string_view user_sv(u_entry.ptr, u_entry.len);

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
                            out.user_rows.push_back(parquet::UserDailyRow{
                                user_sv,
                                cur_day,
                                day_sessions,
                                day_events,
                                day_revenue,
                                has_top ? std::string_view(category_names[top_cat]) : std::string_view(),
                                has_top
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
                            while (j < n_ev && (j == i || ev_begin[j].ts - ev_begin[j - 1].ts <= TUNABLE_SESSION_GAP_MS)) {
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

                            while (clk_R < n_clk && clk_begin[clk_R].ts <= session_start) {
                                uint32_t c = clk_begin[clk_R].campaign_idx;
                                if (active_camp_count[c]++ == 0) {
                                    active_camp_pos[c] = static_cast<uint32_t>(active_camps.size());
                                    active_camps.push_back(c);
                                }
                                clk_R++;
                            }
                            int64_t min_click_ts = session_start - TUNABLE_CLICK_LOOKBACK_MS;
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

                            size_t day_off = static_cast<size_t>(s_day - base_day);
                            uint32_t credited_camp = (clk_L < clk_R) ? clk_begin[clk_R - 1].campaign_idx : organic_campaign_idx;
                            auto& cred_agg = out.camp_grid[credited_camp * num_days + day_off];
                            cred_agg.sessions += 1;
                            if (s_has_purchase) cred_agg.purchasing_sessions += 1;
                            cred_agg.revenue_cents += s_rev;

                            for (uint32_t ac : active_camps) {
                                out.camp_grid[ac * num_days + day_off].assisted_sessions += 1;
                            }
                        }

                        flush_user_day();
                        for (uint32_t ac : active_camps) {
                            active_camp_count[ac] = 0;
                        }
                        active_camps.clear();
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // 9. Stage 5: Parallel Multi-File Parquet Output
    fs::create_directories(fs::path(out_dir) / "user_daily");
    fs::create_directories(fs::path(out_dir) / "campaign_daily");

    bool any_user_rows = false;
    for (const auto& to : thread_outputs) {
        if (!to.user_rows.empty()) {
            any_user_rows = true;
            break;
        }
    }

    if (!any_user_rows) {
        std::vector<parquet::UserDailyRow> empty_rows;
        parquet::write_user_daily_parquet((fs::path(out_dir) / "user_daily" / "part-0.parquet").string(), empty_rows);
    } else {
        std::vector<std::thread> writers;
        writers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            if (thread_outputs[t].user_rows.empty()) continue;
            writers.emplace_back([&, t]() {
                std::string fname = "part-" + std::to_string(t) + ".parquet";
                parquet::write_user_daily_parquet(
                    (fs::path(out_dir) / "user_daily" / fname).string(),
                    thread_outputs[t].user_rows
                );
            });
        }
        for (auto& w : writers) w.join();
    }

    std::vector<CampaignAgg> merged_grid(num_campaigns * num_days, CampaignAgg{});
    for (const auto& to : thread_outputs) {
        for (size_t idx = 0; idx < merged_grid.size(); ++idx) {
            merged_grid[idx].sessions += to.camp_grid[idx].sessions;
            merged_grid[idx].purchasing_sessions += to.camp_grid[idx].purchasing_sessions;
            merged_grid[idx].revenue_cents += to.camp_grid[idx].revenue_cents;
            merged_grid[idx].assisted_sessions += to.camp_grid[idx].assisted_sessions;
        }
    }

    std::vector<parquet::CampaignDailyRow> final_camp_rows;
    for (size_t c = 0; c < num_campaigns; ++c) {
        for (size_t d = 0; d < num_days; ++d) {
            const auto& agg = merged_grid[c * num_days + d];
            if (agg.sessions > 0 || agg.assisted_sessions > 0) {
                final_camp_rows.push_back(parquet::CampaignDailyRow{
                    campaign_ids[c],
                    campaign_channels[c],
                    base_day + static_cast<int32_t>(d),
                    agg.sessions,
                    agg.purchasing_sessions,
                    agg.revenue_cents,
                    agg.assisted_sessions
                });
            }
        }
    }
    parquet::write_campaign_daily_parquet(
        (fs::path(out_dir) / "campaign_daily" / "part-0.parquet").string(),
        final_camp_rows
    );

    std::fflush(nullptr);
    std::_Exit(0);
}
