// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace {
constexpr size_t TUNABLE_READ_CHUNK_BYTES = 512 * 1024;     // pread() task size; a task ends on the first '\n' past its range
constexpr size_t TUNABLE_NUM_SHARDS = 256;                   // Power of 2; WSL A/B: 256 = 1.50 s, 64 = 1.69 s, 512 = 1.79 s
constexpr unsigned int TUNABLE_DEFAULT_THREADS = 8;
constexpr int64_t TUNABLE_SESSION_GAP_MS = 1800000LL;        // 30 minutes
constexpr int64_t TUNABLE_CLICK_LOOKBACK_MS = 604800000LL;   // 7 days
constexpr size_t TUNABLE_AVG_EVENT_ROW_BYTES = 68;
constexpr size_t TUNABLE_AVG_CLICK_ROW_BYTES = 42;
constexpr size_t TUNABLE_MIN_HASH_CAP = 64;
constexpr uint32_t TUNABLE_FAST_PRODUCT_MAX_ID = 100000;
constexpr uint32_t TUNABLE_FAST_CAMPAIGN_MAX_ID = 10000;
constexpr size_t TUNABLE_RESERVE_SLACK_PCT = 130;            // shard vector reserve = estimate * 1.3 + 64 (avoids regrowth)
constexpr size_t TUNABLE_INSERTION_SORT_MAX = 32;            // per-user event sort: insertion sort up to this size
constexpr size_t TUNABLE_ARENA_SLACK_PCT = 150;               // per-thread huge-page arena = shard reserves * 1.5 (room to regrow)
constexpr uint32_t TUNABLE_DEDUP_PREFETCH_DIST = 16;         // dedup hash table: prefetch the home slot this many records ahead
constexpr size_t TUNABLE_RESOLVE_PREFETCH_DIST = 16;        // user-id resolve: prefetch the table slot this many records ahead
constexpr size_t TUNABLE_RADIX_SORT_MIN = 128;               // per-user event sort: 4-pass 32-bit LSD radix sort from this size
} // namespace
// ============================================================================

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "arena.hpp"
#include "dsu.hpp"
#include "parquet_writer.hpp"
#include "parser.hpp"

namespace fs = std::filesystem;
using namespace pipeline;

// Stage boundary trace for profiling builds only (-DPIPELINE_STAGE_TRACE, Linux).
// Prints "PIPELINE_STAGE <name> <CLOCK_MONOTONIC ns>" lines to stderr at exit so
// perf samples (recorded with -k CLOCK_MONOTONIC) can be attributed to stages.
// Release builds compile these macros to nothing.
#ifdef PIPELINE_STAGE_TRACE
#include <cstdio>
#include <ctime>
namespace {
struct StageMark { const char* name; long long ns; };
StageMark g_stage_marks[16];
int g_stage_count = 0;
inline void stage_mark(const char* name) {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    if (g_stage_count < 16) g_stage_marks[g_stage_count++] = {name, ts.tv_sec * 1000000000LL + ts.tv_nsec};
}
inline void stage_dump() {
    for (int i = 0; i < g_stage_count; ++i)
        std::fprintf(stderr, "PIPELINE_STAGE %s %lld\n", g_stage_marks[i].name, g_stage_marks[i].ns);
}
} // namespace
#define STAGE_MARK(name) stage_mark(name)
#define STAGE_DUMP() stage_dump()
#else
#define STAGE_MARK(name) ((void)0)
#define STAGE_DUMP() ((void)0)
#endif

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

struct PersonIdMap {
    struct Slot {
        uint64_t user_id;
        uint32_t pid;
        uint32_t pad;
    };
    std::vector<uint64_t> persons;
    std::vector<Slot> table;
    uint64_t mask = 0;

    void reset(size_t expected) {
        persons.clear();
        size_t cap = TUNABLE_MIN_HASH_CAP;
        while (cap < expected * 2) cap <<= 1;
        table.assign(cap, Slot{0ULL, 0u, 0u});
        mask = cap - 1;
    }

    void rehash() {
        size_t new_cap = table.size() * 2;
        std::vector<Slot> new_table(new_cap, Slot{0ULL, 0u, 0u});
        uint64_t new_mask = new_cap - 1;
        for (const auto& sl : table) {
            if (sl.user_id != 0) {
                uint64_t pos = (hash_id64(sl.user_id) >> 8) & new_mask;
                while (new_table[pos].user_id != 0) pos = (pos + 1) & new_mask;
                new_table[pos] = sl;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    uint32_t find_or_insert(uint64_t uid, bool& inserted) {
        if (persons.size() * 2 >= table.size()) rehash();
        uint64_t pos = (hash_id64(uid) >> 8) & mask;
        while (true) {
            Slot& sl = table[pos];
            if (sl.user_id == 0) {
                uint32_t new_pid = static_cast<uint32_t>(persons.size());
                persons.push_back(uid);
                sl.user_id = uid;
                sl.pid = new_pid;
                inserted = true;
                return new_pid;
            }
            if (sl.user_id == uid) {
                inserted = false;
                return sl.pid;
            }
            pos = (pos + 1) & mask;
        }
    }

    uint32_t lookup(uint64_t uid) const {
        if (persons.empty()) return 0xFFFFFFFFu;
        uint64_t pos = (hash_id64(uid) >> 8) & mask;
        while (true) {
            const Slot& sl = table[pos];
            if (sl.user_id == 0) return 0xFFFFFFFFu;
            if (sl.user_id == uid) return sl.pid;
            pos = (pos + 1) & mask;
        }
    }
};

struct RawEventRecord {
    uint64_t event_id;
    uint64_t user_id;        // raw PackedId64 (0 if the record is invalid); resolved in Stage 2
    int32_t ingest_rel_sec;
    uint32_t rel_ts;
    uint32_t price_cat_valid;
    uint32_t ev_hash_hi;
};

struct SurvivingEventRecord {
    uint64_t user_id;
    uint64_t packed_ev;
};

struct ValidClickRecord {
    uint64_t user_id;
    uint64_t click_id;
    uint32_t click_rel_ts;
    uint32_t campaign_idx;
};

using RawEventShard = std::vector<RawEventRecord, ArenaAllocator<RawEventRecord>>;
using SurvivingEventShard = std::vector<SurvivingEventRecord, ArenaAllocator<SurvivingEventRecord>>;
using ValidClickShard = std::vector<ValidClickRecord, ArenaAllocator<ValidClickRecord>>;

// Gives each of a worker's shard vectors `per_shard` reserved elements carved from one
// per-thread huge-page arena (one page fault per 2 MiB instead of per 4 KiB).
template <typename Shard>
void reserve_shards_in_arena(std::vector<Shard>& shards, HugeArena& arena, size_t per_shard) {
    using T = typename Shard::value_type;
    arena.prepare(shards.size() * per_shard * sizeof(T) * TUNABLE_ARENA_SLACK_PCT / 100);
    for (auto& s : shards) {
        s = Shard(ArenaAllocator<T>(&arena));
        s.reserve(per_shard);
    }
}

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

inline size_t reserve_with_slack(size_t est) {
    return est * TUNABLE_RESERVE_SLACK_PCT / 100 + 64;
}

inline bool sv_eq4(std::string_view sv, const char (&lit)[5]) {
    if (sv.size() != 4) return false;
    uint32_t a, b;
    std::memcpy(&a, sv.data(), 4);
    std::memcpy(&b, lit, 4);
    return a == b;
}

inline bool sv_eq8(std::string_view sv, const char (&lit)[9]) {
    if (sv.size() != 8) return false;
    uint64_t a, b;
    std::memcpy(&a, sv.data(), 8);
    std::memcpy(&b, lit, 8);
    return a == b;
}

// Ascending sort of packed (rel_ts << 32 | meta) event words by upper 32-bit rel_ts.
// Small: 2-elem / insertion sort. Large: 4-pass 8-bit LSD radix sort over rel_ts bytes (skips constant bytes).
void sort_events_u64(uint64_t* a, size_t n, std::vector<uint64_t>& scratch) {
    if (n == 2) {
        if (a[0] > a[1]) std::swap(a[0], a[1]);
        return;
    }
    if (n <= TUNABLE_INSERTION_SORT_MAX) {
        for (size_t i = 1; i < n; ++i) {
            uint64_t v = a[i];
            size_t j = i;
            while (j > 0 && a[j - 1] > v) {
                a[j] = a[j - 1];
                --j;
            }
            a[j] = v;
        }
        return;
    }
    if (n < TUNABLE_RADIX_SORT_MIN) {
        std::sort(a, a + n);
        return;
    }
    uint32_t hist[4][256] = {};
    for (size_t i = 0; i < n; ++i) {
        uint32_t ts = static_cast<uint32_t>(a[i] >> 32);
        hist[0][ts & 0xFFu]++;
        hist[1][(ts >> 8) & 0xFFu]++;
        hist[2][(ts >> 16) & 0xFFu]++;
        hist[3][(ts >> 24) & 0xFFu]++;
    }
    scratch.resize(n);
    uint64_t* src = a;
    uint64_t* dst = scratch.data();
    for (int b = 0; b < 4; ++b) {
        uint32_t* h = hist[b];
        unsigned int shift = static_cast<unsigned int>(32 + 8 * b);
        if (h[(src[0] >> shift) & 0xFFu] == n) continue; // byte is constant across all keys
        uint32_t sum = 0;
        for (int k = 0; k < 256; ++k) {
            uint32_t c = h[k];
            h[k] = sum;
            sum += c;
        }
        for (size_t i = 0; i < n; ++i) {
            uint64_t v = src[i];
            dst[h[(v >> shift) & 0xFFu]++] = v;
        }
        std::swap(src, dst);
    }
    if (src != a) std::memcpy(a, src, n * sizeof(uint64_t));
}

} // namespace

int main(int argc, char** argv) {
    STAGE_MARK("Stage0_Lookups");
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

    std::string manifest_str = read_file_binary((fs::path(data_dir) / "manifest.json").string());
    int64_t window_start = 0, window_end = 0;
    parse_timestamp(extract_json_string(manifest_str, "window_start"), window_start);
    parse_timestamp(extract_json_string(manifest_str, "window_end"), window_end);
    int64_t click_window_start = window_start - TUNABLE_CLICK_LOOKBACK_MS;
    int64_t click_window_end = window_end;

    int32_t base_day = static_cast<int32_t>(floor_div_day(window_start)) - 1;
    int32_t max_day = static_cast<int32_t>(floor_div_day(window_end)) + 1;
    size_t num_days = static_cast<size_t>(std::max<int32_t>(1, max_day - base_day + 1));

    std::string prod_buf = read_file_binary((fs::path(data_dir) / "products.csv").string());
    std::vector<std::pair<std::string_view, std::string_view>> raw_products;
    std::vector<std::string> category_names;
    category_names.push_back("unknown");
    {
        size_t pos = prod_buf.find('\n');
        if (pos != std::string::npos && pos + 1 < prod_buf.size()) {
            for_each_csv_record_in_slice<2>(std::string_view(prod_buf).substr(pos + 1), [&](const std::array<std::string_view, 2>& f) {
                if (!f[0].empty() && !f[1].empty()) {
                    raw_products.emplace_back(f[0], f[1]);
                    bool found = false;
                    for (const auto& c : category_names) {
                        if (c == f[1]) { found = true; break; }
                    }
                    if (!found) category_names.emplace_back(f[1]);
                }
            });
        }
    }
    std::sort(category_names.begin(), category_names.end());

    auto find_cat_idx = [&](std::string_view sv) -> uint16_t {
        for (size_t i = 0; i < category_names.size(); ++i) {
            if (category_names[i] == sv) return static_cast<uint16_t>(i);
        }
        return 0;
    };
    uint16_t unknown_cat_idx = find_cat_idx("unknown");

    std::vector<uint16_t> fast_product_cat(TUNABLE_FAST_PRODUCT_MAX_ID + 1, unknown_cat_idx);
    FlatStringMap product_map;
    for (const auto& kv : raw_products) {
        uint16_t cidx = find_cat_idx(kv.second);
        uint32_t pid = 0;
        if (try_parse_prefixed_uint(kv.first, "p", 1, TUNABLE_FAST_PRODUCT_MAX_ID, pid)) {
            fast_product_cat[pid] = cidx;
        } else {
            if (product_map.table.empty()) product_map.init(1024);
            product_map.insert(kv.first, cidx);
        }
    }
    raw_products.clear();
    std::string().swap(prod_buf);

    auto lookup_product_cat = [&](std::string_view prod_sv) -> uint16_t {
        uint32_t pid = 0;
        if (try_parse_prefixed_uint(prod_sv, "p", 1, TUNABLE_FAST_PRODUCT_MAX_ID, pid)) {
            return fast_product_cat[pid];
        }
        return static_cast<uint16_t>(product_map.lookup(prod_sv, unknown_cat_idx));
    };

    std::vector<std::string> campaign_ids;
    std::vector<std::string> campaign_channels;
    std::vector<uint32_t> fast_campaign_idx(TUNABLE_FAST_CAMPAIGN_MAX_ID + 1, 0xFFFFFFFFu);
    FlatStringMap campaign_map;
    {
        std::string camp_buf = read_file_binary((fs::path(data_dir) / "campaigns.csv").string());
        size_t pos = camp_buf.find('\n');
        if (pos != std::string::npos && pos + 1 < camp_buf.size()) {
            for_each_csv_record_in_slice<2>(std::string_view(camp_buf).substr(pos + 1), [&](const std::array<std::string_view, 2>& f) {
                if (!f[0].empty() && !f[1].empty()) {
                    uint32_t idx = static_cast<uint32_t>(campaign_ids.size());
                    campaign_ids.emplace_back(f[0]);
                    campaign_channels.emplace_back(f[1]);
                    uint32_t cid = 0;
                    if (try_parse_prefixed_uint(f[0], "c_", 2, TUNABLE_FAST_CAMPAIGN_MAX_ID, cid)) {
                        fast_campaign_idx[cid] = idx;
                    } else {
                        if (campaign_map.table.empty()) campaign_map.init(256);
                        campaign_map.insert(f[0], idx);
                    }
                }
            });
        }
    }
    uint32_t organic_campaign_idx = static_cast<uint32_t>(campaign_ids.size());
    campaign_ids.push_back("organic");
    campaign_channels.push_back("organic");
    size_t num_campaigns = campaign_ids.size();

    auto lookup_campaign_idx = [&](std::string_view camp_sv) -> uint32_t {
        uint32_t cid = 0;
        if (try_parse_prefixed_uint(camp_sv, "c_", 2, TUNABLE_FAST_CAMPAIGN_MAX_ID, cid)) {
            return fast_campaign_idx[cid];
        }
        return campaign_map.lookup(camp_sv, 0xFFFFFFFFu);
    };

    // Stage 0b: Parallel Links + 16-Byte PackedId64 DSU
    STAGE_MARK("Stage0b_Links_DSU");
    LinkResolver dsu;
    {
        CsvFileSet link_files(list_csv_files(fs::path(data_dir) / "links"), TUNABLE_READ_CHUNK_BYTES);
        unsigned int l_threads = std::min<unsigned int>(num_threads, std::max<size_t>(1, link_files.tasks.size()));
        std::vector<std::vector<LinkPair>> thread_links(l_threads);
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(l_threads);
        for (unsigned int t = 0; t < l_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& out_l = thread_links[t];
                CsvChunkReader reader;
                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= link_files.tasks.size()) break;
                    for_each_csv_record_in_slice<2>(reader.read(link_files, c_idx), [&](const std::array<std::string_view, 2>& f) {
                        if (!f[0].empty() && !f[1].empty()) {
                            out_l.push_back(LinkPair{pack_id64(f[0]), pack_id64(f[1])});
                        }
                    });
                }
            });
        }
        for (auto& w : workers) w.join();
        dsu.build(thread_links, num_threads);
    }

    // Stage 1: Single-Pass Events Ingest with PackedId64
    STAGE_MARK("Stage1_Events_Ingest");
    CsvFileSet event_files(list_csv_files(fs::path(data_dir) / "events"), TUNABLE_READ_CHUNK_BYTES);
    size_t est_recs_per_thread_shard = reserve_with_slack(
        (event_files.total_bytes / TUNABLE_AVG_EVENT_ROW_BYTES) / (num_threads * TUNABLE_NUM_SHARDS));

    std::vector<std::vector<RawEventShard>> thread_ev_shards(
        num_threads, std::vector<RawEventShard>(TUNABLE_NUM_SHARDS));
    std::unique_ptr<HugeArena[]> ev_arenas(new HugeArena[num_threads]);
    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_shards = thread_ev_shards[t];
                reserve_shards_in_arena(my_shards, ev_arenas[t], est_recs_per_thread_shard);
                CsvChunkReader reader;

                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= event_files.tasks.size()) break;
                    for_each_csv_record_in_slice<7>(reader.read(event_files, c_idx), [&](const std::array<std::string_view, 7>& f) {
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
                            if (sv_eq4(etype, "view") || sv_eq4(etype, "cart")) {
                                price = 0;
                            } else if (sv_eq8(etype, "purchase")) {
                                if (!parse_price_cents(f[6], price)) {
                                    valid = false;
                                } else {
                                    cat_idx = lookup_product_cat(f[5]);
                                }
                            } else {
                                valid = false;
                            }
                        }

                        // Raw id only; Stage 2 resolves survivors to their person in prefetched batches.
                        uint64_t raw_user_id = valid ? pack_id64(f[1]) : 0;

                        uint64_t ev_id = pack_id64(f[0]);
                        uint64_t ev_hash = hash_id64(ev_id);
                        size_t shard_id = static_cast<size_t>(ev_hash & SHARD_MASK);
                        int32_t ingest_rel_sec = static_cast<int32_t>((ingest_ts - window_start) / 1000LL);
                        uint32_t rel_ts = valid ? static_cast<uint32_t>(ts - window_start) : 0u;
                        uint32_t packed_meta = (valid ? 0x80000000u : 0u) |
                                               (static_cast<uint32_t>(cat_idx & 0x7Fu) << 24) |
                                               static_cast<uint32_t>(price & 0xFFFFFFu);

                        my_shards[shard_id].push_back(RawEventRecord{
                            ev_id,
                            raw_user_id,
                            ingest_rel_sec,
                            rel_ts,
                            packed_meta,
                            static_cast<uint32_t>(ev_hash >> 8)
                        });
                    });
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Stage 2: Parallel Deduplication
    STAGE_MARK("Stage2_Events_Dedup");
    std::vector<std::vector<SurvivingEventShard>> thread_person_ev_shards(
        num_threads, std::vector<SurvivingEventShard>(TUNABLE_NUM_SHARDS));
    std::unique_ptr<HugeArena[]> surv_arenas(new HugeArena[num_threads]);
    {
        std::atomic<size_t> next_shard{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_ev_shards[t];
                reserve_shards_in_arena(my_out, surv_arenas[t], est_recs_per_thread_shard);

                // Packed 8-byte slot: bit 63 = occupied (1), bits 32..62 = 31-bit event_id tag,
                // bits 24..31 = thread_idx, bits 0..23 = rec_idx. Empty slot = 0ULL.
                std::vector<uint64_t> table;

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
                    table.assign(cap, 0ULL);
                    uint64_t mask = cap - 1;

                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        auto& recs = thread_ev_shards[rt][s_id];
                        const uint32_t n_recs = static_cast<uint32_t>(recs.size());
                        for (uint32_t ri = 0; ri < n_recs; ++ri) {
                            // The table is larger than L2: start loading a later record's home slot now.
                            if (ri + TUNABLE_DEDUP_PREFETCH_DIST < n_recs) {
                                __builtin_prefetch(&table[recs[ri + TUNABLE_DEDUP_PREFETCH_DIST].ev_hash_hi & mask], 1);
                            }
                            auto& rec = recs[ri];
                            uint64_t eid = rec.event_id;
                            uint64_t tag = (1ULL << 63) | (((eid ^ (eid >> 31)) & 0x7FFFFFFFULL) << 32);
                            uint64_t pos = rec.ev_hash_hi & mask;
                            while (true) {
                                uint64_t sl = table[pos];
                                if (sl == 0) {
                                    table[pos] = tag | (static_cast<uint64_t>(rt) << 24) | ri;
                                    break;
                                }
                                if (((sl ^ tag) >> 32) == 0) {
                                    uint32_t ex_rt = static_cast<uint32_t>((sl >> 24) & 0xFFu);
                                    uint32_t ex_ri = static_cast<uint32_t>(sl & 0xFFFFFFu);
                                    auto& ex_rec = thread_ev_shards[ex_rt][s_id][ex_ri];
                                    if (ex_rec.event_id == eid) {
                                        if (rec.ingest_rel_sec < ex_rec.ingest_rel_sec) {
                                            ex_rec.price_cat_valid = 0;
                                            table[pos] = tag | (static_cast<uint64_t>(rt) << 24) | ri;
                                        } else {
                                            rec.price_cat_valid = 0;
                                        }
                                        break;
                                    }
                                }
                                pos = (pos + 1) & mask;
                            }
                        }
                    }

                    // Survivors: resolve raw user id -> person, prefetching the resolve-table
                    // slot TUNABLE_RESOLVE_PREFETCH_DIST records ahead so the misses overlap.
                    // Dropped records (raw id 0) prefetch a harmless slot; no branch needed.
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_ev_shards[rt][s_id];
                        const size_t n = recs.size();
                        for (size_t i = 0; i < n; ++i) {
                            if (i + TUNABLE_RESOLVE_PREFETCH_DIST < n) {
                                dsu.prefetch(hash_id64(recs[i + TUNABLE_RESOLVE_PREFETCH_DIST].user_id));
                            }
                            const auto& rec = recs[i];
                            if ((rec.price_cat_valid & 0x80000000u) == 0) continue;
                            uint64_t canon_user_id = dsu.resolve(rec.user_id, hash_id64(rec.user_id));
                            size_t p_shard = static_cast<size_t>(hash_id64(canon_user_id) & SHARD_MASK);
                            uint64_t packed_ev = (static_cast<uint64_t>(rec.rel_ts) << 32) |
                                                 static_cast<uint64_t>(rec.price_cat_valid & 0x7FFFFFFFu);
                            my_out[p_shard].push_back(SurvivingEventRecord{
                                canon_user_id,
                                packed_ev
                            });
                        }
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Stage 3: Clicks Ingest
    STAGE_MARK("Stage3_Clicks_Ingest");
    CsvFileSet click_files(list_csv_files(fs::path(data_dir) / "clicks"), TUNABLE_READ_CHUNK_BYTES);
    size_t est_clicks_per_thread_shard = reserve_with_slack(
        (click_files.total_bytes / TUNABLE_AVG_CLICK_ROW_BYTES) / (num_threads * TUNABLE_NUM_SHARDS));

    std::vector<std::vector<ValidClickShard>> thread_person_click_shards(
        num_threads, std::vector<ValidClickShard>(TUNABLE_NUM_SHARDS));
    // Raw event shards are dead after dedup: their already-faulted arenas hold the clicks.
    thread_ev_shards.clear();
    std::unique_ptr<HugeArena[]> click_arenas = std::move(ev_arenas);
    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = thread_person_click_shards[t];
                reserve_shards_in_arena(my_out, click_arenas[t], est_clicks_per_thread_shard);
                CsvChunkReader reader;
                std::vector<ValidClickRecord> chunk_clicks; // valid clicks of one chunk, raw user ids

                while (true) {
                    size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (c_idx >= click_files.tasks.size()) break;
                    chunk_clicks.clear();
                    for_each_csv_record_in_slice<4>(reader.read(click_files, c_idx), [&](const std::array<std::string_view, 4>& f) {
                        if (f[0].empty() || f[1].empty() || f[2].empty()) return;

                        uint32_t camp_idx = lookup_campaign_idx(f[2]);
                        if (camp_idx == 0xFFFFFFFFu) return;

                        int64_t ts = 0;
                        if (!parse_timestamp(f[3], ts) || ts < click_window_start || ts >= click_window_end) return;

                        chunk_clicks.push_back(ValidClickRecord{
                            pack_id64(f[1]),
                            pack_id64(f[0]),
                            static_cast<uint32_t>(ts - click_window_start),
                            camp_idx
                        });
                    });

                    // Resolve the chunk's raw user ids with prefetch, then shard by person.
                    const size_t n = chunk_clicks.size();
                    for (size_t i = 0; i < n; ++i) {
                        if (i + TUNABLE_RESOLVE_PREFETCH_DIST < n) {
                            dsu.prefetch(hash_id64(chunk_clicks[i + TUNABLE_RESOLVE_PREFETCH_DIST].user_id));
                        }
                        ValidClickRecord rec = chunk_clicks[i];
                        rec.user_id = dsu.resolve(rec.user_id, hash_id64(rec.user_id));
                        my_out[static_cast<size_t>(hash_id64(rec.user_id) & SHARD_MASK)].push_back(rec);
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Stage 4: Sessionize & Attribute
    STAGE_MARK("Stage4_Sessionize_Attrib");
    struct ThreadOutput {
        std::vector<parquet::UserDailyRow> user_rows;
        std::vector<CampaignAgg> camp_grid;
    };
    std::vector<ThreadOutput> thread_outputs(num_threads);

    {
        // Largest shards first (whale persons make a few shards several times bigger),
        // so no big shard is left to run alone at the end of the stage.
        std::vector<uint32_t> shard_order(TUNABLE_NUM_SHARDS);
        {
            std::vector<size_t> shard_load(TUNABLE_NUM_SHARDS, 0);
            for (size_t sh = 0; sh < TUNABLE_NUM_SHARDS; ++sh) {
                shard_order[sh] = static_cast<uint32_t>(sh);
                for (unsigned int rt = 0; rt < num_threads; ++rt) {
                    shard_load[sh] += thread_person_ev_shards[rt][sh].size() + thread_person_click_shards[rt][sh].size();
                }
            }
            std::stable_sort(shard_order.begin(), shard_order.end(),
                             [&](uint32_t a, uint32_t b) { return shard_load[a] > shard_load[b]; });
        }
        std::atomic<size_t> next_p_shard{0};
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        size_t num_cats = category_names.size();
        constexpr uint32_t kLookbackMsU32 = static_cast<uint32_t>(TUNABLE_CLICK_LOOKBACK_MS);
        const int32_t ws_day = static_cast<int32_t>(floor_div_day(window_start));
        const uint64_t ws_day_off = static_cast<uint64_t>(window_start - static_cast<int64_t>(ws_day) * TUNABLE_DAY_MS);

        for (unsigned int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t, num_cats]() {
                auto& out = thread_outputs[t];
                out.camp_grid.assign(num_campaigns * num_days, CampaignAgg{});

                std::vector<int64_t> day_cat_rev(num_cats, 0);
                std::vector<int32_t> active_camp_count(num_campaigns, 0);
                std::vector<uint32_t> active_camp_pos(num_campaigns, 0);
                std::vector<uint32_t> active_camps;
                active_camps.reserve(num_campaigns);

                struct CompactClick {
                    uint64_t click_id;
                    uint32_t click_rel_ts;
                    uint32_t campaign_idx;
                };
                struct MatchedClickRef {
                    uint32_t pid;
                    uint32_t rt;
                    uint32_t ri;
                };

                PersonIdMap person_map;
                std::vector<uint32_t> ev_person_ids;
                std::vector<uint32_t> person_ev_counts;
                std::vector<uint32_t> person_ev_offsets;
                std::vector<uint32_t> write_pos;
                std::vector<uint64_t> csr_events;
                std::vector<uint32_t> person_clk_counts;
                std::vector<MatchedClickRef> matched_clicks;
                std::vector<uint32_t> person_clk_offsets;
                std::vector<uint32_t> clk_write_pos;
                std::vector<CompactClick> csr_clicks;
                std::vector<uint64_t> sort_scratch;

                while (true) {
                    size_t order_idx = next_p_shard.fetch_add(1, std::memory_order_relaxed);
                    if (order_idx >= TUNABLE_NUM_SHARDS) break;
                    size_t p_shard = shard_order[order_idx];

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
                            uint32_t pid = person_map.find_or_insert(rec.user_id, inserted);
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
                            csr_events[write_pos[pid]++] = rec.packed_ev;
                        }
                    }

                    person_clk_counts.assign(num_persons, 0);
                    matched_clicks.clear();
                    for (unsigned int rt = 0; rt < num_threads; ++rt) {
                        const auto& recs = thread_person_click_shards[rt][p_shard];
                        for (uint32_t ri = 0; ri < recs.size(); ++ri) {
                            const auto& rec = recs[ri];
                            uint32_t pid = person_map.lookup(rec.user_id);
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
                            rec.click_id, rec.click_rel_ts, rec.campaign_idx
                        };
                    }

                    for (size_t p = 0; p < num_persons; ++p) {
                        uint64_t* ev_begin = csr_events.data() + person_ev_offsets[p];
                        uint64_t* ev_end = csr_events.data() + person_ev_offsets[p + 1];
                        size_t n_ev = static_cast<size_t>(ev_end - ev_begin);
                        if (n_ev > 1) {
                            sort_events_u64(ev_begin, n_ev, sort_scratch);
                        }

                        auto* clk_begin = csr_clicks.data() + person_clk_offsets[p];
                        auto* clk_end = csr_clicks.data() + person_clk_offsets[p + 1];
                        if (clk_end - clk_begin > 1) {
                            std::sort(clk_begin, clk_end, [](const CompactClick& a, const CompactClick& b) {
                                if (a.click_rel_ts != b.click_rel_ts) return a.click_rel_ts < b.click_rel_ts;
                                return a.click_id > b.click_id;
                            });
                            clk_end = std::unique(clk_begin, clk_end, [](const CompactClick& a, const CompactClick& b) {
                                return a.click_rel_ts == b.click_rel_ts && a.click_id == b.click_id;
                            });
                        }
                        size_t n_clk = static_cast<size_t>(clk_end - clk_begin);
                        size_t clk_L = 0, clk_R = 0;

                        uint64_t user_id = person_map.persons[p];

                        int32_t cur_day = std::numeric_limits<int32_t>::min();
                        int32_t day_sessions = 0;
                        int32_t day_events = 0;
                        int64_t day_revenue = 0;

                        auto flush_user_day = [&]() {
                            if (day_sessions == 0) return;
                            int16_t top_cat = -1;
                            if (day_revenue > 0) {
                                int64_t best_rev = 0;
                                for (size_t c = 0; c < num_cats; ++c) {
                                    if (day_cat_rev[c] > best_rev) {
                                        best_rev = day_cat_rev[c];
                                        top_cat = static_cast<int16_t>(c);
                                    }
                                    day_cat_rev[c] = 0;
                                }
                            }
                            out.user_rows.push_back(parquet::UserDailyRow{
                                user_id,
                                day_revenue,
                                cur_day,
                                day_sessions,
                                day_events,
                                top_cat,
                                0
                            });
                        };

                        size_t i = 0;
                        while (i < n_ev) {
                            uint32_t start_rel_ts = static_cast<uint32_t>(ev_begin[i] >> 32);
                            // floor((window_start + rel) / DAY) with a non-negative unsigned dividend:
                            // the compiler turns the constant division into a multiply-high.
                            int32_t s_day = ws_day + static_cast<int32_t>(
                                (static_cast<uint64_t>(start_rel_ts) + ws_day_off) / static_cast<uint64_t>(TUNABLE_DAY_MS));
                            if (s_day != cur_day) {
                                flush_user_day();
                                cur_day = s_day;
                                day_sessions = 0;
                                day_events = 0;
                                day_revenue = 0;
                            }

                            int32_t s_events = 0;
                            int64_t s_rev = 0;
                            size_t j = i;
                            uint32_t prev_rel_ts = start_rel_ts;
                            while (j < n_ev) {
                                uint64_t pev = ev_begin[j];
                                uint32_t cur_rel_ts = static_cast<uint32_t>(pev >> 32);
                                if (j > i && static_cast<int64_t>(cur_rel_ts - prev_rel_ts) > TUNABLE_SESSION_GAP_MS) {
                                    break;
                                }
                                prev_rel_ts = cur_rel_ts;
                                s_events++;
                                uint32_t price = static_cast<uint32_t>(pev) & 0xFFFFFFu;
                                if (price > 0) {
                                    uint16_t cat_idx = static_cast<uint16_t>((static_cast<uint32_t>(pev) >> 24) & 0x7Fu);
                                    s_rev += price;
                                    day_cat_rev[cat_idx] += price;
                                }
                                j++;
                            }
                            i = j;

                            day_sessions += 1;
                            day_events += s_events;
                            day_revenue += s_rev;
                            bool s_has_purchase = (s_rev > 0);

                            uint32_t max_click_rel_ts = start_rel_ts + kLookbackMsU32;
                            while (clk_R < n_clk && clk_begin[clk_R].click_rel_ts <= max_click_rel_ts) {
                                uint32_t c = clk_begin[clk_R].campaign_idx;
                                if (active_camp_count[c]++ == 0) {
                                    active_camp_pos[c] = static_cast<uint32_t>(active_camps.size());
                                    active_camps.push_back(c);
                                }
                                clk_R++;
                            }
                            while (clk_L < clk_R && clk_begin[clk_L].click_rel_ts < start_rel_ts) {
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

    // Stage 5: Parallel Multi-File Parquet Output
    STAGE_MARK("Stage5_Parquet_Write");
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
        parquet::write_user_daily_parquet(
            (fs::path(out_dir) / "user_daily" / "part-0.parquet").string(),
            empty_rows,
            category_names
        );
    } else {
        std::vector<std::thread> writers;
        writers.reserve(num_threads);
        for (unsigned int t = 0; t < num_threads; ++t) {
            if (thread_outputs[t].user_rows.empty()) continue;
            writers.emplace_back([&, t]() {
                std::string fname = "part-" + std::to_string(t) + ".parquet";
                parquet::write_user_daily_parquet(
                    (fs::path(out_dir) / "user_daily" / fname).string(),
                    thread_outputs[t].user_rows,
                    category_names
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

    STAGE_MARK("END");
    STAGE_DUMP();
    std::fflush(nullptr);
    std::_Exit(0);
}
