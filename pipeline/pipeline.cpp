// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace {
constexpr size_t TUNABLE_READ_CHUNK_BYTES = 512 * 1024;     // pread() task size; a task ends on the first '\n' past its range
constexpr size_t TUNABLE_NUM_SHARDS = 256;                   // Power of 2, at most 256 (Stage 2 stores a shard index in one byte)
constexpr unsigned int TUNABLE_DEFAULT_THREADS = 8;
constexpr int64_t TUNABLE_SESSION_GAP_MS = 1800000LL;        // 30 minutes
constexpr int64_t TUNABLE_CLICK_LOOKBACK_MS = 604800000LL;   // 7 days
constexpr size_t TUNABLE_AVG_EVENT_ROW_BYTES = 68;
constexpr size_t TUNABLE_AVG_CLICK_ROW_BYTES = 42;
constexpr size_t TUNABLE_MIN_HASH_CAP = 64;
constexpr uint32_t TUNABLE_FAST_PRODUCT_MAX_ID = 100000;
constexpr uint32_t TUNABLE_FAST_CAMPAIGN_MAX_ID = 10000;
constexpr size_t TUNABLE_RESERVE_SLACK_PCT = 100;            // shard vector reserve = estimate * 1.0 + 64; less untouched slack = less kernel page zeroing (ShardWriter regrows on overflow)
constexpr size_t TUNABLE_RANK_SORT_MAX = 64;                 // per-user event sort: branch-free AVX2 rank sort up to this size, LSD radix above
constexpr size_t TUNABLE_ARENA_SLACK_PCT = 150;               // per-thread huge-page arena = shard reserves * 1.5 (room to regrow)
constexpr uint32_t TUNABLE_DEDUP_PREFETCH_DIST = 16;         // dedup hash table: prefetch the home slot this many records ahead
constexpr size_t TUNABLE_FORWARD_PCT = 50;                  // Stage 3b buffer reserve: share of records expected to move (measured ~40% on 20M)
constexpr size_t TUNABLE_DISTINCT_ID_DIV = 16;              // Stage 3b id map sized for records/16 distinct ids (20M: ~16 records per id); grows if needed
constexpr size_t TUNABLE_EVENTS_PER_PERSON = 16;            // Stage 4 person map sized for events/16 persons (generator: ~30 events per person); grows if needed
constexpr size_t TUNABLE_RESOLVE_PREFETCH_DIST = 16;        // Stage 3b batch resolve: prefetch the link-table slot this many distinct ids ahead
constexpr size_t TUNABLE_SPAN_PREFETCH_DIST = 4;            // survivor spans: prefetch this many spans ahead (Stage 3b, Stage 4)
constexpr size_t TUNABLE_SPAN_PREFETCH_BYTES = 512;          // survivor spans: bytes prefetched from the start of a span
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
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// Built only for the scorer target (Dockerfile: -march=x86-64-v3 on Linux); no portable fallbacks.
#if !defined(__AVX2__) || !defined(__BMI2__) || !defined(__SIZEOF_INT128__) || !defined(__linux__)
#error "pipeline needs x86-64-v3 (AVX2, BMI2) and Linux"
#endif

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

// Gives shard vector i `per_shard` reserved elements carved from arenas[i * stride]
// (stride 0: all from one arena). Regrowth stays in that arena while it has room, then
// falls back to the heap.
template <typename Shard>
void reserve_shards(std::vector<Shard>& shards, HugeArena* arenas, size_t stride, size_t per_shard) {
    using T = typename Shard::value_type;
    for (size_t i = 0; i < shards.size(); ++i) {
        shards[i] = Shard(ArenaAllocator<T>(&arenas[i * stride]));
        shards[i].reserve(per_shard);
    }
}

// A run of survivors inside a raw event block (Stage 2 output, compacted by Stage 3b).
struct SurvSpan {
    SurvivingEventRecord* p = nullptr;
    uint32_t n = 0;
};

// Starts loading the first cache lines of a span (spans are short and scattered, so the
// hardware prefetcher restarts at every one).
inline void prefetch_span(const SurvSpan& sp) {
    const char* c = reinterpret_cast<const char*>(sp.p);
    const size_t bytes = static_cast<size_t>(sp.n) * sizeof(SurvivingEventRecord);
    for (size_t off = 0; off < bytes && off < TUNABLE_SPAN_PREFETCH_BYTES; off += 64) __builtin_prefetch(c + off);
}

// Appends records to one worker's shard vectors through raw pointers kept on the stack, so a
// push touches no vector header (push_back reloads and stores size per record). On open each
// vector is resized to its capacity (the arena allocator default-initializes: nothing is
// written); the destructor trims it to the records actually written.
template <typename Shard>
class ShardWriter {
public:
    using T = typename Shard::value_type;
    explicit ShardWriter(std::vector<Shard>& shards) : shards_(shards) {
        for (size_t i = 0; i < TUNABLE_NUM_SHARDS; ++i) open(i);
    }
    ~ShardWriter() {
        for (size_t i = 0; i < TUNABLE_NUM_SHARDS; ++i) shards_[i].resize(static_cast<size_t>(cur_[i] - shards_[i].data()));
    }
    ShardWriter(const ShardWriter&) = delete;
    ShardWriter& operator=(const ShardWriter&) = delete;

    void push(size_t i, const T& rec) {
        if (cur_[i] == lim_[i]) [[unlikely]] grow(i);
        *cur_[i]++ = rec;
    }

private:
    void open(size_t i) {
        Shard& v = shards_[i];
        const size_t used = v.size();
        v.resize(v.capacity());
        cur_[i] = v.data() + used;
        lim_[i] = v.data() + v.size();
    }
    void grow(size_t i) {
        Shard& v = shards_[i];   // full: size() == capacity() == records written
        v.reserve(v.size() * 2 + 64);
        open(i);
    }
    std::vector<Shard>& shards_;
    T* cur_[TUNABLE_NUM_SHARDS];
    T* lim_[TUNABLE_NUM_SHARDS];
};

// One worker's cell of the campaign x day grid (24 bytes instead of 32: the grid is updated at
// random per session, so its size decides how much of it stays in L2). Counts within one worker
// fit 32 bits (at most one per session); Stage 5 adds them into 64-bit row fields.
struct CampaignCell {
    int64_t revenue_cents = 0;
    uint32_t sessions = 0;
    uint32_t purchasing_sessions = 0;
    uint32_t assisted_sessions = 0;
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

// Ascending sort of packed (rel_ts << 32 | meta) event words.
// Small (most persons: ~30 events): branch-free AVX2 rank sort. Element i goes to position
// #{j : a[j] < a[i]} + #{j < i : a[j] == a[i]}; all n * n compares run 4 lanes at a time with no
// data-dependent branch (insertion sort mispredicts at the end of every inner loop).
// Large: 4-pass 8-bit LSD radix sort over the rel_ts bytes (skips constant bytes).
void sort_events_u64(uint64_t* a, size_t n, std::vector<uint64_t>& scratch) {
    if (n <= TUNABLE_RANK_SORT_MAX) {
        constexpr uint64_t kSign = 1ULL << 63;   // flipped sign bit: unsigned order via signed compares
        alignas(32) int64_t buf[TUNABLE_RANK_SORT_MAX + 4];
        const size_t nv = (n + 3) & ~size_t{3};
        for (size_t k = 0; k < n; ++k) buf[k] = static_cast<int64_t>(a[k] ^ kSign);
        for (size_t k = n; k < nv; ++k) buf[k] = std::numeric_limits<int64_t>::max();   // = UINT64_MAX: never below a key
        const __m256i lane0 = _mm256_setr_epi64x(0, 1, 2, 3);
        const __m256i four = _mm256_set1_epi64x(4);
        for (size_t i = 0; i < n; ++i) {
            const __m256i vi = _mm256_set1_epi64x(buf[i]);
            const __m256i ii = _mm256_set1_epi64x(static_cast<int64_t>(i));
            __m256i jj = lane0;
            __m256i cnt = _mm256_setzero_si256();
            for (size_t j = 0; j < nv; j += 4) {
                const __m256i vj = _mm256_load_si256(reinterpret_cast<const __m256i*>(buf + j));
                const __m256i lt = _mm256_cmpgt_epi64(vi, vj);
                const __m256i eq_before = _mm256_and_si256(_mm256_cmpeq_epi64(vi, vj), _mm256_cmpgt_epi64(ii, jj));
                cnt = _mm256_sub_epi64(cnt, _mm256_or_si256(lt, eq_before));
                jj = _mm256_add_epi64(jj, four);
            }
            const __m128i s2 = _mm_add_epi64(_mm256_castsi256_si128(cnt), _mm256_extracti128_si256(cnt, 1));
            const size_t r = static_cast<size_t>(_mm_cvtsi128_si64(s2) + _mm_extract_epi64(s2, 1));
            a[r] = static_cast<uint64_t>(buf[i]) ^ kSign;
        }
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
        run_workers(l_threads, [&](unsigned int t) {
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
        run_workers(num_threads, [&](unsigned int t) {
            auto& my_shards = thread_ev_shards[t];
            // One huge-page arena per worker: one page fault per 2 MiB instead of per 4 KiB.
            ev_arenas[t].prepare(TUNABLE_NUM_SHARDS * est_recs_per_thread_shard * sizeof(RawEventRecord) *
                                 TUNABLE_ARENA_SLACK_PCT / 100);
            reserve_shards(my_shards, &ev_arenas[t], 0, est_recs_per_thread_shard);
            ShardWriter<RawEventShard> out(my_shards);
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

                    out.push(shard_id, RawEventRecord{
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

    // Stage 2: Parallel Deduplication
    // Survivors are written in place into the raw block they come from (16-byte records into a
    // 32-byte-record buffer that is already faulted in), grouped by the person shard of their RAW
    // user id. Block b = rt * NUM_SHARDS + event shard; surv_spans[sh * num_blocks + b] is block
    // b's run of survivors for person shard sh. Fresh memory would cost a kernel page zeroing.
    STAGE_MARK("Stage2_Events_Dedup");
    static_assert(TUNABLE_NUM_SHARDS <= 256, "survivor shard index is stored in one byte");
    const size_t num_blocks = static_cast<size_t>(num_threads) * TUNABLE_NUM_SHARDS;
    std::vector<SurvSpan> surv_spans(TUNABLE_NUM_SHARDS * num_blocks);
    std::vector<size_t> block_surv_bytes(num_blocks, 0);
    {
        std::atomic<size_t> next_shard{0};
        run_workers(num_threads, [&](unsigned int t) {
            std::vector<SurvivingEventRecord> part_recs;   // one block's survivors, before grouping
            std::vector<uint8_t> part_dest;                // their person shards

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

                // Survivors are grouped by the person shard of their RAW user id; Stage 3b
                // resolves each distinct id once and forwards the events of non-canonical ids.
                // Dedup of this event shard is complete, so each raw block is read once more
                // and then overwritten with its own grouped survivors.
                for (unsigned int rt = 0; rt < num_threads; ++rt) {
                    auto& recs = thread_ev_shards[rt][s_id];
                    const size_t n_recs = recs.size();
                    if (part_recs.size() < n_recs) {
                        part_recs.resize(n_recs);
                        part_dest.resize(n_recs);
                    }
                    uint32_t cnt[TUNABLE_NUM_SHARDS] = {};
                    size_t m = 0;
                    for (size_t i = 0; i < n_recs; ++i) {
                        const auto& rec = recs[i];
                        if ((rec.price_cat_valid & 0x80000000u) == 0) continue;
                        const uint8_t u_shard = static_cast<uint8_t>(hash_id64(rec.user_id) & SHARD_MASK);
                        uint64_t packed_ev = (static_cast<uint64_t>(rec.rel_ts) << 32) |
                                             static_cast<uint64_t>(rec.price_cat_valid & 0x7FFFFFFFu);
                        part_recs[m] = SurvivingEventRecord{rec.user_id, packed_ev};
                        part_dest[m] = u_shard;
                        ++cnt[u_shard];
                        ++m;
                    }
                    const size_t b = static_cast<size_t>(rt) * TUNABLE_NUM_SHARDS + s_id;
                    SurvivingEventRecord* base = reinterpret_cast<SurvivingEventRecord*>(recs.data());
                    uint32_t pos[TUNABLE_NUM_SHARDS];
                    uint32_t run = 0;
                    for (size_t sh = 0; sh < TUNABLE_NUM_SHARDS; ++sh) {
                        pos[sh] = run;
                        surv_spans[sh * num_blocks + b] = SurvSpan{base + run, cnt[sh]};
                        run += cnt[sh];
                    }
                    for (size_t i = 0; i < m; ++i) {
                        std::memcpy(base + pos[part_dest[i]]++, &part_recs[i], sizeof(SurvivingEventRecord));
                    }
                    block_surv_bytes[b] = m * sizeof(SurvivingEventRecord);
                }
            }
        });
    }

    // Stage 3: Clicks Ingest
    STAGE_MARK("Stage3_Clicks_Ingest");
    CsvFileSet click_files(list_csv_files(fs::path(data_dir) / "clicks"), TUNABLE_READ_CHUNK_BYTES);
    size_t est_clicks_per_thread_shard = reserve_with_slack(
        (click_files.total_bytes / TUNABLE_AVG_CLICK_ROW_BYTES) / (num_threads * TUNABLE_NUM_SHARDS));

    std::vector<std::vector<ValidClickShard>> thread_person_click_shards(
        num_threads, std::vector<ValidClickShard>(TUNABLE_NUM_SHARDS));
    // Each raw block is now half used by its survivors; the free tail is already faulted in and
    // holds this thread's click buffer for the same shard index, then its Stage 3b forwarding
    // buffers. thread_ev_shards stays alive: it owns the blocks (heap fallbacks included).
    std::unique_ptr<HugeArena[]> tail_arenas(new HugeArena[num_blocks]);
    for (unsigned int rt = 0; rt < num_threads; ++rt) {
        for (size_t s = 0; s < TUNABLE_NUM_SHARDS; ++s) {
            const size_t b = static_cast<size_t>(rt) * TUNABLE_NUM_SHARDS + s;
            auto& raw = thread_ev_shards[rt][s];
            const size_t used = (block_surv_bytes[b] + 63) & ~size_t{63};
            const size_t cap = raw.capacity() * sizeof(RawEventRecord);
            if (cap > used) tail_arenas[b].adopt(reinterpret_cast<char*>(raw.data()) + used, cap - used);
        }
    }
    {
        std::atomic<size_t> next_chunk{0};
        run_workers(num_threads, [&](unsigned int t) {
            auto& my_out = thread_person_click_shards[t];
            reserve_shards(my_out, &tail_arenas[static_cast<size_t>(t) * TUNABLE_NUM_SHARDS], 1, est_clicks_per_thread_shard);
            ShardWriter<ValidClickShard> out(my_out);
            CsvChunkReader reader;

            while (true) {
                size_t c_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                if (c_idx >= click_files.tasks.size()) break;
                for_each_csv_record_in_slice<4>(reader.read(click_files, c_idx), [&](const std::array<std::string_view, 4>& f) {
                    if (f[0].empty() || f[1].empty() || f[2].empty()) return;

                    uint32_t camp_idx = lookup_campaign_idx(f[2]);
                    if (camp_idx == 0xFFFFFFFFu) return;

                    int64_t ts = 0;
                    if (!parse_timestamp(f[3], ts) || ts < click_window_start || ts >= click_window_end) return;

                    // Sharded by the RAW user id; Stage 3b resolves and forwards.
                    uint64_t raw_user_id = pack_id64(f[1]);
                    out.push(static_cast<size_t>(hash_id64(raw_user_id) & SHARD_MASK), ValidClickRecord{
                        raw_user_id,
                        pack_id64(f[0]),
                        static_cast<uint32_t>(ts - click_window_start),
                        camp_idx
                    });
                });
            }
        });
    }

    // Stage 3b: Resolve each distinct user id once and forward non-canonical records.
    // Shard s holds the events and clicks whose RAW user id hashes to s. A record whose id
    // is its person's name (canonical id) is already in its person's shard and stays; the
    // others (~40% of events) move to the shard of their canonical id. One link-table lookup
    // per distinct id per shard (~1.3M on 20M events) instead of one per record (~23M).
    STAGE_MARK("Stage3b_Resolve_Forward");
    std::vector<std::vector<SurvivingEventShard>> thread_fwd_ev_shards(
        num_threads, std::vector<SurvivingEventShard>(TUNABLE_NUM_SHARDS));
    std::vector<std::vector<ValidClickShard>> thread_fwd_click_shards(
        num_threads, std::vector<ValidClickShard>(TUNABLE_NUM_SHARDS));
    {
        size_t total_surv = 0, total_clicks = 0;
        for (size_t b = 0; b < num_blocks; ++b) total_surv += block_surv_bytes[b] / sizeof(SurvivingEventRecord);
        for (unsigned int rt = 0; rt < num_threads; ++rt) {
            for (size_t sh = 0; sh < TUNABLE_NUM_SHARDS; ++sh) total_clicks += thread_person_click_shards[rt][sh].size();
        }
        const size_t per_shard_div = static_cast<size_t>(num_threads) * TUNABLE_NUM_SHARDS;
        const size_t est_fwd_ev = reserve_with_slack(total_surv * TUNABLE_FORWARD_PCT / 100 / per_shard_div);
        const size_t est_fwd_clk = reserve_with_slack(total_clicks * TUNABLE_FORWARD_PCT / 100 / per_shard_div);

        std::atomic<size_t> next_shard{0};
        run_workers(num_threads, [&](unsigned int t) {
            auto& fwd_ev = thread_fwd_ev_shards[t];
            auto& fwd_clk = thread_fwd_click_shards[t];
            // Forwarding buffers go after the click buffers in the raw blocks' free tails
            // (already faulted in: no new page faults).
            HugeArena* my_tails = &tail_arenas[static_cast<size_t>(t) * TUNABLE_NUM_SHARDS];
            reserve_shards(fwd_ev, my_tails, 1, est_fwd_ev);
            reserve_shards(fwd_clk, my_tails, 1, est_fwd_clk);
            ShardWriter<SurvivingEventShard> fwd_ev_out(fwd_ev);
            ShardWriter<ValidClickShard> fwd_clk_out(fwd_clk);

            constexpr uint16_t kStay = 0xFFFF;  // dest_of value: id is canonical, record stays
            PersonIdMap id_map;                 // raw id -> distinct-id index k (this shard only)
            std::vector<uint32_t> rec_k;        // k of each record, in pass order
            std::vector<uint64_t> canon_of;     // canonical id of distinct id k
            std::vector<uint16_t> dest_of;      // person shard of canon_of[k], or kStay

            while (true) {
                size_t sh = next_shard.fetch_add(1, std::memory_order_relaxed);
                if (sh >= TUNABLE_NUM_SHARDS) break;
                SurvSpan* const spans = &surv_spans[sh * num_blocks];
                size_t n_recs = 0;
                for (size_t b = 0; b < num_blocks; ++b) n_recs += spans[b].n;
                for (unsigned int rt = 0; rt < num_threads; ++rt) n_recs += thread_person_click_shards[rt][sh].size();
                if (n_recs == 0) continue;
                id_map.reset(n_recs / TUNABLE_DISTINCT_ID_DIV + 16);
                if (rec_k.size() < n_recs) rec_k.resize(n_recs);

                // Pass 1: number the distinct raw ids (the id map stays in L2).
                size_t ri = 0;
                bool inserted = false;
                for (size_t b = 0; b < num_blocks; ++b) {
                    if (b + TUNABLE_SPAN_PREFETCH_DIST < num_blocks) prefetch_span(spans[b + TUNABLE_SPAN_PREFETCH_DIST]);
                    const SurvivingEventRecord* p = spans[b].p;
                    for (uint32_t i = 0, n = spans[b].n; i < n; ++i) rec_k[ri++] = id_map.find_or_insert(p[i].user_id, inserted);
                }
                for (unsigned int rt = 0; rt < num_threads; ++rt) {
                    for (const auto& rec : thread_person_click_shards[rt][sh]) rec_k[ri++] = id_map.find_or_insert(rec.user_id, inserted);
                }

                // Resolve every distinct id once. Link-table lookups miss cache, so prefetch
                // later ids' slots to keep several misses in flight.
                const auto& ids = id_map.persons;
                const size_t n_ids = ids.size();
                canon_of.resize(n_ids);
                dest_of.resize(n_ids);
                for (size_t j = 0; j < n_ids; ++j) {
                    if (j + TUNABLE_RESOLVE_PREFETCH_DIST < n_ids) dsu.prefetch(hash_id64(ids[j + TUNABLE_RESOLVE_PREFETCH_DIST]));
                    uint64_t raw = ids[j];
                    uint64_t c = dsu.resolve(raw, hash_id64(raw));
                    canon_of[j] = c;
                    dest_of[j] = (c == raw) ? kStay : static_cast<uint16_t>(hash_id64(c) & SHARD_MASK);
                }

                // Pass 2: compact each source vector in place; canonical records stay, the rest move.
                ri = 0;
                for (size_t b = 0; b < num_blocks; ++b) {
                    if (b + TUNABLE_SPAN_PREFETCH_DIST < num_blocks) prefetch_span(spans[b + TUNABLE_SPAN_PREFETCH_DIST]);
                    SurvivingEventRecord* v = spans[b].p;
                    const uint32_t n = spans[b].n;
                    uint32_t keep = 0;
                    for (uint32_t i = 0; i < n; ++i) {
                        SurvivingEventRecord rec = v[i];
                        uint32_t k = rec_k[ri++];
                        uint16_t d = dest_of[k];
                        if (d == kStay) {
                            v[keep++] = rec;
                        } else {
                            rec.user_id = canon_of[k];
                            fwd_ev_out.push(d, rec);
                        }
                    }
                    spans[b].n = keep;
                }
                for (unsigned int rt = 0; rt < num_threads; ++rt) {
                    auto& v = thread_person_click_shards[rt][sh];
                    size_t keep = 0;
                    for (size_t i = 0; i < v.size(); ++i) {
                        ValidClickRecord rec = v[i];
                        uint32_t k = rec_k[ri++];
                        uint16_t d = dest_of[k];
                        if (d == kStay) {
                            v[keep++] = rec;
                        } else {
                            rec.user_id = canon_of[k];
                            fwd_clk_out.push(d, rec);
                        }
                    }
                    v.resize(keep);
                }
            }
        });
    }

    // Stage 4: Sessionize & Attribute
    STAGE_MARK("Stage4_Sessionize_Attrib");
    struct ThreadOutput {
        std::vector<parquet::UserDailyRow> user_rows;
        std::vector<CampaignCell> camp_grid;
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
                    shard_load[sh] += thread_person_click_shards[rt][sh].size() +
                                      thread_fwd_ev_shards[rt][sh].size() + thread_fwd_click_shards[rt][sh].size();
                }
            }
            for (size_t sh = 0; sh < TUNABLE_NUM_SHARDS; ++sh) {
                for (size_t b = 0; b < num_blocks; ++b) shard_load[sh] += surv_spans[sh * num_blocks + b].n;
            }
            std::stable_sort(shard_order.begin(), shard_order.end(),
                             [&](uint32_t a, uint32_t b) { return shard_load[a] > shard_load[b]; });
        }
        std::atomic<size_t> next_p_shard{0};
        const size_t num_cats = category_names.size();
        constexpr uint32_t kLookbackMsU32 = static_cast<uint32_t>(TUNABLE_CLICK_LOOKBACK_MS);
        const int32_t ws_day = static_cast<int32_t>(floor_div_day(window_start));
        const uint64_t ws_day_off = static_cast<uint64_t>(window_start - static_cast<int64_t>(ws_day) * TUNABLE_DAY_MS);

        run_workers(num_threads, [&](unsigned int t) {
            auto& out = thread_outputs[t];
            out.camp_grid.assign(num_campaigns * num_days, CampaignCell{});

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
            // Click sources of a person shard: [0, T) stayed in Stage 3b, [T, 2T) were forwarded to it.
            const unsigned int num_srcs = 2 * num_threads;
            // Event runs of a person shard: the non-empty survivor spans that stayed in Stage 3b,
            // then the forwarded vectors.
            std::vector<SurvSpan> ev_runs;
            ev_runs.reserve(num_blocks + num_threads);
            std::vector<const ValidClickShard*> clk_srcs(num_srcs);

            while (true) {
                size_t order_idx = next_p_shard.fetch_add(1, std::memory_order_relaxed);
                if (order_idx >= TUNABLE_NUM_SHARDS) break;
                size_t p_shard = shard_order[order_idx];
                ev_runs.clear();
                size_t total_evs = 0;
                for (size_t b = 0; b < num_blocks; ++b) {
                    const SurvSpan& sp = surv_spans[p_shard * num_blocks + b];
                    if (sp.n != 0) ev_runs.push_back(sp);
                    total_evs += sp.n;
                }
                for (unsigned int rt = 0; rt < num_threads; ++rt) {
                    auto& fv = thread_fwd_ev_shards[rt][p_shard];
                    if (!fv.empty()) ev_runs.push_back(SurvSpan{fv.data(), static_cast<uint32_t>(fv.size())});
                    total_evs += fv.size();
                    clk_srcs[rt] = &thread_person_click_shards[rt][p_shard];
                    clk_srcs[num_threads + rt] = &thread_fwd_click_shards[rt][p_shard];
                }

                if (total_evs == 0) continue;

                person_map.reset(total_evs / TUNABLE_EVENTS_PER_PERSON + 16);
                ev_person_ids.clear();
                ev_person_ids.reserve(total_evs);
                person_ev_counts.clear();

                for (size_t r = 0; r < ev_runs.size(); ++r) {
                    if (r + TUNABLE_SPAN_PREFETCH_DIST < ev_runs.size()) prefetch_span(ev_runs[r + TUNABLE_SPAN_PREFETCH_DIST]);
                    const SurvSpan& run = ev_runs[r];
                    for (const SurvivingEventRecord& rec : std::span<const SurvivingEventRecord>(run.p, run.n)) {
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
                for (size_t r = 0; r < ev_runs.size(); ++r) {
                    if (r + TUNABLE_SPAN_PREFETCH_DIST < ev_runs.size()) prefetch_span(ev_runs[r + TUNABLE_SPAN_PREFETCH_DIST]);
                    const SurvSpan& run = ev_runs[r];
                    for (const SurvivingEventRecord& rec : std::span<const SurvivingEventRecord>(run.p, run.n)) {
                        uint32_t pid = ev_person_ids[global_ei++];
                        csr_events[write_pos[pid]++] = rec.packed_ev;
                    }
                }

                person_clk_counts.assign(num_persons, 0);
                matched_clicks.clear();
                for (unsigned int k = 0; k < num_srcs; ++k) {
                    const auto& recs = *clk_srcs[k];
                    for (uint32_t ri = 0; ri < recs.size(); ++ri) {
                        const auto& rec = recs[ri];
                        uint32_t pid = person_map.lookup(rec.user_id);
                        if (pid != 0xFFFFFFFFu) {
                            person_clk_counts[pid]++;
                            matched_clicks.push_back(MatchedClickRef{pid, k, ri});
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
                    const auto& rec = (*clk_srcs[mc.rt])[mc.ri];
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

    // Stage 5: Parallel Multi-File Parquet Output
    STAGE_MARK("Stage5_Parquet_Write");
    fs::create_directories(fs::path(out_dir) / "user_daily");
    fs::create_directories(fs::path(out_dir) / "campaign_daily");

    // One file per worker with rows; part-0 is always written so the table exists when empty.
    run_workers(num_threads, [&](unsigned int t) {
        if (t != 0 && thread_outputs[t].user_rows.empty()) return;
        parquet::write_user_daily_parquet(
            (fs::path(out_dir) / "user_daily" / ("part-" + std::to_string(t) + ".parquet")).string(),
            thread_outputs[t].user_rows,
            category_names
        );
    });

    std::vector<parquet::CampaignDailyRow> final_camp_rows;
    for (size_t c = 0; c < num_campaigns; ++c) {
        for (size_t d = 0; d < num_days; ++d) {
            parquet::CampaignDailyRow row{campaign_ids[c], campaign_channels[c], base_day + static_cast<int32_t>(d), 0, 0, 0, 0};
            for (const auto& to : thread_outputs) {
                const CampaignCell& cell = to.camp_grid[c * num_days + d];
                row.sessions += cell.sessions;
                row.purchasing_sessions += cell.purchasing_sessions;
                row.revenue_cents += cell.revenue_cents;
                row.assisted_sessions += cell.assisted_sessions;
            }
            if (row.sessions > 0 || row.assisted_sessions > 0) final_camp_rows.push_back(row);
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
