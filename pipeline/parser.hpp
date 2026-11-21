#pragma once
// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace pipeline {
constexpr int64_t TUNABLE_MIN_PRICE_CENTS = 1LL;
constexpr int64_t TUNABLE_MAX_PRICE_CENTS = 10000000LL;
constexpr int64_t TUNABLE_DAY_MS = 86400000LL;
constexpr int TUNABLE_CAL_BASE_YEAR = 1970;
constexpr int TUNABLE_CAL_NUM_YEARS = 200;            // Precomputed L1 calendar table for 1970..2169
constexpr size_t TUNABLE_READ_OVERLAP_BYTES = 64 * 1024; // Extra bytes read past a chunk to find its closing '\n'
constexpr size_t TUNABLE_READ_PAD_BYTES = 64;         // Readable slack after chunk data (8-byte ID loads, SIMD)
constexpr size_t TUNABLE_SPLIT_BATCH_BLOCKS = 32;     // CSV splitter: 64-byte blocks flattened per batch (32 = 2 KB of input, 8 KB index on the stack)
} // namespace pipeline
// ============================================================================

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include <immintrin.h>

#include <fcntl.h>
#include <unistd.h>

namespace pipeline {

constexpr bool is_ascii_digit(char c) {
    return c >= '0' && c <= '9';
}

inline int parse_2digits(const char* p) {
    unsigned int d0 = static_cast<unsigned int>(static_cast<unsigned char>(p[0]) - '0');
    unsigned int d1 = static_cast<unsigned int>(static_cast<unsigned char>(p[1]) - '0');
    if ((d0 > 9) | (d1 > 9)) return -1;
    return static_cast<int>(d0 * 10 + d1);
}

constexpr bool is_leap_year(int year) {
    return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

constexpr int max_days_in_month(int year, int month) {
    constexpr int kDays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) return 0;
    if (month == 2 && is_leap_year(year)) return 29;
    return kDays[month];
}

constexpr int64_t days_from_civil(int64_t year, int64_t month, int64_t day) {
    if (month <= 2) year -= 1;
    int64_t era = (year >= 0 ? year : year - 399) / 400;
    int64_t yoe = year - era * 400;
    int64_t doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// Compile-time L1-cache-resident calendar lookup table (eliminates 8 integer divisions per timestamp)
struct PrecomputedCalendar {
    int32_t month_epoch_days[TUNABLE_CAL_NUM_YEARS][13];
    uint8_t max_dom[TUNABLE_CAL_NUM_YEARS][13];

    constexpr PrecomputedCalendar() : month_epoch_days{}, max_dom{} {
        for (int y = 0; y < TUNABLE_CAL_NUM_YEARS; ++y) {
            int real_y = TUNABLE_CAL_BASE_YEAR + y;
            for (int m = 1; m <= 12; ++m) {
                month_epoch_days[y][m] = static_cast<int32_t>(days_from_civil(real_y, m, 1));
                max_dom[y][m] = static_cast<uint8_t>(max_days_in_month(real_y, m));
            }
        }
    }
};

inline constexpr PrecomputedCalendar kCalendar{};

// "YYYY-MM-DDTHH:MM:SS" core (caller has checked the length (>= 19), 'T' and the suffix).
// SSE: "YYYY-MM-DDTHH:MM" in one 16-byte load; digits and separators validated with one mask;
// digit pairs combined by pmaddubsw into u16 lanes YYh YYl MM DD hh mm, range-checked in the
// vector. year and hh*3600+mm*60 come from one pmaddwd; no store/reload. ":SS" is scalar.
[[gnu::always_inline]] inline bool parse_iso_core(const char* s, int64_t& out_ms) {
    const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s));
    const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
    const __m128i sep_pos = _mm_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0);
    const __m128i sep_chr = _mm_setr_epi8(0, 0, 0, 0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0);
    const __m128i is_digit = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    const __m128i is_sep = _mm_cmpeq_epi8(v, sep_chr);
    const __m128i ok_chars = _mm_blendv_epi8(is_digit, is_sep, sep_pos);
    // u16 lanes: YY(hi) YY(lo) MM DD hh mm 0 0 (each 0..99 once the digits are valid)
    const __m128i pairs = _mm_shuffle_epi8(d, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
    const __m128i v2 = _mm_maddubs_epi16(pairs, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0));
    const __m128i out_of_range = _mm_or_si128(
        _mm_cmpgt_epi16(_mm_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0), v2),
        _mm_cmpgt_epi16(v2, _mm_setr_epi16(99, 99, 12, 31, 23, 59, 0, 0)));
    // i32 lanes: year, (unused), hh*3600 + mm*60, 0
    const __m128i v3 = _mm_madd_epi16(v2, _mm_setr_epi16(100, 1, 0, 0, 3600, 60, 0, 0));
    const unsigned int s0 = static_cast<unsigned int>(static_cast<unsigned char>(s[17])) - '0';
    const unsigned int s1 = static_cast<unsigned int>(static_cast<unsigned char>(s[18])) - '0';
    const uint32_t ss = s0 * 10 + s1;
    if ((_mm_movemask_epi8(_mm_andnot_si128(out_of_range, ok_chars)) != 0xFFFF) |
        (s[16] != ':') | (s0 > 9) | (s1 > 9) | (ss > 59)) return false;
    const int year = _mm_cvtsi128_si32(v3);
    const int month = _mm_extract_epi16(v2, 2);
    const int day = _mm_extract_epi16(v2, 3);
    const uint32_t hm_sec = static_cast<uint32_t>(_mm_extract_epi32(v3, 2));
    int32_t days;
    const unsigned int y_idx = static_cast<unsigned int>(year - TUNABLE_CAL_BASE_YEAR);
    if (y_idx < static_cast<unsigned int>(TUNABLE_CAL_NUM_YEARS)) [[likely]] {
        if (day > kCalendar.max_dom[y_idx][month]) return false;
        days = kCalendar.month_epoch_days[y_idx][month] + (day - 1);
    } else {
        if (day > max_days_in_month(year, month)) return false;
        days = static_cast<int32_t>(days_from_civil(year, month, day));
    }
    out_ms = static_cast<int64_t>(days) * TUNABLE_DAY_MS + static_cast<int64_t>(hm_sec + ss) * 1000LL;
    return true;
}

// F1 timestamp parser for ingest_ts (YYYY-MM-DDTHH:MM:SSZ)
[[gnu::always_inline]] inline bool parse_timestamp_f1(std::string_view sv, int64_t& out_ms) {
    if (sv.size() != 20) return false;
    const char* s = sv.data();
    if (((s[10] ^ 'T') | (s[19] ^ 'Z')) != 0) return false;
    return parse_iso_core(s, out_ms);
}

[[gnu::always_inline]] inline bool parse_timestamp(std::string_view sv, int64_t& out_ms) {
    if (sv.empty()) return false;
    const char* s = sv.data();
    size_t len = sv.size();

    if ((len == 20 || len == 25) && s[10] == 'T') {
        int64_t offset_minutes = 0;
        if (len == 20) {
            if (s[19] != 'Z') return false;
        } else {
            char sign = s[19];
            if (sign != '+' && sign != '-') return false;
            if (s[22] != ':') return false;
            int off_h = parse_2digits(s + 20);
            int off_m = parse_2digits(s + 23);
            if (off_h < 0 || off_h > 23 || off_m < 0 || off_m > 59) return false;
            offset_minutes = static_cast<int64_t>(off_h) * 60 + off_m;
            if (sign == '-') offset_minutes = -offset_minutes;
        }
        int64_t base_ms;
        if (!parse_iso_core(s, base_ms)) return false;
        out_ms = base_ms - offset_minutes * 60000LL;
        return true;
    }

    // Form F3: digits only (epoch milliseconds)
    if (len == 13) {
        // 16-byte load (the 3 bytes past the field are readable: padded chunk buffers / string tails).
        const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s));
        const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
        const __m128i is_digit = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
        if ((_mm_movemask_epi8(is_digit) & 0x1FFF) != 0x1FFF) return false;
        // Right-align the 13 digits in 16 lanes, then 2 -> 4 -> 8 digit combines.
        __m128i t = _mm_shuffle_epi8(d, _mm_setr_epi8(-1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
        t = _mm_maddubs_epi16(t, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        t = _mm_madd_epi16(t, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        t = _mm_packus_epi32(t, t);
        t = _mm_madd_epi16(t, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
        const uint64_t hi = static_cast<uint32_t>(_mm_cvtsi128_si32(t));
        const uint64_t lo = static_cast<uint32_t>(_mm_extract_epi32(t, 1));
        out_ms = static_cast<int64_t>(hi * 100000000ULL + lo);
        return true;
    }
    if (len <= 18) { // < 10^18: cannot overflow int64
        uint64_t val = 0;
        for (size_t i = 0; i < len; ++i) {
            unsigned int d = static_cast<unsigned int>(static_cast<unsigned char>(s[i]) - '0');
            if (d > 9) return false;
            val = val * 10 + d;
        }
        out_ms = static_cast<int64_t>(val);
        return true;
    }
    uint64_t val = 0;
    constexpr uint64_t kMax = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    for (size_t i = 0; i < len; ++i) {
        if (!is_ascii_digit(s[i])) return false;
        uint64_t d = static_cast<uint64_t>(s[i] - '0');
        if (val > (kMax - d) / 10ULL) {
            out_ms = std::numeric_limits<int64_t>::max();
        } else {
            val = val * 10ULL + d;
            out_ms = static_cast<int64_t>(val);
        }
    }
    return true;
}

inline bool parse_price_cents(std::string_view sv, int32_t& out_price) {
    if (sv.empty()) return false;
    int64_t val = 0;
    for (char c : sv) {
        if (!is_ascii_digit(c)) return false;
        val = val * 10 + (c - '0');
        if (val > TUNABLE_MAX_PRICE_CENTS) return false;
    }
    if (val < TUNABLE_MIN_PRICE_CENTS || val > TUNABLE_MAX_PRICE_CENTS) return false;
    out_price = static_cast<int32_t>(val);
    return true;
}

// Fast-path integer ID parser for "p1".."p50000" and "c_1".."c_2000" (rejects leading zeros like "p01")
inline bool try_parse_prefixed_uint(std::string_view sv, const char* prefix, size_t prefix_len, uint32_t max_val, uint32_t& out_id) {
    size_t len = sv.size();
    if (len <= prefix_len || len > prefix_len + 6) return false;
    const char* s = sv.data();
    for (size_t i = 0; i < prefix_len; ++i) {
        if (s[i] != prefix[i]) return false;
    }
    if (s[prefix_len] < '1' || s[prefix_len] > '9') return false;
    uint32_t v = static_cast<uint32_t>(s[prefix_len] - '0');
    for (size_t i = prefix_len + 1; i < len; ++i) {
        unsigned int d = static_cast<unsigned int>(static_cast<unsigned char>(s[i]) - '0');
        if (d > 9) return false;
        v = v * 10 + d;
    }
    if (v > max_val) return false;
    out_id = v;
    return true;
}

// Bitmasks of ',', '\n' and '"' over the 64 bytes at b (bit i = byte b[i]).
struct CsvMasks64 {
    uint64_t comma;
    uint64_t newline;
    uint64_t quote;
};

inline CsvMasks64 csv_masks64(const char* b) {
    const __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
    const __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + 32));
    auto mask = [&](char c) -> uint64_t {
        const __m256i cv = _mm256_set1_epi8(c);
        uint32_t lo = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, cv)));
        uint32_t hi = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, cv)));
        return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
    };
    return CsvMasks64{mask(','), mask('\n'), mask('"')};
}

// Calls cb(fields) for every well-formed record (line) of the slice; malformed and empty
// lines are skipped. A record is well-formed when it has exactly K fields and each field
// holds either no '"' or exactly two, one at each end (the quotes are stripped).
// A last line without '\n' is a record too.
//
// Per 64-byte block: SIMD masks of ',', '\n', '"' (S = separators). Quote validity is
// branch-free over the whole block: a quote must open a field (A: the byte before is a
// separator) or close one (B: the byte after is a separator), never both, and every opening
// quote's field must end at the separator right after a closing quote. The field end of
// each A is found with one add: the carry runs from A through the non-separator bits (~S)
// to the field's separator. Invalid quote positions (rare) go to a sorted list; a line
// holding one is skipped. Separator positions (bit 31 = '\n') are flattened into a small
// batch array; complete lines are then cut with K fixed loads (fast path: K-1 commas, '\n').
// Reads only inside the slice plus slice[len] (the last partial block is copied to a local
// buffer; an empty last field peeks at slice[len], which callers keep readable).
template <size_t K, typename Func>
inline void for_each_csv_record_in_slice(std::string_view slice, Func&& cb) {
    static_assert(K >= 2, "a 1-field record could not tell an empty line from an empty field");
    constexpr uint32_t kNl = 1u << 31;
    constexpr size_t kBatch = TUNABLE_SPLIT_BATCH_BLOCKS;
    const char* base = slice.data();
    const size_t len = slice.size();
    const size_t full_blocks = len / 64;
    const size_t num_blocks = full_blocks + 1;   // the last block is partial (maybe empty)

    alignas(64) char tail[64];
    {
        const size_t rem = len - full_blocks * 64;
        std::memcpy(tail, base + full_blocks * 64, rem);
        std::memset(tail + rem, 0, 64 - rem);
        // A virtual '\n' ends a last line that has none. (len % 64 == 0: the last full block
        // ends the slice, so its last byte is checked here too and tail[0] gets the '\n'.)
        if (len > 0 && base[len - 1] != '\n') tail[rem] = '\n';
    }
    auto block_ptr = [&](size_t k) { return k < full_blocks ? base + k * 64 : tail; };

    uint32_t idx[kBatch * 64 + 2 * K + 8];   // separator positions (+ garbage slots); an unfinished line's (< K) entries stay in front
    size_t n = 0;                        // entries in idx
    std::vector<uint32_t> bad;           // positions of invalid quotes, ascending
    size_t bad_at = 0;
    uint32_t line_start = 0;
    bool skipping = false;               // the current line already has >= K separators: drop it up to its '\n'

    uint64_t prev_sep = 1;               // the byte before the slice ends a line
    unsigned char add_carry = 0;         // carry of (~S + A) into the next block
    uint64_t close_carry = 0;            // B << 1 overflow into the next block
    CsvMasks64 cur = csv_masks64(block_ptr(0));

    for (size_t k = 0; k < num_blocks; k += kBatch) {
        const size_t kend = std::min(num_blocks, k + kBatch);
        // ---- Flatten the separators of this batch ----
        size_t last_nl = SIZE_MAX;       // index of the last '\n' entry
        for (size_t blk = k; blk < kend; ++blk) {
            const CsvMasks64 m = cur;
            uint64_t next_s0 = 1;
            if (blk + 1 < num_blocks) {
                cur = csv_masks64(block_ptr(blk + 1));
                next_s0 = (cur.comma | cur.newline) & 1;
            }
            const uint64_t S = m.comma | m.newline;
            const uint64_t Q = m.quote;
            const uint64_t A = Q & ((S << 1) | prev_sep);
            const uint64_t B = Q & ((S >> 1) | (next_s0 << 63));
            unsigned long long sum;
            add_carry = _addcarry_u64(add_carry, ~S, A, &sum);
            uint64_t badm = (Q & ~(A | B)) | (A & B) | ((sum & S) ^ ((B << 1) | close_carry));
            close_carry = B >> 63;
            prev_sep = S >> 63;
            while (badm) {
                bad.push_back(static_cast<uint32_t>(blk * 64 + _tzcnt_u64(badm)));
                badm = _blsr_u64(badm);
            }

            const uint32_t off = static_cast<uint32_t>(blk * 64);
            const uint64_t nlm = m.newline;
            if (nlm) last_nl = n + static_cast<size_t>(_mm_popcnt_u64(S & (~0ULL >> _lzcnt_u64(nlm)))) - 1;
            uint64_t s = S;
            const size_t cnt = static_cast<size_t>(_mm_popcnt_u64(s));
            uint32_t* out = idx + n;
            // 8 unconditional slots (a line is ~70 bytes: ~7 separators per block); slots past
            // cnt hold garbage that later entries overwrite (idx has 2K spare slots).
            for (int j = 0; j < 8; ++j) {
                out[j] = (off + static_cast<uint32_t>(_tzcnt_u64(s))) | ((_blsi_u64(s) & nlm) ? kNl : 0u);
                s = _blsr_u64(s);
            }
            for (size_t j = 8; j < cnt; ++j) {
                out[j] = (off + static_cast<uint32_t>(_tzcnt_u64(s))) | ((_blsi_u64(s) & nlm) ? kNl : 0u);
                s = _blsr_u64(s);
            }
            n += cnt;
        }

        // ---- Cut the complete lines ----
        size_t c = 0;
        if (last_nl != SIZE_MAX) {
            if (skipping) {
                while (!(idx[c] & kNl)) ++c;
                line_start = (idx[c] & ~kNl) + 1;
                ++c;
                skipping = false;
            }
            while (c <= last_nl) {
                const uint32_t* e = idx + c;
                uint32_t flags = 0;
                for (size_t i = 0; i + 1 < K; ++i) flags |= e[i];
                if (!(flags & kNl) && (e[K - 1] & kNl)) {
                    const uint32_t line_end = e[K - 1] & ~kNl;
                    while (bad_at < bad.size() && bad[bad_at] < line_start) ++bad_at;
                    if (bad_at == bad.size() || bad[bad_at] > line_end) {
                        std::array<std::string_view, K> fields;
                        uint32_t st = line_start;
                        for (size_t i = 0; i < K; ++i) {
                            const uint32_t en = e[i] & ~kNl;
                            const char* fs = base + st;
                            // Valid line: a field is quoted iff its first byte is '"' (an empty
                            // field's first byte is its separator).
                            const size_t q = fs[0] == '"';
                            fields[i] = std::string_view(fs + q, en - st - 2 * q);
                            st = en + 1;
                        }
                        cb(fields);
                    }
                    line_start = line_end + 1;
                    c += K;
                } else {
                    // Fewer or more than K fields: skip to the line's '\n' (c <= last_nl, so one exists).
                    while (!(idx[c] & kNl)) ++c;
                    line_start = (idx[c] & ~kNl) + 1;
                    ++c;
                }
            }
        }
        // ---- Keep the unfinished line ----
        const size_t pending = n - c;
        if (skipping || pending >= K) {
            skipping = true;   // >= K separators before its '\n': malformed
            n = 0;
        } else {
            std::memmove(idx, idx + c, pending * sizeof(uint32_t));
            n = pending;
        }
        if (bad_at == bad.size()) {
            bad.clear();
            bad_at = 0;
        }
    }
}

inline std::string read_file_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) return {};
    auto sz = in.tellg();
    if (sz <= 0) return {};
    std::string buf(static_cast<size_t>(sz), '\0');
    in.seekg(0, std::ios::beg);
    in.read(buf.data(), sz);
    return buf;
}

// A set of CSV files split into fixed-size byte ranges ("tasks") for parallel pread().
// Each task owns the lines that start inside its range; the header line of each file is skipped.
struct CsvFileSet {
    struct Task {
        uint32_t file;
        uint64_t off;
        uint64_t len;
    };
    std::vector<std::string> paths;
    std::vector<uint64_t> sizes;
    std::vector<int> fds;
    std::vector<Task> tasks;
    uint64_t total_bytes = 0;

    CsvFileSet(const std::vector<std::string>& file_paths, size_t chunk_bytes) : paths(file_paths) {
        sizes.resize(paths.size(), 0);
        fds.resize(paths.size(), -1);
        for (size_t f = 0; f < paths.size(); ++f) {
            std::error_code ec;
            uint64_t sz = static_cast<uint64_t>(std::filesystem::file_size(paths[f], ec));
            if (ec) sz = 0;
            if (sz > 0) {
                fds[f] = ::open(paths[f].c_str(), O_RDONLY);
                if (fds[f] < 0) sz = 0;
            }
            sizes[f] = sz;
            total_bytes += sz;
            for (uint64_t off = 0; off < sz; off += chunk_bytes) {
                tasks.push_back(Task{static_cast<uint32_t>(f), off, std::min<uint64_t>(chunk_bytes, sz - off)});
            }
        }
    }
    CsvFileSet(const CsvFileSet&) = delete;
    CsvFileSet& operator=(const CsvFileSet&) = delete;
    ~CsvFileSet() {
        for (int fd : fds) {
            if (fd >= 0) ::close(fd);
        }
    }

    // Reads up to n bytes at off into dst; returns the number of bytes read.
    size_t read_at(uint32_t f, char* dst, size_t n, uint64_t off) const {
        size_t got = 0;
        while (got < n) {
            ssize_t r = ::pread(fds[f], dst + got, n - got, static_cast<off_t>(off + got));
            if (r <= 0) break;
            got += static_cast<size_t>(r);
        }
        return got;
    }
};

// Per-worker reusable buffer that turns a CsvFileSet task into a view of whole lines.
// The view is followed by at least TUNABLE_READ_PAD_BYTES readable bytes.
class CsvChunkReader {
public:
    std::string_view read(const CsvFileSet& fs, size_t task_idx) {
        const CsvFileSet::Task& t = fs.tasks[task_idx];
        const uint64_t size = fs.sizes[t.file];
        const uint64_t read_off = (t.off == 0) ? 0 : t.off - 1;
        const uint64_t nominal_end = t.off + t.len;   // exclusive
        const bool last = nominal_end >= size;
        uint64_t want = std::min<uint64_t>(size, nominal_end + (last ? 0 : TUNABLE_READ_OVERLAP_BYTES)) - read_off;
        ensure(want);
        size_t have = fs.read_at(t.file, buf_.data(), want, read_off);

        // Start: after the first '\n' at abs >= read_off (for off 0 this skips the header).
        // It must lie before the closing-newline search point, or the chunk owns no line.
        size_t start_limit = last ? have : std::min<size_t>(have, nominal_end - 1 - read_off);
        const char* nl0 = static_cast<const char*>(std::memchr(buf_.data(), '\n', start_limit));
        if (!nl0) return {};
        size_t start = static_cast<size_t>(nl0 - buf_.data()) + 1;
        if (last) return std::string_view(buf_.data() + start, have - start);

        // End: after the first '\n' at abs >= nominal_end - 1; read further if needed.
        size_t search = std::min<size_t>(have, nominal_end - 1 - read_off);
        while (true) {
            const char* nl = static_cast<const char*>(std::memchr(buf_.data() + search, '\n', have - search));
            if (nl) return std::string_view(buf_.data() + start, static_cast<size_t>(nl - buf_.data()) + 1 - start);
            if (read_off + have >= size || have < want) {
                return std::string_view(buf_.data() + start, have - start);
            }
            search = have;
            uint64_t more = std::min<uint64_t>(size - (read_off + have), TUNABLE_READ_OVERLAP_BYTES);
            want = have + more;
            ensure(want);
            have += fs.read_at(t.file, buf_.data() + have, more, read_off + have);
        }
    }

private:
    void ensure(size_t n) {
        if (buf_.size() < n + TUNABLE_READ_PAD_BYTES) buf_.resize(n + TUNABLE_READ_PAD_BYTES);
    }
    std::vector<char> buf_;
};

} // namespace pipeline
