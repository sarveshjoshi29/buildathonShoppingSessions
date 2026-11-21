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
constexpr size_t TUNABLE_DATE_CACHE_SLOTS = 64;       // Thread-local direct-mapped YYYY-MM-DD -> epoch-day cache (power of 2)
constexpr size_t TUNABLE_READ_OVERLAP_BYTES = 64 * 1024; // Extra bytes read past a chunk to find its closing '\n'
constexpr size_t TUNABLE_READ_PAD_BYTES = 64;         // Readable slack after chunk data (8-byte ID loads, SIMD)
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

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

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

inline int parse_4digits(const char* p) {
    unsigned int d0 = static_cast<unsigned int>(static_cast<unsigned char>(p[0]) - '0');
    unsigned int d1 = static_cast<unsigned int>(static_cast<unsigned char>(p[1]) - '0');
    unsigned int d2 = static_cast<unsigned int>(static_cast<unsigned char>(p[2]) - '0');
    unsigned int d3 = static_cast<unsigned int>(static_cast<unsigned char>(p[3]) - '0');
    if ((d0 > 9) | (d1 > 9) | (d2 > 9) | (d3 > 9)) return -1;
    return static_cast<int>(d0 * 1000 + d1 * 100 + d2 * 10 + d3);
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

inline uint64_t load_u64(const char* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

// True when all 8 bytes of v are ASCII digits.
inline bool all_digits8(uint64_t v) {
    return (((v + 0x4646464646464646ULL) | (v - 0x3030303030303030ULL)) & 0x8080808080808080ULL) == 0;
}

// Lemire SWAR: 8 ASCII digits (first digit in the lowest byte) -> integer.
inline uint64_t parse_8digits_swar(uint64_t v) {
    v -= 0x3030303030303030ULL;
    v = (v * 10) + (v >> 8);
    v = (((v & 0x000000FF000000FFULL) * (100 + (1000000ULL << 32))) +
         (((v >> 16) & 0x000000FF000000FFULL) * (1 + (10000ULL << 32)))) >> 32;
    return v;
}

// Validated "YYYY-MM-DD" -> days since 1970-01-01 (no cache).
inline bool parse_date_days_slow(const char* s, int32_t& out_days) {
    if ((s[4] != '-') | (s[7] != '-')) return false;
    int year = parse_4digits(s);
    int month = parse_2digits(s + 5);
    int day = parse_2digits(s + 8);
    if ((year | month | day) < 0) return false;
    if ((month < 1) | (month > 12) | (day < 1)) return false;
    unsigned int y_idx = static_cast<unsigned int>(year - TUNABLE_CAL_BASE_YEAR);
    if (y_idx < static_cast<unsigned int>(TUNABLE_CAL_NUM_YEARS)) {
        if (day > kCalendar.max_dom[y_idx][month]) return false;
        out_days = kCalendar.month_epoch_days[y_idx][month] + (day - 1);
    } else {
        if (day > max_days_in_month(year, month)) return false;
        out_days = static_cast<int32_t>(days_from_civil(year, month, day));
    }
    return true;
}

// Thread-local direct-mapped cache of validated dates. Zero-initialised (tag 0 = empty),
// so the thread_local needs no init guard. tag = ("DD" bytes) + 1, never 0 for a stored date.
struct DateCacheSlot {
    uint64_t ymd8;   // bytes "YYYY-MM-"
    uint32_t tag;
    int32_t days;
};
inline thread_local DateCacheSlot tl_date_cache[TUNABLE_DATE_CACHE_SLOTS];

inline bool lookup_date_days(const char* s, int32_t& out_days) {
    uint64_t ymd8 = load_u64(s);
    uint16_t dd16;
    std::memcpy(&dd16, s + 8, 2);
    uint32_t tag = static_cast<uint32_t>(dd16) + 1u;
    size_t idx = (static_cast<size_t>(static_cast<unsigned char>(s[8])) * 10u +
                  static_cast<size_t>(static_cast<unsigned char>(s[9])) +
                  (static_cast<size_t>(static_cast<unsigned char>(s[6])) << 5)) & (TUNABLE_DATE_CACHE_SLOTS - 1);
    DateCacheSlot& e = tl_date_cache[idx];
    if (e.ymd8 == ymd8 && e.tag == tag) {
        out_days = e.days;
        return true;
    }
    int32_t days;
    if (!parse_date_days_slow(s, days)) return false;
    e.ymd8 = ymd8;
    e.tag = tag;
    e.days = days;
    out_days = days;
    return true;
}

// "YYYY-MM-DDTHH:MM:SS" core (caller has checked the length (>= 19), 'T' and the suffix).
#ifdef __AVX2__
// SSE: "YYYY-MM-DDTHH:MM" in one 16-byte load; digits and separators validated with one mask;
// digit pairs combined by pmaddubsw. ":SS" is checked separately. No date cache needed.
inline bool parse_iso_core(const char* s, int64_t& out_ms) {
    const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s));
    const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
    const __m128i sep_pos = _mm_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0);
    const __m128i sep_chr = _mm_setr_epi8(0, 0, 0, 0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0);
    const __m128i is_digit = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    const __m128i is_sep = _mm_cmpeq_epi8(v, sep_chr);
    if (_mm_movemask_epi8(_mm_blendv_epi8(is_digit, is_sep, sep_pos)) != 0xFFFF) return false;
    // u16 lanes: YY(hi) YY(lo) MM DD hh mm
    const __m128i pairs = _mm_shuffle_epi8(d, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
    const __m128i v2 = _mm_maddubs_epi16(pairs, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0));
    alignas(16) uint16_t u[8];
    _mm_store_si128(reinterpret_cast<__m128i*>(u), v2);
    const unsigned int s0 = static_cast<unsigned int>(static_cast<unsigned char>(s[17])) - '0';
    const unsigned int s1 = static_cast<unsigned int>(static_cast<unsigned char>(s[18])) - '0';
    const int year = u[0] * 100 + u[1];
    const int month = u[2];
    const int day = u[3];
    const uint32_t hh = u[4], mm = u[5], ss = s0 * 10 + s1;
    if ((s[16] != ':') | (s0 > 9) | (s1 > 9) | (month < 1) | (month > 12) | (day < 1) |
        (hh > 23) | (mm > 59) | (ss > 59)) return false;
    int32_t days;
    const unsigned int y_idx = static_cast<unsigned int>(year - TUNABLE_CAL_BASE_YEAR);
    if (y_idx < static_cast<unsigned int>(TUNABLE_CAL_NUM_YEARS)) {
        if (day > kCalendar.max_dom[y_idx][month]) return false;
        days = kCalendar.month_epoch_days[y_idx][month] + (day - 1);
    } else {
        if (day > max_days_in_month(year, month)) return false;
        days = static_cast<int32_t>(days_from_civil(year, month, day));
    }
    out_ms = static_cast<int64_t>(days) * TUNABLE_DAY_MS +
             static_cast<int64_t>(hh * 3600u + mm * 60u + ss) * 1000LL;
    return true;
}
#else
// SWAR HH:MM:SS: one 8-byte load at s+11, colon check, 6-digit check, pairwise combine.
inline bool parse_iso_core(const char* s, int64_t& out_ms) {
    int32_t days;
    if (!lookup_date_days(s, days)) return false;
    uint64_t v = load_u64(s + 11);
    if ((v & 0x0000FF0000FF0000ULL) != 0x00003A00003A0000ULL) return false;
    uint64_t x = v & 0xFFFF00FFFF00FFFFULL;
    if (((x + 0x4646004646004646ULL) | (x - 0x3030003030003030ULL)) & 0x8080008080008080ULL) return false;
    uint64_t d = (v - 0x3030003030003030ULL) & 0x0F0F000F0F000F0FULL;
    uint64_t t = d * 10 + (d >> 8);
    uint32_t hh = static_cast<uint32_t>(t & 0xFF);
    uint32_t mm = static_cast<uint32_t>((t >> 24) & 0xFF);
    uint32_t ss = static_cast<uint32_t>((t >> 48) & 0xFF);
    if ((hh > 23) | (mm > 59) | (ss > 59)) return false;
    out_ms = static_cast<int64_t>(days) * TUNABLE_DAY_MS +
             static_cast<int64_t>(hh * 3600u + mm * 60u + ss) * 1000LL;
    return true;
}
#endif

// F1 timestamp parser for ingest_ts (YYYY-MM-DDTHH:MM:SSZ)
inline bool parse_timestamp_f1(std::string_view sv, int64_t& out_ms) {
    if (sv.size() != 20) return false;
    const char* s = sv.data();
    if (((s[10] ^ 'T') | (s[19] ^ 'Z')) != 0) return false;
    return parse_iso_core(s, out_ms);
}

inline bool parse_timestamp(std::string_view sv, int64_t& out_ms) {
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
#ifdef __AVX2__
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
#else
        uint64_t hi8 = load_u64(s);
        if (!all_digits8(hi8)) return false;
        uint64_t val = parse_8digits_swar(hi8);
        for (size_t i = 8; i < 13; ++i) {
            unsigned int d = static_cast<unsigned int>(static_cast<unsigned char>(s[i]) - '0');
            if (d > 9) return false;
            val = val * 10 + d;
        }
        out_ms = static_cast<int64_t>(val);
        return true;
#endif
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
#ifdef __AVX2__
    const __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
    const __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + 32));
    auto mask = [&](char c) -> uint64_t {
        const __m256i cv = _mm256_set1_epi8(c);
        uint32_t lo = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, cv)));
        uint32_t hi = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, cv)));
        return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
    };
    return CsvMasks64{mask(','), mask('\n'), mask('"')};
#else
    CsvMasks64 m{0, 0, 0};
    for (int i = 0; i < 64; ++i) {
        m.comma |= static_cast<uint64_t>(b[i] == ',') << i;
        m.newline |= static_cast<uint64_t>(b[i] == '\n') << i;
        m.quote |= static_cast<uint64_t>(b[i] == '"') << i;
    }
    return m;
#endif
}

template <size_t K>
inline bool parse_csv_line(std::string_view line, std::array<std::string_view, K>& fields) {
    if (line.empty()) return false;
    const char* p = line.data();
    const char* end = p + line.size();

    for (size_t i = 0; i < K; ++i) {
        if (p < end && *p == '"') {
            ++p;
            const char* fstart = p;
            while (p < end && *p != '"' && *p != ',') ++p;
            if (p == end || *p != '"') return false;
            fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
            ++p;
            if (i + 1 < K) {
                if (p == end || *p != ',') return false;
                ++p;
            } else {
                if (p != end) return false;
            }
        } else {
            const char* fstart = p;
            while (p < end && *p != ',' && *p != '"') ++p;
            if (p < end && *p == '"') return false;
            fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
            if (i + 1 < K) {
                if (p == end) return false;
                ++p;
            } else {
                if (p != end) return false;
            }
        }
    }
    return true;
}

// Calls cb(fields) for every well-formed record (line) of the slice; malformed and empty
// lines are skipped. A record is well-formed when it has exactly K fields and each field
// holds either no '"' or exactly two, one at each end (the quotes are stripped).
// A last line without '\n' is a record too.
//
// One pass over 64-byte blocks: SIMD bitmasks of ',', '\n' and '"'; separators are visited
// in order with tzcnt, and a field's quote count comes from popcount over the quote mask.
// Reads only inside the slice (the last partial block is copied to a local buffer).
template <size_t K, typename Func>
inline void for_each_csv_record_in_slice(std::string_view slice, Func&& cb) {
    static_assert(K >= 2, "a 1-field record could not tell an empty line from an empty field");
    const char* base = slice.data();
    const size_t len = slice.size();
    std::array<std::string_view, K> fields;
    size_t fi = 0;          // index of the field being read (K = too many fields)
    size_t fstart = 0;      // slice offset of its first byte
    uint32_t carry_q = 0;   // quotes of the current field seen in earlier blocks
    bool bad = false;       // the current line is malformed

    // Ends the field [fstart, e) holding q quotes; at_eol: its separator ends the line.
    auto end_field = [&](size_t e, uint32_t q, bool at_eol) {
        const char* fs = base + fstart;
        size_t flen = e - fstart;
        // Quoting is random per field, so this is branch-free. A quoted field is valid when it
        // has exactly 2 quotes, one at each end. fs[0] / fs[last] are readable even for an empty
        // field at the slice end (CsvChunkReader pads its buffers; std::string ends in '\0').
        // An invalid field marks the line bad, so its (possibly wrapped) length is never used.
        const bool quoted = q != 0;
        const size_t last = flen - (flen != 0);
        const bool ok = !quoted | ((q == 2) & (flen >= 2) & (fs[0] == '"') & (fs[last] == '"'));
        bad |= !ok;
        fs += quoted;
        flen -= 2 * static_cast<size_t>(quoted);
        if (fi < K) fields[fi] = std::string_view(fs, flen);
        if (at_eol) {
            if (!bad && fi == K - 1) cb(fields);
            fi = 0;
            bad = false;
        } else if (++fi >= K) {
            fi = K;
            bad = true;
        }
        fstart = e + 1;
    };

    alignas(32) char tail[64];
    for (size_t blk = 0; blk < len; blk += 64) {
        const char* b = base + blk;
        const size_t rem = len - blk;
        if (rem < 64) {
            std::memcpy(tail, b, rem);
            std::memset(tail + rem, 0, 64 - rem);
            b = tail;
        }
        const CsvMasks64 m = csv_masks64(b);
        uint64_t seps = m.comma | m.newline;
        uint64_t counted = 0;   // bits at or below the previous separator in this block
        while (seps) {
            const unsigned p = static_cast<unsigned>(__builtin_ctzll(seps));
            seps &= seps - 1;
            const uint64_t below = (1ULL << p) - 1;
            const uint32_t q = carry_q + static_cast<uint32_t>(__builtin_popcountll(m.quote & below & ~counted));
            carry_q = 0;
            counted = (below << 1) | 1;
            end_field(blk + p, q, ((m.newline >> p) & 1) != 0);
        }
        carry_q += static_cast<uint32_t>(__builtin_popcountll(m.quote & ~counted));
    }
    // Last line without a trailing '\n'.
    if (fstart < len || fi > 0) end_field(len, carry_q, true);
}

template <typename Func>
inline void for_each_line_in_slice(std::string_view slice, Func&& cb) {
    const char* data = slice.data();
    size_t size = slice.size();
    size_t pos = 0;
    while (pos < size) {
        const char* line_start = data + pos;
        const char* nl = static_cast<const char*>(std::memchr(line_start, '\n', size - pos));
        if (nl) {
            size_t len = static_cast<size_t>(nl - line_start);
            cb(std::string_view(line_start, len));
            pos += len + 1;
        } else {
            cb(std::string_view(line_start, size - pos));
            break;
        }
    }
}

template <typename Func>
inline void for_each_csv_data_line_in_buffer(std::string_view content, Func&& cb) {
    size_t pos = content.find('\n');
    if (pos == std::string_view::npos || pos + 1 >= content.size()) return;
    for_each_line_in_slice(content.substr(pos + 1), std::forward<Func>(cb));
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
#ifdef __linux__
    std::vector<int> fds;
#endif
    std::vector<Task> tasks;
    uint64_t total_bytes = 0;

    CsvFileSet(const std::vector<std::string>& file_paths, size_t chunk_bytes) : paths(file_paths) {
        sizes.resize(paths.size(), 0);
#ifdef __linux__
        fds.resize(paths.size(), -1);
#endif
        for (size_t f = 0; f < paths.size(); ++f) {
            std::error_code ec;
            uint64_t sz = static_cast<uint64_t>(std::filesystem::file_size(paths[f], ec));
            if (ec) sz = 0;
#ifdef __linux__
            if (sz > 0) {
                fds[f] = ::open(paths[f].c_str(), O_RDONLY);
                if (fds[f] < 0) sz = 0;
            }
#endif
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
#ifdef __linux__
        for (int fd : fds) {
            if (fd >= 0) ::close(fd);
        }
#endif
    }

    // Reads up to n bytes at off into dst; returns the number of bytes read.
    size_t read_at(uint32_t f, char* dst, size_t n, uint64_t off) const {
#ifdef __linux__
        size_t got = 0;
        while (got < n) {
            ssize_t r = ::pread(fds[f], dst + got, n - got, static_cast<off_t>(off + got));
            if (r <= 0) break;
            got += static_cast<size_t>(r);
        }
        return got;
#else
        std::ifstream in(paths[f], std::ios::binary);
        if (!in) return 0;
        in.seekg(static_cast<std::streamoff>(off));
        in.read(dst, static_cast<std::streamsize>(n));
        return static_cast<size_t>(in.gcount());
#endif
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
