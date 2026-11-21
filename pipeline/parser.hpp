#pragma once
#include <array>
#include <cstdint>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace pipeline {

inline bool is_ascii_digit(char c) {
    return c >= '0' && c <= '9';
}

inline int parse_2digits(const char* p) {
    if (!is_ascii_digit(p[0]) || !is_ascii_digit(p[1])) return -1;
    return (p[0] - '0') * 10 + (p[1] - '0');
}

inline int parse_4digits(const char* p) {
    if (!is_ascii_digit(p[0]) || !is_ascii_digit(p[1]) ||
        !is_ascii_digit(p[2]) || !is_ascii_digit(p[3])) return -1;
    return (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
}

inline bool is_leap_year(int year) {
    return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

inline int max_days_in_month(int year, int month) {
    static constexpr int kDays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) return 0;
    if (month == 2 && is_leap_year(year)) return 29;
    return kDays[month];
}

// Howard Hinnant's days_from_civil (matches generator/src/text.rs)
inline int64_t days_from_civil(int64_t year, int64_t month, int64_t day) {
    if (month <= 2) year -= 1;
    int64_t era = (year >= 0 ? year : year - 399) / 400;
    int64_t yoe = year - era * 400;
    int64_t doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// Strict timestamp parser for F1, F2, F3 -> UTC epoch milliseconds.
inline bool parse_timestamp(std::string_view sv, int64_t& out_ms) {
    if (sv.empty()) return false;
    const char* s = sv.data();
    size_t len = sv.size();

    // Check if all characters are ASCII digits (Form F3)
    bool all_digits = true;
    for (size_t i = 0; i < len; ++i) {
        if (!is_ascii_digit(s[i])) {
            all_digits = false;
            break;
        }
    }
    if (all_digits) {
        // F3: digits only, leading zeros allowed. Very large numbers saturate to INT64_MAX (outside window).
        uint64_t val = 0;
        constexpr uint64_t kMax = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        for (size_t i = 0; i < len; ++i) {
            uint64_t d = static_cast<uint64_t>(s[i] - '0');
            if (val > (kMax - d) / 10ULL) {
                out_ms = std::numeric_limits<int64_t>::max();
                return true;
            }
            val = val * 10ULL + d;
        }
        out_ms = static_cast<int64_t>(val);
        return true;
    }

    // Must be Form F1 (len 20: YYYY-MM-DDTHH:MM:SSZ) or Form F2 (len 25: YYYY-MM-DDTHH:MM:SS+HH:MM)
    if (len != 20 && len != 25) return false;
    if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;

    int year = parse_4digits(s);
    int month = parse_2digits(s + 5);
    int day = parse_2digits(s + 8);
    int hour = parse_2digits(s + 11);
    int minute = parse_2digits(s + 14);
    int second = parse_2digits(s + 17);

    if (year < 0 || month < 1 || month > 12) return false;
    if (day < 1 || day > max_days_in_month(year, month)) return false;
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return false;

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

    int64_t days = days_from_civil(year, month, day);
    int64_t sec_of_day = static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60 + second;
    out_ms = days * 86400000LL + sec_of_day * 1000LL - offset_minutes * 60000LL;
    return true;
}

// Strict purchase price parser: ASCII digits only, value in [1, 10000000].
inline bool parse_price_cents(std::string_view sv, int32_t& out_price) {
    if (sv.empty()) return false;
    int64_t val = 0;
    for (char c : sv) {
        if (!is_ascii_digit(c)) return false;
        val = val * 10 + (c - '0');
        if (val > 10000000LL) return false;
    }
    if (val < 1 || val > 10000000LL) return false;
    out_price = static_cast<int32_t>(val);
    return true;
}

// Strict CSV line parser for exact column count K.
// Rules:
// - Empty line is malformed.
// - No value ever holds a comma, quote, or newline, quoted or not: every comma separates fields.
// - Any field may be wrapped in double quotes ("u1" -> u1, "" -> empty).
// - A line with wrong number of fields, or a quote anywhere except around a whole field, is malformed.
template <size_t K>
inline bool parse_csv_line(std::string_view line, std::array<std::string_view, K>& fields) {
    if (line.empty()) return false;
    const char* p = line.data();
    const char* end = p + line.size();

    for (size_t i = 0; i < K; ++i) {
        if (p < end && *p == '"') {
            ++p; // skip opening quote
            const char* fstart = p;
            while (p < end && *p != '"' && *p != ',') {
                ++p;
            }
            if (p == end || *p != '"') {
                return false; // missing closing quote before comma or end
            }
            fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
            ++p; // skip closing quote
            if (i + 1 < K) {
                if (p == end || *p != ',') return false;
                ++p; // consume comma
            } else {
                if (p != end) return false; // trailing characters or extra fields
            }
        } else {
            const char* fstart = p;
            while (p < end && *p != ',' && *p != '"') {
                ++p;
            }
            if (p < end && *p == '"') {
                return false; // quote inside an unquoted field
            }
            fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
            if (i + 1 < K) {
                if (p == end) return false; // too few fields
                ++p; // consume comma
            } else {
                if (p != end) return false; // too many fields
            }
        }
    }
    return true;
}

// Read entire file into memory buffer (binary mode, preserving \r) and iterate data lines (skipping line 1 header).
template <typename Func>
inline void for_each_csv_data_line_in_buffer(std::string_view content, Func&& cb) {
    // Skip header row (first line up to '\n')
    size_t pos = content.find('\n');
    if (pos == std::string_view::npos) return;
    pos += 1;

    const char* data = content.data();
    size_t size = content.size();
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

} // namespace pipeline
