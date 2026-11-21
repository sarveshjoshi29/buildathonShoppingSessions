#pragma once
// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstdint>

namespace pipeline {
constexpr int64_t TUNABLE_MIN_PRICE_CENTS = 1LL;
constexpr int64_t TUNABLE_MAX_PRICE_CENTS = 10000000LL;
constexpr int64_t TUNABLE_DAY_MS = 86400000LL;
constexpr int TUNABLE_CAL_BASE_YEAR = 1970;
constexpr int TUNABLE_CAL_NUM_YEARS = 200; // Precomputed L1 calendar table for 1970..2169
} // namespace pipeline
// ============================================================================

#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
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

// Branch-collapsed SWAR-style F1 timestamp parser for ingest_ts (YYYY-MM-DDTHH:MM:SSZ)
inline bool parse_timestamp_f1(std::string_view sv, int64_t& out_ms) {
    if (sv.size() != 20) return false;
    const char* s = sv.data();
    if (((s[10] ^ 'T') | (s[19] ^ 'Z') | (s[4] ^ '-') | (s[7] ^ '-') | (s[13] ^ ':') | (s[16] ^ ':')) != 0) {
        return false;
    }

    unsigned int y0 = static_cast<unsigned char>(s[0]) - '0';
    unsigned int y1 = static_cast<unsigned char>(s[1]) - '0';
    unsigned int y2 = static_cast<unsigned char>(s[2]) - '0';
    unsigned int y3 = static_cast<unsigned char>(s[3]) - '0';
    unsigned int m0 = static_cast<unsigned char>(s[5]) - '0';
    unsigned int m1 = static_cast<unsigned char>(s[6]) - '0';
    unsigned int d0 = static_cast<unsigned char>(s[8]) - '0';
    unsigned int d1 = static_cast<unsigned char>(s[9]) - '0';
    unsigned int h0 = static_cast<unsigned char>(s[11]) - '0';
    unsigned int h1 = static_cast<unsigned char>(s[12]) - '0';
    unsigned int mi0 = static_cast<unsigned char>(s[14]) - '0';
    unsigned int mi1 = static_cast<unsigned char>(s[15]) - '0';
    unsigned int s0 = static_cast<unsigned char>(s[17]) - '0';
    unsigned int s1 = static_cast<unsigned char>(s[18]) - '0';

    if ((y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) |
        (m0 > 9) | (m1 > 9) | (d0 > 9) | (d1 > 9) |
        (h0 > 9) | (h1 > 9) | (mi0 > 9) | (mi1 > 9) |
        (s0 > 9) | (s1 > 9)) {
        return false;
    }

    int year = static_cast<int>(y0 * 1000 + y1 * 100 + y2 * 10 + y3);
    int month = static_cast<int>(m0 * 10 + m1);
    int day = static_cast<int>(d0 * 10 + d1);
    int hour = static_cast<int>(h0 * 10 + h1);
    int minute = static_cast<int>(mi0 * 10 + mi1);
    int second = static_cast<int>(s0 * 10 + s1);

    if ((month < 1) | (month > 12) | (day < 1) | (hour > 23) | (minute > 59) | (second > 59)) return false;

    int64_t days;
    unsigned int y_idx = static_cast<unsigned int>(year - TUNABLE_CAL_BASE_YEAR);
    if (y_idx < static_cast<unsigned int>(TUNABLE_CAL_NUM_YEARS)) {
        if (day > kCalendar.max_dom[y_idx][month]) return false;
        days = kCalendar.month_epoch_days[y_idx][month] + (day - 1);
    } else {
        if (day > max_days_in_month(year, month)) return false;
        days = days_from_civil(year, month, day);
    }

    int64_t sec_of_day = static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60 + second;
    out_ms = days * TUNABLE_DAY_MS + sec_of_day * 1000LL;
    return true;
}

inline bool parse_timestamp(std::string_view sv, int64_t& out_ms) {
    if (sv.empty()) return false;
    const char* s = sv.data();
    size_t len = sv.size();

    if ((len == 20 || len == 25) && s[10] == 'T') {
        if (((s[4] ^ '-') | (s[7] ^ '-') | (s[13] ^ ':') | (s[16] ^ ':')) != 0) return false;

        unsigned int y0 = static_cast<unsigned char>(s[0]) - '0';
        unsigned int y1 = static_cast<unsigned char>(s[1]) - '0';
        unsigned int y2 = static_cast<unsigned char>(s[2]) - '0';
        unsigned int y3 = static_cast<unsigned char>(s[3]) - '0';
        unsigned int m0 = static_cast<unsigned char>(s[5]) - '0';
        unsigned int m1 = static_cast<unsigned char>(s[6]) - '0';
        unsigned int d0 = static_cast<unsigned char>(s[8]) - '0';
        unsigned int d1 = static_cast<unsigned char>(s[9]) - '0';
        unsigned int h0 = static_cast<unsigned char>(s[11]) - '0';
        unsigned int h1 = static_cast<unsigned char>(s[12]) - '0';
        unsigned int mi0 = static_cast<unsigned char>(s[14]) - '0';
        unsigned int mi1 = static_cast<unsigned char>(s[15]) - '0';
        unsigned int s0 = static_cast<unsigned char>(s[17]) - '0';
        unsigned int s1 = static_cast<unsigned char>(s[18]) - '0';

        if ((y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) |
            (m0 > 9) | (m1 > 9) | (d0 > 9) | (d1 > 9) |
            (h0 > 9) | (h1 > 9) | (mi0 > 9) | (mi1 > 9) |
            (s0 > 9) | (s1 > 9)) {
            return false;
        }

        int year = static_cast<int>(y0 * 1000 + y1 * 100 + y2 * 10 + y3);
        int month = static_cast<int>(m0 * 10 + m1);
        int day = static_cast<int>(d0 * 10 + d1);
        int hour = static_cast<int>(h0 * 10 + h1);
        int minute = static_cast<int>(mi0 * 10 + mi1);
        int second = static_cast<int>(s0 * 10 + s1);

        if ((month < 1) | (month > 12) | (day < 1) | (hour > 23) | (minute > 59) | (second > 59)) return false;

        int64_t days;
        unsigned int y_idx = static_cast<unsigned int>(year - TUNABLE_CAL_BASE_YEAR);
        if (y_idx < static_cast<unsigned int>(TUNABLE_CAL_NUM_YEARS)) {
            if (day > kCalendar.max_dom[y_idx][month]) return false;
            days = kCalendar.month_epoch_days[y_idx][month] + (day - 1);
        } else {
            if (day > max_days_in_month(year, month)) return false;
            days = days_from_civil(year, month, day);
        }

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

        int64_t sec_of_day = static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60 + second;
        out_ms = days * TUNABLE_DAY_MS + sec_of_day * 1000LL - offset_minutes * 60000LL;
        return true;
    }

    // Form F3: digits only
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

template <size_t K, typename Func>
inline void for_each_csv_record_in_slice(std::string_view slice, Func&& cb) {
    const char* p = slice.data();
    const char* end = p + slice.size();
    std::array<std::string_view, K> fields;

    while (p < end) {
        if (*p == '\n') {
            ++p;
            continue;
        }
        bool ok = true;
        for (size_t i = 0; i < K; ++i) {
            if (p < end && *p == '"') {
                ++p;
                const char* fstart = p;
                while (p < end && *p != '"' && *p != ',' && *p != '\n') ++p;
                if (p == end || *p != '"') {
                    ok = false;
                    break;
                }
                fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
                ++p;
                if (i + 1 < K) {
                    if (p == end || *p != ',') {
                        ok = false;
                        break;
                    }
                    ++p;
                } else {
                    if (p < end) {
                        if (*p != '\n') {
                            ok = false;
                            break;
                        }
                        ++p;
                    }
                }
            } else {
                const char* fstart = p;
                if (i + 1 < K) {
                    while (p < end && *p != ',' && *p != '\n' && *p != '"') ++p;
                    if (p == end || *p != ',') {
                        ok = false;
                        break;
                    }
                    fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
                    ++p;
                } else {
                    while (p < end && *p != '\n' && *p != ',' && *p != '"') ++p;
                    if (p < end && *p != '\n') {
                        ok = false;
                        break;
                    }
                    fields[i] = std::string_view(fstart, static_cast<size_t>(p - fstart));
                    if (p < end) ++p;
                }
            }
        }
        if (ok) {
            cb(fields);
        } else {
            if (p < end && *p == '\n') {
                ++p;
            } else if (p < end) {
                const char* nl = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                p = nl ? (nl + 1) : end;
            }
        }
    }
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

struct MappedFile {
    const char* data_ptr = nullptr;
    size_t data_size = 0;
#ifdef __linux__
    int fd = -1;
#else
    std::string buf;
#endif

    MappedFile() = default;
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    MappedFile(MappedFile&& o) noexcept
        : data_ptr(o.data_ptr), data_size(o.data_size)
#ifdef __linux__
        , fd(o.fd)
#else
        , buf(std::move(o.buf))
#endif
    {
        o.data_ptr = nullptr;
        o.data_size = 0;
#ifdef __linux__
        o.fd = -1;
#endif
    }

    MappedFile& operator=(MappedFile&& o) noexcept {
        if (this != &o) {
            close();
            data_ptr = o.data_ptr;
            data_size = o.data_size;
#ifdef __linux__
            fd = o.fd;
            o.fd = -1;
#else
            buf = std::move(o.buf);
#endif
            o.data_ptr = nullptr;
            o.data_size = 0;
        }
        return *this;
    }

    bool open(const std::string& path) {
#ifdef __linux__
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat st;
        if (::fstat(fd, &st) < 0 || st.st_size <= 0) {
            ::close(fd);
            fd = -1;
            return false;
        }
        data_size = static_cast<size_t>(st.st_size);
        void* p = ::mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (p == MAP_FAILED) {
            ::close(fd);
            fd = -1;
            data_size = 0;
            return false;
        }
        ::madvise(p, data_size, MADV_SEQUENTIAL);
        data_ptr = static_cast<const char*>(p);
        return true;
#else
        buf = read_file_binary(path);
        data_ptr = buf.data();
        data_size = buf.size();
        return !buf.empty();
#endif
    }

    void close() {
#ifdef __linux__
        if (data_ptr && data_size > 0) {
            ::munmap(const_cast<char*>(data_ptr), data_size);
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
#else
        std::string().swap(buf);
#endif
        data_ptr = nullptr;
        data_size = 0;
    }

    ~MappedFile() {
        close();
    }

    std::string_view view() const {
        return std::string_view(data_ptr, data_size);
    }
};

inline std::vector<std::string_view> build_csv_chunks(const std::vector<MappedFile>& files, size_t chunk_bytes) {
    std::vector<std::string_view> chunks;
    for (const auto& mf : files) {
        std::string_view v = mf.view();
        if (v.empty()) continue;
        const char* hdr_nl = static_cast<const char*>(std::memchr(v.data(), '\n', v.size()));
        if (!hdr_nl) continue;
        size_t pos = static_cast<size_t>(hdr_nl - v.data()) + 1;
        size_t sz = v.size();
        while (pos < sz) {
            size_t target = pos + chunk_bytes;
            if (target >= sz) {
                chunks.emplace_back(v.data() + pos, sz - pos);
                break;
            }
            const char* nl = static_cast<const char*>(std::memchr(v.data() + target, '\n', sz - target));
            size_t end_pos = nl ? (static_cast<size_t>(nl - v.data()) + 1) : sz;
            chunks.emplace_back(v.data() + pos, end_pos - pos);
            pos = end_pos;
        }
    }
    return chunks;
}

} // namespace pipeline
