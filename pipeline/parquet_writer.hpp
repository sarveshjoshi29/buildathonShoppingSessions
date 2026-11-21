#pragma once
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace parquet {

// Minimal zero-dependency Apache Parquet v1 writer using Thrift Compact Protocol.
struct ThriftWriter {
    std::vector<uint8_t> buf;

    void write_u8(uint8_t b) { buf.push_back(b); }

    void write_varint(uint64_t v) {
        while (v >= 0x80) {
            buf.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        buf.push_back(static_cast<uint8_t>(v));
    }

    void write_zigzag(int64_t n) {
        uint64_t z = (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
        write_varint(z);
    }

    void write_field_header(int16_t field_id, int16_t& prev_id, uint8_t ctype) {
        int16_t delta = field_id - prev_id;
        if (delta > 0 && delta <= 15) {
            write_u8(static_cast<uint8_t>((delta << 4) | ctype));
        } else {
            write_u8(ctype);
            write_zigzag(field_id);
        }
        prev_id = field_id;
    }

    void write_i32_field(int16_t field_id, int16_t& prev_id, int32_t val) {
        write_field_header(field_id, prev_id, 5); // CT_I32
        write_zigzag(val);
    }

    void write_i64_field(int16_t field_id, int16_t& prev_id, int64_t val) {
        write_field_header(field_id, prev_id, 6); // CT_I64
        write_zigzag(val);
    }

    void write_string_field(int16_t field_id, int16_t& prev_id, std::string_view s) {
        write_field_header(field_id, prev_id, 8); // CT_BINARY
        write_varint(s.size());
        buf.insert(buf.end(), s.begin(), s.end());
    }

    void write_list_header(int16_t field_id, int16_t& prev_id, uint8_t elem_type, size_t count) {
        write_field_header(field_id, prev_id, 9); // CT_LIST
        if (count < 15) {
            write_u8(static_cast<uint8_t>((count << 4) | elem_type));
        } else {
            write_u8(static_cast<uint8_t>(0xF0 | elem_type));
            write_varint(count);
        }
    }

    void write_struct_begin(int16_t field_id, int16_t& prev_id) {
        write_field_header(field_id, prev_id, 12); // CT_STRUCT
    }

    void write_struct_end() {
        write_u8(0x00); // CT_STOP
    }
};

enum class ColType : int32_t {
    INT32 = 1,
    INT64 = 2,
    BYTE_ARRAY = 6
};

struct ColSpec {
    std::string name;
    ColType type;
    bool optional;
    int32_t converted_type; // -1 for none, 0 for UTF8, 6 for DATE
};

struct ColChunkWriteInfo {
    int64_t file_offset;
    int64_t total_size;
};

inline void append_u32_le(std::vector<uint8_t>& out, uint32_t v) {
    uint8_t b[4];
    std::memcpy(b, &v, 4);
    out.insert(out.end(), b, b + 4);
}

inline std::vector<uint8_t> make_page_header(int32_t uncompressed_size, int32_t num_rows) {
    ThriftWriter tw;
    int16_t prev = 0;
    tw.write_i32_field(1, prev, 0); // PageType::DATA_PAGE = 0
    tw.write_i32_field(2, prev, uncompressed_size);
    tw.write_i32_field(3, prev, uncompressed_size);
    tw.write_struct_begin(5, prev); // data_page_header
    {
        int16_t dprev = 0;
        tw.write_i32_field(1, dprev, num_rows);
        tw.write_i32_field(2, dprev, 0); // Encoding::PLAIN = 0
        tw.write_i32_field(3, dprev, 3); // Encoding::RLE = 3
        tw.write_i32_field(4, dprev, 3); // Encoding::RLE = 3
        tw.write_struct_end();
    }
    tw.write_struct_end();
    return tw.buf;
}

struct UserDailyRow {
    std::string_view user_id;
    int32_t day;
    int64_t sessions;
    int64_t events;
    int64_t revenue_cents;
    std::string_view top_category;
    bool has_top_category;
};

struct CampaignDailyRow {
    std::string_view campaign_id;
    std::string_view channel;
    int32_t day;
    int64_t sessions;
    int64_t purchasing_sessions;
    int64_t revenue_cents;
    int64_t assisted_sessions;
};

inline void write_parquet_file(
    const std::string& path,
    const std::vector<ColSpec>& specs,
    int64_t num_rows,
    const std::vector<std::vector<uint8_t>>& col_payloads
) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open output parquet file: " + path);
    }
    out.write("PAR1", 4);
    int64_t current_offset = 4;

    std::vector<ColChunkWriteInfo> chunks;
    int64_t total_row_group_bytes = 0;

    if (num_rows > 0) {
        chunks.resize(specs.size());
        for (size_t i = 0; i < specs.size(); ++i) {
            const auto& payload = col_payloads[i];
            auto hdr = make_page_header(static_cast<int32_t>(payload.size()), static_cast<int32_t>(num_rows));
            int64_t col_start = current_offset;
            out.write(reinterpret_cast<const char*>(hdr.data()), hdr.size());
            out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
            int64_t col_size = static_cast<int64_t>(hdr.size() + payload.size());
            current_offset += col_size;
            chunks[i] = {col_start, col_size};
            total_row_group_bytes += col_size;
        }
    }

    // Build FileMetaData
    ThriftWriter fw;
    int16_t fprev = 0;
    fw.write_i32_field(1, fprev, 1); // version = 1

    // schema: 1 root + specs.size() leaves
    fw.write_list_header(2, fprev, 12, specs.size() + 1);
    {
        // root schema element
        int16_t sprev = 0;
        fw.write_string_field(4, sprev, "schema");
        fw.write_i32_field(5, sprev, static_cast<int32_t>(specs.size()));
        fw.write_struct_end();
    }
    for (const auto& s : specs) {
        int16_t sprev = 0;
        fw.write_i32_field(1, sprev, static_cast<int32_t>(s.type));
        fw.write_i32_field(3, sprev, s.optional ? 1 : 0); // 0=REQUIRED, 1=OPTIONAL
        fw.write_string_field(4, sprev, s.name);
        if (s.converted_type >= 0) {
            fw.write_i32_field(6, sprev, s.converted_type);
        }
        fw.write_struct_end();
    }

    fw.write_i64_field(3, fprev, num_rows); // num_rows

    // row_groups
    size_t num_rg = (num_rows > 0) ? 1 : 0;
    fw.write_list_header(4, fprev, 12, num_rg);
    if (num_rows > 0) {
        int16_t rg_prev = 0;
        fw.write_list_header(1, rg_prev, 12, specs.size()); // columns
        for (size_t i = 0; i < specs.size(); ++i) {
            int16_t cc_prev = 0;
            fw.write_i64_field(2, cc_prev, chunks[i].file_offset);
            fw.write_struct_begin(3, cc_prev); // ColumnMetaData
            {
                int16_t cm_prev = 0;
                fw.write_i32_field(1, cm_prev, static_cast<int32_t>(specs[i].type));
                fw.write_list_header(2, cm_prev, 5, 2); // encodings: PLAIN(0), RLE(3)
                fw.write_zigzag(0);
                fw.write_zigzag(3);
                fw.write_list_header(3, cm_prev, 8, 1); // path_in_schema
                fw.write_varint(specs[i].name.size());
                fw.buf.insert(fw.buf.end(), specs[i].name.begin(), specs[i].name.end());
                fw.write_i32_field(4, cm_prev, 0); // CompressionCodec::UNCOMPRESSED = 0
                fw.write_i64_field(5, cm_prev, num_rows);
                fw.write_i64_field(6, cm_prev, chunks[i].total_size);
                fw.write_i64_field(7, cm_prev, chunks[i].total_size);
                fw.write_i64_field(9, cm_prev, chunks[i].file_offset);
                fw.write_struct_end();
            }
            fw.write_struct_end();
        }
        fw.write_i64_field(2, rg_prev, total_row_group_bytes);
        fw.write_i64_field(3, rg_prev, num_rows);
        fw.write_struct_end();
    }

    fw.write_struct_end(); // end FileMetaData

    out.write(reinterpret_cast<const char*>(fw.buf.data()), fw.buf.size());
    uint32_t footer_len = static_cast<uint32_t>(fw.buf.size());
    out.write(reinterpret_cast<const char*>(&footer_len), 4);
    out.write("PAR1", 4);
}

inline void write_user_daily_parquet(const std::string& path, const std::vector<UserDailyRow>& rows) {
    std::vector<ColSpec> specs = {
        {"user_id", ColType::BYTE_ARRAY, false, 0},
        {"day", ColType::INT32, false, 6},
        {"sessions", ColType::INT64, false, -1},
        {"events", ColType::INT64, false, -1},
        {"revenue_cents", ColType::INT64, false, -1},
        {"top_category", ColType::BYTE_ARRAY, true, 0}
    };

    int64_t n = static_cast<int64_t>(rows.size());
    std::vector<std::vector<uint8_t>> payloads(6);

    if (n > 0) {
        payloads[0].reserve(n * 12);
        payloads[1].resize(n * 4);
        payloads[2].resize(n * 8);
        payloads[3].resize(n * 8);
        payloads[4].resize(n * 8);

        int32_t* p_day = reinterpret_cast<int32_t*>(payloads[1].data());
        int64_t* p_sess = reinterpret_cast<int64_t*>(payloads[2].data());
        int64_t* p_ev = reinterpret_cast<int64_t*>(payloads[3].data());
        int64_t* p_rev = reinterpret_cast<int64_t*>(payloads[4].data());

        // Optional top_category: RLE/bit-packed definition levels (bit-width = 1)
        size_t num_groups = (rows.size() + 7) / 8;
        std::vector<uint8_t> def_bits(num_groups, 0);

        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& r = rows[i];
            append_u32_le(payloads[0], static_cast<uint32_t>(r.user_id.size()));
            payloads[0].insert(payloads[0].end(), r.user_id.begin(), r.user_id.end());

            p_day[i] = r.day;
            p_sess[i] = r.sessions;
            p_ev[i] = r.events;
            p_rev[i] = r.revenue_cents;

            if (r.has_top_category) {
                def_bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }

        // Build RLE definition level block for payloads[5]
        ThriftWriter rle_hdr;
        rle_hdr.write_varint((static_cast<uint64_t>(num_groups) << 1) | 1ULL);
        uint32_t rle_len = static_cast<uint32_t>(rle_hdr.buf.size() + def_bits.size());
        append_u32_le(payloads[5], rle_len);
        payloads[5].insert(payloads[5].end(), rle_hdr.buf.begin(), rle_hdr.buf.end());
        payloads[5].insert(payloads[5].end(), def_bits.begin(), def_bits.end());

        for (size_t i = 0; i < rows.size(); ++i) {
            if (rows[i].has_top_category) {
                const auto& tc = rows[i].top_category;
                append_u32_le(payloads[5], static_cast<uint32_t>(tc.size()));
                payloads[5].insert(payloads[5].end(), tc.begin(), tc.end());
            }
        }
    }

    write_parquet_file(path, specs, n, payloads);
}

inline void write_campaign_daily_parquet(const std::string& path, const std::vector<CampaignDailyRow>& rows) {
    std::vector<ColSpec> specs = {
        {"campaign_id", ColType::BYTE_ARRAY, false, 0},
        {"channel", ColType::BYTE_ARRAY, false, 0},
        {"day", ColType::INT32, false, 6},
        {"sessions", ColType::INT64, false, -1},
        {"purchasing_sessions", ColType::INT64, false, -1},
        {"revenue_cents", ColType::INT64, false, -1},
        {"assisted_sessions", ColType::INT64, false, -1}
    };

    int64_t n = static_cast<int64_t>(rows.size());
    std::vector<std::vector<uint8_t>> payloads(7);

    if (n > 0) {
        payloads[0].reserve(n * 10);
        payloads[1].reserve(n * 10);
        payloads[2].resize(n * 4);
        payloads[3].resize(n * 8);
        payloads[4].resize(n * 8);
        payloads[5].resize(n * 8);
        payloads[6].resize(n * 8);

        int32_t* p_day = reinterpret_cast<int32_t*>(payloads[2].data());
        int64_t* p_sess = reinterpret_cast<int64_t*>(payloads[3].data());
        int64_t* p_psess = reinterpret_cast<int64_t*>(payloads[4].data());
        int64_t* p_rev = reinterpret_cast<int64_t*>(payloads[5].data());
        int64_t* p_ass = reinterpret_cast<int64_t*>(payloads[6].data());

        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& r = rows[i];
            append_u32_le(payloads[0], static_cast<uint32_t>(r.campaign_id.size()));
            payloads[0].insert(payloads[0].end(), r.campaign_id.begin(), r.campaign_id.end());

            append_u32_le(payloads[1], static_cast<uint32_t>(r.channel.size()));
            payloads[1].insert(payloads[1].end(), r.channel.begin(), r.channel.end());

            p_day[i] = r.day;
            p_sess[i] = r.sessions;
            p_psess[i] = r.purchasing_sessions;
            p_rev[i] = r.revenue_cents;
            p_ass[i] = r.assisted_sessions;
        }
    }

    write_parquet_file(path, specs, n, payloads);
}

} // namespace parquet
