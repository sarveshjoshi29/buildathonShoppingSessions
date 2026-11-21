#pragma once
// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace pipeline {
constexpr size_t TUNABLE_DSU_MIN_CAPACITY = 1024;
constexpr size_t TUNABLE_DSU_LOAD_FACTOR_DENOM = 2; // Rehash when size * 2 >= cap
constexpr size_t TUNABLE_DSU_BLOOM_WORDS = 4096;    // 4096 * 8 bytes = 32 KB (262,144 bits)
constexpr size_t TUNABLE_MAX_PACKED_ID_CHARS = 9;   // 9 chars * 7 bits = 63 bits inside uint64_t
} // namespace pipeline
// ============================================================================

#include <algorithm>
#include <cstring>
#include <new>
#include <string_view>
#include <thread>
#include <vector>

namespace pipeline {

// Lossless 63-bit big-endian packing of 1..9 character 7-bit ASCII IDs into an 8-byte uint64_t.
// Preserves exact byte-wise lexicographical ordering (str_a < str_b <=> id_a < id_b) and equality.
inline uint64_t pack_id64(std::string_view sv) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(sv.data());
    switch (sv.size()) {
        case 0: return 0ULL;
        case 1: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56);
        case 2: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49);
        case 3: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42);
        case 4: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                       (static_cast<uint64_t>(p[3] & 0x7Fu) << 35);
        case 5: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                       (static_cast<uint64_t>(p[3] & 0x7Fu) << 35) |
                       (static_cast<uint64_t>(p[4] & 0x7Fu) << 28);
        case 6: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                       (static_cast<uint64_t>(p[3] & 0x7Fu) << 35) |
                       (static_cast<uint64_t>(p[4] & 0x7Fu) << 28) |
                       (static_cast<uint64_t>(p[5] & 0x7Fu) << 21);
        case 7: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                       (static_cast<uint64_t>(p[3] & 0x7Fu) << 35) |
                       (static_cast<uint64_t>(p[4] & 0x7Fu) << 28) |
                       (static_cast<uint64_t>(p[5] & 0x7Fu) << 21) |
                       (static_cast<uint64_t>(p[6] & 0x7Fu) << 14);
        case 8: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                       (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                       (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                       (static_cast<uint64_t>(p[3] & 0x7Fu) << 35) |
                       (static_cast<uint64_t>(p[4] & 0x7Fu) << 28) |
                       (static_cast<uint64_t>(p[5] & 0x7Fu) << 21) |
                       (static_cast<uint64_t>(p[6] & 0x7Fu) << 14) |
                       (static_cast<uint64_t>(p[7] & 0x7Fu) << 7);
        default: return (static_cast<uint64_t>(p[0] & 0x7Fu) << 56) |
                        (static_cast<uint64_t>(p[1] & 0x7Fu) << 49) |
                        (static_cast<uint64_t>(p[2] & 0x7Fu) << 42) |
                        (static_cast<uint64_t>(p[3] & 0x7Fu) << 35) |
                        (static_cast<uint64_t>(p[4] & 0x7Fu) << 28) |
                        (static_cast<uint64_t>(p[5] & 0x7Fu) << 21) |
                        (static_cast<uint64_t>(p[6] & 0x7Fu) << 14) |
                        (static_cast<uint64_t>(p[7] & 0x7Fu) << 7) |
                        static_cast<uint64_t>(p[8] & 0x7Fu);
    }
}

// Fast 64-bit integer hash for PackedId64 (5 register instructions, zero memory reads)
inline uint64_t hash_id64(uint64_t id) {
    uint64_t x = id ^ (id >> 33);
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

// Fast 64-bit word-at-a-time string hash (for fallback string maps)
inline uint64_t mix64(uint64_t a, uint64_t b) {
#if defined(__SIZEOF_INT128__)
    __uint128_t r = static_cast<__uint128_t>(a) * static_cast<__uint128_t>(b);
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
#else
    uint64_t x = a ^ b;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
#endif
}

inline uint64_t hash_bytes(std::string_view sv) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(sv.data());
    size_t len = sv.size();
    uint64_t seed = 0x9e3779b97f4a7c15ULL ^ (len * 0xbf58476d1ce4e5b9ULL);

    if (len >= 8) {
        uint64_t a, b;
        std::memcpy(&a, p, 8);
        std::memcpy(&b, p + len - 8, 8);
        seed = mix64(a ^ 0x517cc1b727220a95ULL, b ^ seed);
        if (len > 16) {
            size_t off = 8;
            while (off + 8 <= len - 8) {
                uint64_t w;
                std::memcpy(&w, p + off, 8);
                seed = mix64(w ^ 0x94d049bb133111ebULL, seed);
                off += 8;
            }
        }
        return seed;
    } else if (len >= 4) {
        uint32_t a, b;
        std::memcpy(&a, p, 4);
        std::memcpy(&b, p + len - 4, 4);
        uint64_t combined = (static_cast<uint64_t>(a) << 32) | b;
        return mix64(combined ^ 0x517cc1b727220a95ULL, seed);
    } else if (len > 0) {
        uint64_t combined = (static_cast<uint64_t>(p[0]) << 16) |
                            (static_cast<uint64_t>(p[len >> 1]) << 8) |
                            static_cast<uint64_t>(p[len - 1]);
        return mix64(combined ^ 0x517cc1b727220a95ULL, seed);
    }
    return seed;
}

// 16-byte aligned flat DSU lookup slot: 4 slots fit in one 64-byte CPU cache line!
// During Stage 0b intern(), `canon_id` temporarily stores the uint32_t DSU node index;
// during finalize(), `canon_id` is replaced in-place with the canonical 64-bit PackedId64!
struct alignas(16) FlatDsuSlot {
    uint64_t key_id = 0;   // 0 = empty slot
    uint64_t canon_id = 0; // canonical (smallest) PackedId64
};

struct LinkResolver {
    std::vector<uint64_t> nodes;
    std::vector<int32_t> parent;
    uint64_t mask = 0;

    std::vector<uint64_t> bloom;
    uint64_t bloom_mask = 0;

    FlatDsuSlot* flat_table = nullptr;
    size_t flat_table_size = 0;

    ~LinkResolver() {
        if (flat_table) {
            ::operator delete(flat_table, std::align_val_t{16});
            flat_table = nullptr;
        }
    }

    void init(size_t total_links = TUNABLE_DSU_MIN_CAPACITY) {
        // Unique linked nodes are at most ~1.35 * total_links; rehash() grows automatically if needed.
        size_t expected_nodes = total_links + (total_links >> 1) + 16;
        size_t cap = TUNABLE_DSU_MIN_CAPACITY;
        while (cap < expected_nodes * TUNABLE_DSU_LOAD_FACTOR_DENOM) cap <<= 1;

        flat_table = static_cast<FlatDsuSlot*>(::operator new(cap * sizeof(FlatDsuSlot), std::align_val_t{16}));
        flat_table_size = cap;
        std::memset(flat_table, 0, cap * sizeof(FlatDsuSlot));
        mask = cap - 1;

        size_t b_words = TUNABLE_DSU_BLOOM_WORDS;
        while (b_words * 32 < cap) b_words <<= 1;
        bloom.assign(b_words, 0ULL);
        bloom_mask = b_words - 1;

        nodes.reserve(expected_nodes);
        parent.reserve(expected_nodes);
    }

    void rehash() {
        size_t new_cap = flat_table_size * 2;
        FlatDsuSlot* new_table = static_cast<FlatDsuSlot*>(::operator new(new_cap * sizeof(FlatDsuSlot), std::align_val_t{16}));
        std::memset(new_table, 0, new_cap * sizeof(FlatDsuSlot));
        uint64_t new_mask = new_cap - 1;
        for (size_t i = 0; i < flat_table_size; ++i) {
            uint64_t key = flat_table[i].key_id;
            if (key != 0) {
                uint64_t pos = hash_id64(key) & new_mask;
                while (new_table[pos].key_id != 0) {
                    pos = (pos + 1) & new_mask;
                }
                new_table[pos] = flat_table[i];
            }
        }
        ::operator delete(flat_table, std::align_val_t{16});
        flat_table = new_table;
        flat_table_size = new_cap;
        mask = new_mask;
    }

    uint32_t intern_id64(uint64_t id, uint64_t h) {
        if (nodes.size() * TUNABLE_DSU_LOAD_FACTOR_DENOM >= flat_table_size) {
            rehash();
        }
        bloom[(h >> 12) & bloom_mask] |= (1ULL << (h & 63));
        uint64_t pos = h & mask;
        while (true) {
            FlatDsuSlot& sl = flat_table[pos];
            if (sl.key_id == 0) {
                uint32_t new_id = static_cast<uint32_t>(nodes.size());
                nodes.push_back(id);
                parent.push_back(-1);
                sl.key_id = id;
                sl.canon_id = new_id;
                return new_id;
            }
            if (sl.key_id == id) {
                return static_cast<uint32_t>(sl.canon_id);
            }
            pos = (pos + 1) & mask;
        }
    }

    int32_t find_root(int32_t x) {
        while (parent[x] >= 0) {
            int32_t p = parent[x];
            if (parent[p] < 0) return p;
            parent[x] = parent[p];
            x = parent[x];
        }
        return x;
    }

    void unite(uint32_t a, uint32_t b) {
        int32_t ra = find_root(static_cast<int32_t>(a));
        int32_t rb = find_root(static_cast<int32_t>(b));
        if (ra == rb) return;
        if (parent[ra] > parent[rb]) {
            int32_t tmp = ra;
            ra = rb;
            rb = tmp;
        }
        parent[ra] += parent[rb];
        parent[rb] = ra;
    }

    void add_link_id64(uint64_t id_a, uint64_t ha, uint64_t id_b, uint64_t hb) {
        uint32_t a = intern_id64(id_a, ha);
        uint32_t b = intern_id64(id_b, hb);
        if (a != b) unite(a, b);
    }

    void finalize(unsigned int num_threads = 1) {
        size_t n = nodes.size();
        if (n == 0) return;

        // Pass 1: Compress every non-root directly to its root and store component minimum PackedId64 in nodes[root]
        for (size_t i = 0; i < n; ++i) {
            int32_t r = find_root(static_cast<int32_t>(i));
            if (r != static_cast<int32_t>(i)) {
                parent[i] = r;
                if (nodes[i] < nodes[r]) {
                    nodes[r] = nodes[i];
                }
            }
        }
        // Pass 2: Propagate canonical PackedId64 from root to all non-root nodes
        for (size_t i = 0; i < n; ++i) {
            int32_t r = parent[i];
            if (r >= 0) {
                nodes[i] = nodes[r];
            }
        }

        // Pass 3: Parallel in-place replacement of node index with canonical PackedId64 inside flat_table
        size_t cap = flat_table_size;
        unsigned int nth = (num_threads > 1 && cap >= 16384) ? num_threads : 1;
        if (nth == 1) {
            for (size_t pos = 0; pos < cap; ++pos) {
                if (flat_table[pos].key_id != 0) {
                    uint32_t id = static_cast<uint32_t>(flat_table[pos].canon_id);
                    flat_table[pos].canon_id = nodes[id];
                }
            }
        } else {
            std::vector<std::thread> workers;
            workers.reserve(nth);
            size_t chunk = (cap + nth - 1) / nth;
            for (unsigned int t = 0; t < nth; ++t) {
                size_t start = t * chunk;
                size_t end = std::min(cap, start + chunk);
                if (start >= end) break;
                workers.emplace_back([&, start, end]() {
                    for (size_t pos = start; pos < end; ++pos) {
                        if (flat_table[pos].key_id != 0) {
                            uint32_t id = static_cast<uint32_t>(flat_table[pos].canon_id);
                            flat_table[pos].canon_id = nodes[id];
                        }
                    }
                });
            }
            for (auto& w : workers) w.join();
        }

        std::vector<uint64_t>().swap(nodes);
        std::vector<int32_t>().swap(parent);
    }

    // 16-byte single-cache-line lookup returning canonical PackedId64
    uint64_t resolve_id64(uint64_t uid) const {
        if (flat_table_size == 0) return uid;
        uint64_t h = hash_id64(uid);
        if ((bloom[(h >> 12) & bloom_mask] & (1ULL << (h & 63))) == 0) {
            return uid;
        }
        uint64_t pos = h & mask;
        while (true) {
            const FlatDsuSlot& sl = flat_table[pos];
            if (sl.key_id == 0) return uid;
            if (sl.key_id == uid) return sl.canon_id;
            pos = (pos + 1) & mask;
        }
    }
};

} // namespace pipeline
