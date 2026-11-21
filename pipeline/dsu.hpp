#pragma once
// ============================================================================
// TUNABLE PARAMETERS 
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace pipeline {
constexpr size_t TUNABLE_DSU_MIN_CAPACITY = 1024;
constexpr size_t TUNABLE_DSU_LOAD_FACTOR_DENOM = 2; // Rehash when size * 2 >= cap
constexpr size_t TUNABLE_DSU_AVG_ID_BYTES = 12;
constexpr size_t TUNABLE_DSU_BLOOM_WORDS = 4096;    // 4096 * 8 bytes = 32 KB (262,144 bits, L1/L2 resident)
} // namespace pipeline
// ============================================================================

#include <cstring>
#include <string_view>
#include <vector>

namespace pipeline {

// Fast 64-bit word-at-a-time hash (wyhash-inspired 64-bit mixer)
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

// Cache-aware single-array Disjoint Set Union (DSU) + L1/L2 Bloom filter + contiguous string arena.
struct LinkResolver {
    std::vector<char> arena;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    std::vector<uint64_t> hashes;

    // Single-array DSU: 4 bytes per node
    std::vector<int32_t> parent;

    // Open-addressed hash table: maps string -> node_id (uint32_t, 0xFFFFFFFF = empty)
    std::vector<uint32_t> table;
    uint64_t mask = 0;

    // L1/L2-cache-resident Bloom filter to fast-reject unlinked single-ID users without L3 table probes
    std::vector<uint64_t> bloom;
    uint64_t bloom_mask = 0;

    void init(size_t expected_nodes = TUNABLE_DSU_MIN_CAPACITY) {
        size_t cap = 1;
        while (cap < expected_nodes * TUNABLE_DSU_LOAD_FACTOR_DENOM) cap <<= 1;
        if (cap < TUNABLE_DSU_MIN_CAPACITY) cap = TUNABLE_DSU_MIN_CAPACITY;
        table.assign(cap, 0xFFFFFFFFu);
        mask = cap - 1;

        size_t b_words = TUNABLE_DSU_BLOOM_WORDS;
        while (b_words * 32 < cap) b_words <<= 1;
        bloom.assign(b_words, 0ULL);
        bloom_mask = b_words - 1;

        arena.reserve(expected_nodes * TUNABLE_DSU_AVG_ID_BYTES);
        offsets.reserve(expected_nodes);
        lengths.reserve(expected_nodes);
        hashes.reserve(expected_nodes);
        parent.reserve(expected_nodes);
    }

    std::string_view get_str(uint32_t id) const {
        return std::string_view(arena.data() + offsets[id], lengths[id]);
    }

    void rehash() {
        size_t new_cap = table.size() * 2;
        std::vector<uint32_t> new_table(new_cap, 0xFFFFFFFFu);
        uint64_t new_mask = new_cap - 1;
        for (uint32_t id : table) {
            if (id != 0xFFFFFFFFu) {
                uint64_t pos = hashes[id] & new_mask;
                while (new_table[pos] != 0xFFFFFFFFu) {
                    pos = (pos + 1) & new_mask;
                }
                new_table[pos] = id;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    uint32_t intern_with_hash(std::string_view sv, uint64_t h) {
        if (offsets.size() * TUNABLE_DSU_LOAD_FACTOR_DENOM >= table.size()) {
            rehash();
        }
        bloom[(h >> 12) & bloom_mask] |= (1ULL << (h & 63));
        uint64_t pos = h & mask;
        while (true) {
            uint32_t id = table[pos];
            if (id == 0xFFFFFFFFu) {
                uint32_t new_id = static_cast<uint32_t>(offsets.size());
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), sv.begin(), sv.end());
                offsets.push_back(off);
                lengths.push_back(static_cast<uint32_t>(sv.size()));
                hashes.push_back(h);
                parent.push_back(-1);
                table[pos] = new_id;
                return new_id;
            }
            if (hashes[id] == h && lengths[id] == sv.size() &&
                std::memcmp(arena.data() + offsets[id], sv.data(), sv.size()) == 0) {
                return id;
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

    void add_link(std::string_view a, std::string_view b) {
        if (a.empty() || b.empty()) return;
        uint32_t ida = intern_with_hash(a, hash_bytes(a));
        uint32_t idb = intern_with_hash(b, hash_bytes(b));
        if (ida != idb) unite(ida, idb);
    }

    void add_link_prehashed(std::string_view a, uint64_t ha, std::string_view b, uint64_t hb) {
        uint32_t ida = intern_with_hash(a, ha);
        uint32_t idb = intern_with_hash(b, hb);
        if (ida != idb) unite(ida, idb);
    }

    void finalize() {
        size_t n = offsets.size();
        if (n == 0) return;

        std::vector<int32_t> roots(n);
        std::vector<uint32_t> best_node(n);
        for (size_t i = 0; i < n; ++i) {
            best_node[i] = static_cast<uint32_t>(i);
        }
        for (size_t i = 0; i < n; ++i) {
            int32_t r = find_root(static_cast<int32_t>(i));
            roots[i] = r;
            uint32_t cur_best = best_node[r];
            if (get_str(static_cast<uint32_t>(i)) < get_str(cur_best)) {
                best_node[r] = static_cast<uint32_t>(i);
            }
        }
        for (size_t i = 0; i < n; ++i) {
            parent[i] = static_cast<int32_t>(best_node[roots[i]]);
        }
    }

    // Read-only thread-safe lookup after finalize(): checks L1 Bloom filter before probing L3 table!
    std::string_view resolve_with_hash(std::string_view sv, uint64_t& out_hash) const {
        uint64_t h = hash_bytes(sv);
        if (offsets.empty() || ((bloom[(h >> 12) & bloom_mask] & (1ULL << (h & 63))) == 0)) {
            out_hash = h;
            return sv;
        }
        uint64_t pos = h & mask;
        while (true) {
            uint32_t id = table[pos];
            if (id == 0xFFFFFFFFu) {
                out_hash = h;
                return sv;
            }
            if (hashes[id] == h && lengths[id] == sv.size() &&
                std::memcmp(arena.data() + offsets[id], sv.data(), sv.size()) == 0) {
                uint32_t canon_id = static_cast<uint32_t>(parent[id]);
                out_hash = hashes[canon_id];
                return get_str(canon_id);
            }
            pos = (pos + 1) & mask;
        }
    }
};

} // namespace pipeline
