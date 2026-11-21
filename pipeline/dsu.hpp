#pragma once
// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>
#include <cstdint>

namespace pipeline {
constexpr size_t TUNABLE_MAX_PACKED_ID_CHARS = 9;      // 9 chars * 7 bits = 63 bits inside uint64_t
constexpr unsigned TUNABLE_DSU_REGION_BITS = 6;        // 64 hash regions (top hash bits) for parallel build
constexpr size_t TUNABLE_DSU_TABLE_LOAD_PCT = 70;      // resolve-table fill factor (non-canonical keys only)
constexpr size_t TUNABLE_DSU_PARALLEL_BLOCK = 1 << 16; // elements per parallel work item
} // namespace pipeline
// ============================================================================

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>
#include <thread>
#include <vector>

#ifdef __BMI2__
#include <immintrin.h>
#endif

namespace pipeline {

// Lossless 63-bit big-endian packing of 1..9 character 7-bit ASCII IDs into a uint64_t.
// Preserves byte-wise lexicographic order and equality; empty -> 0.
// BMI2 path loads 8 bytes (and byte 8) unconditionally: the input must be followed by
// at least 8 readable bytes (CsvChunkReader buffers guarantee TUNABLE_READ_PAD_BYTES).
inline uint64_t pack_id64(std::string_view sv) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(sv.data());
    const size_t n = sv.size();
#ifdef __BMI2__
    uint64_t w;
    std::memcpy(&w, p, 8);
    w = __builtin_bswap64(w);                                   // char0 in the top byte
    uint64_t keep = (n >= 8) ? ~0ULL : ~(~0ULL >> (n * 8));     // n == 0 -> 0
    uint64_t r = _pext_u64(w & keep, 0x7F7F7F7F7F7F7F7FULL) << 7;
    uint64_t tail = (n >= 9) ? static_cast<uint64_t>(p[8] & 0x7Fu) : 0ULL;
    return r | tail;
#else
    uint64_t r = 0;
    size_t m = std::min(n, TUNABLE_MAX_PACKED_ID_CHARS);
    for (size_t i = 0; i < m; ++i) r |= static_cast<uint64_t>(p[i] & 0x7Fu) << (56 - 7 * i);
    return r;
#endif
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

inline uint64_t mulhi64(uint64_t a, uint64_t b) {
#if defined(__SIZEOF_INT128__)
    return static_cast<uint64_t>((static_cast<__uint128_t>(a) * b) >> 64);
#else
    uint64_t a_lo = a & 0xFFFFFFFFu, a_hi = a >> 32, b_lo = b & 0xFFFFFFFFu, b_hi = b >> 32;
    uint64_t mid1 = a_hi * b_lo, mid2 = a_lo * b_hi;
    uint64_t carry = ((a_lo * b_lo >> 32) + (mid1 & 0xFFFFFFFFu) + (mid2 & 0xFFFFFFFFu)) >> 32;
    return a_hi * b_hi + (mid1 >> 32) + (mid2 >> 32) + carry;
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

// Runs f(task) for task in [0, ntasks) on up to nth threads (atomic work counter).
template <typename F>
inline void parallel_tasks(unsigned int nth, size_t ntasks, F&& f) {
    if (ntasks == 0) return;
    if (nth > ntasks) nth = static_cast<unsigned int>(ntasks);
    if (nth <= 1) {
        for (size_t i = 0; i < ntasks; ++i) f(i);
        return;
    }
    std::atomic<size_t> next{0};
    std::vector<std::thread> workers;
    workers.reserve(nth);
    for (unsigned int t = 0; t < nth; ++t) {
        workers.emplace_back([&]() {
            while (true) {
                size_t i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= ntasks) break;
                f(i);
            }
        });
    }
    for (auto& w : workers) w.join();
}

// 16-byte slot: 4 per 64-byte cache line.
struct alignas(16) FlatDsuSlot {
    uint64_t key_id = 0;   // 0 = empty slot
    uint64_t canon_id = 0; // canonical (smallest) PackedId64
};

struct LinkPair {
    uint64_t a;
    uint64_t b;
};

// Maps a PackedId64 user to the smallest PackedId64 of its linked component.
// Only non-canonical users are stored; everyone else resolves to themselves.
// The table is split into 2^REGION_BITS regions by the top hash bits; inside a region the
// home slot is fastrange(hash) over the region's exact (non power-of-2) capacity.
// Lookups are latency-bound (the table is larger than L2), so the pipeline resolves each
// distinct id once per shard (Stage 3b) rather than once per record.
class LinkResolver {
public:
    LinkResolver() {
        region_off_.fill(0);
        region_cap_.fill(0);
    }
    ~LinkResolver() { free_table(); }
    LinkResolver(const LinkResolver&) = delete;
    LinkResolver& operator=(const LinkResolver&) = delete;

    size_t num_entries() const { return num_entries_; }
    size_t table_bytes() const { return table_size_ * sizeof(FlatDsuSlot); }

    void build(const std::vector<std::vector<LinkPair>>& thread_links, unsigned int nth) {
        const size_t T = thread_links.size();
        std::vector<size_t> link_base(T + 1, 0);
        for (size_t t = 0; t < T; ++t) link_base[t + 1] = link_base[t] + thread_links[t].size();
        const size_t L = link_base[T];
        if (L == 0) return;

        // 1. Scatter link endpoints by region: (id, endpoint slot = 2 * link + side).
        struct Ref {
            uint64_t id;
            uint64_t slot;
        };
        std::vector<std::vector<std::vector<Ref>>> scatter(T, std::vector<std::vector<Ref>>(kRegions));
        parallel_tasks(nth, T, [&](size_t t) {
            auto& out = scatter[t];
            size_t est = (thread_links[t].size() * 2) / kRegions;
            est += est / 4 + 16;
            for (auto& v : out) v.reserve(est);
            uint64_t slot = link_base[t] * 2;
            for (const LinkPair& lp : thread_links[t]) {
                out[hash_id64(lp.a) >> kRegionShift].push_back(Ref{lp.a, slot});
                out[hash_id64(lp.b) >> kRegionShift].push_back(Ref{lp.b, slot + 1});
                slot += 2;
            }
        });

        // 2. Per-region interning: endpoint -> (region << kLocalBits) | local node index.
        std::vector<uint32_t> edge(2 * L);
        std::vector<std::vector<uint64_t>> region_ids(kRegions);
        parallel_tasks(nth, kRegions, [&](size_t r) {
            size_t cnt = 0;
            for (size_t t = 0; t < T; ++t) cnt += scatter[t][r].size();
            size_t cap = 64;
            while (cap < cnt * 2) cap <<= 1;
            std::vector<FlatDsuSlot> ht(cap);
            const uint64_t mask = cap - 1;
            auto& ids = region_ids[r];
            ids.reserve(cnt / 2 + 16);
            const uint32_t tag = static_cast<uint32_t>(r) << kLocalBits;
            for (size_t t = 0; t < T; ++t) {
                for (const Ref& ref : scatter[t][r]) {
                    uint64_t pos = hash_id64(ref.id) & mask;
                    while (true) {
                        FlatDsuSlot& s = ht[pos];
                        if (s.key_id == 0) {
                            s.key_id = ref.id;
                            s.canon_id = ids.size();
                            edge[ref.slot] = tag | static_cast<uint32_t>(ids.size());
                            ids.push_back(ref.id);
                            break;
                        }
                        if (s.key_id == ref.id) {
                            edge[ref.slot] = tag | static_cast<uint32_t>(s.canon_id);
                            break;
                        }
                        pos = (pos + 1) & mask;
                    }
                }
                std::vector<Ref>().swap(scatter[t][r]);
            }
        });
        scatter.clear();

        // 3. Dense global node ids.
        std::array<uint32_t, kRegions + 1> node_base{};
        for (size_t r = 0; r < kRegions; ++r) {
            node_base[r + 1] = node_base[r] + static_cast<uint32_t>(region_ids[r].size());
        }
        const size_t N = node_base[kRegions];
        std::vector<uint64_t> node_id(N);
        parallel_tasks(nth, kRegions, [&](size_t r) {
            std::copy(region_ids[r].begin(), region_ids[r].end(), node_id.begin() + node_base[r]);
            std::vector<uint64_t>().swap(region_ids[r]);
        });
        parallel_tasks(nth, blocks(2 * L), [&](size_t b) {
            size_t lo = b * TUNABLE_DSU_PARALLEL_BLOCK, hi = std::min(2 * L, lo + TUNABLE_DSU_PARALLEL_BLOCK);
            for (size_t k = lo; k < hi; ++k) {
                uint32_t e = edge[k];
                edge[k] = node_base[e >> kLocalBits] + (e & kLocalMask);
            }
        });

        // 4. Sequential union-find (union by size, path halving).
        std::vector<int32_t> parent(N, -1);
        auto find = [&](uint32_t x) -> uint32_t {
            while (parent[x] >= 0) {
                int32_t p = parent[x];
                if (parent[p] < 0) return static_cast<uint32_t>(p);
                parent[x] = parent[p];
                x = static_cast<uint32_t>(parent[p]);
            }
            return x;
        };
        for (size_t l = 0; l < L; ++l) {
            uint32_t a = find(edge[2 * l]);
            uint32_t b = find(edge[2 * l + 1]);
            if (a == b) continue;
            if (parent[a] > parent[b]) std::swap(a, b);
            parent[a] += parent[b];
            parent[b] = static_cast<int32_t>(a);
        }
        std::vector<uint32_t>().swap(edge);

        // 5. Parallel roots and component minimum ids.
        std::vector<uint32_t> root(N);
        std::vector<uint64_t> comp_min(N, std::numeric_limits<uint64_t>::max());
        parallel_tasks(nth, blocks(N), [&](size_t b) {
            size_t lo = b * TUNABLE_DSU_PARALLEL_BLOCK, hi = std::min(N, lo + TUNABLE_DSU_PARALLEL_BLOCK);
            for (size_t i = lo; i < hi; ++i) {
                uint32_t x = static_cast<uint32_t>(i);
                while (parent[x] >= 0) x = static_cast<uint32_t>(parent[x]);
                root[i] = x;
                std::atomic_ref<uint64_t> m(comp_min[x]);
                uint64_t v = node_id[i];
                uint64_t cur = m.load(std::memory_order_relaxed);
                while (v < cur && !m.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
                }
            }
        });
        std::vector<int32_t>().swap(parent);

        // 6. Size each region for its non-canonical nodes (node regions == table regions).
        std::array<size_t, kRegions> region_cnt{};
        parallel_tasks(nth, kRegions, [&](size_t r) {
            size_t c = 0;
            for (size_t i = node_base[r]; i < node_base[r + 1]; ++i) c += (comp_min[root[i]] != node_id[i]);
            region_cnt[r] = c;
        });
        size_t total = 0;
        num_entries_ = 0;
        for (size_t r = 0; r < kRegions; ++r) {
            size_t c = region_cnt[r];
            num_entries_ += c;
            region_cap_[r] = (c == 0) ? 0 : (c * 100) / TUNABLE_DSU_TABLE_LOAD_PCT + 1;
            region_off_[r] = total;
            total += region_cap_[r];
        }
        free_table();
        if (total == 0) return;
        table_ = static_cast<FlatDsuSlot*>(::operator new(total * sizeof(FlatDsuSlot), std::align_val_t{64}));
        table_size_ = total;

        // 7. Parallel per-region fill.
        parallel_tasks(nth, kRegions, [&](size_t r) {
            const uint64_t cap = region_cap_[r];
            if (cap == 0) return;
            FlatDsuSlot* base = table_ + region_off_[r];
            std::memset(static_cast<void*>(base), 0, cap * sizeof(FlatDsuSlot));
            for (size_t i = node_base[r]; i < node_base[r + 1]; ++i) {
                uint64_t id = node_id[i];
                uint64_t canon = comp_min[root[i]];
                if (canon == id) continue;
                uint64_t h = hash_id64(id);
                uint64_t pos = home(h, cap);
                while (base[pos].key_id != 0) {
                    if (++pos == cap) pos = 0;
                }
                base[pos].key_id = id;
                base[pos].canon_id = canon;
            }
        });
    }

    // Canonical id of uid; h must be hash_id64(uid). An id absent from the table is its own name.
    uint64_t resolve(uint64_t uid, uint64_t h) const {
        size_t r = static_cast<size_t>(h >> kRegionShift);
        uint64_t cap = region_cap_[r];
        if (cap == 0) return uid;
        const FlatDsuSlot* base = table_ + region_off_[r];
        uint64_t pos = home(h, cap);
        while (true) {
            const FlatDsuSlot& sl = base[pos];
            if (sl.key_id == uid) return sl.canon_id;
            if (sl.key_id == 0) return uid;
            if (++pos == cap) pos = 0;
        }
    }

    uint64_t resolve_id64(uint64_t uid) const { return resolve(uid, hash_id64(uid)); }

    // Start loading the home slot of an id with hash h, so a later resolve(uid, h) hits cache.
    void prefetch(uint64_t h) const {
        size_t r = static_cast<size_t>(h >> kRegionShift);
        uint64_t cap = region_cap_[r];
        if (cap != 0) __builtin_prefetch(table_ + region_off_[r] + home(h, cap));
    }

private:
    static constexpr size_t kRegions = size_t{1} << TUNABLE_DSU_REGION_BITS;
    static constexpr unsigned kRegionShift = 64 - TUNABLE_DSU_REGION_BITS;
    static constexpr unsigned kLocalBits = 32 - TUNABLE_DSU_REGION_BITS;
    static constexpr uint32_t kLocalMask = (1u << kLocalBits) - 1;

    static size_t blocks(size_t n) { return (n + TUNABLE_DSU_PARALLEL_BLOCK - 1) / TUNABLE_DSU_PARALLEL_BLOCK; }
    static uint64_t home(uint64_t h, uint64_t cap) { return mulhi64(h << TUNABLE_DSU_REGION_BITS, cap); }

    void free_table() {
        if (table_) {
            ::operator delete(table_, std::align_val_t{64});
            table_ = nullptr;
        }
        table_size_ = 0;
    }

    FlatDsuSlot* table_ = nullptr;
    size_t table_size_ = 0;
    size_t num_entries_ = 0;
    std::array<uint64_t, kRegions> region_off_;
    std::array<uint64_t, kRegions> region_cap_;
};

} // namespace pipeline
