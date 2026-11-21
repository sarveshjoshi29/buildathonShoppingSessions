#pragma once
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

namespace pipeline {

// Fast 64-bit FNV-1a / splitmix64 string hash
inline uint64_t hash_bytes(std::string_view sv) {
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : sv) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return h;
}

// Cache-aware single-array Disjoint Set Union (DSU) + contiguous string arena for Linked IDs.
// - During unions: parent[x] < 0 means x is a root with size = -parent[x];
//                 parent[x] >= 0 means parent index.
// - Uses path halving during find() so every operation only touches the single 4-byte `parent` array.
// - After finalize(): parent[x] directly stores the node index of the byte-wise smallest user_id
//   in x's connected component (O(1) direct lookup, no more tree traversal).
struct LinkResolver {
    // Contiguous string arena for all user_ids that appear in links/*.csv
    std::vector<char> arena;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;

    // Single-array DSU: 4 bytes per node
    std::vector<int32_t> parent;

    // Open-addressed hash table: maps string -> node_id (uint32_t, 0xFFFFFFFF = empty)
    std::vector<uint32_t> table;
    uint64_t mask = 0;

    void init(size_t expected_nodes = 1024) {
        size_t cap = 1;
        while (cap < expected_nodes * 2) cap <<= 1;
        if (cap < 1024) cap = 1024;
        table.assign(cap, 0xFFFFFFFFu);
        mask = cap - 1;
        arena.reserve(expected_nodes * 12);
        offsets.reserve(expected_nodes);
        lengths.reserve(expected_nodes);
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
                uint64_t pos = hash_bytes(get_str(id)) & new_mask;
                while (new_table[pos] != 0xFFFFFFFFu) {
                    pos = (pos + 1) & new_mask;
                }
                new_table[pos] = id;
            }
        }
        table.swap(new_table);
        mask = new_mask;
    }

    uint32_t intern(std::string_view sv) {
        if (offsets.size() * 2 >= table.size()) {
            rehash();
        }
        uint64_t pos = hash_bytes(sv) & mask;
        while (true) {
            uint32_t id = table[pos];
            if (id == 0xFFFFFFFFu) {
                uint32_t new_id = static_cast<uint32_t>(offsets.size());
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), sv.begin(), sv.end());
                offsets.push_back(off);
                lengths.push_back(static_cast<uint32_t>(sv.size()));
                parent.push_back(-1); // root of size 1
                table[pos] = new_id;
                return new_id;
            }
            if (lengths[id] == sv.size() &&
                std::memcmp(arena.data() + offsets[id], sv.data(), sv.size()) == 0) {
                return id;
            }
            pos = (pos + 1) & mask;
        }
    }

    // Single-array find with path halving
    int32_t find_root(int32_t x) {
        while (parent[x] >= 0) {
            int32_t p = parent[x];
            if (parent[p] < 0) return p;
            parent[x] = parent[p];
            x = parent[x];
        }
        return x;
    }

    // Single-array union by size
    void unite(uint32_t a, uint32_t b) {
        int32_t ra = find_root(static_cast<int32_t>(a));
        int32_t rb = find_root(static_cast<int32_t>(b));
        if (ra == rb) return;
        // Remember: parent[ra] and parent[rb] are negative sizes (-size)
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
        uint32_t ida = intern(a);
        uint32_t idb = intern(b);
        if (ida != idb) {
            unite(ida, idb);
        }
    }

    // Flatten the DSU so that parent[i] directly holds the node index of the
    // byte-wise smallest user_id in node i's connected component.
    void finalize() {
        size_t n = offsets.size();
        if (n == 0) return;

        // First pass: for each root, find the node with the byte-wise smallest string.
        // We can reuse a temporary array of size n initialized to root itself.
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
        // Second pass: overwrite parent[i] with the canonical smallest node index!
        for (size_t i = 0; i < n; ++i) {
            parent[i] = static_cast<int32_t>(best_node[roots[i]]);
        }
    }

    // Read-only thread-safe lookup after finalize():
    // Returns the canonical (byte-wise smallest) user_id for sv.
    // If sv never appeared in links/*.csv, it is its own canonical user_id.
    std::string_view resolve(std::string_view sv) const {
        if (offsets.empty()) return sv;
        uint64_t pos = hash_bytes(sv) & mask;
        while (true) {
            uint32_t id = table[pos];
            if (id == 0xFFFFFFFFu) {
                return sv; // Not in links/*.csv -> person of size 1, named by itself
            }
            if (lengths[id] == sv.size() &&
                std::memcmp(arena.data() + offsets[id], sv.data(), sv.size()) == 0) {
                return get_str(static_cast<uint32_t>(parent[id]));
            }
            pos = (pos + 1) & mask;
        }
    }
};

} // namespace pipeline
