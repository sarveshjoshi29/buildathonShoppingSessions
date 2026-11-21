#pragma once
// ============================================================================
// TUNABLE PARAMETERS (Must remain at the start of the file per AGENTS.md)
// ============================================================================
#include <cstddef>

namespace pipeline {
constexpr size_t TUNABLE_ARENA_ALIGN_BYTES = size_t{2} << 20; // 2 MiB: transparent huge page size on x86-64
constexpr bool TUNABLE_ARENA_USE_HUGEPAGES = true;            // madvise(MADV_HUGEPAGE) on arena memory
} // namespace pipeline
// ============================================================================

#include <cstdint>
#include <new>
#include <type_traits>

#ifdef __linux__
#include <sys/mman.h>
#endif

namespace pipeline {

// Per-thread bump arena for the large shard buffers.
//
// Thousands of shard vectors of a few hundred KB each would each get their own
// 4 KiB-page mmap from malloc, so first touch costs one page fault per 4 KiB.
// One 2 MiB-aligned region with MADV_HUGEPAGE faults once per 2 MiB instead.
// Not thread-safe: one arena per worker thread. Memory is released only at exit.
class HugeArena {
public:
    HugeArena() = default;
    HugeArena(const HugeArena&) = delete;
    HugeArena& operator=(const HugeArena&) = delete;

    // Makes the arena empty with room for `bytes`. Memory already mapped (and faulted in)
    // is reused when it is big enough; blocks handed out earlier become invalid.
    void prepare(size_t bytes) {
        used_ = 0;
        if (base_ != nullptr && bytes <= cap_) return;
        init(bytes);
    }

private:
    // Reserves address space for `bytes`; pages are committed on first touch.
    void init(size_t bytes) {
        base_ = nullptr;
        cap_ = 0;
        size_t cap = (bytes + TUNABLE_ARENA_ALIGN_BYTES - 1) & ~(TUNABLE_ARENA_ALIGN_BYTES - 1);
        if (cap == 0) return;
#ifdef __linux__
        size_t span = cap + TUNABLE_ARENA_ALIGN_BYTES;
        void* raw = ::mmap(nullptr, span, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (raw == MAP_FAILED) return;
        uintptr_t a = (reinterpret_cast<uintptr_t>(raw) + TUNABLE_ARENA_ALIGN_BYTES - 1) &
                      ~static_cast<uintptr_t>(TUNABLE_ARENA_ALIGN_BYTES - 1);
        base_ = reinterpret_cast<char*>(a);
        if (TUNABLE_ARENA_USE_HUGEPAGES) ::madvise(base_, cap, MADV_HUGEPAGE);
        cap_ = cap;
#else
        base_ = static_cast<char*>(::operator new(cap, std::align_val_t{64}));
        cap_ = cap;
#endif
    }

public:
    // Returns nullptr when the arena is exhausted (callers fall back to the heap).
    void* alloc(size_t n, size_t align) {
        size_t off = (used_ + align - 1) & ~(align - 1);
        if (off + n > cap_) return nullptr;
        used_ = off + n;
        return base_ + off;
    }

    bool owns(const void* p) const {
        const char* c = static_cast<const char*>(p);
        return c >= base_ && c < base_ + cap_;
    }

private:
    char* base_ = nullptr;
    size_t cap_ = 0;
    size_t used_ = 0;
};

// std::allocator replacement that carves from a HugeArena and falls back to the heap.
// Freed arena blocks are not reused (vectors here are reserved once and rarely regrow).
template <typename T>
struct ArenaAllocator {
    using value_type = T;
    // Vectors are re-seated with s = Vec(ArenaAllocator(&arena)); the allocator must follow.
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    HugeArena* arena = nullptr;

    ArenaAllocator() = default;
    explicit ArenaAllocator(HugeArena* a) : arena(a) {}
    template <typename U>
    ArenaAllocator(const ArenaAllocator<U>& o) : arena(o.arena) {}

    T* allocate(size_t n) {
        if (arena) {
            if (void* p = arena->alloc(n * sizeof(T), alignof(T))) return static_cast<T*>(p);
        }
        return static_cast<T*>(::operator new(n * sizeof(T)));
    }
    void deallocate(T* p, size_t) {
        if (arena && arena->owns(p)) return;
        ::operator delete(p);
    }
    template <typename U>
    bool operator==(const ArenaAllocator<U>& o) const { return arena == o.arena; }
    template <typename U>
    bool operator!=(const ArenaAllocator<U>& o) const { return arena != o.arena; }
};

} // namespace pipeline
