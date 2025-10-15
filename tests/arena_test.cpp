#ifndef ARENA_TEST_CPP
#define ARENA_TEST_CPP

#include "arena.h"
#include "random.h"
#include <gtest/gtest.h>
#include <vector>

namespace prism {

TEST(ArenaTest, Empty) {
    Arena arena;
    EXPECT_EQ(0, arena.MemoryUsage());
}

TEST(ArenaTest, SimpleAllocation) {
    Arena arena;
    
    const size_t kSize = 100;
    char* ptr = arena.Allocate(kSize);
    ASSERT_NE(nullptr, ptr);
    
    // Fill with pattern
    for (size_t i = 0; i < kSize; i++) {
        ptr[i] = static_cast<char>(i);
    }
    
    // Verify pattern
    for (size_t i = 0; i < kSize; i++) {
        EXPECT_EQ(static_cast<unsigned char>(ptr[i]), static_cast<unsigned char>(i));
    }
    
    EXPECT_GT(arena.MemoryUsage(), 0);
}

TEST(ArenaTest, AlignedAllocation) {
    Arena arena;
    
    const size_t kSize = 100;
    char* ptr = arena.AllocateAligned(kSize);
    ASSERT_NE(nullptr, ptr);
    
    // Check alignment
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    EXPECT_EQ(0, addr & (align - 1)) << "Pointer not properly aligned";
    
    // Fill and verify
    for (size_t i = 0; i < kSize; i++) {
        ptr[i] = static_cast<char>(i);
    }
    
    for (size_t i = 0; i < kSize; i++) {
        EXPECT_EQ(static_cast<unsigned char>(ptr[i]), static_cast<unsigned char>(i));
    }
}

TEST(ArenaTest, MultipleAllocations) {
    Arena arena;
    std::vector<std::pair<size_t, char*>> allocated;
    
    // Allocate various sizes
    size_t sizes[] = {10, 20, 50, 100, 500, 1000};
    for (size_t size : sizes) {
        char* ptr = arena.Allocate(size);
        ASSERT_NE(nullptr, ptr);
        
        // Fill with unique pattern based on size
        unsigned char pattern = static_cast<unsigned char>(size);
        for (size_t i = 0; i < size; i++) {
            ptr[i] = static_cast<char>(pattern + i);
        }
        
        allocated.push_back({size, ptr});
    }
    
    // Verify all allocations
    for (const auto& [size, ptr] : allocated) {
        unsigned char pattern = static_cast<unsigned char>(size);
        for (size_t i = 0; i < size; i++) {
            EXPECT_EQ(static_cast<unsigned char>(ptr[i]), 
                     static_cast<unsigned char>(pattern + i));
        }
    }
}

TEST(ArenaTest, LargeAllocation) {
    Arena arena;
    
    // Allocate larger than kBlockSize/4 (1024 bytes)
    const size_t kLargeSize = 2000;
    char* ptr = arena.Allocate(kLargeSize);
    ASSERT_NE(nullptr, ptr);
    
    // Fill with pattern
    for (size_t i = 0; i < kLargeSize; i++) {
        ptr[i] = static_cast<char>(i & 0xFF);
    }
    
    // Verify pattern
    for (size_t i = 0; i < kLargeSize; i++) {
        EXPECT_EQ(static_cast<unsigned char>(ptr[i]), i & 0xFF);
    }
    
    // Large allocation should create its own block
    EXPECT_GT(arena.MemoryUsage(), kLargeSize);
}

TEST(ArenaTest, MixedAllocations) {
    Arena arena;
    
    // Mix of regular and aligned allocations
    for (int i = 0; i < 20; i++) {
        size_t size = (i + 1) * 10;
        char* ptr;
        
        if (i % 2 == 0) {
            ptr = arena.Allocate(size);
        } else {
            ptr = arena.AllocateAligned(size);
            
            // Verify alignment for aligned allocations
            uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
            const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
            EXPECT_EQ(0, addr & (align - 1)) << "Aligned pointer not properly aligned";
        }
        
        ASSERT_NE(nullptr, ptr);
        
        // Fill with simple pattern
        unsigned char pattern = static_cast<unsigned char>(i);
        for (size_t j = 0; j < size; j++) {
            ptr[j] = static_cast<char>(pattern);
        }
    }
}

TEST(ArenaTest, MemoryUsage) {
    Arena arena;
    
    EXPECT_EQ(0, arena.MemoryUsage());
    
    // Allocate some memory
    size_t total_allocated = 0;
    for (int i = 0; i < 10; i++) {
        size_t size = 100;
        arena.Allocate(size);
        total_allocated += size;
    }
    
    // Memory usage should be at least what we allocated
    EXPECT_GE(arena.MemoryUsage(), total_allocated);
    
    // Arena uses 4KB blocks, so for 1000 bytes we expect 1 block (4096 + 8 for vector overhead)
    EXPECT_LE(arena.MemoryUsage(), 5000);
}

TEST(ArenaTest, RandomAllocations) {
    std::vector<std::pair<size_t, char*>> allocated;
    Arena arena;
    const int N = 10000;
    size_t bytes = 0;
    Random rnd(301);
    
    for (int i = 0; i < N; i++) {
        size_t s;
        if (i % (N / 10) == 0) {
            s = i;
        } else {
            s = rnd.OneIn(4000) 
                    ? rnd.Uniform(6000)
                    : (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
        }
        if (s == 0) {
            // Our arena disallows size 0 allocations
            s = 1;
        }
        
        char* r;
        if (rnd.OneIn(10)) {
            r = arena.AllocateAligned(s);
        } else {
            r = arena.Allocate(s);
        }
        
        // Fill the "i"th allocation with a known bit pattern
        unsigned char pattern = static_cast<unsigned char>(i & 0xFF);
        for (size_t b = 0; b < s; b++) {
            r[b] = static_cast<char>(pattern);
        }
        
        bytes += s;
        allocated.push_back(std::make_pair(s, r));
        ASSERT_GE(arena.MemoryUsage(), bytes);
        
        if (i > N / 10) {
            // After initial allocations, total usage should not be too wasteful
            // With 4KB blocks and potentially wasted space, 1.5x is reasonable
            ASSERT_LE(arena.MemoryUsage(), bytes * 1.5);
        }
    }
    
    // Verify all allocations still have correct data
    for (size_t i = 0; i < allocated.size(); i++) {
        size_t num_bytes = allocated[i].first;
        const char* p = allocated[i].second;
        unsigned char pattern = static_cast<unsigned char>(i & 0xFF);
        for (size_t b = 0; b < num_bytes; b++) {
            // Check the "i"th allocation for the known bit pattern
            ASSERT_EQ(static_cast<unsigned char>(p[b]), pattern);
        }
    }
}

TEST(ArenaTest, AlignmentEdgeCases) {
    Arena arena;
    
    // Allocate unaligned first
    arena.Allocate(1);
    
    // Then allocate aligned - should handle misalignment
    char* ptr = arena.AllocateAligned(16);
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    EXPECT_EQ(0, addr & (align - 1));
    
    // Multiple aligned allocations in a row
    for (int i = 0; i < 10; i++) {
        char* p = arena.AllocateAligned(8);
        uintptr_t a = reinterpret_cast<uintptr_t>(p);
        EXPECT_EQ(0, a & (align - 1)) << "Allocation " << i << " not aligned";
    }
}

TEST(ArenaTest, BlockBoundaries) {
    Arena arena;
    
    // Allocate some bytes to establish a current block
    // Must be < kBlockSize/4 (1024) to trigger normal allocation
    arena.Allocate(500);
    
    size_t mem_before = arena.MemoryUsage();
    // mem_before should be 4096 + 8 = 4104 (one 4KB block)
    
    // Small allocation that should fit in remaining space
    // Remaining = 4096 - 500 = 3596 bytes
    arena.Allocate(500);
    
    // Should not allocate new block yet
    EXPECT_EQ(mem_before, arena.MemoryUsage());
    
    // Allocation that exceeds remaining space (need 3000 but only ~3096 left)
    arena.Allocate(3500);
    
    // Should allocate new block now
    EXPECT_GT(arena.MemoryUsage(), mem_before);
}

}  // namespace prism

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif
