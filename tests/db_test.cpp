#include "db.h"
#include "write_batch.h"
#include <iostream>
#include <cassert>

using namespace prism;

void test_basic_operations()
{
    std::cout << "Test 1: Basic Put/Get/Delete\n";
    
    auto db = DB::Open("test_db");
    
    // Put
    auto s1 = db->Put("key1", "value1");
    assert(s1.has_value() && "Put should succeed");
    
    // Get
    auto r1 = db->Get("key1");
    assert(r1.has_value() && "Get should succeed");
    assert(*r1 == "value1" && "Value should match");
    
    // Delete
    auto s2 = db->Delete("key1");
    assert(s2.has_value() && "Delete should succeed");
    
    // Get after delete
    auto r2 = db->Get("key1");
    assert(!r2.has_value() && "Get should fail after delete");
    
    std::cout << "✓ Basic operations passed\n";
}

void test_batch_write()
{
    std::cout << "Test 2: Batch Write\n";
    
    auto db = DB::Open("test_db");
    
    WriteBatch batch;
    batch.Put("batch_key1", "batch_value1");
    batch.Put("batch_key2", "batch_value2");
    batch.Delete("key1");
    
    auto s = db->Write(batch);
    assert(s.has_value() && "Batch write should succeed");
    
    auto r1 = db->Get("batch_key1");
    assert(r1.has_value() && *r1 == "batch_value1");
    
    auto r2 = db->Get("batch_key2");
    assert(r2.has_value() && *r2 == "batch_value2");
    
    std::cout << "✓ Batch write passed\n";
}

void test_recovery()
{
    std::cout << "Test 3: Recovery from WAL\n";
    
    {
        auto db = DB::Open("test_db");
        db->Put("persistent_key", "persistent_value");
    }
    
    {
        auto db = DB::Open("test_db");
        auto r = db->Get("persistent_key");
        assert(r.has_value() && "Should recover data");
        assert(*r == "persistent_value" && "Recovered value should match");
    }
    
    std::cout << "✓ Recovery passed\n";
}

int main()
{
    test_basic_operations();
    test_batch_write();
    test_recovery();
    
    std::cout << "\n✓ All tests passed!\n";
    return 0;
}