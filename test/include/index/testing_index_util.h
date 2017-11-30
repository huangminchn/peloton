//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_util.h
//
// Identification: test/include/index/testing_index_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "type/types.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TestingIndexUtil
//===--------------------------------------------------------------------===//

class TestingIndexUtil {
 public:
  //===--------------------------------------------------------------------===//
  // Test Cases
  //===--------------------------------------------------------------------===//

  static void BasicTest(const IndexType index_type);

  static void MultiMapInsertTest(const IndexType index_type);

  static void UniqueKeyInsertTest(const IndexType index_type);

  static void UniqueKeyDeleteTest(const IndexType index_type);

  static void NonUniqueKeyDeleteTest(const IndexType index_type);

  static void MultiThreadedInsertTest(const IndexType index_type);

  static void UniqueKeyMultiThreadedTest(const IndexType index_type);

  static void NonUniqueKeyMultiThreadedTest(const IndexType index_type);

  static void NonUniqueKeyMultiThreadedStressTest(const IndexType index_type);

  static void NonUniqueKeyMultiThreadedStressTest2(const IndexType index_type);

  //===--------------------------------------------------------------------===//
  // Utility Methods
  //===--------------------------------------------------------------------===//

  /**
   * Builds an index with 4 columns, the first 2 being indexed
   */
  static index::Index *BuildIndex(const IndexType index_type,
                                  const bool unique_keys);

  static void DestroyIndex(index::Index *index);

  /**
   * Insert helper function
   */
  static void InsertHelper(index::Index *index, type::AbstractPool *pool,
                         size_t scale_factor,
                         UNUSED_ATTRIBUTE uint64_t thread_itr);
  static void InsertHelperMicroBench(index::Index *index, type::AbstractPool *pool,
                           size_t scale_factor, int num_rows,
                           UNUSED_ATTRIBUTE uint64_t thread_itr);
  static void ReadHelper(index::Index *index, type::AbstractPool *pool,
                           size_t scale_factor,
                           UNUSED_ATTRIBUTE uint64_t thread_itr);
  static void ReadHelperMicroBench(index::Index *index, type::AbstractPool *pool,
                                   size_t scale_factor, int num_rows,
                                   UNUSED_ATTRIBUTE uint64_t thread_itr);

  static void DeleteHelperMicroBench(index::Index *index, type::AbstractPool *pool,
                                   size_t scale_factor, int num_rows,
                                   UNUSED_ATTRIBUTE uint64_t thread_itr);

  /**
   * Delete helper function
   */
  static void DeleteHelper(index::Index *index, type::AbstractPool *pool,
                         size_t scale_factor,
                         UNUSED_ATTRIBUTE uint64_t thread_itr);

  static std::shared_ptr<ItemPointer> item0;
  static std::shared_ptr<ItemPointer> item1;
  static std::shared_ptr<ItemPointer> item2;

  struct KeyAndValues {
    std::array<uint64_t, 20> values;
    storage::Tuple *key;
  };
  static std::array<KeyAndValues, 1000000> key_to_values;
  static bool map_populated;
  static void PopulateMap(index::Index &index);
};
}  // namespace test
}  // namespace peloton
