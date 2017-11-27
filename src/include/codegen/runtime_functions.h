//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions.h
//
// Identification: src/include/codegen/runtime_functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include "common/internal_types.h"

namespace peloton {

namespace storage {
class DataTable;
class TileGroup;
class ZoneMap;
class ZoneMapManager;
struct PredicateInfo;
class Tuple;
}  // namespace storage

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace index {
class Index;
struct ResultAndKey;
}

namespace codegen {
namespace util {
class IndexScanIterator;
}
}

namespace codegen {

//===----------------------------------------------------------------------===//
// Various common functions that are called from compiled query plans
//===----------------------------------------------------------------------===//
class RuntimeFunctions {
 public:
  // Calculate the Murmur3 hash of the given buffer of the provided length
  static uint64_t HashMurmur3(const char *buf, uint64_t length, uint64_t seed);

  // Calculate the CRC64 checksum of the given buffer of the provided length
  // using the provided CRC as the initial/running CRC value
  static uint64_t HashCrc64(const char *buf, uint64_t length, uint64_t crc);

  // Get the tile group with the given index from the table.  We can't use
  // the version in DataTable because we need to strip off the shared_ptr
  static storage::TileGroup *GetTileGroup(storage::DataTable *table,
                                          uint64_t tile_group_index);

  static void FillPredicateArray(const expression::AbstractExpression *expr,
                                 storage::PredicateInfo *predicate_array);

  static storage::TileGroup *GetTileGroupByGlobalId(storage::DataTable *table,
                                                    uint64_t tile_group_id);

  // This struct represents the layout (or configuration) of a column in a
  // tile group. A configuration is characterized by two properties: its
  // starting address and its stride.  The former indicates where in memory
  // the first value of the column is, and the latter is the number of bytes
  // to skip over to find successive values of the column.  In a sense, we're
  // doing strided accesses to mimic columnar storage.  In a pure row-store,
  // the stride is equivalent to the size of the tuple. In a pure column-store
  // (without compression), the stride is equivalent to the size of data type.
  struct ColumnLayoutInfo {
    char *column;
    uint32_t stride;
    bool is_columnar;
  };

  // Get the column configuration for every column in the tile group
  static void GetTileGroupLayout(const storage::TileGroup *tile_group,
                                 ColumnLayoutInfo *infos, uint32_t num_cols);

  static void ThrowDivideByZeroException();

  static void ThrowOverflowException();

  static index::ResultAndKey *GetOneResultAndKey();

  static void FreeOneResultAndKey(index::ResultAndKey *result);

  static uint64_t GetTileGroupIdFromResult(index::ResultAndKey* result);

  static int32_t GetTileGroupOffsetFromResult(index::ResultAndKey* result);

  static bool IsValidTileGroup(index::ResultAndKey* result);

  static util::IndexScanIterator *GetIterator(index::Index *index, uint64_t point_key_p, uint64_t low_key_p, uint64_t high_key_p);

  static void DeleteIterator(util::IndexScanIterator *iterator);
};

}  // namespace codegen
}  // namespace peloton
