//
// Created by Min Huang on 9/18/17.
//

#ifndef PELOTON_ART_H
#define PELOTON_ART_H

#pragma once

#include "index/N.h"
#include "index/index.h"

namespace peloton {
namespace index {

class Tree {
public:
  using LoadKeyFunction = void (*)(TID tid, Key &key, IndexMetadata *metadata);

private:
  N *const root;

  TID checkKey(const TID tid, const Key &k) const;

  LoadKeyFunction loadKey;

  Epoche epoche;

  IndexMetadata *metadata;

public:
  enum class CheckPrefixResult : uint8_t {
    Match,
    NoMatch,
    OptimisticMatch
  };

  enum class CheckPrefixPessimisticResult : uint8_t {
    Match,
    NoMatch,
  };

  enum class PCCompareResults : uint8_t {
    Smaller,
    Equal,
    Bigger,
  };
  enum class PCEqualsResults : uint8_t {
    BothMatch,
    Contained,
    NoMatch
  };
  static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

  static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                             uint8_t &nonMatchingKey,
                                                             Prefix &nonMatchingPrefix,
                                                             LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

  static CheckPrefixPessimisticResult checkPrefixPessimisticConditional(uint64_t &version, N *n, const Key &k, const Key &realKey, uint32_t &level,
                                                                        uint32_t realKeyLen, uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix, LoadKeyFunction loadKey,
                                                                        bool &needRestart, IndexMetadata *metadata,
                                                                        bool isCondInsert, std::function<bool(const void *)> predicate,
                                                                        TID val, bool &foundEndOfKey, bool &insertCondition);

  static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint8_t fillKey, const Key &realKey, uint32_t &level, LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

  static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, const Key &realStart, const Key &realEnd, LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

  static bool checkAllValues(const N *n, TID val, std::function<bool(const void *)> predicate);

public:

  Tree(LoadKeyFunction loadKey);

  Tree(const Tree &) = delete;

  Tree(Tree &&t) : root(t.root), loadKey(t.loadKey), epoche(256) { }

  ~Tree();

  ThreadInfo &getThreadInfo();

  TID lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;

  bool lookupNonUniqueKey(const Key &start, const Key &end, Key &realKey, std::vector<ItemPointer *> &result, std::size_t resultLen,
                          std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

  bool lookupRange(const Key &start, const Key &end, Key &continueKey, Key &realStart, Key &realEnd, std::vector<ItemPointer *> &result, std::size_t resultLen,
                   std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

  void fullScan(std::vector<ItemPointer *> &result, std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

  void scanAllLeafNodes(const N* node, std::vector<ItemPointer *> &result, std::size_t &resultCount) const;

  void insert(const Key &k, TID tid, ThreadInfo &epocheInfo, bool &insertSuccess);

  void remove(const Key &k, TID tid, ThreadInfo &epocheInfo);

  bool conditionalInsert(const Key &k, const Key &realKey, TID tid, ThreadInfo &epocheInfo, std::function<bool(const void *)> predicate);

  void setIndexMetadata(IndexMetadata *metadata);
};


}
}

#endif //PELOTON_ART_H
