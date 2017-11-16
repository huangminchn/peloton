//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.cpp
//
// Identification: src/codegen/operator/table_scan_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/index_scan_translator.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "planner/index_scan_plan.h"
#include "index/art_index.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// INDEX SCAN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
IndexScanTranslator::IndexScanTranslator(const planner::IndexScanPlan &index_scan, CompilationContext &context,
                                         Pipeline &pipeline)
  : OperatorTranslator(context, pipeline),
    index_scan_(index_scan),
    index_(*index_scan_.GetIndex().get()) {
  LOG_DEBUG("Constructing IndexScanTranslator ...");


  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  selection_vector_id_ = runtime_state.RegisterState(
    "scanSelVec",
    codegen.ArrayType(codegen.Int32Type(), Vector::kDefaultVectorSize), true);

  LOG_DEBUG("Finished constructing IndexScanTranslator ...");
}

// Produce!
void IndexScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &index = GetIndex();

  LOG_DEBUG("TableScan on [%s] starting to produce tuples ...", index.GetName().c_str());

}

// Index accessor
const index::ArtIndex &IndexScanTranslator::GetIndex() const {
  return dynamic_cast<index::ArtIndex &> (*index_scan_.GetIndex().get());
}
}
}

