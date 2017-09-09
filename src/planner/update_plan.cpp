//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_plan.cpp
//
// Identification: src/planner/update_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/update_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "type/types.h"

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
                       std::unique_ptr<const planner::ProjectInfo> project_info)
    : target_table_(table),
      project_info_(std::move(project_info)),
      update_primary_key_(false) {
  LOG_TRACE("Creating an Update Plan");

  if (project_info_ != nullptr) {
    for (const auto target : project_info_->GetTargetList()) {
      auto col_id = target.first;
      update_primary_key_ =
          target_table_->GetSchema()->GetColumn(col_id).IsPrimary();
      if (update_primary_key_)
        break;
    }
  }
}

void UpdatePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Update");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

void UpdatePlan::PerformBinding(BindingContext &binding_context) {
  BindingContext input_context;

  const auto &children = GetChildren();
  PL_ASSERT(children.size() == 1);

  children[0]->PerformBinding(input_context);

  auto *scan = static_cast<planner::AbstractScan *>(children[0].get());
  scan->GetAttributes(ais_);

  // Do projection (if one exists)
  if (GetProjectInfo() != nullptr) {
    std::vector<const BindingContext *> inputs = {&input_context};
    GetProjectInfo()->PerformRebinding(binding_context, inputs);
  }

bool UpdatePlan::Equals(planner::AbstractPlan &plan) const {
  return (*this == plan);
}

bool UpdatePlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::UpdatePlan &>(rhs);
  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PL_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  // Project info
  auto *proj_info = GetProjectInfo();
  auto *other_proj_info = other.GetProjectInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr))
    return false;
  if (proj_info && *proj_info != *other_proj_info)
    return false;

  // Update primary key
  if (GetUpdatePrimaryKey() != other.GetUpdatePrimaryKey())
    return false;

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
