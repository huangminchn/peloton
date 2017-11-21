//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_test.cpp
//
// Identification: /peloton/test/executor/random_select_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "expression/constant_value_expression.h"
#include "parser/insert_statement.h"
#include "planner/insert_plan.h"
#include "type/value_factory.h"
#include "index/index_factory.h"
#include "common/timer.h"
#include "common/logger.h"
#include "planner/index_scan_plan.h"
#include "executor/index_scan_executor.h"

#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "codegen/bloom_filter_accessor.h"
#include "codegen/codegen.h"
#include "codegen/counting_consumer.h"
#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/testing_codegen_util.h"
#include "codegen/util/bloom_filter.h"
#include "common/timer.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "planner/hash_join_plan.h"
#include "planner/seq_scan_plan.h"
#include "sql/testing_sql_util.h"
#include "common/timer.h"
#include "executor/plan_executor.h"
#include "executor/index_scan_executor.h"

namespace peloton {
namespace test {

class RandomSelectTests : public PelotonTest {};

//void ReadHelper(index::ArtIndex *index,
//                size_t scale_factor, int total_rows,
//                int insert_workers,
//                UNUSED_ATTRIBUTE uint64_t
//                thread_itr) {
//
//  for (int i = 0; i < 1000000; i++) {
//    std::string query = "select * from outer_r where c0 = " + std::to_string(std::rand() % 1000000);
//    std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
//      new optimizer::Optimizer());
//    auto plan =
//      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
//
//
//    const std::vector<type::Value> params;
//    std::vector<StatementResult> result;
//    std::vector<int> result_format;
//    result_format.push_back(0);
//    result_format.push_back(1);
//    result_format.push_back(2);
//    executor::ExecuteResult p_status;
//    executor::PlanExecutor::ExecutePlan(plan, txn, params, result, result_format, p_status);
//  }
//}

void InsertTuple(const std::vector<int> &vals, storage::DataTable *table,
                 concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::Tuple tuple{table->GetSchema(), true};
  for (unsigned i = 0; i < vals.size(); i++) {
    tuple.SetValue(i, type::ValueFactory::GetIntegerValue(vals[i]));
  }
  ItemPointer *index_entry_ptr = nullptr;
  auto tuple_slot_id = table->InsertTuple(&tuple, txn, &index_entry_ptr);
  PL_ASSERT(tuple_slot_id.block != INVALID_OID);
  PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
  txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
}

void CreateTable(std::string table_name, int tuple_size,
                 concurrency::Transaction *txn) {
  int curr_size = 0;
  size_t bigint_size = type::Type::GetTypeSize(type::TypeId::INTEGER);
  std::vector<catalog::Column> cols;
  while (curr_size < tuple_size) {
    cols.push_back(
      catalog::Column{type::TypeId::INTEGER, bigint_size,
                      "c" + std::to_string(curr_size / bigint_size), true});
    curr_size += bigint_size;
  }
  auto *catalog = catalog::Catalog::GetInstance();
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(cols));
  catalog->CreateTable(DEFAULT_DB_NAME, table_name, std::move(schema), txn);
}

void LoadTable(std::string table_name, int tuple_size, int target_size,
               std::vector<int> &numbers, std::unordered_set<int> &number_set,
               concurrency::Transaction *txn);

TEST_F(RandomSelectTests, RandomSelectRecord) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn_begin = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn_begin);
  txn_manager.CommitTransaction(txn_begin);

  txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *catalog = catalog::Catalog::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Parameters
  const int outer_tuple_size = 12;

  std::vector<double> selectivities = {0.8, 0.5, 0.2};

  std::srand(std::time(nullptr));

  // Create outer table
  std::string outer_table_name;
  outer_table_name = "outer_r";
  CreateTable(outer_table_name, outer_tuple_size, txn);

  auto *outer_table =
    catalog->GetTableWithName(DEFAULT_DB_NAME, outer_table_name, txn);



  std::string query = "create index art on " + outer_table_name + "(c0)";
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
    new optimizer::Optimizer());
  printf("good 0\n");
  auto plan =
    TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);

  const std::vector<type::Value> params;
  std::vector<StatementResult> result;
  const std::vector<int> result_format;
  executor::ExecuteResult p_status;
  printf("good 1\n");
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result, result_format, p_status);
  printf("good 2\n");


  for (int i = 0; i < 10000000; i++) {
    std::vector<int> vals;
    vals.push_back(std::rand() % 2000000000);
//    vals.push_back(i);
    vals.push_back(std::rand() % 1000000);
    vals.push_back(std::rand() % 1000000);

    InsertTuple(vals, outer_table, txn);
  }
  printf("finish insert!!\n");
  printf("table size = %lu\n", outer_table->GetTupleCount());


  // random select
  Timer<> timer;
  timer.Start();
  for (int i = 0; i < 10000000; i++) {
    std::string  query = "select * from " + outer_table_name + " where c0 = " + std::to_string(std::rand() % 2000000000);
//    std::string query = "select * from " + outer_table_name + " where c0 = 666";
//    printf("good 11\n");
//    std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
//      new optimizer::Optimizer());
    auto plan =
      TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
//    PL_ASSERT(plan->GetPlanNodeType() != ty);
//    PL_ASSERT(plan->GetPlanNodeType() == PlanNodeType::INDEXSCAN);
//    if (plan->GetPlanNodeType() == PlanNodeType::INDEXSCAN) {
//      printf("it is index scan plan!!\n");
//    }

//
//    executor::IndexScanExecutor indexScanExecutor(plan.get(), context.get());
//    indexScanExecutor.Init();
//    indexScanExecutor.Execute();

    const std::vector<type::Value> params;
    std::vector<StatementResult> result;
    std::vector<int> result_format;
    result_format.push_back(0);
//    result_format.push_back(1);
//    result_format.push_back(2);
    executor::ExecuteResult p_status;
    executor::PlanExecutor::ExecutePlan(plan, txn, params, result, result_format, p_status);
  }
  timer.Stop();
  printf("read 10M tuples takes %.8lfs", timer.GetDuration());

  // end of random select

  txn_manager.CommitTransaction(txn);


  txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn_end = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn_end);
}

//TEST_F(RandomSelectTests, RandomSelectRecord) {
//  catalog::Catalog::GetInstance();
//
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  // Insert a table first
//  auto id_column = catalog::Column(
//    type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
//    "dept_id", true);
//  auto name_column =
//    catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);
//
//  std::unique_ptr<catalog::Schema> table_schema(
//    new catalog::Schema({id_column, name_column}));
//
//  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
//  txn_manager.CommitTransaction(txn);
//
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE",
//                                               std::move(table_schema), txn);
//
//  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
//    DEFAULT_DB_NAME, "TEST_TABLE", txn);
//  txn_manager.CommitTransaction(txn);
//
//  // create an ART index on first column
//  txn = txn_manager.BeginTransaction();
//
//  auto tuple_schema = table->GetSchema();
//  std::vector<oid_t> key_attrs;
//  catalog::Schema *key_schema;
//  index::IndexMetadata *index_metadata;
//  key_attrs = {0};
//  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
//  key_schema->SetIndexedColumns(key_attrs);
//  index_metadata = new index::IndexMetadata(
//    "secondary_art_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
//    IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
//    false);
//  std::shared_ptr<index::Index> pkey_index(
//    index::IndexFactory::GetIndex(index_metadata));
//  table->AddIndex(pkey_index);
//
//  txn_manager.CommitTransaction(txn);
//
//  txn = txn_manager.BeginTransaction();
//
//  std::unique_ptr<executor::ExecutorContext> context(
//    new executor::ExecutorContext(txn));
//
//  std::unique_ptr<parser::InsertStatement> insert_node(
//    new parser::InsertStatement(InsertType::VALUES));
//
//  std::string name = "TEST_TABLE";
//  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
//  parser::TableInfo *table_info = new parser::TableInfo();
//  table_info->table_name = name;
//
//  std::string col_1 = "dept_id";
//  std::string col_2 = "dept_name";
//
//  table_ref->table_info_.reset(table_info);
//  insert_node->table_ref_.reset(table_ref);
//
//  insert_node->columns.push_back(col_1);
//  insert_node->columns.push_back(col_2);
//
//  insert_node->insert_values.push_back(
//    std::vector<std::unique_ptr<expression::AbstractExpression>>());
//  auto& values_ptr = insert_node->insert_values[0];
//
//  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
//    new expression::ConstantValueExpression(type::ValueFactory::GetIntegerValue(70))));
//
//  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
//    new expression::ConstantValueExpression(type::ValueFactory::GetVarcharValue("Hello"))));
//
//  insert_node->select.reset(new parser::SelectStatement());
//
//  planner::InsertPlan node(table, &insert_node->columns,
//                           &insert_node->insert_values);
//  executor::InsertExecutor executor(&node, context.get());
//
//  EXPECT_TRUE(executor.Init());
//  EXPECT_TRUE(executor.Execute());
//  EXPECT_EQ(1, table->GetTupleCount());
//  txn_manager.CommitTransaction(txn);
//
//  // insert 10M tuple
//  Timer<> timer;
//  timer.Start();
//  std::srand(std::time(nullptr));
//  for (int i = 0; i < 1000000; i++) {
//    txn = txn_manager.BeginTransaction();
//    printf("insert tuple %d\n", i);
//    values_ptr.at(0).reset(new expression::ConstantValueExpression(
//      type::ValueFactory::GetIntegerValue(std::rand() % 1000000)));
//    values_ptr.at(1).reset(new expression::ConstantValueExpression(
//      type::ValueFactory::GetVarcharValue(std::to_string(std::rand()))));
//
//    planner::InsertPlan node2(table, &insert_node->columns,
//                              &insert_node->insert_values);
//    executor::InsertExecutor executor2(&node2, context.get());
//    executor2.Init();
//    executor2.Execute();
//    txn_manager.CommitTransaction(txn);
//  }
//  timer.Stop();
//  LOG_INFO("insert 10M tuples takes %.8lfs", timer.GetDuration());
//
//
//  // randome read
//  timer.Reset();
//  timer.Start();
//
//  auto index = table->GetIndex(0);
//  std::vector<oid_t> column_ids({0, 1});
//  for (int scale = 0; scale < 1000000; scale++) {
//    txn = txn_manager.BeginTransaction();
//    printf("read tuple %d\n", scale);
//
//    std::vector<oid_t> key_column_ids;
//    std::vector<ExpressionType> expr_types;
//    std::vector<type::Value> values;
//    std::vector<expression::AbstractExpression *> runtime_keys;
//
//    key_column_ids.push_back(0);
//    expr_types.push_back(
//      ExpressionType::COMPARE_EQUAL);
//    values.push_back(type::ValueFactory::GetIntegerValue(std::rand() % 1000000).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
//      index, key_column_ids, expr_types, values, runtime_keys);
//
//    expression::AbstractExpression *predicate = nullptr;
//
//    // Create plan node.
//    planner::IndexScanPlan read_node(table, predicate, column_ids,
//                                index_scan_desc);
//
//    std::unique_ptr<executor::ExecutorContext> context(
//      new executor::ExecutorContext(txn));
//    executor::IndexScanExecutor read_executor(&read_node, context.get());
//
//    read_executor.Execute();
//    txn_manager.CommitTransaction(txn);
//  }
//
//
//  timer.Stop();
//  LOG_INFO("read 10M tuples takes %.8lfs", timer.GetDuration());
//
//
//  // free the database just created
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
//  txn_manager.CommitTransaction(txn);
//}

}
}