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

namespace peloton {
namespace test {

class RandomSelectTests : public PelotonTest {};

//void ReadHelper(index::ArtIndex *index,
//                size_t scale_factor, int total_rows,
//                int insert_workers,
//                UNUSED_ATTRIBUTE uint64_t
//                thread_itr) {
//  for (size_t scale_itr = 0; scale_itr < scale_factor; scale_itr++) {
//    int begin_index = std::rand() % (total_rows - 1);
//    int end_index = std::rand() % (total_rows - begin_index) + begin_index + 1;
//    if (end_index >= total_rows) {
//      end_index = total_rows - 1;
//    }
//    index::ARTKey continue_key;
//    std::vector<ItemPointer *> result;
//    size_t result_found = 0;
//    auto &t = index->art_.GetThreadInfo();
//    index->art_.LookupRange(key_to_values[begin_index].key,
//                            key_to_values[end_index].key, continue_key, result,
//                            0, result_found, t);
//    EXPECT_EQ(insert_workers * (end_index - begin_index + 1), result_found);
//  }
//}

TEST_F(RandomSelectTests, RandomSelectRecord) {
  catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(
    type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
    "dept_id", true);
  auto name_column =
    catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
    new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE",
                                               std::move(table_schema), txn);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
    DEFAULT_DB_NAME, "TEST_TABLE", txn);
  txn_manager.CommitTransaction(txn);

  // create an ART index on first column
  txn = txn_manager.BeginTransaction();

  auto tuple_schema = table->GetSchema();
  std::vector<oid_t> key_attrs;
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  index_metadata = new index::IndexMetadata(
    "secondary_art_index", 123, INVALID_OID, INVALID_OID, IndexType::ART,
    IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
    false);
  std::shared_ptr<index::Index> pkey_index(
    index::IndexFactory::GetIndex(index_metadata));
  table->AddIndex(pkey_index);

  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  std::unique_ptr<parser::InsertStatement> insert_node(
    new parser::InsertStatement(InsertType::VALUES));

  std::string name = "TEST_TABLE";
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  parser::TableInfo *table_info = new parser::TableInfo();
  table_info->table_name = name;

  std::string col_1 = "dept_id";
  std::string col_2 = "dept_name";

  table_ref->table_info_.reset(table_info);
  insert_node->table_ref_.reset(table_ref);

  insert_node->columns.push_back(col_1);
  insert_node->columns.push_back(col_2);

  insert_node->insert_values.push_back(
    std::vector<std::unique_ptr<expression::AbstractExpression>>());
  auto& values_ptr = insert_node->insert_values[0];

  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
    new expression::ConstantValueExpression(type::ValueFactory::GetIntegerValue(70))));

  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
    new expression::ConstantValueExpression(type::ValueFactory::GetVarcharValue("Hello"))));

  insert_node->select.reset(new parser::SelectStatement());

  planner::InsertPlan node(table, &insert_node->columns,
                           &insert_node->insert_values);
  executor::InsertExecutor executor(&node, context.get());

  EXPECT_TRUE(executor.Init());
  EXPECT_TRUE(executor.Execute());
  EXPECT_EQ(1, table->GetTupleCount());
  txn_manager.CommitTransaction(txn);

  // insert 10M tuple
  Timer<> timer;
  timer.Start();
  std::srand(std::time(nullptr));
  for (int i = 0; i < 1000000; i++) {
    txn = txn_manager.BeginTransaction();
    printf("insert tuple %d\n", i);
    values_ptr.at(0).reset(new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(std::rand() % 1000000)));
    values_ptr.at(1).reset(new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue(std::to_string(std::rand()))));

    planner::InsertPlan node2(table, &insert_node->columns,
                              &insert_node->insert_values);
    executor::InsertExecutor executor2(&node2, context.get());
    executor2.Init();
    executor2.Execute();
    txn_manager.CommitTransaction(txn);
  }
  timer.Stop();
  LOG_INFO("insert 10M tuples takes %.8lfs", timer.GetDuration());


  // randome read
  timer.Reset();
  timer.Start();

  auto index = table->GetIndex(0);
  std::vector<oid_t> column_ids({0, 1});
  for (int scale = 0; scale < 1000000; scale++) {
    txn = txn_manager.BeginTransaction();
    printf("read tuple %d\n", scale);

    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    std::vector<type::Value> values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    key_column_ids.push_back(0);
    expr_types.push_back(
      ExpressionType::COMPARE_EQUAL);
    values.push_back(type::ValueFactory::GetIntegerValue(std::rand() % 1000000).Copy());

    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

    expression::AbstractExpression *predicate = nullptr;

    // Create plan node.
    planner::IndexScanPlan read_node(table, predicate, column_ids,
                                index_scan_desc);

    std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
    executor::IndexScanExecutor read_executor(&read_node, context.get());

    read_executor.Execute();
    txn_manager.CommitTransaction(txn);
  }


  timer.Stop();
  LOG_INFO("read 10M tuples takes %.8lfs", timer.GetDuration());


  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}
}