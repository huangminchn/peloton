//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_sql_test.cpp
//
// Identification: test/sql/optimizer_sql_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "planner/create_plan.h"
#include "planner/order_by_plan.h"
#include "sql/testing_sql_util.h"

using std::vector;
using std::unordered_set;
using std::string;
using std::unique_ptr;
using std::shared_ptr;

namespace peloton {
namespace test {

class OptimizerSQLTests : public PelotonTest {
 protected:
  virtual void SetUp() override {
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create test database
    CreateAndLoadTable();
    optimizer.reset(new optimizer::Optimizer());
  }

  virtual void TearDown() override {
    // Destroy test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }

  /*** Helper functions **/
  void CreateAndLoadTable() {
    // Create database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Create a table first
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");

    // Insert tuples into table
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
    TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");
  }

  // If the query has OrderBy, the result is deterministic. Specify ordered to
  // be true. Otherwise, specify ordered to be false
  void TestUtil(string query, vector<string> ref_result, bool ordered,
                vector<PlanNodeType> expected_plans = {}) {
    LOG_DEBUG("Running Query \"%s\"", query.c_str());

    // Check Plan Nodes are correct if provided
    if (expected_plans.size() > 0) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      auto plan =
          TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
      txn_manager.CommitTransaction(txn);

      auto plan_ptr = plan.get();
      vector<PlanNodeType> actual_plans;
      while (true) {
        actual_plans.push_back(plan_ptr->GetPlanNodeType());
        if (plan_ptr->GetChildren().size() == 0) break;
        plan_ptr = plan_ptr->GetChildren()[0].get();
      }
      EXPECT_EQ(expected_plans, actual_plans);
    }
    LOG_INFO("Before Exec with Opt");
    // Check plan execution results are correct
    TestingSQLUtil::ExecuteSQLQueryWithOptimizer(optimizer, query, result,
                                                 tuple_descriptor, rows_changed,
                                                 error_message);
    LOG_INFO("After Exec with Opt");
    vector<string> actual_result;
    for (unsigned i = 0; i < result.size(); i++)
      actual_result.push_back(
          TestingSQLUtil::GetResultValueAsString(result, i));

    EXPECT_EQ(ref_result.size(), result.size());
    if (ordered) {
      // If deterministic, do comparision with expected result in order
      EXPECT_EQ(ref_result, actual_result);
    } else {
      // If non-deterministic, make sure they have the same set of value
      unordered_set<string> ref_set(ref_result.begin(), ref_result.end());
      for (auto &result_str : actual_result) {
        if (ref_set.count(result_str) == 0) {
          // Test Failed. Print both actual results and ref results
          EXPECT_EQ(ref_result, actual_result);
          break;
        }
      }
    }
  }

 protected:
  unique_ptr<optimizer::AbstractOptimizer> optimizer;
  vector<ResultValue> result;
  vector<FieldInfo> tuple_descriptor;
  string error_message;
  int rows_changed;
};

TEST_F(OptimizerSQLTests, JoinTest) {
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (1, 22, 333);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (2, 11, 000);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (3, 33, 444);");
  // TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4, 00, 555);");

  // Create another table for join
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (1, 22, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (2, 11, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (3, 22, 444);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test1 VALUES (4, 00, 333);");

  // Create another table for join
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);");

  // Insert tuples into table
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, 000);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, 333);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 22, 555);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (4, 00, 000);");

  /************************* Basic Queries (only joins)
   * *******************************/
//  // Product
//  TestUtil("SELECT * FROM test1, test2 WHERE test1.a = 1 AND test2.b = 0",
//           {"1", "22", "333", "4", "0", "0"}, false);
//  printf("good 1\n");
//  TestUtil(
//      "SELECT test.a, test1.b FROM test, test1 "
//      "WHERE test1.b = 22",
//      {"1", "22", "1", "22", "2", "22", "2", "22", "3", "22", "3", "22", "4",
//       "22", "4", "22"},
//      false);
  printf("good 2\n");
  TestUtil(
      "SELECT A.a, B.b, C.c FROM test as A, test1 as B, test2 as C "
      "WHERE B.a = 1 AND A.b = 22 and C.a = 2",
      {"1", "22", "333"}, false);
//  printf("good 3\n");
//  // Simple 2 table join
//  TestUtil("SELECT test.a, test1.a FROM test JOIN test1 ON test.a = test1.a",
//           {"1", "1", "2", "2", "3", "3", "4", "4"}, false);
//  printf("good 4\n");
//  // Where clause to join
//  TestUtil("SELECT test.a, test1.a FROM test, test1 WHERE test.a = test1.a",
//           {"1", "1", "2", "2", "3", "3", "4", "4"}, false);
//  printf("good 5\n");
//  TestUtil(
//      "SELECT test.a, test.b, test1.b, test1.c FROM test, test1 WHERE test.b = "
//      "test1.b",
//      {"1", "22", "22", "333", "1", "22", "22", "444", "2", "11", "11", "0",
//       "4", "0", "0", "333"},
//      false);
//  printf("good 6\n");
//  // 3 table join
//  TestUtil(
//      "SELECT test.a, test.b, test1.b, test2.c FROM test2 "
//      "JOIN test ON test.b = test2.b "
//      "JOIN test1 ON test2.c = test1.c",
//      {"1", "22", "0", "11", "2", "11", "333", "22", "2", "11", "333", "0", "4",
//       "0", "0", "11"},
//      false);
//  printf("good 7\n");
//  // 3 table join with where clause
//  TestUtil(
//      "SELECT test.a, test.b, test1.b, test2.c FROM test2, test, test1 "
//      "WHERE test.b = test2.b AND test2.c = test1.c",
//      {"1", "22", "11", "0", "2", "11", "22", "333", "2", "11", "0", "333", "4",
//       "0", "11", "0"},
//      false);
//  printf("good 8\n");
//  // 3 table join with where clause
//  // This one test NLJoin.
//  // Currently cannot support this query because
//  // the interpreted hash join is broken.
//  TestUtil(
//      "SELECT test.a, test.b, test1.b, test2.c FROM test, test1, test2 "
//      "WHERE test.b = test2.b AND test2.c = test1.c",
//      {"1", "22", "11", "0", "2", "11", "22", "333", "2", "11", "0", "333", "4",
//       "0", "11", "0"},
//      false);
//  printf("good 9\n");
//  // 2 table join with where clause and predicate
//  TestUtil(
//      "SELECT test.a, test1.b FROM test, test1 "
//      "WHERE test.a = test1.a AND test1.b = 22",
//      {"1", "22", "3", "22"}, false);
//printf("good 10\n");
//  // 2 table join with where clause and predicate
//  // predicate column not in select list
//  TestUtil(
//      "SELECT test.a FROM test, test1 "
//      "WHERE test.a = test1.a AND test1.b = 22",
//      {"1", "3"}, false);
//  printf("good 11\n");
//  // Test joining same table with different alias
//  TestUtil(
//      "SELECT A.a, B.a FROM test1 as A , test1 as B "
//      "WHERE A.a = 1 and B.a = 1",
//      {"1", "1"}, false);
//  printf("good 12\n");
//  TestUtil(
//      "SELECT A.b, B.b FROM test1 as A, test1 as B "
//      "WHERE A.a = B.a",
//      {
//          "22", "22", "22", "22", "11", "11", "0", "0",
//      },
//      false);
//  printf("good 13\n");
//  // Test mixing single table predicates with join predicates
//  TestUtil(
//      "SELECT test.b FROM TEST, TEST1 "
//      "WHERE test.a = test1.a and test.c > 333 ",
//      {"33", "0"}, false);

//  /************************* Complex Queries *******************************/
//  // Test projection with join
//  TestUtil(
//      "SELECT test.a, test.b+test2.b FROM TEST, TEST2 WHERE test.a = test2.a",
//      {"1", "44", "2", "22", "3", "55", "4", "0"}, false);
//
//  // Test order by, limit, projection with join
//  TestUtil(
//      "SELECT test.a, test.b+test2.b FROM TEST, TEST2 "
//      "WHERE test.a = test2.a "
//      "ORDER BY test.c+test2.c LIMIT 3",
//      {"1", "44", "2", "22", "4", "0"}, true);
//
//  // Test group by with join
//  TestUtil(
//      "SELECT SUM(test2.b) FROM TEST, TEST2 "
//      "WHERE test.a = test2.a "
//      "GROUP BY test.a",
//      {"11", "0", "22", "22"}, false);
//
//  // Test group by, order by with join
//  TestUtil(
//      "SELECT SUM(test2.b), test.a FROM TEST, TEST2 "
//      "WHERE test.a = test2.a "
//      "GROUP BY test.a "
//      "ORDER BY test.a",
//      {"22", "1", "11", "2", "22", "3", "0", "4"}, true);
}

//TEST_F(OptimizerSQLTests, QueryDerivedTableTest) {
//  // Create extra table
//  TestingSQLUtil::ExecuteSQLQuery(
//      "CREATE TABLE test2(a int primary key, b int, c varchar(32))");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, '1st');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, '2nd');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 33, '3rd');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (5, 00, '4th');");
//  TestUtil("select A.b from (select b from test where a = 1) as A", {"22"},
//           false);
//  TestUtil("select * from (select b from test where a = 1) as A", {"22"},
//           false);
//  TestUtil(
//      "select A.b, B.b from (select b from test where a = 1) as A, (select b "
//      "from test as t where a=2) as B",
//      {"22", "11"}, false);
//  TestUtil(
//      "select B.b from (select b from test where a = 1) as A, (select b from "
//      "test as t where a=2) as B",
//      {"11"}, false);
//  TestUtil(
//      "select * from (select b from test where a = 1) as A, (select b from "
//      "test as t where a=2) as B",
//      {"22", "11"}, false);
//  TestUtil(
//      "select * from (select b from test) as A, (select b from test as t) as B "
//      "where A.b = B.b",
//      {"22", "22", "11", "11", "33", "33", "0", "0"}, false);
//  TestUtil(
//      "select * from (select b from test) as A, (select b from test) as B "
//      "where A.b = B.b",
//      {"22", "22", "11", "11", "33", "33", "0", "0"}, false);
//  TestUtil(
//      "select * from (select a+b as a, c from test) as A, (select a+b as a, c "
//      "as c from test2) as B where A.a=B.a",
//      {"13", "0", "13", "2nd", "23", "333", "23", "1st", "36", "444", "36",
//       "3rd"},
//      false);
//  TestUtil(
//      "select A.c, B.c from (select a+b as a, c from test) as A, (select a+b "
//      "as a, c as c from test2) as B where A.a=B.a order by A.a",
//      {"0", "2nd", "333", "1st", "444", "3rd"}, true);
//  TestUtil(
//      "select A.a, B.c from (select count(*) as a from test) as A, (select "
//      "avg(a) as C from test2) as B",
//      {"4", "2.75"}, false);
//}
//
//TEST_F(OptimizerSQLTests, NestedQueryTest) {
//  // Create extra table
//  TestingSQLUtil::ExecuteSQLQuery(
//      "CREATE TABLE test2(a int primary key, b int, c varchar(32))");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (1, 22, '1st');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (2, 11, '2nd');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (3, 33, '3rd');");
//  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test2 VALUES (5, 00, '4th');");
//  TestUtil(
//      "select B.a from test as B where exists (select b as a from test where a "
//      "= B.a);",
//      {"1", "2", "3", "4"}, false);
//  TestUtil(
//      "select b from test where a in (select a from test as t where a = "
//      "test.a)",
//      {"11", "22", "33", "0"}, false);
//  TestUtil(
//      "select B.a from test as B where exists (select b as a from test2 where "
//      "a = B.a) and "
//      "b in (select b from test where b > 22);",
//      {"3"}, false);
//  TestUtil(
//      "select B.a from test as B where exists (select b as a from test2 where "
//      "a = B.a) and "
//      "b in (select b from test) and c > 0;",
//      {"1", "3"}, false);
//  TestUtil(
//      "select t1.a, t2.a from test as t1 join test as t2 on t1.a=t2.a "
//      "where t1.b+t2.b in (select 2*b from test2 where a > 2)",
//      {"3", "3", "4", "4"}, false);
//  TestUtil(
//      "select B.a from test as B where exists (select b as a from test as T "
//      "where a = B.a and exists (select c from test where T.c = c));",
//      {"1", "2", "3", "4"}, false);
//}

}  // namespace test
}  // namespace peloton
