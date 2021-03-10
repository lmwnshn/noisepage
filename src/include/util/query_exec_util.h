#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_settings.h"
#include "type/type_id.h"

namespace noisepage::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {
class ExecutableQuery;
}  // namespace noisepage::execution::compiler

namespace noisepage::execution::sql {
struct Val;
}  // namespace noisepage::execution::sql

namespace noisepage::settings {
class SettingsManager;
}  // namespace noisepage::settings

namespace noisepage::metrics {
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::catalog {
class Catalog;
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::optimizer {
class AbstractCostModel;
class StatsStorage;
}  // namespace noisepage::optimizer

namespace noisepage::planner {
class AbstractPlanNode;
class OutputSchema;
}  // namespace noisepage::planner

namespace noisepage::network {
class Statement;
}  // namespace noisepage::network

namespace noisepage::util {

/**
 * Signature of a function that is capable of processing rows retrieved
 * from ExecuteDML or ExecuteQuery. This function is invoked once per
 * row, with the argument being a row's attributes.
 */
using TupleFunction = std::function<void(const std::vector<execution::sql::Val *> &)>;

/**
 * Utility class for query execution. This class is not thread-safe.
 *
 * A QueryExecUtil only supports running 1 transaction at a time. If multiple
 * components may run multiple transactions interleaved, each component should
 * then have is own QueryExecUtil for use.
 */
class QueryExecUtil {
 public:
  /**
   * Construct a copy of useful members state.
   * This allows creating another QueryExecUtil from an existing one.
   */
  static std::unique_ptr<util::QueryExecUtil> ConstructThreadLocal(common::ManagedPointer<util::QueryExecUtil> util);

  /**
   * Construct a QueryExecUtil
   *
   * @param db_oid Database OID to use
   * @param txn_manager Transaction manager
   * @param catalog Catalog
   * @param settings Settings manager
   * @param stats Stats storage
   * @param optimizer_timeout Timeout for optimizer
   */
  QueryExecUtil(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionManager> txn_manager,
                common::ManagedPointer<catalog::Catalog> catalog,
                common::ManagedPointer<settings::SettingsManager> settings,
                common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout);

  /**
   * Starts a new transaction from the utility's viewpoint.
   */
  void BeginTransaction();

  /**
   * Instructs the utility to utilize the specified transaction.
   * A transaction must not already be started.
   *
   * @note It is the caller's responsibility to invoke UseTransaction(nullptr)
   * once the transaction no longer requires this utility.
   *
   * @param txn Transaction to use
   */
  void UseTransaction(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Specifies a function to invoke to retrieve the cost model.
   * Caller is responsible for ensuring function stays in relevant state.
   * @param func Function to invoke for costing.
   */
  void SetCostModelFunction(std::function<std::unique_ptr<optimizer::AbstractCostModel>()> func);

  /**
   * Specifies the database that is being targeted.
   * @param db_oid Database to target
   */
  void SetDatabase(catalog::db_oid_t db_oid);

  /**
   * Set database identifier to the default database
   */
  void SetDefaultDatabase();

  /**
   * Set external execution settings to adopt
   * @param exec_settings Settings to adopt
   */
  void SetExecutionSettings(execution::exec::ExecutionSettings exec_settings);

  /**
   * End the transaction
   * @param commit Commit or abort
   */
  void EndTransaction(bool commit);

  /**
   * Execute a standalone DDL
   * @param query DDL query to execute
   * @return true if success
   */
  bool ExecuteDDL(const std::string &query);

  /**
   * Execute a standalone DML statement
   * @param query DML query to execute
   * @param params query parameters to utilize
   * @param param_types Types of query parameters
   * @param tuple_fn A function to be called per row
   * @param metrics Metrics manager to use for recording
   * @return true if success
   */
  bool ExecuteDML(const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                  common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                  common::ManagedPointer<metrics::MetricsManager> metrics);

  /**
   * Compiles a query and caches the resultant plan
   * @param statement Statement to compile
   * @param params placeholder parameters for query
   * @param param_types Types of the query parameters
   * @param success Flag indicating if compile succeeded
   * @return compiled query identifier passed into ExecuteQuery
   */
  size_t CompileQuery(const std::string &statement,
                      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                      common::ManagedPointer<std::vector<type::TypeId>> param_types, bool *success);

  /**
   * Executes a pre-compiled query
   * @param idx Compiled query identifier to execute
   * @param tuple_fn Per-row function invoked during output
   * @param params Parameters to use for execution
   * @param metrics Metrics manager to use for recording
   * @return true if success
   */
  bool ExecuteQuery(size_t idx, TupleFunction tuple_fn,
                    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                    common::ManagedPointer<metrics::MetricsManager> metrics);

  /**
   * Plans a query
   * @param query Statement to plan
   * @param params Placeholder parameters for query plan
   * @param param_types Types of query parameters
   * @return pair of resultant statement and plan node
   */
  std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> PlanStatement(
      const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
      common::ManagedPointer<std::vector<type::TypeId>> param_types);

  /** Erases all cached plans */
  void ClearPlans();

 private:
  /**
   * Gets the transaction context to use, optionally starting one.
   * @return pair where first element is txn to use and second is whether it was started
   */
  std::pair<common::ManagedPointer<transaction::TransactionContext>, bool> GetTxn();

  /**
   * "Inverse" of GetTxn. This function commits any implicitly started transaction
   * with the specified commit flag.
   *
   * @param txn Transaction to commit (if started)
   * @param require_commit Whether transaction was implicitly started
   * @param commit Whether to commit or abort
   */
  void ReturnTransaction(common::ManagedPointer<transaction::TransactionContext> txn, bool require_commit, bool commit);

  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<settings::SettingsManager> settings_;
  common::ManagedPointer<optimizer::StatsStorage> stats_;
  uint64_t optimizer_timeout_;

  bool own_txn_ = false;
  std::function<std::unique_ptr<optimizer::AbstractCostModel>()> cost_func_;
  transaction::TransactionContext *txn_ = nullptr;

  std::vector<std::unique_ptr<planner::OutputSchema>> schemas_;
  std::vector<std::unique_ptr<execution::compiler::ExecutableQuery>> exec_queries_;

  execution::exec::ExecutionSettings exec_settings_;
};

}  // namespace noisepage::util
