#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "common/dedicated_thread_owner.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/sql_table.h"

namespace noisepage {
class RecoveryBenchmark;
}  // namespace noisepage

namespace noisepage::replication {
class ReplicationManager;
}  // namespace noisepage::replication

namespace noisepage::transaction {
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::storage {

/**
 * Recovery Manager
 * TODO(Gus): Add more documentation when API is finalized
 */
class RecoveryManager : public common::DedicatedThreadOwner {
  /**
   * Task in charge of initializing recovery. This way recovery can be non-blocking in a background thread.
   */
  class RecoveryTask : public common::DedicatedThreadTask {
   public:
    /**
     * @param recovery_manager pointer to recovery manager who initialized task
     */
    explicit RecoveryTask(RecoveryManager *recovery_manager) : recovery_manager_(recovery_manager) {}

    /**
     * Runs the recovery task. Our task only calls Recover on the log manager.
     */
    void RunTask() override {
      // If RunTask is invoked at all, we want to perform recovery at least once, necessitating a do-while loop.
      // In particular, the following ordering of calls is disastrous with a normal while loop:
      //    RunTask() (nothing happens yet) -> Terminate() (sets flag to stop looping) -> no recovery happens
      // However, that ordering of calls is exactly what could happen by the simple invocation of:
      //    RecoveryManager::StartRecovery() -> RecoveryManager::WaitForRecoveryToFinish()
      do {
        recovery_manager_->Recover();
      } while (recovery_manager_->recovery_task_loop_again_);
    }

    /**
     * Terminate stops the recovery task loop from looping again.
     * This allows the current iteration of recovery to complete.
     */
    void Terminate() override { recovery_manager_->recovery_task_loop_again_ = false; }

   private:
    RecoveryManager *recovery_manager_;
  };

 public:
  /**
   * @param log_provider arbitrary provider to receive logs from
   * @param catalog system catalog to interface with sql tables
   * @param txn_manager txn manager to use for re-executing recovered transactions
   * @param deferred_action_manager manager to use for deferred deletes
   * @param replication_manager replication manager to acknowledge applied changes
   * @param thread_registry thread registry to register tasks
   * @param store block store used for SQLTable creation during recovery
   */
  explicit RecoveryManager(const common::ManagedPointer<AbstractLogProvider> log_provider,
                           const common::ManagedPointer<catalog::Catalog> catalog,
                           const common::ManagedPointer<transaction::TransactionManager> txn_manager,
                           const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
                           const common::ManagedPointer<replication::ReplicationManager> replication_manager,
                           const common::ManagedPointer<noisepage::common::DedicatedThreadRegistry> thread_registry,
                           const common::ManagedPointer<BlockStore> store)
      : DedicatedThreadOwner(thread_registry),
        log_provider_(log_provider),
        catalog_(catalog),
        txn_manager_(txn_manager),
        deferred_action_manager_(deferred_action_manager),
        replication_manager_(replication_manager),
        block_store_(store) {
    // Initialize catalog_table_schemas_ map
    catalog_table_schemas_[catalog::postgres::PgClass::CLASS_TABLE_OID] =
        catalog::postgres::Builder::GetClassTableSchema();
    catalog_table_schemas_[catalog::postgres::PgNamespace::NAMESPACE_TABLE_OID] =
        catalog::postgres::Builder::GetNamespaceTableSchema();
    catalog_table_schemas_[catalog::postgres::PgAttribute::COLUMN_TABLE_OID] =
        catalog::postgres::Builder::GetColumnTableSchema();
    catalog_table_schemas_[catalog::postgres::PgConstraint::CONSTRAINT_TABLE_OID] =
        catalog::postgres::Builder::GetConstraintTableSchema();
    catalog_table_schemas_[catalog::postgres::PgIndex::INDEX_TABLE_OID] =
        catalog::postgres::Builder::GetIndexTableSchema();
    catalog_table_schemas_[catalog::postgres::PgType::TYPE_TABLE_OID] =
        catalog::postgres::Builder::GetTypeTableSchema();
  }

  /** Starts a background recovery thread, which does not stop until WaitForRecoveryToFinish() is called. */
  void StartRecovery();

  /** Blocks until the current recovery finishes (runs out of logs), then stops the background thread. */
  void WaitForRecoveryToFinish();

  /** @return True if the recovery task is still running. */
  bool IsRecoveryTaskRunning() const { return recovery_task_ != nullptr; }

  /** @return The ID of the last transaction that was applied. */
  transaction::timestamp_t GetLastAppliedTransactionId() const { return last_applied_txn_id_; }

 private:
  FRIEND_TEST(RecoveryTests, DoubleRecoveryTest);
  friend class RecoveryTests;
  friend class noisepage::RecoveryBenchmark;

  // Log provider for reading in logs
  const common::ManagedPointer<AbstractLogProvider> log_provider_;

  // Catalog to fetch table pointers
  const common::ManagedPointer<catalog::Catalog> catalog_;

  // Transaction manager to create transactions for recovery
  const common::ManagedPointer<transaction::TransactionManager> txn_manager_;

  // DeferredActions manager to defer record deletes
  const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;

  // Replication manager to acknowledge when commits are finished.
  const common::ManagedPointer<replication::ReplicationManager> replication_manager_;

  // TODO(Gus): The recovery manager should be passed a specific block store for table construction. Block store
  // management/assignment is probably a larger system issue that needs to be adddressed. Block store, used to create
  // tables during recovery
  const common::ManagedPointer<BlockStore> block_store_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  // TODO(Gus): This map may get huge, benchmark whether this becomes a problem and if we need a more sophisticated data
  // structure
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Stores deferred transactions in sorted sorted order to be able to execute them in
  // serial order. Transactions are defered when there is an older active transaction at the time it committed. Even
  // though snapshot isolation would handle write-write conflicts, DDL changes such as DROP TABLE combined with GC could
  // lead to issues if we don't execute transactions in complete serial order.
  std::set<transaction::timestamp_t> deferred_txns_;

  // Used during recovery from log. Maps a the txn id from the persisted txn to its changes we have buffered. We buffer
  // changes until commit time. This ensures serializability, and allows us to skip changes from aborted txns.
  std::unordered_map<transaction::timestamp_t, std::vector<std::pair<LogRecord *, std::vector<byte *>>>>
      buffered_changes_map_;

  // Background recovery task
  common::ManagedPointer<RecoveryTask> recovery_task_ = nullptr;
  /**
   * The RecoveryManager is used for applying records to replicas in replication.
   * This is a change from the initial RecoveryManager design where recovery happens in one shot on startup.
   * Currently, this is achieved by "just" looping the recovery task at the end of recovery based on this variable.
   * Unfortunately, this is slightly error-prone in practice -- tread with caution here.
   * One consequence is that non-replication uses of the RecoveryManager must manually call WaitForRecoveryToFinish()
   * for the recovery task to end, but this seems natural enough.
   */
  bool recovery_task_loop_again_ = false;

  // Its possible during recovery that the schemas for catalog tables may not yet exist in pg_class. Thus, we hardcode
  // them here
  std::unordered_map<catalog::table_oid_t, catalog::Schema> catalog_table_schemas_;

  transaction::timestamp_t last_applied_txn_id_ = transaction::INITIAL_TXN_TIMESTAMP;  ///< The last applied txn's ID.
  uint32_t recovered_txns_ = 0;  ///< The number of recovered committed txns.

  /**
   * Recovers the databases using the provided log provider
   */
  void Recover() {
    if (log_provider_ != nullptr) RecoverFromLogs(log_provider_);
  }

  /**
   * Recovers the databases from the logs.
   * @note this is a separate method so in the future, we can also have a RecoverFromCheckpoint method
   */
  void RecoverFromLogs(common::ManagedPointer<AbstractLogProvider> log_provider_);

  /**
   * @brief Replay a committed transaction corresponding to txn_id.
   * @param txn_id start timestamp for committed transaction
   * @return number of records replayed
   */
  uint32_t ProcessCommittedTransaction(transaction::timestamp_t txn_id);

  /**
   * Defers log records deletes with the transaction manager
   * @param txn_id txn_id for txn who's records to delete
   * @param delete_varlens true if we should delete varlens allocated for txn
   */
  void DeferRecordDeletes(transaction::timestamp_t txn_id, bool delete_varlens);

  /**
   * Replay any transaction who's txn start time is less than upper_bound. If upper_bound == transaction::NO_ACTIVE_TXN,
   * it will replay all deferred transactions
   * @param upper_bound upper bound for replaying
   * @return number of transactions and records replayed
   */
  std::pair<uint32_t, uint32_t> ProcessDeferredTransactions(transaction::timestamp_t upper_bound);

  /**
   * Handles mapping of old tuple slot (before recovery) to new tuple slot (after recovery)
   * @param slot old tuple slot
   * @return new tuple slot
   */
  TupleSlot GetTupleSlotMapping(TupleSlot slot) {
    NOISEPAGE_ASSERT(tuple_slot_map_.find(slot) != tuple_slot_map_.end(), "No tuple slot mapping exists");
    return tuple_slot_map_[slot];
  }

  /**
   * Wrapper over GetDatabaseCatalog method that asserts the database exists
   * @param txn txn for catalog lookup
   * @param database oid for database we want
   * @return pointer to database catalog
   */
  common::ManagedPointer<catalog::DatabaseCatalog> GetDatabaseCatalog(transaction::TransactionContext *txn,
                                                                      catalog::db_oid_t db_oid) {
    auto db_catalog_ptr = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
    NOISEPAGE_ASSERT(db_catalog_ptr != nullptr, "No catalog for given database oid");
    auto result UNUSED_ATTRIBUTE = db_catalog_ptr->TryLock(common::ManagedPointer(txn));
    NOISEPAGE_ASSERT(result, "There should not be concurrent DDL changes during recovery.");
    return db_catalog_ptr;
  }

  /**
   * @param txn transaction to use for catalog lookup
   * @param db_oid database oid for requested table
   * @param table_oid table oid for requested table
   * @return pointer to requested Sql table
   */
  common::ManagedPointer<storage::SqlTable> GetSqlTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                                                        catalog::table_oid_t table_oid);

  /**
   * Inserts or deletes a tuple slot from all indexes on a table.
   * @warning For an insert, must be called after the tuple slot is inserted into the table, for a delete, it must be
   * called before it is deleted from the table
   * @param txn transaction to delete with
   * @param db_oid database oid for table
   * @param table_oid indexed table
   * @param table_ptr pointer to sql table
   * @param tuple tuple slot to delete
   * @param table_pr pointer to PR with values for index update
   * @param insert true if we should insert into indexes, false for delete
   */
  void UpdateIndexesOnTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                            catalog::table_oid_t table_oid, common::ManagedPointer<storage::SqlTable> table_ptr,
                            const TupleSlot &tuple_slot, ProjectedRow *table_pr, bool insert);

  /**
   * NYS = Not yet supported
   * Returns whether a delete or redo record is a special case catalog record. The special cases we consider are:
   *   1. Insert into pg_database (creating a database)
   *   2. Updates into pg_class (updating a pointer, updating a schema (NYS), update to next col_oid)
   *   3. Delete into pg_database (renaming a database, drop a database)
   *   4. Delete into pg_class (renaming a table/index, drop a table/index)
   *   5. Delete into pg_index (cascading delete from drop index)
   *   6. Delete into pg_attribute (drop column (NYS) / cascading delete from drop table)
   *   7. Insert into pg_proc
   *   8. Updates into pg_proc
   * @param record log record we want to determine if its a special case
   * @return true if log record is a special case catalog record, false otherwise
   */
  bool IsSpecialCaseCatalogRecord(const LogRecord *record) {
    NOISEPAGE_ASSERT(record->RecordType() == LogRecordType::REDO || record->RecordType() == LogRecordType::DELETE,
                     "Special case catalog records must only be delete or redo records");

    if (record->RecordType() == LogRecordType::REDO) {
      auto *redo_record = record->GetUnderlyingRecordBodyAs<RedoRecord>();
      if (IsInsertRecord(redo_record)) {
        // Case 1
        return redo_record->GetTableOid() == catalog::postgres::PgDatabase::DATABASE_TABLE_OID ||
               redo_record->GetTableOid() == catalog::postgres::PgProc::PRO_TABLE_OID;
      }

      // Case 2
      return redo_record->GetTableOid() == catalog::postgres::PgClass::CLASS_TABLE_OID ||
             redo_record->GetTableOid() == catalog::postgres::PgProc::PRO_TABLE_OID;
    }

    // Case 3, 4, 5, and 6
    auto *delete_record = record->GetUnderlyingRecordBodyAs<DeleteRecord>();
    return delete_record->GetTableOid() == catalog::postgres::PgDatabase::DATABASE_TABLE_OID ||
           delete_record->GetTableOid() == catalog::postgres::PgClass::CLASS_TABLE_OID ||
           delete_record->GetTableOid() == catalog::postgres::PgIndex::INDEX_TABLE_OID ||
           delete_record->GetTableOid() == catalog::postgres::PgAttribute::COLUMN_TABLE_OID;
  }

  /**
   * Returns whether a given record is an insert into a table. We know it is an insert record if the tuple slot it
   * contains is previously unseen. An update will contain a tuple slot that has been previously inserted.
   * @param record record we want to determine redo type of
   * @return true if record is an insert redo, false if it is an update redo
   */
  bool IsInsertRecord(const RedoRecord *record) const {
    return tuple_slot_map_.find(record->GetTupleSlot()) == tuple_slot_map_.end();
  }

  /**
   * Processes records that modify the catalog tables. Because catalog modifications usually result in multiple log
   * records that result in custom handling logic, this function can process more than one log record.
   * @param txn transaction to use to replay the catalog changes
   * @param buffered_changes list of buffered log records
   * @param start_idx index of current log record in the list
   * @return number of EXTRA log records processed
   */
  uint32_t ProcessSpecialCaseCatalogRecord(transaction::TransactionContext *txn,
                                           std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
                                           uint32_t start_idx);

  /**
   * Processes a record that modifies pg_database.
   * @param txn transaction to use to replay the catalog changes
   * @param buffered_changes list of buffered log records
   * @param start_idx index of current log record in the list
   * @return number of EXTRA log records processed
   */
  uint32_t ProcessSpecialCasePGDatabaseRecord(
      transaction::TransactionContext *txn, std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
      uint32_t start_idx);

  /**
   * Processes a record that modifies pg_class.
   * @param txn transaction to use to replay the catalog changes
   * @param buffered_changes list of buffered log records
   * @param start_idx index of current log record in the list
   * @return number of EXTRA log records processed
   */
  uint32_t ProcessSpecialCasePGClassRecord(transaction::TransactionContext *txn,
                                           std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
                                           uint32_t start_idx);

  /**
   * Processes a record that modifies pg_proc.
   * @param txn transaction to use to replay the catalog changes
   * @param buffered_changes list of buffered log records
   * @param start_idx index of current log record in the list
   * @return number of EXTRA log records processed
   */
  uint32_t ProcessSpecialCasePGProcRecord(
      noisepage::transaction::TransactionContext *txn,
      std::vector<std::pair<noisepage::storage::LogRecord *, std::vector<noisepage::byte *>>> *buffered_changes,
      uint32_t start_idx);

  /**
   * Replays a redo record. Updates necessary metadata maps
   * @param txn txn to use for replay
   * @param record record to replay
   */
  void ReplayRedoRecord(transaction::TransactionContext *txn, LogRecord *record);

  /**
   * Replays a delete record. Updates necessary metadata
   * @param txn txn to use for delete
   * @param record record to replay
   */
  void ReplayDeleteRecord(transaction::TransactionContext *txn, LogRecord *record);

  /**
   * Returns the list of col oids this redo record modified
   * @param sql_table sql table that redo record modifies
   * @param record record we want oids for
   * @return list of oids
   */
  // TODO(John): Currently this is being used to extract values from redo records for catalog tables. You should look at
  // adding constants for col_id_t for catalog tables.
  std::vector<catalog::col_oid_t> GetOidsForRedoRecord(common::ManagedPointer<storage::SqlTable> sql_table,
                                                       RedoRecord *record);

  /**
   * @param oid oid of catalog index
   * @param db_catalog database catalog that has given index
   * @return pointer to catalog index
   */
  common::ManagedPointer<storage::index::Index> GetCatalogIndex(
      catalog::index_oid_t oid, const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog);

  /**
   * Fetches a table's schema. If the table is a catalog table, we return the cached schema, otherwise we go to the
   * catalog
   * @param txn txn to use for catalog lookup
   * @param db_catalog database catalog to use for lookup
   * @param table_oid oid of table we want schema for
   * @return schema for table with oid table_oid
   */
  const catalog::Schema &GetTableSchema(transaction::TransactionContext *txn,
                                        const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog,
                                        catalog::table_oid_t table_oid) const;
};
}  // namespace noisepage::storage
