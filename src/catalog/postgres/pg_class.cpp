#include "catalog/postgres/pg_class.h"

#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "transaction/transaction_context.h"

namespace noisepage::catalog::postgres {

PgClass::PgClass(const db_oid_t db_oid) : db_oid_(db_oid) {}

void PgClass::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_class_all_oids{PG_CLASS_ALL_COL_OIDS.cbegin(), PG_CLASS_ALL_COL_OIDS.cend()};
  pg_class_all_cols_pri_ = classes_->InitializerForProjectedRow(pg_class_all_oids);
  pg_class_all_cols_prm_ = classes_->ProjectionMapForOids(pg_class_all_oids);

  const std::vector<col_oid_t> get_class_oid_kind_oids{RELOID_COL_OID, RELKIND_COL_OID};
  get_class_oid_kind_pri_ = classes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> set_class_pointer_oids{REL_PTR_COL_OID};
  set_class_pointer_pri_ = classes_->InitializerForProjectedRow(set_class_pointer_oids);

  const std::vector<col_oid_t> set_class_schema_oids{REL_SCHEMA_COL_OID};
  set_class_schema_pri_ = classes_->InitializerForProjectedRow(set_class_schema_oids);

  const std::vector<col_oid_t> get_class_pointer_kind_oids{REL_PTR_COL_OID, RELKIND_COL_OID};
  get_class_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_schema_pointer_kind_oids{REL_SCHEMA_COL_OID, RELKIND_COL_OID};
  get_class_schema_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_schema_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_object_and_schema_oids{REL_PTR_COL_OID, REL_SCHEMA_COL_OID};
  get_class_object_and_schema_pri_ = classes_->InitializerForProjectedRow(get_class_object_and_schema_oids);
  get_class_object_and_schema_prm_ = classes_->ProjectionMapForOids(get_class_object_and_schema_oids);
}

void PgClass::Bootstrap(const common::ManagedPointer<transaction::TransactionContext> txn) {
  bool retval;
  retval = CreateTableEntry(txn, CLASS_TABLE_OID, NAMESPACE_CATALOG_NAMESPACE_OID, "pg_class",
                            Builder::GetClassTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, CLASS_TABLE_OID, classes_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, CLASS_TABLE_OID, CLASS_OID_INDEX_OID,
                            "pg_class_oid_index", Builder::GetClassOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, CLASS_OID_INDEX_OID, classes_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, CLASS_TABLE_OID, CLASS_NAME_INDEX_OID,
                            "pg_class_name_index", Builder::GetClassNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, CLASS_NAME_INDEX_OID, classes_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, CLASS_TABLE_OID, CLASS_NAMESPACE_INDEX_OID,
                            "pg_class_namespace_index", Builder::GetClassNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, CLASS_NAMESPACE_INDEX_OID, classes_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

bool PgClass::CreateTableEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                               const table_oid_t table_oid, const namespace_oid_t ns_oid, const std::string &name,
                               const Schema &schema) {
  auto *const insert_redo = txn->StageWrite(db_oid_, CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto *const insert_pr = insert_redo->Delta();

  // Write the ns_oid into the PR
  const auto ns_offset = pg_class_all_cols_prm_[RELNAMESPACE_COL_OID];
  auto *const ns_ptr = insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<namespace_oid_t *>(ns_ptr)) = ns_oid;

  // Write the table_oid into the PR
  const auto table_oid_offset = pg_class_all_cols_prm_[RELOID_COL_OID];
  auto *const table_oid_ptr = insert_pr->AccessForceNotNull(table_oid_offset);
  *(reinterpret_cast<table_oid_t *>(table_oid_ptr)) = table_oid;

  auto next_col_oid = col_oid_t(static_cast<uint32_t>(schema.GetColumns().size() + 1));

  // Write the next_col_oid into the PR
  const auto next_col_oid_offset = pg_class_all_cols_prm_[REL_NEXTCOLOID_COL_OID];
  auto *const next_col_oid_ptr = insert_pr->AccessForceNotNull(next_col_oid_offset);
  *(reinterpret_cast<col_oid_t *>(next_col_oid_ptr)) = next_col_oid;

  // Write the schema_ptr as nullptr into the PR (need to update once we've recreated the columns)
  const auto schema_ptr_offset = pg_class_all_cols_prm_[REL_SCHEMA_COL_OID];
  auto *const schema_ptr_ptr = insert_pr->AccessForceNotNull(schema_ptr_offset);
  *(reinterpret_cast<Schema **>(schema_ptr_ptr)) = nullptr;

  // Set table_ptr to NULL because it gets set by execution layer after instantiation
  const auto table_ptr_offset = pg_class_all_cols_prm_[REL_PTR_COL_OID];
  insert_pr->SetNull(table_ptr_offset);

  // Write the kind into the PR
  const auto kind_offset = pg_class_all_cols_prm_[RELKIND_COL_OID];
  auto *const kind_ptr = insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<char *>(kind_ptr)) = static_cast<char>(PgClass::ClassKind::REGULAR_TABLE);

  // Create the necessary varlen for storage operations
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Write the name into the PR
  const auto name_offset = pg_class_all_cols_prm_[RELNAME_COL_OID];
  auto *const name_ptr = insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Insert into pg_class table
  const auto tuple_slot = classes_->Insert(txn, insert_redo);

  // Get PR initializers and allocate a buffer from the largest one
  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const index_buffer = common::AllocationUtil::AllocateAligned(name_index_init.ProjectedRowSize());

  // Insert into oid_index
  auto *index_pr = oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<table_oid_t *>(index_pr->AccessForceNotNull(0))) = table_oid;
  if (!classes_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into name_index
  index_pr = name_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  if (!classes_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into namespace_index
  index_pr = ns_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(0))) = ns_oid;
  const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insertion into non-unique namespace index failed.");

  delete[] index_buffer;

  // Write the col oids into a new Schema object
  col_oid_t curr_col_oid(1);
  for (auto &col : schema.GetColumns()) {
    auto success = CreateColumn(txn, table_oid, curr_col_oid++, col);
    if (!success) return false;
  }

  std::vector<Schema::Column> cols = GetColumns<Schema::Column, table_oid_t, col_oid_t>(txn, table_oid);
  auto *new_schema = new Schema(cols);
  txn->RegisterAbortAction([=]() { delete new_schema; });

  auto *const update_redo = txn->StageWrite(db_oid_, CLASS_TABLE_OID, set_class_schema_pri_);
  auto *const update_pr = update_redo->Delta();

  update_redo->SetTupleSlot(tuple_slot);
  *reinterpret_cast<Schema **>(update_pr->AccessForceNotNull(0)) = new_schema;
  auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
  NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");

  return true;
}

bool PgClass::DeleteTable(const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t table) {
  bool result;
  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<table_oid_t *>(key_pr->AccessForceNotNull(0))) = table;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense. "
      "Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than pushing logic here.");

  // Select the tuple out of the table before deletion. We need the attributes to do index deletions later
  auto *const table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
  result = classes_->Select(txn, index_results[0], table_pr);
  NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");

  // Delete from pg_classes table
  txn->StageDelete(db_oid_, CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  DeleteIndexes(txn, table);

  // Get the attributes we need for indexes
  const table_oid_t table_oid = *(
      reinterpret_cast<const table_oid_t *const>(table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELOID_COL_OID])));
  NOISEPAGE_ASSERT(table == table_oid,
                   "table oid from pg_classes did not match what was found by the index scan from the argument.");
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELNAME_COL_OID])));

  // Get the attributes we need for delete
  auto *const schema_ptr = *(reinterpret_cast<const Schema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[REL_SCHEMA_COL_OID])));
  auto *const table_ptr = *(reinterpret_cast<storage::SqlTable *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[REL_PTR_COL_OID])));

  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();

  // Delete from oid_index
  auto *index_pr = oid_index_init.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *const>(index_pr->AccessForceNotNull(0))) = table_oid;
  classes_oid_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from name_index
  index_pr = name_index_init.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *const>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  classes_name_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from namespace_index
  index_pr = ns_index_init.InitializeRow(buffer);
  *(reinterpret_cast<namespace_oid_t *const>(index_pr->AccessForceNotNull(0))) = ns_oid;
  classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);

  // Everything succeeded from an MVCC standpoint, register deferred action for the GC with txn manager. See base
  // function comment.
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      deferred_action_manager->RegisterDeferredAction([=]() {
        // Defer an action upon commit to delete the table. Delete table will need a double deferral because there could
        // be transactions not yet unlinked by the GC that depend on the table
        delete schema_ptr;
        delete table_ptr;
      });
    });
  });

  delete[] buffer;
}

bool PgClass::DeleteIndex(const common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index) {
  bool result;
  // Initialize PRs for pg_class
  const auto class_oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Allocate buffer for largest PR
  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= class_oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());
  auto *key_pr = class_oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<index_oid_t *>(key_pr->AccessForceNotNull(0))) = index;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense. "
      "Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than pushing logic here.");

  // Select the tuple out of the table before deletion. We need the attributes to do index deletions later
  auto *table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
  result = classes_->Select(txn, index_results[0], table_pr);
  NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");

  // Delete from pg_classes table
  txn->StageDelete(db_oid_, CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  // Get the attributes we need for pg_class indexes
  table_oid_t table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELOID_COL_OID])));
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[RELNAME_COL_OID])));

  auto *const schema_ptr = *(reinterpret_cast<const IndexSchema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[REL_SCHEMA_COL_OID])));
  auto *const index_ptr = *(reinterpret_cast<storage::index::Index *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[REL_PTR_COL_OID])));

  const auto class_oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto class_name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto class_ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();

  // Delete from classes_oid_index_
  auto *index_pr = class_oid_index_init.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *const>(index_pr->AccessForceNotNull(0))) = table_oid;
  classes_oid_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from classes_name_index_
  index_pr = class_name_index_init.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *const>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  classes_name_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from classes_namespace_index_
  index_pr = class_ns_index_init.InitializeRow(buffer);
  *(reinterpret_cast<namespace_oid_t *const>(index_pr->AccessForceNotNull(0))) = ns_oid;
  classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);
}

std::pair<uint32_t, PgClass::ClassKind> PgClass::GetClassOidKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid,
    const std::string &name) {
  const auto name_pri = classes_name_index_->GetProjectedRowInitializer();

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Buffer is large enough to hold all prs
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(1))) = ns_oid;

  std::vector<storage::TupleSlot> index_results;
  classes_name_index_->ScanKey(*txn, *pr, &index_results);
  // Clean up the varlen's buffer in the case it wasn't inlined.
  if (!name_varlen.IsInlined()) {
    delete[] name_varlen.Content();
  }

  if (index_results.empty()) {
    delete[] buffer;
    // If the OID is invalid, we don't care the class kind and return a random one.
    return std::make_pair(catalog::NULL_OID, PgClass::ClassKind::REGULAR_TABLE);
  }
  NOISEPAGE_ASSERT(index_results.size() == 1, "name not unique in classes_name_index_");

  NOISEPAGE_ASSERT(get_class_oid_kind_pri_.ProjectedRowSize() <= name_pri.ProjectedRowSize(),
                   "I want to reuse this buffer because I'm lazy and malloc is slow but it needs to be big enough.");
  pr = get_class_oid_kind_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  const auto oid = *(reinterpret_cast<const uint32_t *const>(pr->AccessForceNotNull(0)));
  const auto kind = *(reinterpret_cast<const PgClass::ClassKind *const>(pr->AccessForceNotNull(1)));

  // Finish
  delete[] buffer;
  return std::make_pair(oid, kind);
}

template <typename ClassOid, typename Ptr>
bool PgClass::SetClassPointer(const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid oid,
                              const Ptr *const pointer, const col_oid_t class_col) {
  NOISEPAGE_ASSERT(
      (std::is_same<ClassOid, table_oid_t>::value &&
       (std::is_same<Ptr, storage::SqlTable>::value || std::is_same<Ptr, catalog::Schema>::value)) ||
          (std::is_same<ClassOid, index_oid_t>::value &&
           (std::is_same<Ptr, storage::index::Index>::value || std::is_same<Ptr, catalog::IndexSchema>::value)),
      "OID type must correspond to the same object type (Table or index)");
  NOISEPAGE_ASSERT(pointer != nullptr, "Why are you inserting nullptr here? That seems wrong.");
  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Do not need to store the projection map because it is only a single column
  auto pr_init = classes_->InitializerForProjectedRow({class_col});
  NOISEPAGE_ASSERT(pr_init.ProjectedRowSize() >= oid_pri.ProjectedRowSize(), "Buffer must allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(0))) = oid;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  auto &initializer =
      (class_col == catalog::REL_PTR_COL_OID) ? set_class_pointer_pri_ : set_class_schema_pri_;
  auto *update_redo = txn->StageWrite(db_oid_, CLASS_TABLE_OID, initializer);
  update_redo->SetTupleSlot(index_results[0]);
  auto *update_pr = update_redo->Delta();
  auto *const class_ptr_ptr = update_pr->AccessForceNotNull(0);
  *(reinterpret_cast<const Ptr **>(class_ptr_ptr)) = pointer;

  // Finish
  delete[] buffer;
  return classes_->Update(txn, update_redo);
}

}  // namespace noisepage::catalog::postgres
