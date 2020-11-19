#pragma once

#include <array>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"

namespace noisepage::catalog {
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::catalog::postgres {

class PgClass {
 public:
  PgClass(db_oid_t db_oid);

  enum class ClassKind : char {
    REGULAR_TABLE = 'r',
    INDEX = 'i',
    SEQUENCE = 'S',  // yes, this really is the only capitalized one. Ask postgres wtf.
    VIEW = 'v',
    MATERIALIZED_VIEW = 'm',
    COMPOSITE_TYPE = 'c',
    TOAST_TABLE = 't',
    FOREIGN_TABLE = 'f',
  };

  void BootstrapPRIs();
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn);

  bool CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                        namespace_oid_t ns_oid, const std::string &name, const Schema &schema);

  bool DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  bool DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index);

  std::pair<uint32_t, ClassKind> GetClassOidKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                 namespace_oid_t ns_oid, const std::string &name);

  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid oid,
                       Ptr *const pointer, col_oid_t class_col);

 private:
  friend class storage::RecoveryManager;

  static constexpr table_oid_t CLASS_TABLE_OID = table_oid_t(21);
  static constexpr index_oid_t CLASS_OID_INDEX_OID = index_oid_t(22);
  static constexpr index_oid_t CLASS_NAME_INDEX_OID = index_oid_t(23);
  static constexpr index_oid_t CLASS_NAMESPACE_INDEX_OID = index_oid_t(24);

  /*
   * Column names of the form "REL[name]_COL_OID" are present in the PostgreSQL
   * catalog specification and columns of the form "REL_[name]_COL_OID" are
   * noisepage-specific addtions (generally pointers to internal objects).
   */
  static constexpr col_oid_t RELOID_COL_OID = col_oid_t(1);          // INTEGER (pkey)
  static constexpr col_oid_t RELNAME_COL_OID = col_oid_t(2);         // VARCHAR
  static constexpr col_oid_t RELNAMESPACE_COL_OID = col_oid_t(3);    // INTEGER (fkey: pg_namespace)
  static constexpr col_oid_t RELKIND_COL_OID = col_oid_t(4);         // CHAR
  static constexpr col_oid_t REL_SCHEMA_COL_OID = col_oid_t(5);      // BIGINT (assumes 64-bit pointers)
  static constexpr col_oid_t REL_PTR_COL_OID = col_oid_t(6);         // BIGINT (assumes 64-bit pointers)
  static constexpr col_oid_t REL_NEXTCOLOID_COL_OID = col_oid_t(7);  // INTEGER

  static constexpr uint8_t NUM_PG_CLASS_COLS = 7;

  static constexpr std::array<col_oid_t, NUM_PG_CLASS_COLS> PG_CLASS_ALL_COL_OIDS = {
      RELOID_COL_OID,     RELNAME_COL_OID, RELNAMESPACE_COL_OID,  RELKIND_COL_OID,
      REL_SCHEMA_COL_OID, REL_PTR_COL_OID, REL_NEXTCOLOID_COL_OID};

 private:
  const db_oid_t db_oid_;

  storage::SqlTable *classes_;
  storage::index::Index *classes_oid_index_;
  storage::index::Index *classes_name_index_;  // indexed on namespace OID and name
  storage::index::Index *classes_namespace_index_;
  storage::ProjectedRowInitializer pg_class_all_cols_pri_;
  storage::ProjectionMap pg_class_all_cols_prm_;
  storage::ProjectedRowInitializer get_class_oid_kind_pri_;
  storage::ProjectedRowInitializer set_class_pointer_pri_;
  storage::ProjectedRowInitializer set_class_schema_pri_;
  storage::ProjectedRowInitializer get_class_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_schema_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_object_and_schema_pri_;
  storage::ProjectionMap get_class_object_and_schema_prm_;
};

}  // namespace noisepage::catalog::postgres
