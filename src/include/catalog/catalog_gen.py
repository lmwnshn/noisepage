
class Table:

    def __init__(self, name):
        self.name_l = name.lower()  # Lowercase name.
        self.name_u = name.upper()  # Uppercase name.
        self.all_oids = f"{self.name_l}_all_oids"
        self.all_oids_pri = f"{self.all_oids}_pri_"
        self.all_oids_prm = f"{self.all_oids}_prm_"
        self.all_oids_enum = f"{self.name_u}_ALL_COL_OIDS"
        self.sql_table = f"{self.name_l}_"

    def gen_pri_code(self):
        return f"""
        const std::vector<col_oid_t> {self.all_oids}{{postgres::{self.all_oids_enum}.cbegin(), postgres::{self.all_oids_enum}.cend()}};
        {self.all_oids_pri} = {self.sql_table}->InitializerForProjectedRow({self.all_oids});
        {self.all_oids_prm} = {self.sql_table}->ProjectionMapForOids({self.all_oids});
        """


tables = ['pg_namespace', 'pg_attribute', 'pg_class', 'pg_index', 'pg_type', 'pg_language', 'pg_proc']
for table in [Table(x) for x in tables]:
    print(table.gen_pri_code())

# random bootstrap PRI code that doesn't fit in
"""
  const std::vector<col_oid_t> delete_namespace_oids{postgres::NSPNAME_COL_OID};
  delete_namespace_pri_ = namespaces_->InitializerForProjectedRow(delete_namespace_oids);

  const std::vector<col_oid_t> get_namespace_oids{postgres::NSPOID_COL_OID};
  get_namespace_pri_ = namespaces_->InitializerForProjectedRow(get_namespace_oids);

  const std::vector<col_oid_t> get_columns_oids{postgres::ATTNUM_COL_OID,     postgres::ATTNAME_COL_OID,
                                                postgres::ATTTYPID_COL_OID,   postgres::ATTLEN_COL_OID,
                                                postgres::ATTNOTNULL_COL_OID, postgres::ADSRC_COL_OID};
  get_columns_pri_ = columns_->InitializerForProjectedRow(get_columns_oids);
  get_columns_prm_ = columns_->ProjectionMapForOids(get_columns_oids);

  const std::vector<col_oid_t> delete_columns_oids{postgres::ATTNUM_COL_OID, postgres::ATTNAME_COL_OID};
  delete_columns_pri_ = columns_->InitializerForProjectedRow(delete_columns_oids);
  delete_columns_prm_ = columns_->ProjectionMapForOids(delete_columns_oids);

  const std::vector<col_oid_t> get_class_oid_kind_oids{postgres::RELOID_COL_OID, postgres::RELKIND_COL_OID};
  get_class_oid_kind_pri_ = classes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> set_class_pointer_oids{postgres::REL_PTR_COL_OID};
  set_class_pointer_pri_ = classes_->InitializerForProjectedRow(set_class_pointer_oids);

  const std::vector<col_oid_t> set_class_schema_oids{postgres::REL_SCHEMA_COL_OID};
  set_class_schema_pri_ = classes_->InitializerForProjectedRow(set_class_schema_oids);

  const std::vector<col_oid_t> get_class_pointer_kind_oids{postgres::REL_PTR_COL_OID, postgres::RELKIND_COL_OID};
  get_class_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_schema_pointer_kind_oids{postgres::REL_SCHEMA_COL_OID,
                                                                  postgres::RELKIND_COL_OID};
  get_class_schema_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_schema_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_object_and_schema_oids{postgres::REL_PTR_COL_OID,
                                                                postgres::REL_SCHEMA_COL_OID};
  get_class_object_and_schema_pri_ = classes_->InitializerForProjectedRow(get_class_object_and_schema_oids);
  get_class_object_and_schema_prm_ = classes_->ProjectionMapForOids(get_class_object_and_schema_oids);

  const std::vector<col_oid_t> get_indexes_oids{postgres::INDOID_COL_OID};
  get_indexes_pri_ = indexes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> delete_index_oids{postgres::INDOID_COL_OID, postgres::INDRELID_COL_OID};
  delete_index_pri_ = indexes_->InitializerForProjectedRow(delete_index_oids);
  delete_index_prm_ = indexes_->ProjectionMapForOids(delete_index_oids);

  const std::vector<col_oid_t> set_pg_proc_ptr_oids{postgres::PRO_CTX_PTR_COL_OID};
  pg_proc_ptr_pri_ = procs_->InitializerForProjectedRow(set_pg_proc_ptr_oids);
"""

# High-level DSL goals:
# - Should be plain SQL that can be copypasted for the same effect -- so any additional logic needs to be comments.
# - It is OK to be a little brittle to storage API changes, we don't predict those changing too much.

# CREATE TABLE
# ARG       : adds a parameter to the function definition, can be used in C++ code.
# SET       : the value that an attribute is set to, directly C++ code.
# CPP_PRE   : C++ code that belongs before the usual code behavior.
# CPP_POST  : C++ code that belongs after the usual code behavior.
# HEADER    : C++ code that belongs in the header file.
# OTHER     : Other random crap not directly related to an attribute.
#   unique_indexes  [(type, value)]
#   indexes         [(type, value)]

'''
CREATE TABLE pg_class (
  reloid           INTEGER NOT NULL,
  relname          VARCHAR NOT NULL,
  relnamespace     INTEGER NOT NULL,
  relkind          CHAR NOT NULL,
  rel_schema       BIGINT NOT NULL,
  rel_ptr          BIGINT,
  rel_nextcoloid   INTEGER NOT NULL
);
/*
{
  "reloid": {
    "ARG":          "const table_oid_t table_oid",
    "SET":          "table_oid",
  },
  "relname": {
    "ARG":          "const std::string &relname",
    "CPP_PRE":
        """
        const auto relname_varlen = storage::StorageUtil::CreateVarlen(relname);
        """,
  },
  "relnamespace": {
    "ARG":          "const namespace_oid_t ns_oid",
    "SET":          "ns_oid",
  },
  "relkind": {
    "SET":          "static_cast<char>(postgres::PG_CLASS_RELKIND::REGULAR_TABLE)",
    "HEADER":
        """
        enum class PG_CLASS_RELKIND : char {
          REGULAR_TABLE = 'r',
          INDEX = 'i',
          SEQUENCE = 'S',  // yes, this really is the only capitalized one. Ask postgres wtf.
          VIEW = 'v',
          MATERIALIZED_VIEW = 'm',
          COMPOSITE_TYPE = 'c',
          TOAST_TABLE = 't',
          FOREIGN_TABLE = 'f',
        };
        """,
  },
  "rel_schema": {
    "SET":          "nullptr",
    "CPP_PRE":      "// The schema pointer needs to be updated again after we have recreated the columns.",
  },
  "rel_ptr": {
    "SET-NULL":     "",
    "CPP_PRE":      "// The rel_ptr is set by the execution layer after instantiation.",
  },
  "rel_nextcoloid": {
    "ARG":          "const Schema &schema",
    "SET":          "col_oid_t(static_cast<uint32_t>(schema.GetColumns.size() + 1));",
    "CPP_PRE":      "// The next column OID for this relation is after all the existing columns.",
  },
  "OTHER": {
    "unique_indexes": {
      "classes_oid_index_": [
        ("table_oid_t", "table_oid"),
      ],
      "classes_name_index_": [
        ("storage::VarlenEntry", "name_varlen"), 
        ("namespace_oid_t", "ns_oid"),
      ],    
    },
    "indexes": {
      "namespace_index": [
        ("namespace_oid_t", "ns_oid"),
      ]
    },
  }
}
*/

'''



# RELOID_COL_OID,     RELNAME_COL_OID, RELNAMESPACE_COL_OID,  RELKIND_COL_OID,
#     REL_SCHEMA_COL_OID, REL_PTR_COL_OID, REL_NEXTCOLOID_COL_OID};
#
# enum class ClassKind : char {
#   REGULAR_TABLE = 'r',
#   INDEX = 'i',
#   SEQUENCE = 'S',  // yes, this really is the only capitalized one. Ask postgres wtf.
#   VIEW = 'v',
#   MATERIALIZED_VIEW = 'm',
#   COMPOSITE_TYPE = 'c',
#   TOAST_TABLE = 't',
#   FOREIGN_TABLE = 'f',


# Broadly speaking, two steps:
# 1. Initialize the catalog table itself
# - Generate functions for PRI initialization, insertion, deletion, etc
# 2. Initialize entries, for example pg_proc is horrendously long and we keep shoving stuff in there

# But foreign key index checks in the pg_constraint PR complicate this...
# Indexes in general are something that I am uncertain about right now.