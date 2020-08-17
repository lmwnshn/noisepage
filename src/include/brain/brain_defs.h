#pragma once

namespace terrier::brain {

enum class ExecutionOperatingUnitType : uint32_t {
  INVALID,

  AGGREGATE_BUILD,
  AGGREGATE_ITERATE,

  HASHJOIN_BUILD,
  HASHJOIN_PROBE,

  NLJOIN_LEFT,
  NLJOIN_RIGHT,
  IDXJOIN,

  SORT_BUILD,
  SORT_ITERATE,

  SEQ_SCAN,
  IDX_SCAN,

  INSERT,
  UPDATE,
  DELETE,

  PROJECTION,
  OUTPUT,
  LIMIT,

  HASH_JOIN,
  HASH_AGGREGATE,
  CSV_SCAN,
  NL_JOIN,
  SORT,
  STATIC_AGGREGATE,

  // Use to demarcate plan and operations
  PLAN_OPS_DELIMITER,

  OP_INTEGER_PLUS_OR_MINUS,
  OP_INTEGER_MULTIPLY,
  OP_INTEGER_DIVIDE,
  OP_INTEGER_COMPARE,
  OP_DECIMAL_PLUS_OR_MINUS,
  OP_DECIMAL_MULTIPLY,
  OP_DECIMAL_DIVIDE,
  OP_DECIMAL_COMPARE,
  OP_BOOL_COMPARE
};

/** The attributes of an ExecutionOperatingUnitFeature that can be set from TPL. */
enum class ExecutionOperatingUnitFeatureAttribute : uint8_t { NUM_ROWS, CARDINALITY };

}  // namespace terrier::brain
