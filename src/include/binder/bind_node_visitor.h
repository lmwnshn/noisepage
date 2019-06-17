#pragma once

#include "binder/binder_context.h"
#include "common/sql_node_visitor.h"
#include "parser/statements.h"
#include "catalog/catalog_defs.h"


namespace terrier {

namespace expression {
class CaseExpression;
class ConstantValueExpression;
class TupleValueExpression;
class SubqueryExpression;
class StarExpression;
class OperatorExpression;
class AggregateExpression;
}  // namespace expression

namespace parser {
class SQLStatement;
}  // namespace parser

namespace catalog {
class CatalogAccessor;
}  // namespace catalog

namespace binder {

/**
 * @brief Interface to be notified of the composition of a bind node.
 */
class BindNodeVisitor : public SqlNodeVisitor {
 public:
  BindNodeVisitor(catalog::CatalogAccessor* catalog_accessor, transaction::TransactionContext *txn, std::string default_database_name);

  void BindNameToNode(parser::SQLStatement *tree);
  void Visit(parser::SelectStatement *) override;

  // Some sub query nodes inside SelectStatement
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::TableRef *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderByDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *) override;
  void Visit(parser::CreateFunctionStatement *) override;
  void Visit(parser::InsertStatement *) override;
  void Visit(parser::DeleteStatement *) override;
  void Visit(parser::DropStatement *) override;
  void Visit(parser::PrepareStatement *) override;
  void Visit(parser::ExecuteStatement *) override;
  void Visit(parser::TransactionStatement *) override;
  void Visit(parser::UpdateStatement *) override;
  void Visit(parser::CopyStatement *) override;
  void Visit(parser::AnalyzeStatement *) override;

  void Visit(parser::CaseExpression *expr) override;
  void Visit(parser::SubqueryExpression *expr) override;

  // void Visit(parser::ConstantValueExpression *expr) override;
  void Visit(parser::TupleValueExpression *expr) override;
  void Visit(parser::StarExpression *expr) override;
  void Visit(parser::FunctionExpression *expr) override;

  // Deduce value type for these expressions
  void Visit(parser::OperatorExpression *expr) override;
  void Visit(parser::AggregateExpression *expr) override;

  // TODO: LING: this method was never used in the peloton code
  void SetTxn(transaction::TransactionContext *txn) { this->txn_ = txn; }

 private:
  std::shared_ptr<BinderContext> context_;
  catalog::CatalogAccessor *catalog_accessor_;
  transaction::TransactionContext *txn_;
  std::string default_database_name_;
};

}  // namespace binder
}  // namespace terrier