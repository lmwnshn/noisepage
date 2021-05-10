#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/mcts/tree_node.h"
#include "self_driving/planning/pilot.h"

namespace noisepage::selfdriving {
class Pilot;

namespace pilot {

/** A complete action to be taken as part of the MCTS tree search. */
class MCTSAction {
 public:
  /**
   * Constructor.
   * @param db_oid          The database that this action should be applied to.
   * @param action_sql      The SQL string corresponding to the action to be applied.
   * @param cost            The predicted cost of this action.
   */
  MCTSAction(catalog::db_oid_t db_oid, std::string action_sql, double predicted_cost)
      : db_oid_(db_oid), action_sql_(std::move(action_sql)), predicted_cost_(predicted_cost) {}

  /** @return The database that this action should be applied to. */
  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }
  /** @return The SQL string corresponding to the action to be applied. */
  const std::string &GetActionSQL() const { return action_sql_; }
  /** @return The predicted cost of this action. */
  double GetPredictedCost() const { return predicted_cost_; }

 private:
  const catalog::db_oid_t db_oid_;  ///< The database that this action should be applied to.
  const std::string action_sql_;    ///< The SQL string corresponding to the action to be applied.
  const double predicted_cost_;     ///< The predicted cost of this action.
};

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class MonteCarloTreeSearch {
 public:
  /**
   * Constructor for the monte carlo search tree
   * @param pilot pointer to pilot
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   * @param use_min_cost whether to use the minimum cost of all leaves as the cost for internal nodes
   */
  MonteCarloTreeSearch(common::ManagedPointer<Pilot> pilot,
                       common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
                       bool use_min_cost = true);

  /**
   * Returns query string of the best action to take at the root of the current tree
   * @param simulation_number number of simulations to run
   * @param best_action_seq
   * @return query string of the best first action as well as the associated database oid
   */
  void BestAction(uint64_t simulation_number, std::vector<MCTSAction> *best_action_seq);

 private:
  const common::ManagedPointer<Pilot> pilot_;
  const common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  const uint64_t end_segment_index_;
  std::unique_ptr<TreeNode> root_;
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map_;
  std::vector<action_id_t> candidate_actions_;
  bool use_min_cost_;  // Use the minimum cost of all leaves (instead of the average) as the cost for internal nodes
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
