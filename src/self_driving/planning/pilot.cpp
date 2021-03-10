#include "self_driving/planning/pilot.h"

#include <cstdio>
#include <memory>
#include <utility>

#include "common/action_context.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/vm/vm_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"
#include "self_driving/planning/pilot_util.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_manager.h"
#include "util/query_exec_util.h"
#include "util/query_internal_thread.h"

namespace noisepage::selfdriving {

uint64_t Pilot::planning_iteration = 1;

void Pilot::SetQueryExecUtil(std::unique_ptr<util::QueryExecUtil> query_exec_util) {
  query_exec_util_ = std::move(query_exec_util);
}

Pilot::Pilot(std::string model_save_path, std::string forecast_model_save_path,
             common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval)
    : model_save_path_(std::move(model_save_path)),
      forecast_model_save_path_(std::move(forecast_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;
  while (!model_server_manager_->ModelServerStarted()) {
  }
}

void Pilot::PerformForecasterTrain() {
  // TODO(wz2): Maybe instead of passing the CSV file to the model server,
  // we should instead just read the last forecast interval and send it.
  std::vector<std::string> models{"LSTM"};
  std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};
  modelserver::ModelServerFuture<std::string> future;
  model_server_manager_->TrainForecastModel(models, input_path, forecast_model_save_path_, workload_forecast_interval_,
                                            common::ManagedPointer(&future));
  future.Wait();
}

std::pair<WorkloadMetadata, bool> Pilot::RetrieveWorkloadMetadata(
    uint64_t iteration,
    const std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> &out_metadata,
    const std::unordered_map<execution::query_id_t, std::vector<std::string>> &out_params) {
  // Initialize the workload metadata
  WorkloadMetadata metadata;

  // Lambda function to convert a JSON-serialized param string to a vector of type ids
  auto types_conv = [](const std::string &param_types) {
    std::vector<type::TypeId> types;
    auto json_decomp = nlohmann::json::parse(param_types);
    for (auto &elem : json_decomp) {
      types.push_back(type::TypeUtil::TypeIdFromString(elem));
    }
    return types;
  };

  // Lambda function to convert a JSON-serialized constants to a vector of cexpressions
  auto cves_conv = [](const WorkloadMetadata &metadata, execution::query_id_t qid, const std::string &cve) {
    // Read the parameters. In the worse case
    std::vector<parser::ConstantValueExpression> cves;
    const std::vector<type::TypeId> &types = metadata.query_id_to_param_types_.find(qid)->second;
    auto json_decomp = nlohmann::json::parse(cve);
    for (size_t i = 0; i < json_decomp.size(); i++) {
      cves.emplace_back(parser::ConstantValueExpression::FromString(json_decomp[i], types[i]));
    }
    return cves;
  };

  for (auto &info : out_metadata) {
    metadata.query_id_to_dboid_[info.first] = info.second.db_oid_.UnderlyingValue();
    metadata.query_id_to_text_[info.first] = info.second.text_.substr(1, info.second.text_.size() - 2);
    metadata.query_id_to_param_types_[info.first] = types_conv(info.second.param_type_);
  }

  for (auto &info : out_params) {
    for (auto &cve : info.second) {
      metadata.query_id_to_params_[info.first].emplace_back(cves_conv(metadata, info.first, cve));
    }
  }

  bool result = true;
  query_exec_util_->BeginTransaction();
  {
    // Metadata query
    auto to_row_fn = [&metadata, types_conv](const std::vector<execution::sql::Val *> &values) {
      auto db_oid = static_cast<execution::sql::Integer *>(values[0])->val_;
      auto qid = execution::query_id_t(static_cast<execution::sql::Integer *>(values[1])->val_);

      // Only insert new if not convered already
      if (metadata.query_id_to_dboid_.find(qid) == metadata.query_id_to_dboid_.end()) {
        metadata.query_id_to_dboid_[qid] = db_oid;

        auto *text_val = static_cast<execution::sql::StringVal *>(values[2]);
        // We do this since the string has been quoted by the metric
        metadata.query_id_to_text_[qid] =
            std::string(text_val->StringView().data() + 1, text_val->StringView().size() - 2);

        auto *param_types = static_cast<execution::sql::StringVal *>(values[3]);
        metadata.query_id_to_param_types_[qid] = types_conv(std::string(param_types->StringView()));
      }
    };

    // This loads the entire query text history from the internal tables. It might be possible to
    // do on-demand fetching or windowed fetching at a futrure time. We do this because a interval
    // can execute a prepared query without a corresponding text recording (if the query was
    // already prepared during a prior interval).
    auto query = "SELECT * FROM noisepage_forecast_texts";
    query_exec_util_->SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
    result &= query_exec_util_->ExecuteDML(query, nullptr, nullptr, to_row_fn, nullptr);
  }

  {
    auto to_row_fn = [&metadata, cves_conv](const std::vector<execution::sql::Val *> &values) {
      auto qid = execution::query_id_t(static_cast<execution::sql::Integer *>(values[1])->val_);
      auto *param_val = static_cast<execution::sql::StringVal *>(values[2]);
      {
        // Read the parameters. In the worse case, we will have double the parameters, but that is
        // okay since every parameter will be duplicated. This can happen since the parameters
        // could already be visible by the time this select query runs.
        metadata.query_id_to_params_[qid].emplace_back(cves_conv(metadata, qid, std::string(param_val->StringView())));
      }
    };

    auto query = fmt::format("SELECT * FROM noisepage_forecast_parameters WHERE iteration = {}", iteration);
    query_exec_util_->SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
    result &= query_exec_util_->ExecuteDML(query, nullptr, nullptr, to_row_fn, nullptr);
  }

  query_exec_util_->EndTransaction(true);
  return std::make_pair(std::move(metadata), result);
}

void Pilot::RecordWorkloadForecastPrediction(uint64_t iteration,
                                             const selfdriving::WorkloadForecastPrediction &prediction,
                                             const WorkloadMetadata &metadata) {
  if (query_internal_thread_ == nullptr) {
    return;
  }

  // We don't want to do these inserts as part of forecasting itself.
  // So we create the result and let a background thread actually handle it.
  util::ExecuteRequest cluster_request;
  util::ExecuteRequest forecast_request;

  {
    // Clusters
    cluster_request.type_ = util::RequestType::DML;
    cluster_request.notify_ = nullptr;
    cluster_request.db_oid_ = catalog::INVALID_DATABASE_OID;
    cluster_request.query_text_ = "INSERT INTO noisepage_forecast_clusters VALUES ($1, $2, $3, $4)";
    cluster_request.cost_model_ = nullptr;
    cluster_request.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER,
                                    type::TypeId::INTEGER};
  }

  {
    // Forecasts
    forecast_request.type_ = util::RequestType::DML;
    forecast_request.notify_ = nullptr;
    forecast_request.db_oid_ = catalog::INVALID_DATABASE_OID;
    forecast_request.query_text_ = "INSERT INTO noisepage_forecast_forecasts VALUES ($1, $2, $3, $4)";
    forecast_request.cost_model_ = nullptr;
    forecast_request.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER,
                                     type::TypeId::REAL};
  }

  // This is a bit more memory intensive, since we have to copy all the parameters
  for (auto &cluster : prediction) {
    for (auto &qid_info : cluster.second) {
      execution::query_id_t qid{static_cast<uint32_t>(qid_info.first)};

      // This assert is correct because we loaded the entire query history from the internal tables.
      NOISEPAGE_ASSERT(metadata.query_id_to_dboid_.find(qid) != metadata.query_id_to_dboid_.end(),
                       "Expected QID info to exist");
      std::vector<parser::ConstantValueExpression> clusters_params(4);
      clusters_params[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
      clusters_params[1] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(cluster.first));
      clusters_params[2] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(qid_info.first));
      clusters_params[3] = parser::ConstantValueExpression(
          type::TypeId::INTEGER, execution::sql::Integer(metadata.query_id_to_dboid_.find(qid)->second));
      cluster_request.params_.emplace_back(std::move(clusters_params));

      for (size_t interval = 0; interval < qid_info.second.size(); interval++) {
        std::vector<parser::ConstantValueExpression> forecasts_params(4);
        forecasts_params[0] =
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
        forecasts_params[1] =
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(cluster.first));
        forecasts_params[2] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(interval));
        forecasts_params[3] =
            parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(qid_info.second[interval]));
        forecast_request.params_.emplace_back(std::move(forecasts_params));
      }
    }
  }

  // Submit the request to be executed
  query_internal_thread_->AddRequest(std::move(cluster_request));
  query_internal_thread_->AddRequest(std::move(forecast_request));
}

void Pilot::LoadWorkloadForecast() {
  // Metrics thread is suspended at this point
  auto metrics_output = metrics_thread_->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  metrics_thread_->GetMetricsManager()->Aggregate();
  metrics_thread_->GetMetricsManager()->ToOutput();

  std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> out_metadata;
  std::unordered_map<execution::query_id_t, std::vector<std::string>> out_params;
  if (metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_DB) {
    auto raw = reinterpret_cast<metrics::QueryTraceMetricRawData *>(
        metrics_thread_->GetMetricsManager()
            ->AggregatedMetrics()
            .at(static_cast<uint8_t>(metrics::MetricsComponent::QUERY_TRACE))
            .get());
    if (raw != nullptr) {
      // Perform a flush to database. This will also get any temporary data.
      // This is also used to flush all parameter information at a forecast interval.
      raw->WriteToDB(common::ManagedPointer(query_exec_util_), common::ManagedPointer(query_internal_thread_), true,
                     true, &out_metadata, &out_params);
    }
  }

  auto iteration = Pilot::planning_iteration++;
  std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};

  // For now, forecast relies on the CSV file.
  // TODO(wz2): Pass the "seen" data directly to the forecast model?
  std::vector<std::string> models{"LSTM"};
  auto result = model_server_manager_->InferForecastModel(input_path, forecast_model_save_path_, models, nullptr,
                                                          workload_forecast_interval_);
  if (!result.second) {
    SELFDRIVING_LOG_ERROR("Forecast model inference failed");
    metrics_thread_->ResumeMetrics();
    return;
  }

  if (query_exec_util_ &&
      (metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_DB)) {
    // Retrieve query information from internal tables
    auto metadata_result = RetrieveWorkloadMetadata(iteration, out_metadata, out_params);
    if (!metadata_result.second) {
      SELFDRIVING_LOG_ERROR("Failed to read from internal trace metadata tables");
      metrics_thread_->ResumeMetrics();
      return;
    }

    // Record forecast into internal tables
    RecordWorkloadForecastPrediction(iteration, result.first, metadata_result.first);

    // Construct workload forecast
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(result.first, &metadata_result.first);
  } else {
    auto sample = settings_manager_->GetInt(settings::Param::forecast_sample_limit);
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(workload_forecast_interval_, sample);
  }

  // Copy file for backup -- future will not use this data
  // /TODO(wz2): Do we want to do this?
  for (size_t i = 0; i < 2; i++) {
    std::string input_path{metrics::QueryTraceMetricRawData::FILES[i]};
    auto filename = fmt::format("{}_{}", input_path.c_str(), iteration);
    std::rename(input_path.c_str(), filename.c_str());
  }
}

void Pilot::PerformPlanning() {
  // We do the inference by having the python process read in the query_trace.csv file.
  // However, for the sampled parameters and query information, we will actually pull
  // that data directly from the internal SQL tables.

  // Suspend the metrics thread while we are handling the data (snapshot).
  metrics_thread_->PauseMetrics();

  // Populate the workload forecast
  LoadWorkloadForecast();

  // Perform planning
  std::vector<std::pair<const std::string, catalog::db_oid_t>> best_action_seq;
  Pilot::ActionSearch(&best_action_seq);

  metrics_thread_->ResumeMetrics();
}

void Pilot::ActionSearch(std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq) {
  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);

  auto mcst =
      pilot::MonteCarloTreeSearch(common::ManagedPointer(this), common::ManagedPointer(forecast_), end_segment_index);
  mcst.BestAction(simulation_number_, best_action_seq);
  for (uint64_t i = 0; i < best_action_seq->size(); i++) {
    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}", i,
                                     best_action_seq->at(i).first,
                                     static_cast<uint32_t>(best_action_seq->at(i).second)));
  }
  PilotUtil::ApplyAction(common::ManagedPointer(this), best_action_seq->begin()->first,
                         best_action_seq->begin()->second);
}

void Pilot::ExecuteForecast(std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                     std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction,
                            uint64_t start_segment_index, uint64_t end_segment_index) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  // record previous parameters to be restored at the end of this function
  const bool old_metrics_enable = settings_manager_->GetBool(settings::Param::pipeline_metrics_enable);
  const bool old_counters_enable = settings_manager_->GetBool(settings::Param::counters_enable);
  const auto old_sample_rate = settings_manager_->GetInt64(settings::Param::pipeline_metrics_sample_rate);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, 100, common::ManagedPointer(action_context),
                            EmptySetterCallback);

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_), start_segment_index,
                                                          end_segment_index, &pipeline_qids);
  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::InferenceWithFeatures(model_save_path_, model_server_manager_, pipeline_qids, pipeline_data,
                                   pipeline_to_prediction);

  // restore the old parameters
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, old_sample_rate,
                            common::ManagedPointer(action_context), EmptySetterCallback);
}

}  // namespace noisepage::selfdriving
