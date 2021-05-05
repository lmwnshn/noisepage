#include "execution/compiler/executable_query.h"

#include <algorithm>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/scoped_timer.h"
#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/compiler.h"
#include "execution/exec/execution_context.h"
#include "execution/sema/error_reporter.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"
#include "self_driving/modeling/operating_unit.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::compiler {

//===----------------------------------------------------------------------===//
//
// Executable Query Fragment
//
//===----------------------------------------------------------------------===//

ExecutableQuery::Fragment::Fragment(std::vector<std::string> &&functions, std::vector<std::string> &&teardown_fn,
                                    std::unique_ptr<vm::Module> module)
    : functions_(std::move(functions)), teardown_fn_(std::move(teardown_fn)), module_(std::move(module)) {
  // Note which are the step and teardown functions.
  module_->GetFunctionProfile()->RegisterSteps(functions_);
  module_->GetFunctionProfile()->RegisterTeardowns(teardown_fn_);
}

ExecutableQuery::Fragment::~Fragment() = default;

void ExecutableQuery::Fragment::Run(byte query_state[], vm::ExecutionMode mode) const {
  using Function = std::function<void(void *)>;

  auto exec_ctx = *reinterpret_cast<exec::ExecutionContext **>(query_state);
  if (exec_ctx->GetTxn()->MustAbort()) {
    return;
  }
  for (const auto &func_name : functions_) {
    Function func;
    if (!module_->GetFunction(func_name, mode, &func)) {
      throw EXECUTION_EXCEPTION(fmt::format("Could not find function '{}' in query fragment.", func_name),
                                common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
    try {
      // TODO(WAN): This is a temporary hack that can only capture execution times for registered steps.
      //            Kyle to replace this with TaggedDictionary approach to capture execution times for all functions.
      uint64_t func_duration_ns;
      {
        common::ScopedTimer<std::chrono::nanoseconds> func_timer(&func_duration_ns);
        func(query_state);
      }
      module_->GetFunctionProfile()->GetCurr(func_name)->exec_ns_ = func_duration_ns;
    } catch (const AbortException &e) {
      for (const auto &teardown_name : teardown_fn_) {
        if (!module_->GetFunction(teardown_name, mode, &func)) {
          throw EXECUTION_EXCEPTION(fmt::format("Could not find teardown function '{}' in query fragment.", func_name),
                                    common::ErrorCode::ERRCODE_INTERNAL_ERROR);
        }
        func(query_state);
      }
      return;
    }
  }
}

void ExecutableQuery::Fragment::ForceRecompile() { module_->DangerousRecompile(); }

common::ManagedPointer<vm::FunctionProfile> ExecutableQuery::Fragment::GetFunctionProfile() {
  return module_->GetFunctionProfile();
}

void ExecutableQuery::Fragment::PrintFragment() { GetFunctionProfile()->PrintModule(); }

void ExecutableQuery::Fragment::ResetFragment() { module_->ResetProfile(); }

//===----------------------------------------------------------------------===//
//
// Executable Query
//
//===----------------------------------------------------------------------===//

// For mini_runners.cpp.
namespace {
std::string GetFileName(const std::string &path) {
  std::size_t size = path.size();
  std::size_t found = path.find_last_of("/\\");
  return path.substr(found + 1, size - found - 5);
}
}  // namespace

std::atomic<query_id_t> ExecutableQuery::query_identifier{0};

void ExecutableQuery::SetPipelineOperatingUnits(std::unique_ptr<selfdriving::PipelineOperatingUnits> &&units) {
  pipeline_operating_units_ = std::move(units);
}

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan, const exec::ExecutionSettings &exec_settings)
    : plan_(plan),
      exec_settings_(exec_settings),
      errors_region_(std::make_unique<util::Region>("errors_region")),
      context_region_(std::make_unique<util::Region>("context_region")),
      errors_(std::make_unique<sema::ErrorReporter>(errors_region_.get())),
      ast_context_(std::make_unique<ast::Context>(context_region_.get(), errors_.get())),
      query_state_size_(0),
      pipeline_operating_units_(nullptr),
      query_id_(query_identifier++) {}

ExecutableQuery::ExecutableQuery(const std::string &contents,
                                 const common::ManagedPointer<exec::ExecutionContext> exec_ctx, bool is_file,
                                 size_t query_state_size, const exec::ExecutionSettings &exec_settings)
    // TODO(WAN): Giant hack for the plan. The whole point is that you have no plan.
    : plan_(reinterpret_cast<const planner::AbstractPlanNode &>(exec_settings)), exec_settings_(exec_settings) {
  context_region_ = std::make_unique<util::Region>("context_region");
  errors_region_ = std::make_unique<util::Region>("error_region");
  errors_ = std::make_unique<sema::ErrorReporter>(errors_region_.get());
  ast_context_ = std::make_unique<ast::Context>(context_region_.get(), errors_.get());

  // Let's scan the source
  std::string source;
  if (is_file) {
    auto file = llvm::MemoryBuffer::getFile(contents);
    if (std::error_code error = file.getError()) {
      EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", contents, error.message());
      return;
    }

    // Copy the source into a temporary, compile, and run
    source = (*file)->getBuffer().str();
  } else {
    source = contents;
  }

  auto input = Compiler::Input("tpl_source", ast_context_.get(), &source);
  auto module = compiler::Compiler::RunCompilationSimple(input);

  std::vector<std::string> functions{"main"};
  std::vector<std::string> teardown_functions;
  auto fragment = std::make_unique<Fragment>(std::move(functions), std::move(teardown_functions), std::move(module));

  std::vector<std::unique_ptr<Fragment>> fragments;
  fragments.emplace_back(std::move(fragment));

  Setup(std::move(fragments), query_state_size, nullptr);

  if (is_file) {
    // acquire the output format
    query_name_ = GetFileName(contents);
  }
}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() = default;

void ExecutableQuery::Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, const std::size_t query_state_size,
                            std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_operating_units) {
  NOISEPAGE_ASSERT(
      std::all_of(fragments.begin(), fragments.end(), [](const auto &fragment) { return fragment->IsCompiled(); }),
      "All query fragments are not compiled!");
  NOISEPAGE_ASSERT(query_state_size >= sizeof(void *),
                   "Query state must be large enough to store at least an ExecutionContext pointer.");

  fragments_ = std::move(fragments);
  query_state_size_ = query_state_size;
  pipeline_operating_units_ = std::move(pipeline_operating_units);

  EXECUTION_LOG_TRACE("Query has {} fragment{} with {}-byte query state.", fragments_.size(),
                      fragments_.size() > 1 ? "s" : "", query_state_size_);
}

void ExecutableQuery::Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<exec::ExecutionContext **>(query_state.get()) = exec_ctx.Get();
  exec_ctx->SetQueryState(query_state.get());

  exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));
  exec_ctx->SetPipelineOperatingUnits(GetPipelineOperatingUnits());
  exec_ctx->SetQueryId(query_id_);

  // Now run through fragments.
  for (const auto &fragment : fragments_) {
    fragment->Run(query_state.get(), mode);
  }

  // We do not currently re-use ExecutionContexts. However, this is unset to help ensure
  // we don't *intentionally* retain any dangling pointers.
  exec_ctx->SetQueryState(nullptr);
}

void ExecutableQuery::RunProfileRecompile(common::ManagedPointer<exec::ExecutionContext> exec_ctx,
                                          const vm::ProfilerControls &controls) {
  auto mode = vm::ExecutionMode::Compiled;
  auto query_state = std::make_unique<byte[]>(query_state_size_);

  *reinterpret_cast<exec::ExecutionContext **>(query_state.get()) = exec_ctx.Get();
  exec_ctx->SetQueryState(query_state.get());

  exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));
  exec_ctx->SetPipelineOperatingUnits(GetPipelineOperatingUnits());
  exec_ctx->SetQueryId(query_id_);

  for (const auto &fragment : fragments_) {
    auto profile = fragment->GetFunctionProfile();
    profile->SetStrategy(controls.strategy_);
    if (controls.should_agg_) {
      if (!profile->IsAgg()) {
        profile->StartAgg();
        std::cout << "|--| AGG START." << std::endl;
      }
    } else {
      if (profile->IsAgg()) {
        profile->StopAgg();
        std::cout << "|--| AGG STOP." << std::endl;
      }
    }
    profile->SetNumIterationsLeft(controls.num_iterations_left_);
    fragment->Run(query_state.get(), mode);
    profile->EndIteration();

    std::cout << "|--| RECOMPILE." << std::endl;

    std::string strat = "wtf";
    switch (controls.strategy_) {
      case vm::OptimizationStrategy::NOOP:
        strat = "NOOP";
        break;
      case vm::OptimizationStrategy::PMENON:
        strat = "PMENON";
        break;
      case vm::OptimizationStrategy::RANDOM_ADD:
        strat = "RANDOM_ADD";
        break;
      case vm::OptimizationStrategy::RANDOM_DISTINCT:
        strat = "RANDOM_DISTINCT";
        break;
      case vm::OptimizationStrategy::RANDOM_GENETIC:
        strat = "RANDOM_GENETIC";
        break;
      case vm::OptimizationStrategy::RANDOM_MUTATE:
        strat = "RANDOM_MUTATE";
        break;
    }

    std::cout << "|--| Profile strategy " << strat << ", input (combined): " << profile->GetCombinedPrev().ToStrLong()
              << std::endl;
    fragment->ForceRecompile();
    if (controls.should_print_agg_) {
      auto agg = profile->GetCombinedAgg();
      std::cout << "|--| AGG DATA." << std::endl;
      std::cout << "|----| Agg num_samples: " << agg->num_samples_ << std::endl;
      std::cout << "|----| Agg original: " << agg->original_.ToStrLong() << std::endl;
      std::cout << "|----| Agg last: " << agg->last_.ToStrLong() << std::endl;
      std::cout << "|----| Agg min: " << agg->min_.ToStrLong() << std::endl;
      std::cout << "|----| Agg mean: " << agg->mean_.ToStrShort() << std::endl;
      std::cout << "|----| Agg max: " << agg->max_.ToStrLong() << std::endl;
    }
    if (controls.should_print_fragment_) {
      fragment->PrintFragment();
    }
  }

  // All profiling runs must abort!
  exec_ctx->GetTxn()->SetMustAbort();
}

void ExecutableQuery::ResetFragmentProfiles() {
  for (auto &fragment : fragments_) {
    fragment->ResetFragment();
  }
}

}  // namespace noisepage::execution::compiler
