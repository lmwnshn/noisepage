#pragma once

#include <functional>
#include <random>
#include <string>

#include "common/managed_pointer.h"

namespace llvm {
class Module;
class TargetMachine;

namespace legacy {
class FunctionPassManager;
}  // namespace legacy
}  // namespace llvm

namespace noisepage::execution::vm {

class LLVMEngineCompilerOptions;

/** Optimization strategy. */
enum class OptimizationStrategy {
  PMENON,
  NOOP,
  RANDOM_ADD,
};

class ProfilerControls {
 public:
  OptimizationStrategy strategy_;
  uint64_t num_iterations_left_{0};
  bool should_agg_{false};
  bool should_print_agg_{false};
  bool should_print_fragment_{false};
};

struct FunctionTransform {
  std::string name_;
  std::function<void(llvm::legacy::FunctionPassManager &)> transform_;

  bool operator==(const FunctionTransform &other) const { return name_ == other.name_; }
  // Any other metadata...
};

/** Metadata for each function. */
struct FunctionMetadata {
  // TODO(WAN): Break out counters?
  std::string ir_{""};                                         ///< The IR of the function.
  int64_t inst_count_{0};                                      ///< The instruction count of the function.
  int64_t optimize_ns_{0};                                     ///< Time taken to optimize the function.
  int64_t exec_ns_{0};                                         ///< Time taken to run the function.
  OptimizationStrategy strategy_{OptimizationStrategy::NOOP};  ///< The strategy used to optimize this function.
  std::vector<FunctionTransform> transforms_{};  ///< The transforms applied to optimize this function. TODO(WAN): space

  bool operator==(const FunctionMetadata &other) const {
    return ir_ == other.ir_ && inst_count_ == other.inst_count_ && optimize_ns_ == other.optimize_ns_ &&
           exec_ns_ == other.exec_ns_ && strategy_ == other.strategy_ && transforms_ == other.transforms_;
  }

  FunctionMetadata operator-(const FunctionMetadata &operand) const {
    FunctionMetadata md;
    md.inst_count_ = inst_count_ - operand.inst_count_;
    md.optimize_ns_ = optimize_ns_ - operand.optimize_ns_;
    md.exec_ns_ = exec_ns_ - operand.exec_ns_;
    return md;
  }

  std::string ToStrLong() const;
  std::string ToStrShort() const;
  std::string ToStrOnlyTransforms() const;
};

/**
 * A mockup of information that we hope to obtain through Kyle's implementation of Tagged Dictionaries from
 * http://db.in.tum.de/~beischl/papers/Profiling_Dataflow_Systems_on_Multiple_Abstraction_Levels.pdf
 * lorem ipsum
 *
 * TODO(WAN): I guess in the absence of profile information it
 */
class FunctionProfile {
 public:
  struct MetadataAgg {
    uint64_t num_samples_;
    FunctionMetadata original_;
    FunctionMetadata last_;
    FunctionMetadata min_;
    FunctionMetadata mean_;
    FunctionMetadata max_;
  };

  FunctionProfile() = default;

  void SetStrategy(OptimizationStrategy strategy) { strategy_ = strategy; }
  OptimizationStrategy GetStrategy() const { return strategy_; }

  void StartAgg();
  void StopAgg() { should_update_agg_ = false; }
  bool IsAgg() const { return should_update_agg_; }

  void SetNumIterationsLeft(uint64_t num_iterations_left) { num_iterations_left_ = num_iterations_left; }
  void EndIteration();

  void RegisterSteps(const std::vector<std::string> &steps) { steps_ = steps; }
  void RegisterTeardowns(const std::vector<std::string> &teardowns) { teardowns_ = teardowns; }
  const std::vector<std::string> &GetSteps() const { return steps_; }
  const std::vector<std::string> &GetTeardowns() const { return teardowns_; }

  common::ManagedPointer<FunctionMetadata> GetPrev(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].prev_);
  }
  common::ManagedPointer<FunctionMetadata> GetCurr(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].curr_);
  }
  common::ManagedPointer<MetadataAgg> GetAgg(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].agg_);
  }

  FunctionMetadata GetCombinedPrevPrev() const;
  FunctionMetadata GetCombinedPrev() const;
  common::ManagedPointer<MetadataAgg> GetCombinedAgg() { return common::ManagedPointer(&combined_agg_); }

  const std::vector<FunctionTransform> &GetProfileLevelTransforms() const { return transforms_; }

  void PrintModule();

  static std::string GetTransformsStr(const std::vector<FunctionTransform> &transforms) {
    std::string blah{"["};
    for (const auto &transform : transforms) {
      blah += transform.name_ + ";";
    }
    blah += "]";
    return blah;
  }

  void SetProfileLevelTransforms(std::vector<FunctionTransform> transforms);
  uint64_t GetIterationTransformCount() const { return iteration_transform_count_; }
  void IncrementIterationTransformCount() { iteration_transform_count_++; }

 private:
  OptimizationStrategy strategy_{OptimizationStrategy::PMENON};  ///< Default to Prashanth's picks.

  uint64_t num_iterations_left_;  ///< When this reaches 0, there are no more profiling iterations coming. Last chance.
  std::vector<std::string> steps_;
  std::vector<std::string> teardowns_;

  struct MetadataContainer {
    FunctionMetadata prev_prev_;
    FunctionMetadata prev_;
    FunctionMetadata curr_;
    MetadataAgg agg_;
  };

  std::unordered_map<std::string, MetadataContainer> functions_;
  std::vector<FunctionTransform> transforms_;  ///< The transforms applied to optimize entire profile, if relevant
  MetadataAgg combined_agg_;
  bool should_update_agg_{false};
  bool is_agg_initialized_{false};
  uint64_t iteration_transform_count_{0};
};

/**
 * Integration work where you decide how functions are represented, how they get applied, costing,
 * maybe connect this to metrics
 *
 * tpl.cpp
 *
 */
class FunctionOptimizer {
 public:
  explicit FunctionOptimizer(common::ManagedPointer<llvm::TargetMachine> target_machine);

  void Simplify(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                common::ManagedPointer<FunctionProfile> profile);

  void Optimize(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                common::ManagedPointer<FunctionProfile> profile);

 private:
  std::vector<FunctionTransform> GetTransforms(const std::string &func_name, OptimizationStrategy strategy,
                                               OptimizationStrategy prev_strategy,
                                               common::ManagedPointer<FunctionProfile> profile);

  void FinalizeStats(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                     common::ManagedPointer<FunctionProfile> profile);

  static const FunctionTransform TRANSFORMS[];

  static const uint64_t TRANSFORMS_IDX_LAST_LLVM;  ///< Index of last LLVM builtin transform, inclusive.
  static const uint64_t TRANSFORMS_IDX_PMENON;     ///< Index of Prashanth's hand-picked transform.

  const common::ManagedPointer<llvm::TargetMachine> target_machine_;

  std::random_device rd_{};
  std::mt19937 gen_{rd_()};
  std::uniform_int_distribution<int> rng_llvm_only_;
};

}  // namespace noisepage::execution::vm