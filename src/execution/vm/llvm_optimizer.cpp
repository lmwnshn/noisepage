#include "execution/vm/llvm_optimizer.h"

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>

#include <iostream>

#include "common/macros.h"
#include "common/scoped_timer.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::vm {

const FunctionTransform FunctionOptimizer::TRANSFORMS[] = {
    // ---------------------------------------------------------------------------------------------------------------
    // LLVM transformations.
    // Names are copied from the corresponding LLVM argument and may have a suffix representing different configs.
    // High-level descriptions are sourced from https://releases.llvm.org/8.0.0/docs/Passes.html and/or LLVM source.
    // ---------------------------------------------------------------------------------------------------------------

    /// -adce: Aggressive dead code elimination.
    /// ADCE aggressively tries to eliminate code. This pass is similar to DCE but it assumes that values are dead until
    /// proven otherwise. This is similar to SCCP, except applied to the liveness of values.
    {"adce", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createAggressiveDCEPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -always-inline: Inline and remove functions marked as "always_inline".
    //    {"always-inline-lifetime",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createAlwaysInlinerLegacyPass(true)); }},
    //    {"always-inline-no-lifetime",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createAlwaysInlinerLegacyPass(false)); }},

    // TODO(WAN): SIGSEGV.
    //    /// -argpromotion: Promote "by reference" arguments to scalars.
    //    /// This pass promotes "by reference" arguments to be passed by value if the number of elements passed is
    //    smaller or
    //    /// equal to maxElements (maxElements == 0 means always promote).
    //    {"argpromotion-always",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createArgumentPromotionPass(0)); }},
    //    {"argpromotion-default",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createArgumentPromotionPass()); }},

    // TODO(WAN): Unknown API equivalent: -bb-vectorize: Basic-block vectorization.
    // TODO(WAN): Unknown API equivalent: -block-placement: Profile guided basic block placement.
    // TODO(WAN): Unknown API equivalent: -break-crit-edges: Break critical edges in CFG.
    // TODO(WAN): Unknown API equivalent: -codegenprepare: Optimize for code generation.

    // TODO(WAN): SIGSEGV.
    //    /// -constmerge: Merge duplicate global constants.
    //    {"constmerge", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createConstantMergePass()); }},

    /// -constprop: Simple constant propagation.
    {"constprop", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createConstantPropagationPass()); }},

    /// -dce: Dead Code Elimination.
    /// Dead code elimination is similar to dead instruction elimination, but it rechecks instructions that were used by
    /// removed instructions to see if they are newly dead.
    {"dce", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createDeadCodeEliminationPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -deadargelim: Dead argument elimination.
    //    {"deadargelim", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createDeadArgEliminationPass());
    //    }},

    // TODO(WAN): Unknown API equivalent: -deadtypeelim: Dead Type Elimination.

    /// -die: Dead Instruction Elimination.
    /// A single pass over the function removing instructions that are obviously dead.
    {"die", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createDeadInstEliminationPass()); }},

    /// -dse: Dead Store Elimination.
    /// A trivial dead store elimination that only considers basic-block local redundant stores.
    {"dse", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createDeadStoreEliminationPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -functionattrs: Deduce function attributes.
    //    /// This pass walks SCCs of the call graph in reverse post-order to deduce and propagate function attributes.
    //    /// Currently it only handles synthesizing norecurse attributes.
    //    {"functionattrs",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createReversePostOrderFunctionAttrsPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -globaldce: Dead global elimination.
    //    /// This transform is designed to eliminate unreachable internal globals from the program.
    //    {"globaldce", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createGlobalDCEPass()); }},

    // TODO(WAN): SIGTRAP.
    //    /// -globalopt: Global variable optimizer.
    //    /// This pass transforms simple global variables that never have their address taken. If obviously true, it
    //    marks
    //    /// read/write globals as constant, deletes variables only stored to, etc.
    //    {"globalopt", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createGlobalOptimizerPass()); }},

    /// -gvn: Global value numbering.
    /// This pass performs global value numbering to eliminate fully and partially redundant instructions.
    /// It also (optionally) performs redundant load elimination.
    {"gvn", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createGVNPass()); }},
    {"gvn-no-load-elimination", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createGVNPass(true)); }},

    /// -indvars: Canonicalize induction variables.
    /// Analyze and simplify induction variables. See details in the docs.
    /// @warning This should be followed by strength reduction.
    {"indvars", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createIndVarSimplifyPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -inline: Function inlining.
    //    /// -inline-aggressive is what TPL was initially doing.
    //    {"inline", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createFunctionInliningPass()); }},
    //    {"inline-aggressive",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createFunctionInliningPass(3, 0, false)); }},

    /// -instcombine: Combine redundant instructions.
    /// Performs algebraic simplifications. May be enhanced with -functionattrs depending on LLVM's library knowledge.
    {"instcombine",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createInstructionCombiningPass(false)); }},
    /// -aggressive-instcombine: Combine expression patterns.
    {"aggressive-instcombine",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createInstructionCombiningPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -internalize: Internalize global symbols.
    //    /// TODO(WAN): This may kill our symbol resolution.
    //    {"internalize", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createInternalizePass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -ipconstprop: Interprocedural constant propagation.
    //    /// Propagate constants from call sites into bodies of functions.
    //    {"ipconstprop", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createIPConstantPropagationPass());
    //    }},

    // TODO(WAN): SIGSEGV.
    //    /// -ipsccp: Interprocedural sparse conditional constant propagation.
    //    /// Propagate constants from call sites into the bodies of functions, tracking if basic blocks are executable.
    //    {"ipsccp", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createIPSCCPPass()); }},

    /// -jump-threading: Jump Threading
    /// If one or more predecessors of a basic block provably always jumps to a specific successor, forward the edge.
    /// TODO(WAN): I don't think that we need to expose the Threshold argument, which controls BB duplication threshold.
    {"jump-threading", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createJumpThreadingPass()); }},

    // TODO(WAN): Unknown API equivalent: -lcssa: Loop-Closed SSA Form Pass

    /// -licm: Loop invariant code motion.
    {"licm", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLICMPass()); }},

    /// -loop-deletion: Delete dead loops.
    {"loop-deletion", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopDeletionPass()); }},

    // TODO(WAN): Predictably, this blows up the optimization time. The extract-single seems OK though.
    //    /// -loop-extract: Extract loops into new functions.
    //    {"loop-extract", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopExtractorPass()); }},

    /// -loop-extract-single: Extract at most one loop into a new function.
    {"loop-extract-single",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createSingleLoopExtractorPass()); }},

    /// -loop-reduce: Loop strength reduction.
    {"loop-reduce", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopStrengthReducePass()); }},

    /// -loop-rotate: Rotate loops.
    {"loop-rotate", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopRotatePass()); }},

    /// -loop-simplify: Canonicalize natural loops.
    {"loop-simplify", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopSimplifyCFGPass()); }},

    /// -loop-unroll: Unroll loops.
    /// @warning Works best when -indvars has been run.
    /// TODO(WAN): Huge number of tunable parameters to expose.
    {"loop-unroll", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopUnrollPass()); }},

    /// -loop-unroll-and-jam: Unroll-and-jam loops.
    /// Unroll the outer loop and fuse the inner loops into one. See the documentation.
    /// TODO(WAN): OptLevel.
    {"loop-unroll-and-jam",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopUnrollAndJamPass()); }},

    /// -loop-unswitch: Unswitch loops.
    /// TODO(WAN): OptimizeForSize, BranchDivergence parameters.
    {"loop-unswitch", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopUnswitchPass()); }},

    // TODO(WAN): Unsafe.
    //    /// -loweratomic: Lower atomic intrinsics to non-atomic form
    //    /// This pass lowers atomic intrinsics to non-atomic form for use in a known non-preemptible environment.
    //    /// @warning Does not actually verify the environment is non-preemptible. Practically, can't use this.
    //    {"loweratomic", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLowerAtomicPass()); }},

    // TODO(WAN): Unknown API equivalent: -lowerinvoke: Lower invokes to calls, for unwindless code generators
    // TODO(WAN): Unknown API equivalent: -lowerswitch: Lower SwitchInsts to branches
    // TODO(WAN): Unknown API equivalent: -mem2reg: Promote Memory to Register

    /// -memcpyopt: memcpy optimization.
    /// Transformations related to eliminating memcpy calls, or transforming sets of stores into memsets.
    {"memcpyopt", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createMemCpyOptPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -mergefunc: Merge functions.
    //    /// Look for equivalent functions that are mergable and fold them.
    //    {"mergefunc", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createMergeFunctionsPass()); }},

    // TODO(WAN): Unknown API equivalent: -mergereturn: Unify function exit nodes

    // TODO(WAN): SIGSEGV.
    //    /// -partial-inliner: Partial Inliner
    //    /// Partial inlining, typically by inlining an if statement that surrounds the body of the function.
    //    {"partial-inliner", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createPartialInliningPass());
    //    }},

    // TODO(WAN): SIGSEGV.
    //    /// -prune-eh: Remove unused exception handling info.
    //    /// Turn invoke instructions into call instructions if and only if the callee cannot throw an exception.
    //    {"prune-eh", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createPruneEHPass()); }},

    /// -reassociate: Reassociate expressions
    /// Reassociate commutative expressions to promote better constant propagation, GCSE, LICM, PRE, etc.
    {"reassociate", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createReassociatePass()); }},

    // TODO(WAN): I don't think you want to enable this.
    //    /// -reg2mem: Demote all values to stack slot.
    //    {"reg2mem", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createDemoteRegisterToMemoryPass());
    //    }},

    /// -sroa: Scalar replacement of aggregates.
    /// Break up alloca for structs into individual alloca for members, then transform into SSA if possible.
    {"sroa", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createSROAPass()); }},

    /// -sccp: Sparse conditional constant propagation.
    /// @note Good idea to run DCE of some kind afterwards.
    {"sccp", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createSCCPPass()); }},

    // -simplifycfg: Simplify the CFG
    /// TODO(WAN): Many parameters to optimize.
    //    unsigned Threshold = 1, bool ForwardSwitchCond = false,
    //    bool ConvertSwitch = false, bool KeepLoops = true, bool SinkCommon = false,
    //    std::function<bool(const Function &)> Ftor = nullptr
    {"simplifycfg", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createCFGSimplificationPass()); }},

    /// -sink: Code sinking
    /// Move instructions into successor blocks when possible, avoid execution on paths where results not needed.
    {"sink", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createSinkingPass()); }},

    // TODO(WAN): Unknown API equivalent: -strip: Strip all symbols from a module

    // TODO(WAN): SIGSEGV.
    //    /// -strip-dead-debug-info: Strip debug info for unused symbols.
    //    {"strip-dead-debug-info",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createStripDeadDebugInfoPass()); }},

    // TODO(WAN): SIGSEGV.
    //    /// -strip-dead-prototypes: Strip unused function prototypes.
    //    {"strip-dead-prototypes",
    //     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createStripDeadPrototypesPass()); }},

    // TODO(WAN): Unknown API equivalent: -strip-debug-declare: Strip all llvm.dbg.declare intrinsics
    // TODO(WAN): Unknown API equivalent: -strip-nondebug: Strip all symbols, except dbg symbols, from a module

    /// -tailcallelim: Tail-call elimination.
    {"tailcallelim", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createTailCallEliminationPass()); }},

    // ---------------------------------------------------------------------------------------------------------------
    // LLVM transformations not documented in https://releases.llvm.org/8.0.0/docs/Passes.html but in API.
    // Prefixed with nd- to mean not documented.
    // ---------------------------------------------------------------------------------------------------------------

    // TODO(WAN): Exhaustively check stuff in the API. Here are some examples.
    {"nd-correlated-value-propagation",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createCorrelatedValuePropagationPass()); }},
    {"nd-early-cse", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createEarlyCSEPass()); }},
    {"nd-flatten-cfg", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createFlattenCFGPass()); }},
    {"nd-gvn", [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createNewGVNPass()); }},
    {"nd-loop-inst-simplify",
     [](llvm::legacy::FunctionPassManager &fpm) { fpm.add(llvm::createLoopInstSimplifyPass()); }},

    // WARNING: Keep the name of the last LLVM transform in sync with LAST_LLVM_TRANSFORM_IDX's lookup name.

    // ---------------------------------------------------------------------------------------------------------------
    // Custom hand-picked sets of transformations.
    // ---------------------------------------------------------------------------------------------------------------

    /// Harness the power of Prashanth Menon.
    {"pmenon",
     [](llvm::legacy::FunctionPassManager &fpm) {
       // Add custom passes. Hand-selected based on empirical evaluation.
       fpm.add(llvm::createInstructionCombiningPass());
       fpm.add(llvm::createReassociatePass());
       fpm.add(llvm::createGVNPass());
       fpm.add(llvm::createCFGSimplificationPass());
       fpm.add(llvm::createAggressiveDCEPass());
       fpm.add(llvm::createCFGSimplificationPass());
     }},
};

const uint64_t FunctionOptimizer::TRANSFORMS_IDX_LAST_LLVM = std::distance(
    std::begin(TRANSFORMS),
    std::find_if(TRANSFORMS, TRANSFORMS + (sizeof(TRANSFORMS) / sizeof(TRANSFORMS[0])),
                 [](const FunctionTransform &transform) { return transform.name_ == "nd-loop-inst-simplify"; }));
const uint64_t FunctionOptimizer::TRANSFORMS_IDX_PMENON =
    std::distance(std::begin(TRANSFORMS),
                  std::find_if(TRANSFORMS, TRANSFORMS + (sizeof(TRANSFORMS) / sizeof(TRANSFORMS[0])),
                               [](const FunctionTransform &transform) { return transform.name_ == "pmenon"; }));

// Start of actual code.

// FunctionMetadata.

std::string FunctionMetadata::ToStrLong() const { return ToStrShort() + " " + ToStrOnlyInputTransforms(); }

std::string FunctionMetadata::ToStrShort() const {
  return fmt::format("[{} insts, {} opt ns, {} exec ns]", inst_count_, optimize_ns_, exec_ns_);
}

std::string FunctionMetadata::ToStrOnlyInputTransforms() const {
  // TODO(WAN): stringstreamblablabla
  std::string result{"["};
  for (const auto &transform : input_transforms_) {
    result += transform.name_ + ";";
  }
  result += "]";
  return result;
}

// FunctionProfile.

void FunctionProfile::EndIteration() {
  auto init_agg = [](MetadataAgg *agg, const FunctionMetadata &sample) {
    agg->num_samples_ = 1;
    agg->original_ = sample;
    agg->last_ = sample;
    agg->min_ = sample;
    agg->mean_ = sample;
    agg->max_ = sample;
  };

  auto update_agg = [](MetadataAgg *agg, const FunctionMetadata &sample) {
    agg->num_samples_++;
    agg->last_ = sample;
    // min
    if (sample.exec_ns_ < agg->min_.exec_ns_) {
      agg->min_ = sample;
    }
    // mean (no thought was given to numerical stability)
    agg->mean_.inst_count_ = (agg->mean_.inst_count_ * (agg->num_samples_ - 1) + sample.inst_count_) /
                             static_cast<double>(agg->num_samples_);
    agg->mean_.optimize_ns_ = (agg->mean_.optimize_ns_ * (agg->num_samples_ - 1) + sample.optimize_ns_) /
                              static_cast<double>(agg->num_samples_);
    agg->mean_.exec_ns_ =
        (agg->mean_.exec_ns_ * (agg->num_samples_ - 1) + sample.exec_ns_) / static_cast<double>(agg->num_samples_);
    // max
    if (sample.exec_ns_ > agg->max_.exec_ns_) {
      agg->max_ = sample;
    }
  };

  for (auto &entry : functions_) {
    entry.second.prev_prev_ = entry.second.prev_;
    entry.second.prev_ = entry.second.curr_;

    if (should_update_agg_) {
      if (!is_agg_initialized_) {
        init_agg(&entry.second.agg_, entry.second.prev_);
        // The is_agg_initialized_ flag is set later after updating combined_agg_.
      } else {
        update_agg(&entry.second.agg_, entry.second.prev_);
      }
    }
    entry.second.curr_ = FunctionMetadata{};
  }
  if (!is_agg_initialized_) {
    init_agg(&combined_agg_, GetCombinedPrev());
    is_agg_initialized_ = true;
  } else {
    update_agg(&combined_agg_, GetCombinedPrev());
  }

  iteration_transform_count_ = 0;
}

FunctionMetadata FunctionProfile::GetCombinedPrev() const {
  FunctionMetadata result;
  for (auto &entry : functions_) {
    bool is_step = std::find(steps_.cbegin(), steps_.cend(), entry.first) != steps_.cend();
    bool is_teardown = std::find(teardowns_.cbegin(), teardowns_.cend(), entry.first) != teardowns_.cend();
    if (is_step || is_teardown) {
      const FunctionMetadata &md = entry.second.prev_;
      result.inst_count_ += abs(md.inst_count_);
      result.optimize_ns_ += abs(md.optimize_ns_);
      result.exec_ns_ += abs(md.exec_ns_);
      result.strategy_ = md.strategy_;
      result.input_transforms_ = md.input_transforms_;
      result.transforms_ = md.transforms_;
    }
  }
  return result;
}

FunctionMetadata FunctionProfile::GetCombinedPrevPrev() const {
  FunctionMetadata result;
  for (auto &entry : functions_) {
    bool is_step = std::find(steps_.cbegin(), steps_.cend(), entry.first) != steps_.cend();
    bool is_teardown = std::find(teardowns_.cbegin(), teardowns_.cend(), entry.first) != teardowns_.cend();
    if (is_step || is_teardown) {
      const FunctionMetadata &md = entry.second.prev_prev_;
      result.inst_count_ += abs(md.inst_count_);
      result.optimize_ns_ += abs(md.optimize_ns_);
      result.exec_ns_ += abs(md.exec_ns_);
      result.strategy_ = md.strategy_;
      result.input_transforms_ = md.input_transforms_;
      result.transforms_ = md.transforms_;
    }
  }
  return result;
}

void FunctionProfile::StartAgg() {
  NOISEPAGE_ASSERT(!should_update_agg_, "Already aggregating.");
  should_update_agg_ = true;
  is_agg_initialized_ = false;
  for (auto &entry : functions_) {
    entry.second.agg_ = MetadataAgg{};
  }
  combined_agg_ = MetadataAgg{};
}

void FunctionProfile::SetProfileLevelTransforms(std::vector<FunctionTransform> transforms) {
  transforms_ = std::move(transforms);
}

void FunctionProfile::PrintModule() {
  for (const auto &it : functions_) {
    std::cout << "IR for: " << it.first << GetCurr(it.first)->ir_ << std::endl;
  }
}

// FunctionOptimizer.

FunctionOptimizer::FunctionOptimizer(common::ManagedPointer<llvm::TargetMachine> target_machine)
    : target_machine_(target_machine), rng_llvm_only_(0, TRANSFORMS_IDX_LAST_LLVM) {
  beam_search_transforms_.resize(TRANSFORMS_IDX_LAST_LLVM + 1, 0);
  for (uint64_t i = 0; i <= TRANSFORMS_IDX_LAST_LLVM; i++) {
    beam_search_transforms_[i] = i;
  }
}

void FunctionOptimizer::Simplify(const common::ManagedPointer<llvm::Module> llvm_module,
                                 UNUSED_ATTRIBUTE const LLVMEngineCompilerOptions &options,
                                 UNUSED_ATTRIBUTE const common::ManagedPointer<FunctionProfile> profile) {
  // When this function is called, the generated IR consists of many function
  // calls to cross-compiled bytecode handler functions. We now inline those
  // function calls directly into the body of the functions we've generated
  // by running the 'AlwaysInliner' pass.
  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createAlwaysInlinerLegacyPass());
  pass_manager.add(llvm::createGlobalDCEPass());
  pass_manager.run(*llvm_module);
}

void FunctionOptimizer::Optimize(const common::ManagedPointer<llvm::Module> llvm_module,
                                 UNUSED_ATTRIBUTE const LLVMEngineCompilerOptions &options,
                                 const common::ManagedPointer<FunctionProfile> profile) {
  // Check if the previous pass was good.
  bool was_good = true;
  if (was_good) {
    // This currently picks the minimum execution time always.

    // Evaluate the result of the last time, if it exists.
    FunctionMetadata default_metadata{};
    auto aggmin = profile->GetCombinedAgg()->min_;
    auto prev = profile->GetCombinedPrev();
    if (prev.transforms_ != aggmin.transforms_ && !(aggmin == default_metadata)) {
      auto pct = static_cast<double>(prev.exec_ns_ - aggmin.exec_ns_) / aggmin.exec_ns_;
      double epsilon_pct = -0.05;  // At least 5% faster AND 500 ns better.
      int64_t epsilon_ns = -500;
      if (pct < epsilon_pct && (prev.exec_ns_ - aggmin.exec_ns_) < epsilon_ns) {
        std::cout << fmt::format("|----| (Pre-strategy) Better by {} exec ns ({} opt), keeping {}.",
                                 prev.exec_ns_ - aggmin.exec_ns_, prev.optimize_ns_,
                                 FunctionProfile::GetTransformsStr(prev.transforms_))
                  << std::endl;
        profile->SetProfileLevelTransforms(prev.transforms_);
      } else {
        std::cout
            << fmt::format(
                   "|----| (Pre-strategy) Sucks. Change of {} exec ns ({} opt), discarding {} and reverting to {}.",
                   prev.exec_ns_ - aggmin.exec_ns_, prev.optimize_ns_,
                   FunctionProfile::GetTransformsStr(prev.transforms_),
                   FunctionProfile::GetTransformsStr(aggmin.transforms_))
            << std::endl;
        profile->SetProfileLevelTransforms(aggmin.transforms_);
      }
    }
  }
  auto strategy = profile->GetStrategy();
  auto transforms = GetTransforms("", strategy, profile->GetCombinedPrev().strategy_, profile);
  std::cout << "|----| (Post-strategy) Transforms: " << FunctionProfile::GetTransformsStr(transforms) << std::endl;

  for (llvm::Function &func : *llvm_module) {
    std::string func_name{func.getName()};
    // TODO(WAN): Only prints out functions that we can collect execution times for right now.
    bool is_step =
        std::find(profile->GetSteps().cbegin(), profile->GetSteps().cend(), func_name) != profile->GetSteps().cend();
    bool is_teardown = std::find(profile->GetTeardowns().cbegin(), profile->GetTeardowns().cend(), func_name) !=
                       profile->GetTeardowns().cend();
    bool can_profile = is_step || is_teardown;

    profile->GetCurr(func_name)->input_transforms_ = profile->GetProfileLevelTransforms();

    llvm::legacy::FunctionPassManager function_passes(llvm_module.Get());

    // Add the appropriate TargetTransformInfo.
    function_passes.add(llvm::createTargetTransformInfoWrapperPass(target_machine_->getTargetIRAnalysis()));

    // Build up optimization pipeline.
    llvm::PassManagerBuilder pm_builder;
    uint32_t opt_level = 3;
    uint32_t size_opt_level = 0;
    bool disable_inline_hot_call_site = false;
    pm_builder.OptLevel = opt_level;
    pm_builder.Inliner = llvm::createFunctionInliningPass(opt_level, size_opt_level, disable_inline_hot_call_site);
    pm_builder.populateFunctionPassManager(function_passes);

    // Add passes as determined by the strategy.

    std::string applied_transforms_dbg{""};
    for (FunctionTransform &pass : transforms) {
      pass.transform_(function_passes);
      applied_transforms_dbg += pass.name_ + ";";
    }
    if (can_profile && false) {
      std::cout << fmt::format("|--| Picked ({}): {}", func_name, applied_transforms_dbg) << std::endl;
    }

    // Run optimization passes on the current function.
    function_passes.doInitialization();
    uint64_t elapsed_ns;
    {
      if (can_profile && false) {
        auto func_profile = profile->GetPrev(func_name);
        std::cout << fmt::format("|----| Profile input ({}): {}", func_name, func_profile->ToStrLong()) << std::endl;
      }
    }
    {
      common::ScopedTimer<std::chrono::nanoseconds> timer(&elapsed_ns);
      function_passes.run(func);
    }
    profile->GetCurr(func_name)->optimize_ns_ = elapsed_ns;
    profile->GetCurr(func_name)->strategy_ = strategy;
    profile->GetCurr(func_name)->transforms_ = transforms;

    function_passes.doFinalization();
  }

  FinalizeStats(llvm_module, options, profile);
}

void FunctionOptimizer::FinalizeStats(const common::ManagedPointer<llvm::Module> llvm_module,
                                      const LLVMEngineCompilerOptions &options,
                                      const common::ManagedPointer<FunctionProfile> profile) {
  // Last chance to grab compile-time attributes.
  for (llvm::Function &func : *llvm_module) {
    std::string func_name = func.getName();

    // Instruction count.
    profile->GetCurr(func_name)->inst_count_ = func.getInstructionCount();
    // IR.
    llvm::Function *llvm_func = llvm_module->getFunction(func_name);
    llvm::raw_string_ostream ostream(profile->GetCurr(func_name)->ir_);
    llvm_func->print(ostream, nullptr);
  }
}

const uint64_t FunctionOptimizer::SlowGetTransformIdx(const std::string &transform_name) {
  for (uint64_t i = 0; i < sizeof(TRANSFORMS) / sizeof(TRANSFORMS[0]); ++i) {
    if (TRANSFORMS[i].name_ == transform_name) {
      return i;
    }
  }
  return -1;
}

std::vector<FunctionTransform> FunctionOptimizer::GetTransforms(const std::string &func_name,
                                                                const OptimizationStrategy strategy,
                                                                const OptimizationStrategy prev_strategy,
                                                                const common::ManagedPointer<FunctionProfile> profile) {
  std::vector<FunctionTransform> transforms = profile->GetProfileLevelTransforms();
  switch (strategy) {
    case OptimizationStrategy::NOOP: {
      if (profile->GetIterationTransformCount() == 0) {
        profile->SetProfileLevelTransforms(transforms);
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
    case OptimizationStrategy::PMENON: {
      if (profile->GetIterationTransformCount() == 0) {
        //        profile->SetProfileLevelTransforms({TRANSFORMS[TRANSFORMS_IDX_PMENON]});
        profile->SetProfileLevelTransforms(
            {TRANSFORMS[SlowGetTransformIdx("aggressive-instcombine")], TRANSFORMS[SlowGetTransformIdx("reassociate")],
             TRANSFORMS[SlowGetTransformIdx("gvn")], TRANSFORMS[SlowGetTransformIdx("simplifycfg")],
             TRANSFORMS[SlowGetTransformIdx("adce")], TRANSFORMS[SlowGetTransformIdx("simplifycfg")]});
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
    case OptimizationStrategy::RANDOM_ADD: {
      // TODO(WAN): While we support per-function stuff, it is a headache to explain those results.
      //            Instead, just use profile-level transforms.
      // std::vector<FunctionTransform> transforms = profile->GetPrev(func_name)->transforms_;
      std::vector<FunctionTransform> transforms = profile->GetProfileLevelTransforms();
      // Only do work if this is the first time this iteration (hacks around GetTransform called by every func).
      if (profile->GetIterationTransformCount() == 0) {
        // Add a random transform.
        transforms.emplace_back(TRANSFORMS[rng_llvm_only_(gen_)]);
        profile->SetProfileLevelTransforms(transforms);
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
    case OptimizationStrategy::RANDOM_MUTATE: {
      std::vector<FunctionTransform> transforms = profile->GetProfileLevelTransforms();
      // Only do work if this is the first time this iteration (hacks around GetTransform called by every func).
      if (profile->GetIterationTransformCount() == 0 && !transforms.empty()) {
        std::uniform_int_distribution<uint32_t> dist(0, transforms.size() - 1);
        // Mutate a random transform.
        uint64_t mutate_idx = dist(gen_);
        transforms[mutate_idx] = TRANSFORMS[rng_llvm_only_(gen_)];
        profile->SetProfileLevelTransforms(transforms);
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
    case OptimizationStrategy::RANDOM_GENETIC: {
      std::vector<FunctionTransform> transforms = profile->GetProfileLevelTransforms();
      if (profile->GetIterationTransformCount() == 0) {
        std::uniform_int_distribution<uint32_t> dist(1, 4);
        uint32_t choice = dist(gen_);
        std::uniform_int_distribution<uint32_t> idx_dist(0, transforms.size() - 1);
        // Mutate a random transform.
        uint64_t choice_idx = idx_dist(gen_);
        if (transforms.empty()) {
          // Don't waste time by doing an empty run.
          transforms.emplace_back(TRANSFORMS[rng_llvm_only_(gen_)]);
        } else if (choice == 1 && !transforms.empty()) {
          // Delete a transform
          transforms.erase(transforms.begin() + choice_idx);
        } else if (choice == 2 && !transforms.empty()) {
          // Mutate a transform
          transforms[choice_idx] = TRANSFORMS[rng_llvm_only_(gen_)];
        } else if (choice == 3) {
          // Add a transform
          transforms.emplace_back(TRANSFORMS[rng_llvm_only_(gen_)]);
        }
        profile->SetProfileLevelTransforms(transforms);
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
    case OptimizationStrategy::RANDOM_DISTINCT: {
      // Select a random index to add, optimize will sort out the min
      std::vector<FunctionTransform> transforms = profile->GetProfileLevelTransforms();
      // Only do work if this is the first time this iteration (hacks around GetTransform called by every func).
      if (profile->GetIterationTransformCount() == 0 && !beam_search_transforms_.empty()) {
        // Add a random previously unselected transform.
        std::uniform_int_distribution<uint32_t> dist(0, beam_search_transforms_.size() - 1);
        uint32_t choice = dist(gen_);
        transforms.emplace_back(TRANSFORMS[beam_search_transforms_[choice]]);
        beam_search_transforms_.erase(beam_search_transforms_.begin() + choice);
        profile->SetProfileLevelTransforms(transforms);
        profile->IncrementIterationTransformCount();
      }
      return transforms;
    }
  }
}

}  // namespace noisepage::execution::vm
