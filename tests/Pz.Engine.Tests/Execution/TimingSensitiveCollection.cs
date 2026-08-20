namespace Pz.Engine.Tests.Execution;

/// <summary>Serializes tests whose PASS/FAIL hinges on wall-clock promptness of a bounded wait, not
/// merely on eventual correctness. <see cref="DuckSession"/> executes every operation as a blocking
/// <c>_gate.Wait</c> inside <c>Task.Run</c> (a known pre-existing perf caveat, not something this
/// collection works around) -- with 400+ tests running in parallel across the full suite the
/// ThreadPool can starve badly enough that a bounded <c>WaitAsync(TimeSpan.FromSeconds(30))</c>
/// legitimately watching for a PROMPT self-cancel expires before that self-cancel gets scheduled, even
/// though the underlying engine behavior is correct (finite delay vs. hung forever is what the
/// assertion actually discriminates). That is an environment-dependent flake, not a product bug, so
/// isolating the handful of tests that make this class of assertion from the rest of the suite's
/// ThreadPool pressure removes the starvation instead of further loosening bounds that already exist
/// to be generous.
///
/// Only move a class into this collection if it has a genuine wall-clock-bounded promptness assertion
/// (a wait that can resolve ONLY via a prompt scheduling race, where the discrimination is
/// finite-vs-infinite) -- do not use this as a blanket "flaky test" bucket; parallelism is suite
/// speed and most tests do not need this.</summary>
[CollectionDefinition("partition-fault-timing", DisableParallelization = true)]
public sealed class TimingSensitiveCollection;
