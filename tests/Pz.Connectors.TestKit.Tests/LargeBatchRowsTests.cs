using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;

/// <summary>A destination that throttles writes cannot take the default large batch in reasonable
/// time, and dropping the fact altogether would lose the multi-batch coverage it gives. The size is the
/// subclass's to set.</summary>
public class LargeBatchRowsTests
{
    [Fact]
    public async Task The_large_batch_fact_writes_as_many_rows_as_the_subclass_asks_for()
    {
        var acceptance = new ThrottledDestination();

        await acceptance.Commit_persists_a_large_batch();

        Assert.Equal(400, acceptance.Last!.Committed.SelectMany(c => c.Batches).Sum(b => (long)b.Length));
    }

    private sealed class ThrottledDestination : InMemorySinkAcceptance
    {
        public InMemoryConnector? Last { get; private set; }

        protected override int LargeBatchRows => 400;

        protected override ISinkConnector CreateSink() => Last = new InMemoryConnector();
    }
}
