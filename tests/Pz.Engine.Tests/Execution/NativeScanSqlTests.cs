using Pz.Core.Dag;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

public sealed class NativeScanSqlTests
{
    [Fact]
    public void No_hints_is_todays_statement() =>
        Assert.Equal("select * from read_parquet('f')", NativeScanSql.Staging("read_parquet('f')", null));

    [Fact]
    public void Empty_hints_is_todays_statement() =>
        Assert.Equal("select * from read_parquet('f')", NativeScanSql.Staging("read_parquet('f')", ReadHintPlan.None));

    [Fact]
    public void Columns_and_predicate_wrap_the_fragment() =>
        Assert.Equal("select \"id\", \"we\"\"ird\" from read_parquet('f') where (k = 7)",
            NativeScanSql.Staging("read_parquet('f')", new ReadHintPlan(["id", "we\"ird"], "k = 7")));

    [Fact]
    public void Predicate_only_keeps_every_column() =>
        Assert.Equal("select * from read_parquet('f') where (k = 7)",
            NativeScanSql.Staging("read_parquet('f')", new ReadHintPlan(null, "k = 7")));
}
