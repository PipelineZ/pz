using Pz.DuckDb;
using Pz.Engine.Preview;

namespace Pz.Engine.Tests.Preview;

/// <summary>The cell rules every preview shares: redaction inside DuckDB, JSON-safe kinds, the 200-character
/// cut.</summary>
public sealed class PreviewSamplerTests
{
    private static async Task<DuckSession> With(string sql)
    {
        var duck = DuckSession.Open(":memory:");
        await duck.ExecuteAsync("create schema staging");
        await duck.ExecuteAsync($"create table staging.t as {sql}");
        return duck;
    }

    [Fact]
    public async Task Columns_carry_duckdb_type_names_and_redaction()
    {
        await using var duck = await With("select 1::integer as id, 'a@b'::varchar as email");

        var (columns, _) = await PreviewSampler.SampleAsync(duck, "staging.t", ["email"], 50, default);

        Assert.Equal([new PreviewColumn("id", "INTEGER", false), new PreviewColumn("email", "VARCHAR", true)], columns);
    }

    [Fact]
    public async Task Redacted_cells_are_dots_including_nulls()
    {
        await using var duck = await With("select * from (values ('a@b'), (null)) v(email)");

        var (_, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", ["email"], 50, default);

        Assert.All(rows, r => Assert.Equal(new PreviewCell(PreviewCellKind.Text, "•••"), r[0]));
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Redaction_matches_column_names_case_insensitively()
    {
        await using var duck = await With("select 'a@b' as email");

        var (columns, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", ["EMAIL"], 50, default);

        Assert.True(columns[0].Redacted);
        Assert.Equal("•••", rows[0][0].Text);
    }

    [Fact]
    public async Task Numbers_and_booleans_keep_their_kind()
    {
        await using var duck = await With(
            "select 7::integer as i, 1.5::double as d, 12.50::decimal(18,2) as m, " +
            "170141183460469231731687303715884105727::hugeint as h, true as b, null::integer as n");

        var (_, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", [], 50, default);

        Assert.Equal(new PreviewCell(PreviewCellKind.Number, "7"), rows[0][0]);
        Assert.Equal(new PreviewCell(PreviewCellKind.Number, "1.5"), rows[0][1]);
        Assert.Equal(new PreviewCell(PreviewCellKind.Number, "12.50"), rows[0][2]);
        Assert.Equal(new PreviewCell(PreviewCellKind.Number, "170141183460469231731687303715884105727"), rows[0][3]);
        Assert.Equal(new PreviewCell(PreviewCellKind.Boolean, "true"), rows[0][4]);
        Assert.Equal(PreviewCellKind.Null, rows[0][5].Kind);
    }

    [Fact]
    public async Task Non_finite_doubles_become_text()
    {
        await using var duck = await With("select * from (values ('nan'::double), ('inf'::double), ('-inf'::double)) v(d)");

        var (_, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", [], 50, default);

        Assert.All(rows, r => Assert.Equal(PreviewCellKind.Text, r[0].Kind));
    }

    [Fact]
    public async Task Other_types_become_text()
    {
        await using var duck = await With(
            "select date '2026-11-01' as d, timestamp '2026-11-01 10:00:00' as ts, {'a': 1} as s, [1, 2] as l, " +
            "'\\xAA'::blob as b");

        var (_, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", [], 50, default);

        Assert.All(rows[0], c => Assert.Equal(PreviewCellKind.Text, c.Kind));
        Assert.Equal("2026-11-01", rows[0][0].Text);
    }

    [Fact]
    public void Truncate_cuts_at_200_and_appends_ellipsis()
    {
        Assert.Equal(new string('a', 200) + "…", PreviewSampler.Truncate(new string('a', 300)));
        Assert.Equal(new string('a', 200), PreviewSampler.Truncate(new string('a', 200)));
    }

    [Fact]
    public void Truncate_never_splits_a_surrogate_pair()
    {
        var cut = PreviewSampler.Truncate(new string('a', 199) + "😀" + new string('b', 10));

        Assert.EndsWith("…", cut, StringComparison.Ordinal);
        Assert.False(char.IsHighSurrogate(cut[^2]));
        Assert.Equal(new string('a', 199) + "…", cut);
    }

    [Fact]
    public async Task Limit_caps_the_rows()
    {
        await using var duck = await With("select range as id from range(120)");

        var (_, rows) = await PreviewSampler.SampleAsync(duck, "staging.t", [], 50, default);

        Assert.Equal(50, rows.Count);
    }
}
