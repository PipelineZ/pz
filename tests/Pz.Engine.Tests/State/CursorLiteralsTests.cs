using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary><see cref="CursorLiterals.Typed"/> renders a canonical cursor value as a typed
/// DuckDB/SQL literal. Numeric types render bare (canonical digits are valid SQL literals as-is);
/// date/timestamp render as typed quoted literals.</summary>
public sealed class CursorLiteralsTests
{
    [Fact]
    public void Timestamp_renders_typed_quoted_literal() =>
        Assert.Equal("TIMESTAMP '2026-07-15T08:00:00.000000'",
            CursorLiterals.Typed("timestamp", "2026-07-15T08:00:00.000000"));

    [Fact]
    public void Date_renders_typed_quoted_literal() =>
        Assert.Equal("DATE '2026-07-15'", CursorLiterals.Typed("date", "2026-07-15"));

    [Fact]
    public void Int_renders_bare_digits() =>
        Assert.Equal("42", CursorLiterals.Typed("int", "42"));

    [Fact]
    public void Bigint_renders_bare_digits() =>
        Assert.Equal("9000000000", CursorLiterals.Typed("bigint", "9000000000"));

    [Fact]
    public void Decimal_renders_bare_digits() =>
        Assert.Equal("12.50", CursorLiterals.Typed("decimal", "12.50"));
}
