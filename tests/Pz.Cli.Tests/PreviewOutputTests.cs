using Pz.Cli.Commands;
using Pz.Engine.Preview;

namespace Pz.Cli.Tests;

/// <summary>The text table goes to a terminal: values are data from a source, so a control character or a
/// bidi override in a cell must not reach it raw. The JSON is for a program and keeps values as written
/// (JSON escapes control characters itself).</summary>
public sealed class PreviewOutputTests
{
    [Fact]
    public void Text_table_shows_control_and_bidi_characters_escaped()
    {
        var result = new PreviewResult("n", "run", "r",
            [new PreviewColumn("c\u001b[2J", "VARCHAR", false)],
            [[new PreviewCell(PreviewCellKind.Text, "a\u001b]0;pwned\u0007b‮c\u009bd\te")]],
            1, 50, false);

        var table = PreviewOutput.ToTable(result);

        Assert.DoesNotContain('\u001b', table);
        Assert.DoesNotContain('\u0007', table);
        Assert.DoesNotContain('‮', table);
        Assert.DoesNotContain('\u009b', table);
        Assert.Contains("\\x1b", table, StringComparison.Ordinal);
        Assert.Contains("\\u202e", table, StringComparison.Ordinal);
    }

    [Fact]
    public void Json_escapes_control_characters_itself() =>
        Assert.Contains("\\u001B", PreviewOutput.ToJson(new PreviewResult("n", "run", "r",
            [new PreviewColumn("c", "VARCHAR", false)], [[new PreviewCell(PreviewCellKind.Text, "a\u001bb")]], 1, 50, false)),
            StringComparison.OrdinalIgnoreCase);
}
