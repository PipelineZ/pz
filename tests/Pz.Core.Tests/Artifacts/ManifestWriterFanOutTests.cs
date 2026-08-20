using Pz.Core.Artifacts;
using Pz.Core.Dag;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Artifacts;

/// <summary>Fan-out: a pipeline binding N sink outputs via the array
/// `INSERT INTO [{{ sink(...) }}, ...]` form must get N `-- output:` header lines in its compiled
/// `.sql` artifact, one per bound output, sorted by `&lt;sink&gt;.&lt;output&gt;` ordinal for
/// byte-stable output. The 1:1 scalar form is covered byte-for-byte by the hello-pz golden
/// (<see cref="GoldenCompileTests"/>) and must stay unchanged.</summary>
public class ManifestWriterFanOutTests
{
    [Fact]
    public void Fan_out_pipeline_gets_one_header_line_per_bound_output_in_sorted_order()
    {
        // Array order (zzz, aaa) is deliberately the reverse of sorted order (aaa, zzz) so this
        // test fails if ResolveInlineBindingHeader falls back to DAG/declaration order instead of
        // sorting by "<sink>.<output>" ordinal.
        var project = Project(
            [Pipe("combo", "INSERT INTO [{{ sink('zzz', 'out', strategy: 'replace', format: 'parquet') }}, {{ sink('aaa', 'out', strategy: 'replace', format: 'parquet') }}] select 1 as x")],
            sinks: [Sink("zzz"), Sink("aaa")]);
        var dag = DagCompiler.Compile(project, Ctx(project));

        var target = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            ManifestWriter.Write(dag, project, target);
            var sql = File.ReadAllText(Path.Combine(target, "compiled", "combo.sql"));

            Assert.Equal(
                "-- output: aaa.out (parquet, replace)\n" +
                "-- output: zzz.out (parquet, replace)\n" +
                "select 1 as x\n",
                sql);
        }
        finally
        {
            Directory.Delete(target, recursive: true);
        }
    }
}
