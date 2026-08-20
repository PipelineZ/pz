using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

/// <summary>Keeps https://pipelinez.dev/events/ honest: reflects over every exported <see cref="RunEvent"/>-derived record in
/// Pz.Diagnostics and asserts https://pipelinez.dev/events/ has a `##` section for it, named by the same snake_case
/// convention <see cref="Pz.Cli.Rendering.JsonRenderer"/> uses for the `event` field (record name minus
/// the trailing "Event", snake_cased). A new event record with no matching doc heading fails this test
/// instead of silently going undocumented — and, at the field level, every PUBLIC property the record
/// declares (its own positional members; <see cref="RunEvent.At"/>/<see cref="RunEvent.RunId"/> are
/// inherited and documented once in the shared envelope table, not per-section) must appear as a
/// `` `fieldName` `` row in that section's field table, and vice versa (a documented field with no
/// matching property would mean the doc describes something that no longer exists) — so a new
/// <c>NodeTimings</c>-shaped field (or any other) can never slip into the wire format undocumented.</summary>
public class EventsDocReflectionTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        // thisFile: tests/Pz.Cli.Tests/Rendering/EventsDocReflectionTests.cs -> repo root is four
        // directories up.
        var dir = Path.GetDirectoryName(thisFile)!;
        for (var i = 0; i < 3; i++)
        {
            dir = Path.GetDirectoryName(dir)!;
        }

        return dir;
    }

    [Fact]
    public void Every_RunEvent_record_has_a_documented_section()
    {
        var docPath = Path.Combine(RepoRoot(), "docs", "events.md");
        Assert.True(File.Exists(docPath), $"expected {docPath} to exist");
        var doc = File.ReadAllText(docPath);

        var eventTypes = typeof(RunEvent).Assembly.GetExportedTypes()
            .Where(t => t.IsSealed && t.IsAssignableTo(typeof(RunEvent)) && t != typeof(RunEvent))
            .ToList();

        Assert.NotEmpty(eventTypes);

        foreach (var type in eventTypes)
        {
            var eventName = ToSnakeCase(type.Name.Replace("Event", ""));
            Assert.Contains($"## `{eventName}`", doc);
        }
    }

    /// <summary>Field-level companion to <see cref="Every_RunEvent_record_has_a_documented_section"/>
    /// every declared public property of every
    /// <see cref="RunEvent"/> record must show up in its section's field table, and every field
    /// documented there must correspond to a real property — both directions, since either drift means
    /// the doc no longer matches the wire format. Also descends into any PROPERTY whose type is itself a
    /// Pz.Diagnostics record (e.g. <see cref="NodeCompletedEvent.Timings"/>'s <see cref="NodeTimingsPayload"/>)
    /// and applies the same both-directions check to ITS declared properties against the nested
    /// `` `{fieldName}` (when present): `` sub-table https://pipelinez.dev/events/ nests under that event's section, so a
    /// future payload field (like `producerStallMs`) can't join the wire format without also landing in
    /// https://pipelinez.dev/events/.</summary>
    [Fact]
    public void Every_RunEvent_property_is_documented_and_every_documented_field_still_exists()
    {
        var docPath = Path.Combine(RepoRoot(), "docs", "events.md");
        var doc = File.ReadAllText(docPath);

        var eventTypes = typeof(RunEvent).Assembly.GetExportedTypes()
            .Where(t => t.IsSealed && t.IsAssignableTo(typeof(RunEvent)) && t != typeof(RunEvent))
            .ToList();

        foreach (var type in eventTypes)
        {
            var eventName = ToSnakeCase(type.Name.Replace("Event", ""));
            var documentedFields = ExtractDocumentedFields(doc, eventName);

            var declaredProperties = type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .ToList();

            foreach (var property in declaredProperties)
            {
                var propertyName = ToCamelCase(property.Name);
                Assert.True(documentedFields.Contains(propertyName),
                    $"{type.Name}.{propertyName} is not documented in https://pipelinez.dev/events/'s `{eventName}` field table");

                // Nested payload record (e.g. NodeTimingsPayload): recurse into its own properties against
                // the `{propertyName}` (when present): sub-table, both directions.
                if (property.PropertyType.IsClass && property.PropertyType != typeof(string)
                    && property.PropertyType.Assembly == typeof(RunEvent).Assembly
                    && !property.PropertyType.IsAssignableTo(typeof(RunEvent)))
                {
                    var nestedType = property.PropertyType;
                    var nestedDocumentedFields = ExtractNestedDocumentedFields(doc, eventName, propertyName);
                    var nestedProperties = nestedType
                        .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                        .Select(p => ToCamelCase(p.Name))
                        .ToList();

                    foreach (var nestedProperty in nestedProperties)
                    {
                        Assert.True(nestedDocumentedFields.Contains(nestedProperty),
                            $"{nestedType.Name}.{nestedProperty} (nested under {type.Name}.{propertyName}) is not " +
                            $"documented in https://pipelinez.dev/events/'s `{eventName}` `{propertyName}` sub-table");
                    }

                    foreach (var nestedField in nestedDocumentedFields)
                    {
                        Assert.True(nestedProperties.Contains(nestedField),
                            $"https://pipelinez.dev/events/'s `{eventName}` `{propertyName}` sub-table documents `{nestedField}`, " +
                            $"but {nestedType.Name} has no matching public property (documented-but-gone field)");
                    }
                }
            }

            foreach (var field in documentedFields)
            {
                Assert.True(declaredProperties.Any(p => ToCamelCase(p.Name) == field),
                    $"https://pipelinez.dev/events/'s `{eventName}` field table documents `{field}`, but {type.Name} has no " +
                    "matching public property (documented-but-gone field)");
            }
        }
    }

    /// <summary>Extracts the field names (`` `xyz` `` in each row's first cell) of the FIRST markdown
    /// table found after the `## \`{eventName}\`` heading — that's always the section's own primitive
    /// field table (e.g. `node_completed`'s nested `timings` sub-table, or `retry_scheduled`'s trailing
    /// "Retry safety" prose, both start after the first non-`|` line and are correctly excluded).</summary>
    private static IReadOnlyList<string> ExtractDocumentedFields(string doc, string eventName) =>
        ExtractFieldsFromFirstTable(SectionBody(doc, eventName));

    /// <summary>Companion to <see cref="ExtractDocumentedFields"/>: finds the `` `{nestedFieldName}`
    /// (when present): `` marker (https://pipelinez.dev/events/'s convention for a nested payload sub-table, e.g.
    /// `node_completed`'s `timings` table) within one event's section and extracts ITS field table —
    /// scoped to that one section so it can never accidentally match a same-named marker elsewhere in
    /// the doc.</summary>
    private static IReadOnlyList<string> ExtractNestedDocumentedFields(string doc, string eventName,
        string nestedFieldName)
    {
        var sectionBody = SectionBody(doc, eventName);
        var marker = $"`{nestedFieldName}` (when present):";
        var markerIndex = sectionBody.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(markerIndex >= 0,
            $"expected https://pipelinez.dev/events/'s `{eventName}` section to document nested field `{nestedFieldName}` via " +
            $"a `{marker}` sub-table");

        return ExtractFieldsFromFirstTable(sectionBody[(markerIndex + marker.Length)..]);
    }

    /// <summary>The text of one `## \`{eventName}\`` section, from just after its heading up to (but not
    /// including) the next `## ` heading or end of document.</summary>
    private static string SectionBody(string doc, string eventName)
    {
        var heading = $"## `{eventName}`";
        var headingIndex = doc.IndexOf(heading, StringComparison.Ordinal);
        Assert.True(headingIndex >= 0, $"expected https://pipelinez.dev/events/ to contain a `{heading}` section");

        var bodyStart = headingIndex + heading.Length;
        var nextHeadingIndex = doc.IndexOf("\n## ", bodyStart, StringComparison.Ordinal);
        return nextHeadingIndex < 0 ? doc[bodyStart..] : doc[bodyStart..nextHeadingIndex];
    }

    private static IReadOnlyList<string> ExtractFieldsFromFirstTable(string body)
    {
        var fields = new List<string>();
        var inTable = false;

        foreach (var rawLine in body.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r');
            if (!inTable)
            {
                if (line.StartsWith("| Field ", StringComparison.Ordinal))
                {
                    inTable = true;
                }

                continue;
            }

            if (!line.StartsWith("|", StringComparison.Ordinal))
            {
                break;
            }

            var match = Regex.Match(line, "^\\|\\s*`([a-zA-Z0-9]+)`\\s*\\|");
            if (match.Success)
            {
                fields.Add(match.Groups[1].Value);
            }
        }

        return fields;
    }

    private static string ToCamelCase(string pascalCase) =>
        pascalCase.Length == 0 ? pascalCase : char.ToLowerInvariant(pascalCase[0]) + pascalCase[1..];

    private static string ToSnakeCase(string pascalCase)
    {
        var withUnderscores = Regex.Replace(pascalCase, "(?<!^)([A-Z])", "_$1");
        return withUnderscores.ToLowerInvariant();
    }
}
