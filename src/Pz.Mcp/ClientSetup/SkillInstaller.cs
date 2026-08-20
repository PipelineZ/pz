namespace Pz.Mcp.ClientSetup;

/// <summary>Installs real copies of the embedded `pz-pipelines` skill (SKILL.md + a `references/`
/// authoring guide) into one or more AI-client skill directories, following the aspire CLI's convention:
/// real files, not symlinks, into `&lt;location&gt;/pz-pipelines/`,
/// overwritten in place on every re-install since they are pz-generated artifacts, never user-edited
/// ones.
///
/// The authoring guide is the one documentation file still embedded in this assembly. Every other doc
/// is fetched from the site by the pz_docs_* tools, but this one is written to disk for a client to
/// read directly, so it has to be present with no network and no source tree.</summary>
public static class SkillInstaller
{
    private const string SkillMdLogicalName = "skills/pz-pipelines/SKILL.md";
    private const string GuideDocLogicalName = "docs/reference/authoring-for-agents.md";
    private const string SkillDirName = "pz-pipelines";

    /// <summary>Location key -&gt; the directory (relative to the workspace root) that key installs
    /// into. `standard` is the aspire CLI's default location, the
    /// other three are the client-specific locations aspire also installs into -- `opencode` is
    /// deliberately singular ("skill", not "skills"), aspire's exact spelling.</summary>
    public static readonly IReadOnlyDictionary<string, string> Locations = new Dictionary<string, string>
    {
        ["standard"] = ".agents/skills",
        ["claudecode"] = ".claude/skills",
        ["github"] = ".github/skills",
        ["opencode"] = ".opencode/skill",
    };

    /// <summary>Writes the skill into every requested location under <paramref name="workspaceRoot"/>.
    /// Unknown location keys are ignored (the CLI layer is what validates <c>--skill-locations</c>).
    /// Returns the (workspace-relative-turned-absolute) location directories actually written to.</summary>
    public static IReadOnlyList<string> Install(string workspaceRoot, IReadOnlyList<string> locations)
    {
        var skillMd = ReadEmbedded(SkillMdLogicalName);
        var guide = ReadEmbedded(GuideDocLogicalName);

        var written = new List<string>();
        foreach (var location in locations)
        {
            if (!Locations.TryGetValue(location, out var relativeDir))
            {
                continue;
            }

            var locationDir = Path.Combine(workspaceRoot, relativeDir);
            var skillDir = Path.Combine(locationDir, SkillDirName);
            var referencesDir = Path.Combine(skillDir, "references");
            Directory.CreateDirectory(referencesDir);
            File.WriteAllText(Path.Combine(skillDir, "SKILL.md"), skillMd);
            File.WriteAllText(Path.Combine(referencesDir, "authoring-for-agents.md"), guide);
            written.Add(locationDir);
        }

        return written;
    }

    private static string ReadEmbedded(string logicalName)
    {
        var assembly = typeof(SkillInstaller).Assembly;
        using var stream = assembly.GetManifestResourceStream(logicalName)
            ?? throw new InvalidOperationException($"embedded resource '{logicalName}' declared but unreadable");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
