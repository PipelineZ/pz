using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>The `export:` dataset option: the request starts an asynchronous export job, the job is
/// polled until ready, and its result is downloaded and read as a file (so it requires `file:`).</summary>
internal sealed record HttpExportConfig(string JobIdPointer, string StatusPath, string StatusPointer,
    IReadOnlyList<string> Ready, IReadOnlyList<string> Failed, string? MessagePointer,
    string? DownloadPointer, string? DownloadPath, TimeSpan PollInterval, TimeSpan Timeout)
{
    public const string JobIdToken = "{job_id}";

    public static HttpExportConfig? Parse(object? raw, List<string> errors)
    {
        if (raw is null)
        {
            return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'export' must be a map (job_id, status_path, status_pointer, ready, ...)");
            return null;
        }

        var rejected = HttpFileConfig.RejectCollections(block, "export", ["ready", "failed"], errors);
        string? Get(string key) => rejected.Contains(key) || !block.TryGetValue(key, out var v) ? null : YamlScalarText.Of(v);
        var before = errors.Count;

        string? Pointer(string key, bool required)
        {
            var value = Get(key);
            if (value is null)
            {
                if (required && !rejected.Contains(key))
                {
                    errors.Add($"export.{key} is required");
                }

                return null;
            }

            return HttpDatasetConfig.ValidatePointer(value, $"export.{key}", errors);
        }

        var jobId = Pointer("job_id", required: true);
        var statusPointer = Pointer("status_pointer", required: true);
        var messagePointer = Pointer("message_pointer", required: false);
        var downloadPointer = Pointer("download_pointer", required: false);

        var statusPath = Get("status_path");
        if (statusPath is null)
        {
            if (!rejected.Contains("status_path"))
            {
                errors.Add("export.status_path is required");
            }
        }
        else
        {
            if (statusPath.Length == 0 || statusPath[0] != '/')
            {
                errors.Add($"export.status_path must start with '/', got '{statusPath}'");
            }

            if (CountOf(statusPath) > 1)
            {
                errors.Add($"export.status_path may contain {JobIdToken} at most once, got '{statusPath}'");
            }
        }

        var downloadPath = Get("download_path");
        if (downloadPath is not null && (downloadPath.Length == 0 || downloadPath[0] != '/'))
        {
            errors.Add($"export.download_path must start with '/', got '{downloadPath}'");
        }

        if (rejected.Contains("download_pointer") || rejected.Contains("download_path"))
        {
            // Already reported as not a string.
        }
        else if (downloadPointer is not null && downloadPath is not null)
        {
            errors.Add("export takes exactly one of download_pointer and download_path, got both");
        }
        else if (downloadPointer is null && downloadPath is null)
        {
            errors.Add("export needs one of download_pointer or download_path");
        }

        var ready = StringList(block, "ready", required: true, errors);
        var failed = StringList(block, "failed", required: false, errors);

        var poll = Range(Get("poll_interval_seconds"), "export.poll_interval_seconds", 10, 1, 3600, errors);
        var timeout = Range(Get("timeout_minutes"), "export.timeout_minutes", 30, 1, 1440, errors);

        if (errors.Count > before)
        {
            return null;
        }

        return new HttpExportConfig(jobId!, statusPath!, statusPointer!, ready, failed, messagePointer,
            downloadPointer, downloadPath, TimeSpan.FromSeconds(poll), TimeSpan.FromMinutes(timeout));
    }

    private static int CountOf(string path)
    {
        var count = 0;
        for (var i = path.IndexOf(JobIdToken, StringComparison.Ordinal); i >= 0;
            i = path.IndexOf(JobIdToken, i + JobIdToken.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static IReadOnlyList<string> StringList(IReadOnlyDictionary<string, object?> block, string key,
        bool required, List<string> errors)
    {
        if (!block.TryGetValue(key, out var value) || value is null)
        {
            if (required)
            {
                errors.Add($"export.{key} is required (a non-empty list of status values)");
            }

            return [];
        }

        if (value is string || YamlScalarText.NonScalarKind(value) != "list")
        {
            errors.Add($"export.{key} must be a list of strings");
            return [];
        }

        var result = new List<string>();
        foreach (var entry in (System.Collections.IEnumerable)value)
        {
            var text = YamlScalarText.NonScalarKind(entry) is null ? YamlScalarText.Of(entry) : null;
            if (string.IsNullOrWhiteSpace(text))
            {
                errors.Add($"export.{key} entries must be non-empty strings");
                return [];
            }

            result.Add(text);
        }

        if (required && result.Count == 0)
        {
            errors.Add($"export.{key} must not be empty");
        }

        return result;
    }

    private static int Range(string? text, string label, int fallback, int min, int max, List<string> errors)
    {
        if (text is null)
        {
            return fallback;
        }

        if (int.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n)
            && n >= min && n <= max)
        {
            return n;
        }

        errors.Add($"{label} must be a whole number from {min} to {max}, got '{text}'");
        return fallback;
    }
}
