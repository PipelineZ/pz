using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Per-output sink options: request path, HTTP method, body format,
/// and append chunk size. Aggregates every violation into one non-transient
/// PzConnectorException, mirroring HttpDatasetConfig.Parse.</summary>
internal sealed record HttpSinkOutputConfig(string Path, HttpMethod Method, string BodyFormat, int RowsPerRequest)
{
    public static HttpSinkOutputConfig Parse(OutputSpec spec)
    {
        var errors = new List<string>();
        var options = spec.Options;

        string? Get(string key) => options.TryGetValue(key, out var v) ? v?.ToString() : null;

        var path = Get("path");
        if (string.IsNullOrEmpty(path) || path[0] != '/')
        {
            errors.Add("option 'path' is required and must start with '/'");
        }

        var isMerge = spec.Mode == "merge";
        if (isMerge && path is not null && !path.Contains("{key}", StringComparison.Ordinal))
        {
            errors.Add("write.strategy: merge requires a '{key}' token in 'path' (e.g. '/items/{key}')");
        }

        if (!isMerge && path is not null && path.Contains("{key}", StringComparison.Ordinal))
        {
            errors.Add("the '{key}' path token is only valid for write.strategy: merge");
        }

        var methodName = Get("method") ?? (isMerge ? "put" : "post");
        HttpMethod? method = methodName switch
        {
            "post" => HttpMethod.Post,
            "put" => HttpMethod.Put,
            "patch" => HttpMethod.Patch,
            _ => null,
        };
        if (method is null)
        {
            errors.Add($"'method' must be one of post/put/patch, got '{methodName}'");
        }

        // body_format / rows_per_request shape only the append path's chunked bodies -- merge's
        // per-row keyed PUT/PATCH (HttpWriteSession.WriteBatchAsync) never reads either, so a
        // present key would be silently ignored. Fail-loudly house rule: refuse each one by name
        // instead; value validation is skipped for a refused key (one actionable error per option,
        // not two). Present-but-null keeps the same treated-as-absent semantics the value parsers
        // below already use.
        if (isMerge)
        {
            foreach (var appendOnly in new[] { "body_format", "rows_per_request" })
            {
                if (options.TryGetValue(appendOnly, out var v) && v is not null)
                {
                    errors.Add($"'{appendOnly}' applies only to write.strategy: append (merge sends one full-row body per key) -- remove it");
                }
            }
        }

        var bodyFormat = Get("body_format") ?? "json_array";
        if (!isMerge && bodyFormat is not ("json_array" or "ndjson"))
        {
            errors.Add($"'body_format' must be 'json_array' or 'ndjson', got '{bodyFormat}'");
        }

        var rowsPerRequest = 500;
        if (!isMerge && options.TryGetValue("rows_per_request", out var rpr) && rpr is not null)
        {
            if (long.TryParse(rpr.ToString(), out var n) && n >= 1)
            {
                rowsPerRequest = (int)Math.Min(n, int.MaxValue);
            }
            else
            {
                errors.Add($"'rows_per_request' must be a positive integer, got '{rpr}'");
            }
        }

        foreach (var key in options.Keys)
        {
            if (key is not ("path" or "method" or "body_format" or "rows_per_request"))
            {
                errors.Add($"unknown output option '{key}'");
            }
        }

        if (errors.Count > 0)
        {
            throw new PzConnectorException(
                $"http sink output '{spec.Sink}.{spec.Output}': {string.Join("; ", errors)} " +
                "(fix the output options and re-run)", isTransient: false);
        }

        return new HttpSinkOutputConfig(path!, method!, bodyFormat, rowsPerRequest);
    }
}
