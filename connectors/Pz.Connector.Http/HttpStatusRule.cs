using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>What an <c>on_status</c> rule makes of a response it matches.</summary>
internal enum StatusAction
{
    Transient,
    Permanent,
    Skip,
}

/// <summary>One <c>on_status</c> rule: a status set (exact codes and whole classes), an optional plain
/// substring the body must contain, and the classification to apply. Rules only classify; the connector
/// still never retries. <see cref="Label"/> names the rule in every message (<c>on_status[0]</c>,
/// <c>for_each.on_status[1]</c>).</summary>
internal sealed record HttpStatusRule(string Label, IReadOnlySet<int> Codes, IReadOnlySet<int> Classes,
    string? BodyContains, StatusAction Action, TimeSpan? RetryAfter)
{
    /// <summary>The longest wait a rule's own <c>retry_after</c> may ask for, the same bound as the read's
    /// shared pause.</summary>
    internal static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(600);

    /// <summary>Statuses a <c>skip</c> rule may name only together with <c>body_contains</c>.</summary>
    private static readonly int[] BroadSkipCodes = [401, 403, 408, 429];

    private static readonly HashSet<string> Keys = ["status", "body_contains", "action", "retry_after"];

    /// <summary>Whether the rule covers <paramref name="status"/> and, when it names a substring,
    /// <paramref name="body"/> contains it (ordinal, ignoring case).</summary>
    public bool Matches(int status, string body)
        => Covers(status) && (BodyContains is null || body.Contains(BodyContains, StringComparison.OrdinalIgnoreCase));

    /// <summary>Whether the rule's status set names <paramref name="status"/>.</summary>
    public bool Covers(int status) => Codes.Contains(status) || Classes.Contains(status / 100);

    /// <summary>The first rule of <paramref name="rules"/> that matches, or null.</summary>
    public static HttpStatusRule? FirstMatch(IReadOnlyList<HttpStatusRule>? rules, int status, string body)
    {
        foreach (var rule in rules ?? [])
        {
            if (rule.Matches(status, body))
            {
                return rule;
            }
        }

        return null;
    }

    /// <summary>Parses an <c>on_status</c> list. Every problem is appended to <paramref name="errors"/>
    /// under <c>on_status[i]</c>; the result holds the rules that parsed cleanly (null when the key is
    /// absent), so callers can still inspect them while reporting the rest. <paramref name="labelPrefix"/>
    /// is prepended to each rule's runtime label (<c>for_each.</c> for the list request).</summary>
    public static IReadOnlyList<HttpStatusRule>? Parse(object? raw, string labelPrefix, List<string> errors)
    {
        if (raw is null)
        {
            return null;
        }

        if (raw is string or IReadOnlyDictionary<string, object?> || raw is not System.Collections.IEnumerable list)
        {
            errors.Add("'on_status' must be a list of rules ({ status, body_contains, action, retry_after })");
            return [];
        }

        var rules = new List<HttpStatusRule>();
        var index = 0;
        foreach (var entry in list)
        {
            var name = $"on_status[{index}]";
            if (Rule(entry, name, labelPrefix + name, errors) is { } rule)
            {
                rules.Add(rule);
            }

            index++;
        }

        if (index == 0)
        {
            errors.Add("'on_status' must list at least one rule");
        }

        return rules;
    }

    /// <summary>The labels of the entries of a raw <c>on_status</c> list whose action is <c>skip</c>, whatever
    /// else is wrong with them, so refusals that depend only on the action are reported in the same pass as
    /// the rule's other errors.</summary>
    public static List<string> SkipLabels(object? raw, string labelPrefix)
    {
        var labels = new List<string>();
        if (raw is string or IReadOnlyDictionary<string, object?> || raw is not System.Collections.IEnumerable list)
        {
            return labels;
        }

        var index = 0;
        foreach (var entry in list)
        {
            if (entry is IReadOnlyDictionary<string, object?> map
                && YamlScalarText.Of(map.GetValueOrDefault("action")) == "skip")
            {
                labels.Add($"{labelPrefix}on_status[{index}]");
            }

            index++;
        }

        return labels;
    }

    private static HttpStatusRule? Rule(object? entry, string name, string label, List<string> errors)
    {
        if (entry is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add($"{name} must be a map ({{ status, action, ... }})");
            return null;
        }

        var before = errors.Count;
        foreach (var key in map.Keys.Where(k => !Keys.Contains(k)))
        {
            errors.Add($"{name}: unknown key '{key}' (accepted: status, body_contains, action, retry_after)");
        }

        var codes = new HashSet<int>();
        var classes = new HashSet<int>();
        if (!map.TryGetValue("status", out var status) || status is null)
        {
            errors.Add($"{name}.status is required (a status from 400 to 599, '4xx', '5xx', or a list of those)");
        }
        else if (YamlScalarText.NonScalarKind(status) == "map")
        {
            errors.Add($"{name}.status must be a status, a class, or a list of those, not a map");
        }
        else
        {
            var values = YamlScalarText.NonScalarKind(status) == "list"
                ? ((System.Collections.IEnumerable)status).Cast<object?>().ToList()
                : [status];
            if (values.Count == 0)
            {
                errors.Add($"{name}.status must not be an empty list");
            }

            foreach (var value in values)
            {
                AddStatus(value, name, codes, classes, errors);
            }
        }

        string? bodyContains = null;
        if (map.TryGetValue("body_contains", out var bc) && bc is not null)
        {
            bodyContains = YamlScalarText.NonScalarKind(bc) is null ? YamlScalarText.Of(bc) : null;
            if (string.IsNullOrEmpty(bodyContains))
            {
                errors.Add($"{name}.body_contains must be a non-empty string");
            }
        }

        var actionText = map.TryGetValue("action", out var a) ? YamlScalarText.Of(a) : null;
        StatusAction? action = actionText switch
        {
            "transient" => StatusAction.Transient,
            "permanent" => StatusAction.Permanent,
            "skip" => StatusAction.Skip,
            _ => null,
        };
        if (action is null)
        {
            errors.Add(actionText is null
                ? $"{name}.action is required (transient, permanent or skip)"
                : $"{name}.action must be transient, permanent or skip, got '{actionText}'");
        }

        TimeSpan? retryAfter = null;
        if (map.TryGetValue("retry_after", out var ra) && ra is not null)
        {
            if (action is not (null or StatusAction.Transient))
            {
                errors.Add($"{name}.retry_after only applies to action: transient");
            }
            else if (!HttpDuration.TryParse(YamlScalarText.Of(ra), out var wait) || wait <= TimeSpan.Zero
                     || wait > MaxRetryAfter)
            {
                errors.Add($"{name}.retry_after must be a duration above 0 and at most 600s, like 500ms, 30s " +
                    $"or 2m (got '{YamlScalarText.Of(ra)}')");
            }
            else
            {
                retryAfter = wait;
            }
        }

        // A skip with nothing but a status to go on must not catch the responses that mean "your
        // credentials or your pace are wrong": an expired key, a missing permission or a throttle would
        // otherwise end the read as an empty success.
        if (action == StatusAction.Skip && bodyContains is null
            && (classes.Count > 0 || codes.Overlaps(BroadSkipCodes)))
        {
            errors.Add($"{name}: action 'skip' without body_contains must name exact status codes, not " +
                "'4xx'/'5xx', and none of 401, 403, 408, 429 (an expired key, a missing permission or a " +
                "throttle would end the read as an empty success); add body_contains or narrow status");
        }

        return errors.Count > before
            ? null
            : new HttpStatusRule(label, codes, classes, bodyContains, action!.Value, retryAfter);
    }

    private static void AddStatus(object? value, string name, HashSet<int> codes, HashSet<int> classes,
        List<string> errors)
    {
        var text = YamlScalarText.NonScalarKind(value) is null ? YamlScalarText.Of(value) : null;
        if (text is "4xx" or "5xx")
        {
            classes.Add(text[0] - '0');
            return;
        }

        if (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var code) || code is < 400 or > 599)
        {
            errors.Add($"{name}.status '{text ?? YamlScalarText.NonScalarKind(value)}' is not a status from 400 " +
                "to 599, '4xx' or '5xx' (a 2xx is a success and a 3xx a redirect, neither reaches on_status)");
            return;
        }

        if (code == 407)
        {
            errors.Add($"{name}.status 407 cannot be overridden: it is the proxy refusing its credentials, " +
                "never the API (fix proxy.user/proxy.password)");
            return;
        }

        codes.Add(code);
    }
}
