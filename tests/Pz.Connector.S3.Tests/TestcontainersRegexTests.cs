using System.Text.RegularExpressions;
using DotNet.Testcontainers.Images;
using Pz.TestSupport;

namespace Pz.Connector.S3.Tests;

/// <summary>The module initializer has lifted Testcontainers' image-reference regex timeout before any test runs, and
/// image names still parse. A Testcontainers upgrade that changes the regex's shape fails here (and in every docker
/// suite's initializer) rather than quietly bringing back the load-dependent timeout.</summary>
public sealed class TestcontainersRegexTests
{
    [Fact]
    public void The_image_reference_regex_has_no_match_timeout()
    {
        Assert.Equal(Regex.InfiniteMatchTimeout, TestcontainersRegex.CurrentMatchTimeout());
    }

    [Fact]
    public void Lifting_again_is_harmless_and_image_names_still_parse()
    {
        TestcontainersRegex.LiftMatchTimeout();

        var image = new DockerImage("registry.example:5000/team/minio:2025.1@sha256:" + new string('a', 64));

        Assert.Equal("registry.example:5000", image.Registry);
        Assert.Equal("team/minio", image.Repository);
        Assert.Equal("2025.1", image.Tag);
        Assert.Equal(Regex.InfiniteMatchTimeout, TestcontainersRegex.CurrentMatchTimeout());
    }
}
