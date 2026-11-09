using Pz.TestSupport;

namespace Pz.Connector.S3.Tests;

/// <summary>When a docker suite runs, skips or fails, decided without docker: no daemon skips; a missing image is
/// always pulled (PZ_TESTS_OFFLINE governs network tests, not fixtures); a refused pull skips locally and fails under
/// CI, which must never quietly lose a docker suite.</summary>
public sealed class DockerImageGateTests
{
    private static readonly Func<(int, string)> Refused =
        () => (1, "Error response from daemon: pull access denied for minio/minio\nmore");

    [Fact]
    public void No_daemon_skips_even_under_CI()
    {
        var gate = DockerFacts.DecideImage("img", daemonAvailable: false, isPresent: () => true,
            pull: () => throw new InvalidOperationException("must not pull"), underCi: true);

        Assert.Equal(DockerFacts.ImageVerdict.Skip, gate.Verdict);
        Assert.Equal("docker is not available", gate.Reason);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_present_image_runs_without_a_pull(bool underCi)
    {
        var gate = DockerFacts.DecideImage("img", true, () => true,
            pull: () => throw new InvalidOperationException("must not pull"), underCi);

        Assert.Equal(DockerFacts.ImageVerdict.Run, gate.Verdict);
    }

    [Fact]
    public void A_missing_image_is_pulled_and_then_runs()
    {
        var pulls = 0;
        var gate = DockerFacts.DecideImage("img", true, () => false, () => { pulls++; return (0, ""); }, underCi: false);

        Assert.Equal(DockerFacts.ImageVerdict.Run, gate.Verdict);
        Assert.Equal(1, pulls);
    }

    [Fact]
    public void A_refused_pull_skips_locally_with_the_reason()
    {
        var gate = DockerFacts.DecideImage("minio/minio:x", true, () => false, Refused, underCi: false);

        Assert.Equal(DockerFacts.ImageVerdict.Skip, gate.Verdict);
        Assert.Equal("docker pull minio/minio:x failed: Error response from daemon: pull access denied for minio/minio",
            gate.Reason);
    }

    [Fact]
    public void A_refused_or_rate_limited_pull_fails_under_CI()
    {
        var gate = DockerFacts.DecideImage("img", true, () => false,
            () => (1, "toomanyrequests: You have reached your pull rate limit"), underCi: true);

        Assert.Equal(DockerFacts.ImageVerdict.Fail, gate.Verdict);
        Assert.Contains("toomanyrequests", gate.Reason);
    }

    [Fact]
    public void Offline_mode_does_not_stop_an_image_pull()
    {
        // DecideImage takes no offline flag at all: the image gate never consults PZ_TESTS_OFFLINE.
        var pulls = 0;
        DockerFacts.DecideImage("img", true, () => false, () => { pulls++; return (0, ""); }, underCi: true);

        Assert.Equal(1, pulls);
    }

    [SkippableFact]
    public void SkipUnlessImage_skips_locally_for_an_image_docker_cannot_provide()
    {
        DockerFacts.SkipUnlessDocker();
        Skip.If(DockerFacts.UnderCi, "under CI a pull failure fails instead (see the decision tests above)");

        var skip = Assert.Throws<SkipException>(() => DockerFacts.SkipUnlessImage("pz-tests.invalid/no-such-image:0"));
        Assert.Contains("cannot provide image", skip.Message);
    }
}
