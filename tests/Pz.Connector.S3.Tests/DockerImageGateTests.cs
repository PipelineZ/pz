using Pz.TestSupport;

namespace Pz.Connector.S3.Tests;

/// <summary>A docker suite whose image docker cannot provide (a pull denied, a registry out of reach) skips with the
/// reason, like a machine without docker, instead of failing.</summary>
public sealed class DockerImageGateTests
{
    private const string Missing = "pz-tests.invalid/no-such-image:0";

    [SkippableFact]
    public void An_image_docker_cannot_provide_is_reported_with_its_reason()
    {
        DockerFacts.SkipUnlessDocker();

        Assert.False(DockerFacts.CanProvide(Missing));
        Assert.Contains(Missing, DockerFacts.ImageProblem(Missing));
    }

    [SkippableFact]
    public void SkipUnlessImage_skips_rather_than_fails()
    {
        DockerFacts.SkipUnlessDocker();

        var skip = Assert.Throws<SkipException>(() => DockerFacts.SkipUnlessImage(Missing));
        Assert.Contains("cannot provide image", skip.Message);
    }

    [Fact]
    public void Without_docker_every_image_has_a_problem()
    {
        if (DockerFacts.IsAvailable)
        {
            return;
        }

        Assert.False(DockerFacts.CanProvide("postgres:16-alpine"));
    }
}
