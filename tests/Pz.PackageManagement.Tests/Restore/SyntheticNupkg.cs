using NuGet.Packaging;
using NuGet.Versioning;

namespace Pz.PackageManagement.Tests.Restore;

/// <summary>Builds a synthetic <c>FakeNative.1.0.0.nupkg</c> carrying a fake <c>runtimes/</c> tree, so RID
/// native-asset selection can be tested without a real native-bearing fixture project. File content is
/// irrelevant to the resolver — only paths and presence matter.</summary>
public static class SyntheticNupkg
{
    public static string CreateFeedWithNativePackage()
    {
        var feedDir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "pz-tests", "synthetic-feed-" + Guid.NewGuid().ToString("N"))).FullName;

        var placeholder = Path.Combine(feedDir, "placeholder.bin");
        File.WriteAllBytes(placeholder, [1, 2, 3, 4]);

        var builder = new PackageBuilder
        {
            Id = "FakeNative",
            Version = new NuGetVersion("1.0.0"),
        };
        builder.Authors.Add("pz-tests");
        builder.Description = "synthetic native-assets fixture";

        void AddFile(string targetPath) =>
            builder.Files.Add(new PhysicalPackageFile { SourcePath = placeholder, TargetPath = targetPath });

        AddFile("lib/net10.0/FakeNative.dll");
        AddFile("runtimes/linux-x64/native/libfake.so");
        AddFile("runtimes/win-x64/native/fake.dll");

        var nupkgPath = Path.Combine(feedDir, "FakeNative.1.0.0.nupkg");
        using (var stream = File.Create(nupkgPath))
        {
            builder.Save(stream);
        }

        File.Delete(placeholder);
        return feedDir;
    }
}
