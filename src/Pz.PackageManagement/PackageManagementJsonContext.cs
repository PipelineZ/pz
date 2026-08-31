using System.Text.Json.Serialization;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement;

/// <summary>Source-generated JSON metadata for every reflective (de)serialization in this assembly —
/// what lets the CLI publish under Native AOT, where reflection-based System.Text.Json binding is
/// unavailable. Writers stay hand-rolled <c>Utf8JsonWriter</c> code (byte-stable output is their
/// contract); only the readers go through here.</summary>
[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(LockFile))]
[JsonSerializable(typeof(LockFileWriter.VersionProbe))]
[JsonSerializable(typeof(ManifestReader.ManifestDto))]
internal sealed partial class PackageManagementJsonContext : JsonSerializerContext;
