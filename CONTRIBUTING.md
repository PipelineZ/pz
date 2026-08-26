# Contributing to PipelineZ

## Build & test prerequisites

- .NET 10 SDK (`10.0.x`).
- Docker is **optional**. The full solution builds and tests without it: connector acceptance suites
  that need a real Postgres or MinIO instance (via Testcontainers) use
  [`Xunit.SkippableFact`](https://www.nuget.org/packages/Xunit.SkippableFact) and SKIP cleanly (not fail)
  when docker is unreachable -- see `tests/Pz.TestSupport/DockerFacts.cs` and
  `src/Pz.Connectors.TestKit/SourceConnectorAcceptanceTests.cs`'s `GateFact()` hook. With docker present,
  nothing is skipped.

```console
$ dotnet build Pz.slnx -c Release
$ dotnet test Pz.slnx -c Release --no-build
```

Zero warnings (`TreatWarningsAsErrors=true`, `Directory.Build.props`) and zero failures are both required
before opening a PR. If you don't have docker installed locally, that's fine -- CI's ubuntu leg runs with
docker and will still exercise the Postgres/S3 suites; your PR only needs its own build/test run
to be clean modulo those expected skips.

`scripts/verify-tool-install.sh` is the packaging/tool-install end-to-end proof (pack -> install ->
`pz init` -> `pz init --template sample` -> `pz run`, fully offline). Run it after touching anything
under `src/Pz.Cli`, `templates/`, or any packable project's `.csproj`.

## Branch protection

- `main` requires the `ci` workflow (`.github/workflows/ci.yml`) green before merging -- both the
  `build-test` matrix (ubuntu + windows) and `pack-and-verify` (ubuntu).
- No direct pushes to `main`; land changes through a PR.
- (Repo admin action, not code: configure this under Settings -> Branches -> Branch protection rules for
  `main`, requiring the `build-test` and `pack-and-verify` status checks.)

## Release process

Releases are tag-triggered (`.github/workflows/release.yml`, `push: tags: ['v*']`) and publish to
nuget.org via [trusted publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing)
(OIDC) -- no long-lived API key is stored in this repo.

`Directory.Build.props`'s `RepositoryUrl` points at the real repository
(`https://github.com/PipelineZ/pz`), so packed nuspecs ship a resolving repository link; if the
repository is ever renamed, update it in the same change.

**One-time manual prerequisites** (see comments at the top of `release.yml` for the same list):

1. On [nuget.org/account/trustedpublishing](https://www.nuget.org/account/trustedpublishing), add
   **one** trusted publishing policy. One is enough for every package id: the form scopes by glob,
   so there is no policy-per-package. The whole form, case-insensitively:
   - **Policy Name**: anything you'll recognize later, e.g. `pz-release`
   - **Package Owner**: the nuget.org user or organization that will own the packages
   - **Repository Owner**: `PipelineZ`
   - **Repository**: `pz`
   - **Workflow File**: `release.yml`, the filename only, never the `.github/workflows/` path
   - **Environment**: `release`, matching `release.yml`'s `environment: release`, which restricts
     the policy to that environment
   - **Scopes**: `Push` > **"Push new packages and package versions"**. NOT "Push only new package
     versions", which cannot create an id that does not exist yet, and at `v0.1.0` every id is new.
     Leave "Unlist or relist package versions" unchecked: `release.yml` only pushes, and unlisting
     is a manual, occasional act better done from the website than granted to a workflow.
   - **Packages**: the glob **`Pz.*`** *and* the exact id **`pz`**. The field is required (at least
     one glob or package). The glob covers the three connector-author packages and any future
     `Pz.`-prefixed id; the tool package publishes as a bare `pz`, which the glob does **not** match,
     so it has to be listed in its own right (here, or in a second policy with the same repository,
     workflow, environment, and scope). Omitting it fails the push for `pz` alone, after the other
     three have already published.

   Microsoft's [trusted publishing docs](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing)
   still describe a policy as owner-wide, applying to "all packages owned by the selected owner",
   with per-package scoping only requested (NuGet/NuGetGallery#10587). The live form disagrees: it
   asks for scopes and requires a package glob. Follow the form.

   **A new policy on a private repository starts "temporarily active" for 7 days.** If no publish
   happens in that window it goes inactive; you can restart the window at any time. nuget.org needs
   the GitHub repository and owner *IDs* to permanently bind the policy, and it only learns them
   from a successful publish's token. That binding is deliberate anti-resurrection protection: it
   stops someone deleting a repository, recreating it under the same name, and inheriting its
   publishing rights. **So it is also why you should publish only from the repository you intend to
   keep** — replacing this repository with a fresh one of the same name produces different IDs, and
   the bound policy will not match it.
2. Create a GitHub Environment named `release` (Settings -> Environments) with an environment secret
   `NUGET_USER` set to the nuget.org profile name (not email) of the trusted-publishing policy owner.
3. That's it -- no NuGet API key secret is ever stored; `NuGet/login` exchanges the workflow's OIDC token
   for a short-lived nuget.org API key scoped to what step 1 configured.

**Cutting a release**, once the above exists:

```console
$ git tag vX.Y.Z        # e.g. v0.1.1 -- the tag IS the version, so pick it deliberately
$ git push origin vX.Y.Z
```

MinVer (`Directory.Build.props`, `MinVerTagPrefix=v`) computes every packable project's version from
this tag; `release.yml` builds, tests (linux), packs, and pushes every package to nuget.org.

### What publishes, and what deliberately does not

Four ids publish, and each has a real consumer:

| Package | Who installs it |
|---|---|
| `pz` (the `Pz.Cli` project) | anyone running `pz` |
| `Pz.Connectors.Abstractions` | a connector author, as the ABI they compile against |
| `Pz.Connectors.Toolkit` | a connector author, for the shared format codecs |
| `Pz.Connectors.TestKit` | a connector author, for the acceptance suite |

**`Pz.Cli` publishes as `pz`.** The project keeps its name; `<PackageId>pz</PackageId>` in
`src/Pz.Cli/Pz.Cli.csproj` is the only place a published id differs from a project name, and
`scripts/lib/packable-ids.sh` reads the override rather than assuming the two match. Two things
follow that no code can enforce, so they are checklist items for whoever runs the release:

- **The trusted publishing policy must cover `pz`.** The original policy is globbed `Pz.*`, which
  does not match a bare `pz`; without a second policy (or a widened one) `release.yml`'s push fails
  authentication for that package alone, after the other three have already gone out.
- **`Pz.Cli` stays deprecated, listed, and unreplaced.** It published `0.1.0` through `0.2.1` under
  the old id; `pz` picks up at `0.2.2`. That boundary is final, not a running total — the rule below
  is what freezes it, so no release ever has to revisit these numbers. Deprecate `Pz.Cli` on
  nuget.org ("Other", alternate package `pz`) so the gallery and `dotnet` point at the new id; leave
  its versions listed so existing lock files still restore. Do **not** publish new versions under
  both ids — a machine with both installed globally has two packages claiming the `pz` shim, and the
  second install fails.

**The eight builtin connectors deliberately do not publish.** `Pz.Cli` project-references them and
registers them in-process (`BuiltinConnectors`), and `BuiltinConnectors.PackageIds` excludes them
from NuGet resolution, the lock file, and drift checking, so a `project.yml` naming one downloads
nothing. Nor can anything else host them: `ConnectorHost` lives in `Pz.PackageManagement`, which is
itself unpublished. `Pz.Connector.*` packages therefore existed on nuget.org that nobody could
install and use; `0.1.0` and `0.1.1` shipped that way and are unlisted. Their ids stay permanently
owned by that first publish, so unbundling a connector later is just flipping `IsPackable` back on.

The versions of all published packages move together, from the one git tag. There is no way to cut a
connector-only or Toolkit-only release, and none is planned: MinVer derives every version from the
same tag, and runtime compatibility is a separate axis anyway (`ProtocolVersion.Major`, checked
against each connector's `pz.connector.json` range in `ConnectorHost`, raising PZ0306 on mismatch).
A connector built against an old Abstractions keeps loading as long as the protocol major matches,
which is exactly what the additive-only ABI rule buys.

**Merge any docs the release itself makes true BEFORE tagging.** Every packable project embeds
`README.md` (`PackageReadmeFile`), captured at pack time from the tagged commit, and nuget.org
versions are immutable: a README that was wrong at the tag is wrong on that version's package page
permanently, and only a later version can replace it. `v0.1.0` shipped carrying its own
pre-publication note ("the install line works once `v0.1.0` is on NuGet. Until then, build from a
clone") because the PR deleting that note merged just after the tag. The fix is to merge such
changes first and accept that the repository is briefly ahead of nuget.org. That window lasts
minutes and corrects itself; the embedded copy does not.

## Writing comments and docs

**A comment states the constraint it protects, not where that constraint was decided.** Why an
ordering is load-bearing, who owns an Arrow batch, which quoting rule a dialect requires — in terms
that stand on their own, so the comment stays useful to a reader who has only this repository. If
you are tempted to write "per the 2026-08-19 spec, §4", write the rule instead; git history holds
the derivation. That applies to every document outside this repository, with no exceptions.

The kept-drift-free description of the current design lives at
[pipelinez.dev](https://pipelinez.dev), maintained in the `pz-site` repository. If a change here
makes one of those pages wrong, open the matching PR there — a docs fix that lags the code by a
release is how a documented contract quietly stops being one.
