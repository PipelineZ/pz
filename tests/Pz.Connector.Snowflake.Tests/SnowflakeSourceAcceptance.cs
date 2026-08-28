using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Snowflake.Tests;

/// <summary>TestKit source acceptance against a real Snowflake account. There is no Snowflake
/// container (Testcontainers has none, and the vendor offers no free/local emulator), so this suite
/// activates only when PZ_SNOWFLAKE_* env vars are set (<see cref="SnowflakeFacts"/>) -- CI stays
/// green (SKIP) without them, and this repo's sandbox cannot exercise the live half at all.
///
/// <para>Every identifier below is quoted in the seeding SQL, deliberately: the connector
/// (<c>SfDdl.Quote</c>) always double-quotes every identifier it generates, so Snowflake's
/// unquoted-identifiers-fold-to-uppercase behavior never applies to anything this connector reads or
/// writes -- a column created unquoted as `id` folds to `ID`, but the connector (and the TestKit's
/// hardcoded lowercase `WatermarkCursor = "id"`) always queries the quoted, case-preserved `"id"`,
/// which would then not exist. Quoting every column at CREATE time as written keeps the live table's
/// real identifiers byte-identical to what the connector queries. The schema/table names
/// (PZ_TESTKIT/ORDERS) are already uppercase, so quoting them changes nothing observable, but it
/// removes any ambiguity about how they'd fold and matches exactly what <c>SfDdl.SplitEntity("PZ_TESTKIT.ORDERS")</c>
/// hands to <c>SfDdl.Quote</c> for both the dataset spec below and <see cref="BoundedWindowDataset"/>.</para>
///
/// <para>The <c>PZ_TESTKIT.ORDERS</c> table this suite reads is NOT created by any fixture -- it must
/// already exist in the test account, seeded with at least 100 rows and an "id" leading column (the
/// InclusiveWatermarkBound fact assumes "id" names the leading column, per the same convention
/// Postgres/SqlServer's "orders" dataset uses). Seed it once with:</para>
///
/// <code>
/// CREATE SCHEMA IF NOT EXISTS "PZ_TESTKIT";
/// CREATE TABLE IF NOT EXISTS "PZ_TESTKIT"."ORDERS" (
///     "id" INTEGER NOT NULL,
///     "customer" VARCHAR(50) NOT NULL,
///     "amount" NUMBER(10,2) NOT NULL,
///     "order_date" DATE NOT NULL
/// );
/// INSERT INTO "PZ_TESTKIT"."ORDERS" ("id", "customer", "amount", "order_date")
/// SELECT
///     seq4() AS "id",
///     'customer-' || seq4() AS "customer",
///     (seq4() % 500) + 1.00 AS "amount",
///     DATEADD(day, seq4() % 365, '2024-01-01'::date) AS "order_date"
/// FROM TABLE(GENERATOR(ROWCOUNT =&gt; 500));
/// </code>
///
/// <para><see cref="BoundedWindowDataset"/> targets its own table (the TestKit's BoundedWindow_* fact
/// needs an exact 0..10 cursor seed, which ORDERS' 500-row seed does not provide). Seed it once
/// with:</para>
///
/// <code>
/// CREATE TABLE IF NOT EXISTS "PZ_TESTKIT"."BOUNDED_WINDOW" (
///     "id" INTEGER NOT NULL
/// );
/// INSERT INTO "PZ_TESTKIT"."BOUNDED_WINDOW" ("id")
/// SELECT seq4() FROM TABLE(GENERATOR(ROWCOUNT =&gt; 11));
/// </code>
///
/// <para>LargeDataset/TransientFailureDataset/GetSpecWithPartitionOverride/ChangeCaptureFixture/
/// CheckpointDataset are left at their null/default bases: the connector plans a single partition per
/// dataset (see SnowflakeSource.PlanReadAsync), has no self-terminate query analog that the driver
/// classifies IsTransient=true, offers no partition-count override, and declares no ChangeCapture or
/// CheckpointableReads capability. BoundedWindowDataset IS supplied below, because the connector DOES
/// declare ConnectorCapabilities.BoundedWindow.</para></summary>
public sealed class SnowflakeSourceAcceptance : SourceConnectorAcceptanceTests
{
    protected override void GateFact() => SnowflakeFacts.SkipUnlessConfigured();

    protected override ISourceConnector CreateSource() => new SnowflakeConnector();

    protected override ConnectorConfig ValidConfig => new(SnowflakeFacts.Config());

    protected override DatasetSpec SmallDataset => new("sf", "PZ_TESTKIT.ORDERS", new Dictionary<string, object?>());

    // Own table, seeded 0..10 by "id" (see class doc comment) -- SnowflakeConnector declares
    // ConnectorCapabilities.BoundedWindow, so this must be supplied, not left at the null default.
    protected override DatasetSpec? BoundedWindowDataset => new DatasetSpec(
        "sf", "PZ_TESTKIT.BOUNDED_WINDOW", new Dictionary<string, object?>())
    {
        WatermarkCursor = "id",
        WatermarkValue = "3",
        WatermarkUpperBound = "7",
    };
}
