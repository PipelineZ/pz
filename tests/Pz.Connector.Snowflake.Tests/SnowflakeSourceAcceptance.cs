using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Snowflake.Tests;

/// <summary>TestKit source acceptance against a real Snowflake account. There is no Snowflake
/// container (Testcontainers has none, and the vendor offers no free/local emulator), so this suite
/// activates only when PZ_SNOWFLAKE_* env vars are set (<see cref="SnowflakeFacts"/>) -- CI stays
/// green (SKIP) without them, and this repo's sandbox cannot exercise the live half at all.
///
/// <para>The <c>PZ_TESTKIT.ORDERS</c> table this suite reads is NOT created by any fixture -- it must
/// already exist in the test account, seeded with at least 100 rows and an "id" leading column (the
/// InclusiveWatermarkBound fact assumes "id" names the leading column, per the same convention
/// Postgres/SqlServer's "orders" dataset uses). Seed it once with:</para>
///
/// <code>
/// CREATE SCHEMA IF NOT EXISTS PZ_TESTKIT;
/// CREATE TABLE IF NOT EXISTS PZ_TESTKIT.ORDERS (
///     id INTEGER NOT NULL,
///     customer VARCHAR(50) NOT NULL,
///     amount NUMBER(10,2) NOT NULL,
///     order_date DATE NOT NULL
/// );
/// INSERT INTO PZ_TESTKIT.ORDERS (id, customer, amount, order_date)
/// SELECT
///     seq4() AS id,
///     'customer-' || seq4() AS customer,
///     (seq4() % 500) + 1.00 AS amount,
///     DATEADD(day, seq4() % 365, '2024-01-01'::date) AS order_date
/// FROM TABLE(GENERATOR(ROWCOUNT =&gt; 500));
/// </code>
///
/// <para>LargeDataset/TransientFailureDataset/GetSpecWithPartitionOverride/ChangeCaptureFixture are
/// left at their null/default bases: the connector plans a single partition per dataset (see
/// SnowflakeSource.PlanReadAsync), has no self-terminate query analog that the driver classifies
/// IsTransient=true, offers no partition-count override, and declares no ChangeCapture
/// capability.</para></summary>
public sealed class SnowflakeSourceAcceptance : SourceConnectorAcceptanceTests
{
    protected override void GateFact() => SnowflakeFacts.SkipUnlessConfigured();

    protected override ISourceConnector CreateSource() => new SnowflakeConnector();

    protected override ConnectorConfig ValidConfig => new(SnowflakeFacts.Config());

    protected override DatasetSpec SmallDataset => new("sf", "PZ_TESTKIT.ORDERS", new Dictionary<string, object?>());
}
