-- Deterministic seed for the SQL Server scenario projects. Idempotent:
-- drops and recreates every source table, the proc, and the mart targets.
if db_id('pz') is null create database pz;
go
use pz;
go
if schema_id('mart') is null exec('create schema mart');
go

-- ---------------------------------------------------------------- sources
drop table if exists dbo.customers;
create table dbo.customers (
    customer_id int          not null primary key,
    name        nvarchar(100) not null,
    country     varchar(2)   not null,
    created_at  datetime2(6) not null
);

;with n as (
    select top (20) row_number() over (order by (select null)) as i
    from sys.all_objects
)
insert into dbo.customers (customer_id, name, country, created_at)
select i,
       concat('Customer ', i),
       case i % 4 when 0 then 'US' when 1 then 'DE' when 2 then 'RO' else 'NL' end,
       dateadd(day, i, '2026-01-01')
from n;

drop table if exists dbo.orders;
create table dbo.orders (
    order_id    bigint        not null primary key,
    customer_id int           not null,
    amount      decimal(12,2) not null,
    status      varchar(20)   not null,
    updated_at  datetime2(6)  not null
);

;with n as (
    select top (120) row_number() over (order by (select null)) as i
    from sys.all_objects
)
insert into dbo.orders (order_id, customer_id, amount, status, updated_at)
select i,
       1 + i % 20,
       cast(10 + (i * 37) % 990 as decimal(12,2)) + 0.25,
       case i % 3 when 0 then 'pending' when 1 then 'shipped' else 'delivered' end,
       dateadd(minute, i * 7, '2026-07-01')
from n;

drop table if exists dbo.events;
create table dbo.events (
    id          bigint       not null primary key,
    event_type  varchar(30)  not null,
    payload     nvarchar(200) not null,
    occurred_at datetime2(6) not null
);

;with n as (
    select top (2000) row_number() over (order by (select null)) as i
    from sys.all_objects a cross join sys.all_objects b
)
insert into dbo.events (id, event_type, payload, occurred_at)
select i,
       case i % 5 when 0 then 'click' when 1 then 'view' when 2 then 'login'
                  when 3 then 'logout' else 'purchase' end,
       concat('evt-', i),
       dateadd(second, i * 30, '2026-07-01')
from n;
go

-- Stored proc for scenario 04: watermark-bound via the $watermark sentinel.
create or alter procedure dbo.orders_since @min_id bigint = null
as
begin
    set nocount on;
    select order_id, customer_id, amount, status, updated_at
    from dbo.orders
    where @min_id is null or order_id > @min_id
    order by order_id;
end
go

-- ------------------------------------------------------- mart (sink targets)
-- Dropped so every re-seed starts scenarios from a clean slate; the sink
-- recreates them (it creates tables, never schemas -- mart exists above).
drop table if exists mart.customers_dim;
drop table if exists mart.orders_current;
drop table if exists mart.events_log;
drop table if exists mart.order_totals;
drop table if exists mart.orders_from_proc;
drop table if exists mart.orders_checked;
drop table if exists mart.orders_synced;
go

-- ------------------------------------------------ pzstate (06's remote state)
-- pz creates and migrates this schema itself on 06-remote-state's first run; dropped here so a
-- reseed forgets watermarks and run history exactly as it forgets the data tables (dropping
-- schema_version makes the next run recreate everything from scratch). The schema itself may
-- stay -- creation is IF NOT EXISTS on pz's side.
drop table if exists pzstate.state;
drop table if exists pzstate.runs;
drop table if exists pzstate.run_nodes;
drop table if exists pzstate.run_events;
drop table if exists pzstate.schema_version;
go
print 'seed complete: dbo.customers(20) dbo.orders(120) dbo.events(2000) dbo.orders_since';
go
