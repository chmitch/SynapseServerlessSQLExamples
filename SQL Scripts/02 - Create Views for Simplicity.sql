--This view is for access to year-month-day partitioioned data
CREATE VIEW nycyellow_ym AS
SELECT
    CAST(vendorId as INT) as vendorId,
    CAST(tpepPickupDateTime as DATETIME) as tpepPickupDateTime,
    CAST(tpepDropoffDateTime as DATETIME) as tpepDropoffDateTime,
    passengerCount,
    tripDistance,
    puLocationId,
    doLocationId,
    startLon,
    startLat,
    endLon,
    endLat,
    rateCodeId,
    storeAndFwdFlag,
    paymentType,
    fareAmount,
    extra,
    mtaTax,
    improvementSurcharge,
    tipAmount,
    tollsAmount,
    totalAmount,
    CAST(result.filepath(1) as INT) as puYear,
    CAST(result.filepath(2) as INT) as puMonth
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

--This view is for access to year-month-day partitioioned data
CREATE VIEW nycyellow_ymd AS
SELECT
    CAST(vendorId as INT) as vendorId,
    CAST(tpepPickupDateTime as DATETIME) as tpepPickupDateTime,
    CAST(tpepDropoffDateTime as DATETIME) as tpepDropoffDateTime,
    passengerCount,
    tripDistance,
    puLocationId,
    doLocationId,
    startLon,
    startLat,
    endLon,
    endLat,
    rateCodeId,
    storeAndFwdFlag,
    paymentType,
    fareAmount,
    extra,
    mtaTax,
    improvementSurcharge,
    tipAmount,
    tollsAmount,
    totalAmount,
    CAST(result.filepath(1) as INT) as puYear,
    CAST(result.filepath(2) as INT) as puMonth,
    CAST(result.filepath(3) as INT) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]



--Views simplify this
CREATE VIEW nycyellow_ymdate AS
SELECT
    CAST(vendorId as INT) as vendorId,
    CAST(tpepPickupDateTime as DATETIME) as tpepPickupDateTime,
    CAST(tpepDropoffDateTime as DATETIME) as tpepDropoffDateTime,
    passengerCount,
    tripDistance,
    puLocationId,
    doLocationId,
    startLon,
    startLat,
    endLon,
    endLat,
    rateCodeId,
    storeAndFwdFlag,
    paymentType,
    fareAmount,
    extra,
    mtaTax,
    improvementSurcharge,
    tipAmount,
    tollsAmount,
    totalAmount,
    CAST(result.filepath(1) as INT) as puYear,
    CAST(result.filepath(2) as INT) as puMonth,
    CAST(result.filepath(3) as DATE) as puDate
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymdate/puYear=*/puMonth=*/puDate=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

--Views simplify this
CREATE VIEW nycyellow_ymdate_delta AS
SELECT
    CAST(vendorId as INT) as vendorId,
    CAST(tpepPickupDateTime as DATETIME) as tpepPickupDateTime,
    CAST(tpepDropoffDateTime as DATETIME) as tpepDropoffDateTime,
    passengerCount,
    tripDistance,
    puLocationId,
    doLocationId,
    startLon,
    startLat,
    endLon,
    endLat,
    rateCodeId,
    storeAndFwdFlag,
    paymentType,
    fareAmount,
    extra,
    mtaTax,
    improvementSurcharge,
    tipAmount,
    tollsAmount,
    totalAmount,
    puYear,
    puMonth,
    puDate
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymdate-delta/',
        FORMAT = 'DELTA'
    ) AS [result]

