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


--Constrain by day on nycyellow-ymd
--This query is a lot easier to read, and less complex to write, but doesn't yield any performance benefits.
--Scans 7MB moves 64MB
SELECT  *
        ,puYear
        ,puMonth
        ,puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   (puYear = 2021 AND puMonth = 1 AND puDay = 31)
        OR (puYear = 2020 AND puMonth = 12 AND puDay = 1)


