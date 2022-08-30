--This view is on top of the year / month / date structure.
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
    CAST(puDate as DATETIME) as puDate,
    puYear * 100 + puMonth as puYearMonth
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymdate-delta/',
        FORMAT = 'DELTA'
    ) AS [result]


--Constrain by date on nycyellow-ymdate from delta
--This query is a lot easier to read, and now we're using dates which is more natural.
--Most importantly this will work with normal query tools like Power BI
--Scans 6MB moves 55MB
SELECT  *
        ,puYear
        ,puMonth
        ,puDate
FROM    nycyellow_ymdate_delta
WHERE   puDate BETWEEN '12/31/2020' AND '1/1/2021'
