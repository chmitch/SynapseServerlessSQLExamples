ALTER VIEW YellowTaxiTrips
AS
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
    --CAST(puDate as DATE) as puDate
    CAST(result.filepath(3) as DATE) as puDate
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow/puYear=*/puMonth=*/puDate=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
