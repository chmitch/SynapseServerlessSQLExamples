--Examine what a parquet file looks like under a detla table
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellowdelta/puYear=2022/puMonth=6/puDate=2022-06-11/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

--Create a view with format DELTA.
--Notice we only bind at the root folder of the table, we don't:
--  1.  Specify folder paths with wildcards.
--  2.  Parse the filepath to get fields from the folder structure, this is all figured out .
ALTER VIEW YellowTaxiTripsDelta
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
    CAST(puDate as DATE) as puDate --Note... this works just fine on Delta
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellowdelta/',
        FORMAT = 'DELTA'
    ) AS [result]


-- Notice even though we didn't do anything to help the engine, queries still work.
-- The secret to how this works is in the _delta_log folder.
SELECT TOP 100 *
FROM YellowTaxiTripsDelta

--Same as query 1, but leveraging the view.
--Still scans 65MB, and moves 1MB
SELECT COUNT(*)
FROM YellowTaxiTripsDelta

--Same as query 2, but now using the promoted date column that leverages the folder structure
--Still scans 65MB and moves 1MB
SELECT COUNT(*), puDate
FROM YellowTaxiTripsDelta
group by puDate
order by puDate desc

--Same as query 2, but now using the promoted date column that leverages the folder structure
--Now scans 1MB and moves 1MB
SELECT COUNT(*)
FROM YellowTaxiTripsDelta
WHERE puDate = '2022-06-11'

-- Multi day range query, going beyond a single day scales linearly.
SELECT count(*) FROM
YellowTaxiTripsDelta
WHERE puDate BETWEEN '2022-06-01 00:00:00' AND '2022-06-15 00:00:00'


