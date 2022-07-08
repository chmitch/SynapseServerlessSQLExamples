--Create a view with format DELTA.
--Notice we only bind at the root folder of the table, we don't:
--  1.  Specify folder paths with wildcards.
--  2.  Parse the filepath to get fields from the folder structure, this is all figured out .
CREATE VIEW YellowTaxiTripsDelta
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