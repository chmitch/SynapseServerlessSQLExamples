select top 100 * from YellowTaxiTrips

--This is a straight count
-- Scanned 62MB, moved 1MB
SELECT count(*) FROM
OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow/puYear=*/puMonth=*/puDate=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

--This is how to do it without leveraging folder structure
--Even though the result set is more selective, it still scanned 90MB and moved 1MB
SELECT count(*) FROM
OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow/puYear=*/puMonth=*/puDate=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE CAST(tpepPickupDateTime as DATETIME) BETWEEN '2022-06-14 00:00:00' AND '2022-06-15 00:00:00'

--Same as query 1, but leveraging the view.
--Still scans 62MB, and moves 1MB
SELECT COUNT(*)
FROM YellowTaxiTrips

--Same as query 2, but now using the promoted date column that leverages the folder structure
--Now scans only 1MB and moves 1MB
SELECT COUNT(*)
FROM YellowTaxiTrips
WHERE puDate = '2022-06-14'