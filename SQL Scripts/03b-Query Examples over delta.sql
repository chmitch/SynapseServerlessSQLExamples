--Examine what a parquet file looks like under a detla table
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellowdelta/puYear=2022/puMonth=6/puDate=2022-06-11/*.parquet',
        FORMAT = 'PARQUET'
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


