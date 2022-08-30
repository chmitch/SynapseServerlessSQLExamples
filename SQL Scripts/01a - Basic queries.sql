
SELECT TOP 10 *
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ym/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

--SQL doesn't know about the year partition so this query fails
SELECT  *
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ym/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE puYear = 2020

--Constrain by year
--Scans 1683MB, returne 12769MB
SELECT *, CAST(result.filepath(1) as INT) as puYear
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ym/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE result.filepath(1) = '2020'

--Constrain by year and month
--Scans 174MB, returns 1196MB
SELECT  *, CAST(result.filepath(1) as INT) as puYear, CAST(result.filepath(2) as INT) as puMonth
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ym/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   CAST(result.filepath(1) as INT) = 2020
        AND CAST(result.filepath(2) as INT) = 1

--Constrain by day
--Still scans only 174MB only moves 38MB now.
--Scan volume the same because even though it only returns one day it still needs to read the whole partition
SELECT  *
        , CAST(result.filepath(1) as INT) as puYear
        , CAST(result.filepath(2) as INT) as puMonth
        , DAY(tpepPickupDateTime) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ym/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   CAST(result.filepath(1) as INT) = 2020
        AND CAST(result.filepath(2) as INT) = 1
        AND DAY(tpepPickupDateTime) = 1

--Constrain by day on nycyellow-ymd
--Now scans only 7MB and mvoes 40MB.
--Because we've partitioned down to the day queries that include the day in the predicate can eliminate more folders
SELECT  *
        , CAST(result.filepath(1) as INT) as puYear
        , CAST(result.filepath(2) as INT) as puMonth
        , CAST(result.filepath(3) as INT) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   CAST(result.filepath(1) as INT) = 2020
        AND CAST(result.filepath(2) as INT) = 1
        AND CAST(result.filepath(3) as INT) = 1

