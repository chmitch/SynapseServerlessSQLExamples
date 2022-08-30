--Constrain by year
--Scans 1683MB, returne 12769MB
SELECT *, CAST(result.filepath(1) as INT) as puYear
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE result.filepath(1) = '2020'

--Constrain by year and month
--Scans 174MB, returns 1196MB
SELECT  *, CAST(result.filepath(1) as INT) as puYear, CAST(result.filepath(2) as INT) as puMonth
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
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
        , CAST(result.filepath(3) as INT) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   CAST(result.filepath(1) as INT) = 2020
        AND CAST(result.filepath(2) as INT) = 1
        AND CAST(result.filepaht(3) as INT) = 1

--What if we want to do multiple dates?
--Scans 6MB returns 47MB
SELECT  *
        , CAST(result.filepath(1) as INT) as puYear
        , CAST(result.filepath(2) as INT) as puMonth
        , CAST(result.filepath(3) as INT) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   CAST(result.filepath(1) as INT) = 2021
        AND CAST(result.filepath(2) as INT) = 1
        AND CAST(result.filepath(3) as INT) IN(1,2)

--This gets really ugly when you want to do range queries over a series of dates and stil get partition elimination
--Scans 7MB moves 64MB
SELECT  *
        , CAST(result.filepath(1) as INT) as puYear
        , CAST(result.filepath(2) as INT) as puMonth
        , CAST(result.filepath(3) as INT) as puDay
FROM
    OPENROWSET(
        BULK 'https://cgmsynapsewpbi.dfs.core.windows.net/default/output/nycyellow-ymd/puYear=*/puMonth=*/puDay=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE   (
            CAST(result.filepath(1) as INT) = 2021
            AND CAST(result.filepath(2) as INT) = 1
            AND CAST(result.filepath(3) as INT) IN(31)
        )
        OR (
            CAST(result.filepath(1) as INT) = 2020
            AND CAST(result.filepath(2) as INT) = 12
            AND CAST(result.filepath(3) as INT) IN(1)
        )
