-- OpenMetadata Issue #16424: Square bracket syntax breaks lineage
-- https://github.com/open-metadata/OpenMetadata/issues/16424
--
-- No SQL was posted in the issue. This is a reconstructed example based on
-- the reported behavior: MSSQL views using [database].[schema].[table]
-- bracket notation fail to produce lineage.
--
-- The bug was a greedy regex r"\[(.*)\]" in parser.py that matched across
-- multiple bracket pairs, returning "db].[schema" instead of separate
-- identifiers.

CREATE VIEW [ReportDB].[dbo].[vw_CustomerOrders]
AS
SELECT
    [SalesDB].[dbo].[Customers].[CustomerID],
    [SalesDB].[dbo].[Customers].[CustomerName],
    [SalesDB].[dbo].[Orders].[OrderID],
    [SalesDB].[dbo].[Orders].[OrderDate],
    [SalesDB].[dbo].[Orders].[TotalAmount]
FROM [SalesDB].[dbo].[Customers]
INNER JOIN [SalesDB].[dbo].[Orders]
    ON [SalesDB].[dbo].[Customers].[CustomerID] = [SalesDB].[dbo].[Orders].[CustomerID]
WHERE [SalesDB].[dbo].[Orders].[OrderDate] >= '2024-01-01'
