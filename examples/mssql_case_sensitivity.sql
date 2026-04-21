-- MSSQL view with square-bracket identifiers and mixed casing.
-- OpenMetadata's regex-based bracket handling has known bugs.
-- See: https://github.com/open-metadata/OpenMetadata/issues/16710

CREATE VIEW [Analytics].[dbo].[vw_MonthlyRevenue]
AS
SELECT
    [Sales].[dbo].[Invoices].[InvoiceDate] AS [RevenueMonth],
    [Sales].[dbo].[Invoices].[CustomerId],
    [Sales].[dbo].[Customers].[CustomerName],
    SUM([Sales].[dbo].[Invoices].[Amount]) AS [TotalRevenue]
FROM [Sales].[dbo].[Invoices]
INNER JOIN [Sales].[dbo].[Customers]
    ON [Sales].[dbo].[Invoices].[CustomerId] = [Sales].[dbo].[Customers].[CustomerId]
GROUP BY
    [Sales].[dbo].[Invoices].[InvoiceDate],
    [Sales].[dbo].[Invoices].[CustomerId],
    [Sales].[dbo].[Customers].[CustomerName]
