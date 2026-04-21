-- OpenMetadata Issue #25299: Stored Procedure lineage is not supported for MS SQL connector
-- https://github.com/open-metadata/OpenMetadata/issues/25299
--
-- Root causes:
--   1. CREATE PROCEDURE (full keyword) is not recognized (only CREATE PROC works)
--   2. BEGIN...END blocks cause parse failure
--   3. Temp tables (#tempTable) as intermediate steps break multi-hop lineage
--
-- Setup DDL (run in MSSQL to create the test environment):
--   CREATE DATABASE dbName;
--   USE dbName;
--   CREATE SCHEMA schName;
--   CREATE TABLE schName.sourceTable (columnName int);
--   CREATE TABLE schName.targetTable (columnName int);
--   INSERT INTO schName.sourceTable (columnName) VALUES (1),(2),(3);

CREATE PROCEDURE schName.procName
AS
BEGIN
    DROP TABLE IF EXISTS #tempTable

    CREATE TABLE #tempTable (columnName int)

    INSERT INTO #tempTable (columnName)
    SELECT columnName FROM schName.sourceTable

    INSERT INTO schName.targetTable (columnName)
    SELECT columnName FROM #tempTable
END
