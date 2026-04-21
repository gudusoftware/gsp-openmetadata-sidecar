-- OpenMetadata Issue #17586: MS SQL Procedures Lineage Not Picked Up
-- https://github.com/open-metadata/OpenMetadata/issues/17586
--
-- Root cause: OpenMetadata's internal MSSQL query log reader had a filter:
--   AND lower(t.text) NOT LIKE '%%create%%procedure%%'
-- This excluded ALL stored procedure text from lineage parsing.
--
-- Fixed in PR #14586, but even after the filter was removed, the parser
-- still fails on BEGIN/END blocks (see #16737, #25299).

CREATE PROCEDURE myproc
AS
BEGIN
    INSERT INTO test2 SELECT * FROM test1
END
