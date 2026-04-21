-- OpenMetadata Issue #25299: Minimal procedure that DOES work in OpenMetadata
-- https://github.com/open-metadata/OpenMetadata/issues/25299
--
-- This simplified form (CREATE PROC, no BEGIN/END) parses correctly.
-- Used as a baseline to show what OpenMetadata handles vs. what it misses.

CREATE PROC ProcedureName
AS
INSERT INTO targetTable
(column1, column2)
SELECT column1, column2
FROM sourceTable
