-- MSSQL stored procedure that OpenMetadata's parser fails on.
-- See: https://github.com/open-metadata/OpenMetadata/issues/16737
-- See: https://github.com/open-metadata/OpenMetadata/issues/25299

CREATE PROCEDURE [dbo].[usp_UpdateCustomerOrders]
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Stage data into a temp table
    SELECT
        c.customer_id,
        c.customer_name,
        o.order_id,
        o.order_date,
        o.total_amount
    INTO #staged_orders
    FROM [dbo].[customers] c
    INNER JOIN [dbo].[orders] o ON c.customer_id = o.customer_id
    WHERE o.order_date >= DATEADD(day, -30, GETDATE());

    -- Step 2: Merge into the summary table
    MERGE [dbo].[customer_order_summary] AS target
    USING #staged_orders AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            target.last_order_date = source.order_date,
            target.total_amount = source.total_amount,
            target.customer_name = source.customer_name
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, last_order_date, total_amount)
        VALUES (source.customer_id, source.customer_name, source.order_date, source.total_amount);

    -- Step 3: Insert into audit log
    INSERT INTO [dbo].[audit_log] (action, record_count, run_date)
    SELECT 'usp_UpdateCustomerOrders', COUNT(*), GETDATE()
    FROM #staged_orders;

    DROP TABLE #staged_orders;
END
