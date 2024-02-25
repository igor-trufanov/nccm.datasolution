CREATE PROCEDURE [silver.FNC].SP_TRANSACTION_FACT
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    -- Clean up the fact table
    DBCC CHECKIDENT ('[$(SSIReporting)].[gold.FNC].TRANSACTION_FACT', RESEED, 1);
    TRUNCATE TABLE [$(SSIReporting)].[gold.FNC].TRANSACTION_FACT;

    DECLARE @Timestamp DATETIME = GETDATE();

    -- Re-populate the table
    INSERT [$(SSIReporting)].[gold.FNC].TRANSACTION_FACT (
        [DEPARTMENT_DIM_ID],
        [ACCOUNT_DIM_ID],
        [BATCH_DATE],
        [AMOUNT],
        [IS_DELETED],
        [ROW_CREATED_AT],
        [ROW_UPDATED_AT]
    )
    SELECT 
        [DEPARTMENT_DIM_ID],
        [ACCOUNT_DIM_ID],
        [BATCH_DATE],
        [AMOUNT],
        0,
        @Timestamp,
        @Timestamp
        FROM [silver.FNC].VW_TRANSACTION_FACT;

    COMMIT TRANSACTION TR1;

END;