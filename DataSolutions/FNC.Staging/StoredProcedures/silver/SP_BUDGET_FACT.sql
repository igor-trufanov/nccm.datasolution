CREATE PROCEDURE [silver.FNC].SP_BUDGET_FACT
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    -- Clean up the fact table
    DBCC CHECKIDENT ('[$(SSIReporting)].[gold.FNC].BUDGET_FACT', RESEED, 1);
    TRUNCATE TABLE [$(SSIReporting)].[gold.FNC].BUDGET_FACT;

    DECLARE @Timestamp DATETIME = GETDATE();

    -- Re-populate the table
    INSERT [$(SSIReporting)].[gold.FNC].BUDGET_FACT (
        [AMOUNT],
        [DEPARTMENT_DIM_ID],
        [PERIOD_DATE],
        [ACCOUNT_DIM_ID],
        [DESCRIPTION],
        [IS_DELETED],
        [ROW_CREATED_AT],
        [ROW_UPDATED_AT]
    )
    SELECT 
        [AMOUNT],
        [DEPARTMENT_DIM_ID],
        [PERIOD_DATE],
        [ACCOUNT_DIM_ID],
        [DESCRIPTION],
        0,
        @Timestamp,
        @Timestamp
        FROM [silver.FNC].VW_BUDGET_FACT;

    COMMIT TRANSACTION TR1;

END;