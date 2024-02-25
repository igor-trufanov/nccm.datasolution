CREATE PROCEDURE [silver.FNC].SP_ACCOUNT_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'ACCOUNT_DIM';
    DECLARE @Timestamp DATETIME = GETDATE();

    ;WITH trg AS (
        SELECT * FROM [$(SSIReporting)].[gold.FNC].ACCOUNT_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS ACCOUNT_DIM_ID
        FROM [silver.FNC].VW_ACCOUNT_DIM AS dm
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.ACCOUNT_DIM_ID = src.ACCOUNT_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[ACCOUNT_NUMBER] = src.[ACCOUNT_NUMBER],
            trg.[ACCOUNT_TITLE] = src.[ACCOUNT_TITLE],
            trg.[ACCOUNT_NORMAL_BALANCE] = src.[ACCOUNT_NORMAL_BALANCE],
            trg.[ACCOUNT_TYPE] = src.[ACCOUNT_TYPE],
            trg.[ACCOUNT_LOCATION_KEY] = src.[ACCOUNT_LOCATION_KEY],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ACCOUNT_DIM_ID],
            [ROW_HASH_SUM],
            [ACCOUNT_NUMBER],
            [ACCOUNT_TITLE],
            [ACCOUNT_NORMAL_BALANCE],
            [ACCOUNT_TYPE],
            [ACCOUNT_LOCATION_KEY],
            [IS_DELETED],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[ACCOUNT_DIM_ID],
            src.[ROW_HASH_SUM],
            src.[ACCOUNT_NUMBER],
            src.[ACCOUNT_TITLE],
            src.[ACCOUNT_NORMAL_BALANCE],
            src.[ACCOUNT_TYPE],
            src.[ACCOUNT_LOCATION_KEY],
            src.[IS_DELETED],
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;