CREATE PROCEDURE [silver.NCCM].SP_CLAIMS_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'CLAIMS_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].CLAIMS_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS CLAIM_DIM_ID
        FROM [silver.NCCM].VW_AS_CLAIMS_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.CLAIM_DIM_ID = src.CLAIM_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[APPROVED_AMOUNT] = src.[APPROVED_AMOUNT],
            trg.[CLAIM_AMOUNT] = src.[CLAIM_AMOUNT],
            trg.[AS_CLAIM_ID] = src.[AS_CLAIM_ID],
            trg.[CLAIM_RATE_TYPE] = src.[CLAIM_RATE_TYPE],
            trg.[GST_AMOUNT] = src.[GST_AMOUNT],
            trg.[JOB_SEEKER_ID] = src.[JOB_SEEKER_ID],
            trg.[STATUS_DATE] = src.[STATUS_DATE],
            trg.[STATUS] = src.[STATUS],
            trg.[NAME] = src.[NAME],
            trg.[OUTCOME_TYPE] = src.[OUTCOME_TYPE],
            trg.[SITE_CODE] = src.[SITE_CODE],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [CLAIM_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [APPROVED_AMOUNT],
            [CLAIM_AMOUNT],
            [AS_CLAIM_ID],
            [CLAIM_RATE_TYPE],
            [GST_AMOUNT],
            [JOB_SEEKER_ID],
            [STATUS_DATE],
            [STATUS],
            [NAME],
            [OUTCOME_TYPE],
            [SITE_CODE],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[CLAIM_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[APPROVED_AMOUNT],
            src.[CLAIM_AMOUNT],
            src.[AS_CLAIM_ID],
            src.[CLAIM_RATE_TYPE],
            src.[GST_AMOUNT],
            src.[JOB_SEEKER_ID],
            src.[STATUS_DATE],
            src.[STATUS],
            src.[NAME],
            src.[OUTCOME_TYPE],
            src.[SITE_CODE],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;