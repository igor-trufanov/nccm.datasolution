CREATE PROCEDURE [silver.NCCM].SP_OUTCOME_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'OUTCOME_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].OUTCOME_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS OUTCOME_DIM_ID,
            ksc.DIM_ID AS CONTACT_DIM_ID
        FROM [silver.NCCM].VW_OUTCOME_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ksc
                ON ksc.UQ_KEY_HASH = dm.CLIENT_CONTACT_HASH_KEY
                AND ksc.DIMENSION = 'CONTACTS_DIM'
    ) AS src
    ON (
        trg.OUTCOME_DIM_ID = src.OUTCOME_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[OUTCOME_ID] = src.[OUTCOME_ID],
            trg.[CLIENT_CASE_ID] = src.[CLIENT_CASE_ID],
            trg.[CONTACT_DIM_ID] = src.[CONTACT_DIM_ID],
            trg.[WRD_PHASE] = src.[WRD_PHASE],
            trg.[RECORD_TYPE_NAME] = src.[RECORD_TYPE_NAME],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [OUTCOME_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [OUTCOME_ID],
            [CLIENT_CASE_ID],
            [CONTACT_DIM_ID],
            [WRD_PHASE],
            [RECORD_TYPE_NAME],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[OUTCOME_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[OUTCOME_ID],
            src.[CLIENT_CASE_ID],
            src.[CONTACT_DIM_ID],
            src.[WRD_PHASE],
            src.[RECORD_TYPE_NAME],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;