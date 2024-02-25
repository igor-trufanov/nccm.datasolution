CREATE PROCEDURE [silver.FNC].SP_TRANSACTION_TITLE_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'TRANSACTION_TITLE_DIM';
    DECLARE @Timestamp DATETIME = GETDATE();

    ;WITH trg AS (
        SELECT * FROM [$(SSIReporting)].[gold.FNC].TRANSACTION_TITLE_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS TRANSACTION_TITLE_DIM_ID
        FROM [silver.FNC].VW_TRANSACTION_TITLE_DIM AS dm
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.TRANSACTION_TITLE_DIM_ID = src.TRANSACTION_TITLE_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[GLACCTGRP_KEY] = src.[GLACCTGRP_KEY],
            trg.[GLACCTGRP_NAME] = src.[GLACCTGRP_NAME],
            trg.[TOTAL_TITLE] = src.[TOTAL_TITLE],
            trg.[MEMBER_TYPE] = src.[MEMBER_TYPE],
            trg.[RECORD_NUMBER] = src.[RECORD_NUMBER],
            trg.[GLACCTGRP_TITLE] = src.[GLACCTGRP_TITLE],
            trg.[ACCOUNT_NUMBER] = src.[ACCOUNT_NUMBER],
            trg.[ACCOUNT_DIM_ID] = src.[ACCOUNT_DIM_ID],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ROW_HASH_SUM],
            [TRANSACTION_TITLE_DIM_ID],
            [GLACCTGRP_KEY],
            [GLACCTGRP_NAME],
            [TOTAL_TITLE],
            [MEMBER_TYPE],
            [RECORD_NUMBER],
            [GLACCTGRP_TITLE],
            [ACCOUNT_NUMBER],
            [ACCOUNT_DIM_ID],
            [IS_DELETED],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[ROW_HASH_SUM],
            src.[TRANSACTION_TITLE_DIM_ID],
            src.[GLACCTGRP_KEY],
            src.[GLACCTGRP_NAME],
            src.[TOTAL_TITLE],
            src.[MEMBER_TYPE],
            src.[RECORD_NUMBER],
            src.[GLACCTGRP_TITLE],
            src.[ACCOUNT_NUMBER],
            src.[ACCOUNT_DIM_ID],
            src.[IS_DELETED],
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;