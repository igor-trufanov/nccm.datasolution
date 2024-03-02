CREATE PROCEDURE [bronze.NCCM].SP_AS_CLAIMS
AS
BEGIN

    MERGE [bronze.NCCM].AS_CLAIMS AS trg
    USING [bronze.NCCM].VW_AS_CLAIMS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.AS_APPROVED_AMOUNT__C = src.AS_APPROVED_AMOUNT__C,
            trg.AS_CLAIM_AMOUNT__C = src.AS_CLAIM_AMOUNT__C,
            trg.AS_CLAIM_ID__C = src.AS_CLAIM_ID__C,
            trg.AS_CLAIM_RATE_TYPE__C = src.AS_CLAIM_RATE_TYPE__C,
            trg.AS_GST_AMOUNT__C = src.AS_GST_AMOUNT__C,
            trg.AS_JOBSEEKER_ID__C = src.AS_JOBSEEKER_ID__C,
            trg.AS_STATUS_DATE__C = src.AS_STATUS_DATE__C,
            trg.AS_STATUS__C = src.AS_STATUS__C,
            trg.NAME = src.NAME,
            trg.OUTCOME_TYPE__C = src.OUTCOME_TYPE__C,
            trg.SITE_CODE__C = src.SITE_CODE__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            AS_APPROVED_AMOUNT__C,
            AS_CLAIM_AMOUNT__C,
            AS_CLAIM_ID__C,
            AS_CLAIM_RATE_TYPE__C,
            AS_GST_AMOUNT__C,
            AS_JOBSEEKER_ID__C,
            AS_STATUS_DATE__C,
            AS_STATUS__C,
            NAME,
            OUTCOME_TYPE__C,
            SITE_CODE__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.AS_APPROVED_AMOUNT__C,
            src.AS_CLAIM_AMOUNT__C,
            src.AS_CLAIM_ID__C,
            src.AS_CLAIM_RATE_TYPE__C,
            src.AS_GST_AMOUNT__C,
            src.AS_JOBSEEKER_ID__C,
            src.AS_STATUS_DATE__C,
            src.AS_STATUS__C,
            src.NAME,
            src.OUTCOME_TYPE__C,
            src.SITE_CODE__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );

    UPDATE trg
            SET trg.ISDELETED = 1
        FROM [bronze.NCCM].AS_CLAIMS AS trg
            LEFT JOIN [bronze.NCCM].VW_AS_CLAIMS AS src
                ON trg.ID = src.ID
        WHERE 
            trg.ISDELETED = 0 
            AND src.ID IS NULL;
END;