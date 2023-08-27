CREATE PROCEDURE [bronze.NCCM].SP_AS_CLAIM
AS
BEGIN

    MERGE [bronze.HSP].AS_CLAIM AS trg
    USING [bronze.HSP].VW_AS_CLAIM AS src
    ON (trg.Id = src.Id)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[AS_APPROVED_AMOUNT__C] = src.[AS_APPROVED_AMOUNT__C],
            trg.[AS_CASE_RECORD__C] = src.[AS_CASE_RECORD__C],
            trg.[AS_CLAIM_AMOUNT__C] = src.[AS_CLAIM_AMOUNT__C],
            trg.[AS_CLAIM_ID__C] = src.[AS_CLAIM_ID__C],
            trg.[AS_CLAIM_RATE_TYPE__C] = src.[AS_CLAIM_RATE_TYPE__C],
            trg.[AS_GST_AMOUNT__C] = src.[AS_GST_AMOUNT__C],
            trg.[AS_JOBSEEKER_ID__C] = src.[AS_JOBSEEKER_ID__C],
            trg.[AS_PLACEMENT_MILESTONE__C] = src.[AS_PLACEMENT_MILESTONE__C],
            trg.[AS_STATUS_DATE__C] = src.[AS_STATUS_DATE__C],
            trg.[AS_STATUS__C] = src.[AS_STATUS__C],
            trg.[NAME] = src.[NAME],
            trg.[OUTCOME_TYPE__C] = src.[OUTCOME_TYPE__C],
            trg.[SITE_CODE__C] = src.[SITE_CODE__C],
            trg.[IsDeleted] = src.[IsDeleted],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [Id],
            [AS_APPROVED_AMOUNT__C],
            [AS_CASE_RECORD__C],
            [AS_CLAIM_AMOUNT__C],
            [AS_CLAIM_ID__C],
            [AS_CLAIM_RATE_TYPE__C],
            [AS_GST_AMOUNT__C],
            [AS_JOBSEEKER_ID__C],
            [AS_PLACEMENT_MILESTONE__C],
            [AS_STATUS_DATE__C],
            [AS_STATUS__C],
            [NAME],
            [OUTCOME_TYPE__C],
            [SITE_CODE__C],
            [IsDeleted],
            [RowHashSum]
        ) VALUES (
            src.[Id],
            src.[AS_APPROVED_AMOUNT__C],
            src.[AS_CASE_RECORD__C],
            src.[AS_CLAIM_AMOUNT__C],
            src.[AS_CLAIM_ID__C],
            src.[AS_CLAIM_RATE_TYPE__C],
            src.[AS_GST_AMOUNT__C],
            src.[AS_JOBSEEKER_ID__C],
            src.[AS_PLACEMENT_MILESTONE__C],
            src.[AS_STATUS_DATE__C],
            src.[AS_STATUS__C],
            src.[NAME],
            src.[OUTCOME_TYPE__C],
            src.[SITE_CODE__C],
            src.[IsDeleted],
            src.[RowHashSum]
        );

END;