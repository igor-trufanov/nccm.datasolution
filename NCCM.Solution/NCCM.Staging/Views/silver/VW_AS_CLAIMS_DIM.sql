CREATE VIEW [silver.NCCM].VW_AS_CLAIMS_DIM
AS
    SELECT 
        CONCAT(stm.[CLAIM_ID], '||') AS UQ_KEY,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.[APPROVED_AMOUNT],
            stm.[CLAIM_AMOUNT],
            stm.[AS_CLAIM_ID],
            stm.[CLAIM_RATE_TYPE],
            stm.[GST_AMOUNT],
            stm.[JOB_SEEKER_ID],
            stm.[STATUS_DATE],
            stm.[STATUS],
            stm.[NAME],
            stm.[OUTCOME_TYPE],
            stm.[SITE_CODE],
            stm.[IS_DELETED]
        )), 2) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.[ID] AS [CLAIM_ID],
            tbl.[AS_APPROVED_AMOUNT__C] AS [APPROVED_AMOUNT],
            tbl.[AS_CLAIM_AMOUNT__C] AS [CLAIM_AMOUNT],
            tbl.[AS_CLAIM_ID__C] AS [AS_CLAIM_ID],
            tbl.[AS_CLAIM_RATE_TYPE__C] AS [CLAIM_RATE_TYPE],
            tbl.[AS_GST_AMOUNT__C] AS [GST_AMOUNT],
            tbl.[AS_JOBSEEKER_ID__C] AS [JOB_SEEKER_ID],
            tbl.[AS_STATUS_DATE__C] AS [STATUS_DATE],
            tbl.[AS_STATUS__C] AS [STATUS],
            tbl.[NAME] AS [NAME],
            tbl.[OUTCOME_TYPE__C] AS [OUTCOME_TYPE],
            tbl.[SITE_CODE__C] AS [SITE_CODE],
            tbl.[ISDELETED] AS [IS_DELETED]
        FROM [bronze.NCCM].AS_CLAIMS AS tbl
    ) AS stm;
