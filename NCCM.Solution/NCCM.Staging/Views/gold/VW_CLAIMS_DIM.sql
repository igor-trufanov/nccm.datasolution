CREATE VIEW [gold.NCCM].VW_CLAIMS_DIM
AS
    SELECT
        tbl.[CLAIM_DIM_ID],
        tbl.[ARCHIVAL],
        tbl.[APPROVED_AMOUNT],
        tbl.[CLAIM_AMOUNT],
        tbl.[AS_CLAIM_ID],
        tbl.[CLAIM_RATE_TYPE],
        tbl.[GST_AMOUNT],
        tbl.[JOB_SEEKER_ID],
        tbl.[STATUS_DATE],
        tbl.[STATUS],
        tbl.[NAME],
        tbl.[OUTCOME_TYPE],
        tbl.[SITE_CODE],
        tbl.[IS_DELETED],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_CREATED_AT],
        tbl.[ROW_UPDATED_AT]
    FROM [silver.NCCM].CLAIMS_DIM AS tbl