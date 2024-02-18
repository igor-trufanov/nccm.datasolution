CREATE VIEW [gold.NCCM].VW_OUTCOME_DIM
AS
    SELECT
        tbl.[OUTCOME_DIM_ID],
        tbl.[ARCHIVAL],
        tbl.[OUTCOME_ID],
        tbl.[CLIENT_CASE_ID],
        tbl.[CONTACT_DIM_ID],
        tbl.[WRD_PHASE],
        tbl.[RECORD_TYPE_NAME],
        tbl.[IS_DELETED],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_CREATED_AT],
        tbl.[ROW_UPDATED_AT]
    FROM [silver.NCCM].OUTCOME_DIM AS tbl