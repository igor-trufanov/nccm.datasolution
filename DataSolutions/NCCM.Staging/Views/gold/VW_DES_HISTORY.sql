CREATE VIEW [gold.NCCM].VW_DES_HISTORY
AS
    SELECT
        tbl.[CLIENT],
        tbl.[REFERRAL_STATUS_CODE],
        tbl.[SERVICING_PLACEMENT_STATUS],
        tbl.[SITE_CODE],
        tbl.[COMMENCEMENT_OF_SERVICING_DATE],
        tbl.[ROW_IS_CURRENT],
        tbl.[ROW_IS_CURRENT_FOR_THE_DAY],
        tbl.[ROW_EFFECTIVE_DATE_TIME],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_END_DATE_TIME],
        tbl.[ROW_REASON_FOR_CHANGE]
    FROM [silver.NCCM].DES_HISTORY AS tbl