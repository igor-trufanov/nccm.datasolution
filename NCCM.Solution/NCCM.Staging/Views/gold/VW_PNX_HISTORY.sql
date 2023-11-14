CREATE VIEW [gold.NCCM].VW_PNX_HISTORY
AS
    SELECT
        tbl.[CLIENT],
        tbl.[CONTRACT_REFERRAL_STATUS],
        tbl.[SERVICING_PLACEMENT_STATUS],
        tbl.[SITE_CODE],
        tbl.[COMMENCEMENT_OF_SERVICING_DATE],
        tbl.[ROW_IS_CURRENT],
        tbl.[ROW_IS_CURRENT_FOR_THE_DAY],
        tbl.[ROW_EFFECTIVE_DATE_TIME],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_END_DATE_TIME],
        tbl.[ROW_REASON_FOR_CHANGE]
    FROM [silver.NCCM].PNX_HISTORY AS tbl