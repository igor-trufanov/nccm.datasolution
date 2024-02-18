CREATE VIEW [gold.NCCM].RESP_ASSES_HISTORY
AS
    SELECT
        tbl.[CLIENT],
        tbl.[FUNDING_PROGRAM],
        tbl.[REFERRAL_STATUS_CODE],
        tbl.[REGION],
        tbl.[CLIENT_COMMENCEMENT_DATE],
        tbl.[ROW_IS_CURRENT],
        tbl.[ROW_IS_CURRENT_FOR_THE_DAY],
        tbl.[ROW_EFFECTIVE_DATE_TIME],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_END_DATE_TIME],
        tbl.[ROW_REASON_FOR_CHANGE]
    FROM [$(Staging)].[gold.NCCM].VW_RESP_ASSES_HISTORY AS tbl