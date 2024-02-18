CREATE VIEW [gold.NCCM].VW_PLACEMENT_DIM_HISTORY
AS
    SELECT
        tbl.[PLACEMENT_DIM_ID],
        tbl.[ARCHIVAL],
        tbl.[PLACEMENT_ID],
        tbl.[ANCHOR_DATE],
        tbl.[CONTACT_DIM_ID],
        tbl.[END_DATE],
        tbl.[INTERVIEW_DATE],
        tbl.[PLACED_DATE],
        tbl.[PLACEMENT_TYPE],
        tbl.[REFERRED_TO_EMPLOYER_DATE],
        tbl.[START_DATE],
        tbl.[STATUS],
        tbl.[VACANCY_ID],
        tbl.[IS_DELETED],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_CREATED_AT],
        tbl.[ROW_UPDATED_AT],
        tbl.[VALID_FROM],
        tbl.[VALID_TO]
    FROM [silver.NCCM].PLACEMENT_DIM_HISTORY AS tbl