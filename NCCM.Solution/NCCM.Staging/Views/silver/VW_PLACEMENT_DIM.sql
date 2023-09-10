CREATE VIEW [silver.NCCM].VW_PLACEMENT_DIM
AS
    SELECT 
        CONCAT(stm.[PLACEMENT_ID], '||') AS UQ_KEY,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.[ANCHOR_DATE],
            stm.[CONTACT],
            stm.[END_DATE],
            stm.[INTERVIEW_DATE],
            stm.[PLACED_DATE],
            stm.[PLACEMENT_TYPE],
            stm.[REFERRED_TO_EMPLOYER_DATE],
            stm.[START_DATE],
            stm.[STATUS],
            stm.[VACANCY_ID],
            stm.[IS_DELETED]
        )), 2) AS RowHashSum,
        stm.*
    FROM (
        SELECT
            tbl.[ID] AS [PLACEMENT_ID],
            tbl.[ANCHOR_DATE__C] AS [ANCHOR_DATE],
            tbl.[CONTACT__C] AS [CONTACT],
            tbl.[END_DATE__C] AS [END_DATE],
            tbl.[INTERVIEW_DATE__C] AS [INTERVIEW_DATE],
            tbl.[PLACED_DATE__C] AS [PLACED_DATE],
            tbl.[PLACEMENT_TYPE__C] AS [PLACEMENT_TYPE],
            tbl.[REFERRED_TO_EMPLOYER_DATE__C] AS [REFERRED_TO_EMPLOYER_DATE],
            tbl.[START_DATE__C] AS [START_DATE],
            tbl.[STATUS__C] AS [STATUS],
            tbl.[VACANCY_ID__C] AS [VACANCY_ID],
            tbl.[ISDELETED] AS [IS_DELETED]
        FROM [bronze.NCCM].PLACEMENT AS tbl
    ) AS stm;
