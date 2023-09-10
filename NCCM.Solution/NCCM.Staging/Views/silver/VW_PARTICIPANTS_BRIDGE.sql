CREATE VIEW [silver.NCCM].VW_PARTICIPANTS_BRIDGE
AS
    SELECT 
        CONCAT(stm.[CLAIM_ID], '||') AS UQ_KEY,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.[CONTACT],
            stm.[PROGRAM_ENGAGEMENT],
            stm.[IS_DELETED]
        )), 2) AS RowHashSum,
        stm.*
    FROM (
        SELECT
            tbl.[ID] AS [CLAIM_ID],
            tbl.[CONTACT__C] AS [CONTACT],
            tbl.[PROGRAM_ENGAGEMENT__C] AS [PROGRAM_ENGAGEMENT],
            tbl.[ISDELETED] AS [IS_DELETED]
        FROM [bronze.NCCM].PARTICIPANTS AS tbl
    ) AS stm;
