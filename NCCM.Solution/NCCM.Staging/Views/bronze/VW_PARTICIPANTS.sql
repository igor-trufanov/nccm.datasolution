CREATE VIEW [bronze.NCCM].VW_PARTICIPANTS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.CONTACT__C,
            stm.PROGRAM_ENGAGEMENT__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.CONTACT__C AS VARCHAR(18)), '') AS CONTACT__C,
            NULLIF(CAST(tbl.PROGRAM_ENGAGEMENT__C AS VARCHAR(18)), '') AS PROGRAM_ENGAGEMENT__C,
            (CASE LTRIM(RTRIM(LOWER(CAST(tbl.ISDELETED AS VARCHAR(255))))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_Program_Engagement_Participant__c AS tbl
    ) AS stm;