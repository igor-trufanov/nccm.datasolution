﻿CREATE VIEW [bronze.NCCM].VW_PLACEMENT
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.ANCHOR_DATE__C,
            stm.CONTACT__C,
            stm.END_DATE__C,
            stm.INTERVIEW_DATE__C,
            stm.PLACED_DATE__C,
            stm.PLACEMENT_TYPE__C,
            stm.REFERRED_TO_EMPLOYER_DATE__C,
            stm.START_DATE__C,
            stm.STATUS__C,
            stm.VACANCY_ID__C,
            stm.ISDELETED)), 2
        ) AS RowHashSum
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID AS VARCHAR(18)), '') AS ID,
            CAST(NULLIF(NULLIF(CAST(tbl.ANCHOR_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS ANCHOR_DATE__C,
            NULLIF(CAST(tbl.CONTACT__C AS VARCHAR(18)), '') AS CONTACT__C,
            CAST(NULLIF(NULLIF(CAST(tbl.END_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS END_DATE__C,
            CAST(NULLIF(NULLIF(CAST(tbl.INTERVIEW_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS INTERVIEW_DATE__C,
            CAST(NULLIF(NULLIF(CAST(tbl.PLACED_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS PLACED_DATE__C,
            NULLIF(CAST(tbl.PLACEMENT_TYPE__C   AS VARCHAR(255)), '') AS PLACEMENT_TYPE__C,
            NULLIF(CAST(tbl.REFERRED_TO_EMPLOYER_DATE__C   AS VARCHAR(255)), '') AS REFERRED_TO_EMPLOYER_DATE__C,
            CAST(NULLIF(NULLIF(CAST(tbl.START_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS START_DATE__C,
            NULLIF(CAST(tbl.STATUS__C   AS VARCHAR(255)), '') AS STATUS__C,
            NULLIF(CAST(tbl.VACANCY_ID__C   AS VARCHAR(18)), '') AS VACANCY_ID__C,
            CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_SSI_Placement__c AS tbl
    ) AS stm;