CREATE VIEW [silver.NCCM].VW_RESP_ASSES_HISTORY
AS
    SELECT 
        UPPER(stm.CLIENT) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(stm.CLIENT)) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.FUNDING_PROGRAM,
            stm.REFERRAL_STATUS_CODE,
            stm.REGION,
            stm.CLIENT_COMMENCEMENT_DATE
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.ID AS CLIENT,
            tbl.FUNDING_PROGRAM__C AS FUNDING_PROGRAM,
            tbl.STAGE__C AS REFERRAL_STATUS_CODE,
            tbl.CLIENT_COMMENCEMENT_DATE__C AS CLIENT_COMMENCEMENT_DATE,
            cnt.REGION__C AS REGION
        FROM [bronze.NCCM].CLIENT_CASE AS tbl
            LEFT JOIN [bronze.NCCM].CONTACTS AS cnt
                ON cnt.ID = tbl.CLIENT_CONTACT__C
    ) AS stm;