CREATE VIEW [silver.NCCM].VW_OUTCOME_DIM
AS
    SELECT 
        UPPER(CONCAT(stm.OUTCOME_ID, '||', stm.CLIENT_CASE_ID)) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(CONCAT(stm.OUTCOME_ID, '||', stm.CLIENT_CASE_ID))) AS UQ_KEY_HASH,
        '||' AS NK_STRING,
        HASHBYTES('MD5', UPPER(stm.CLIENT_CONTACT)) AS CLIENT_CONTACT_HASH_KEY,
        HASHBYTES('MD5', CONCAT(
            stm.CLIENT_CONTACT,
            stm.WRD_PHASE,
            stm.RECORD_TYPE_NAME,
            stm.IS_DELETED
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.ID AS OUTCOME_ID,
            cl.ID AS CLIENT_CASE_ID,
            cl.CLIENT_CONTACT__C AS CLIENT_CONTACT,
            tbl.WRD_PHASE__C AS WRD_PHASE,
            tbl.RECORD_TYPE_NAME__C AS RECORD_TYPE_NAME,
            tbl.ISDELETED AS IS_DELETED
        FROM [bronze.NCCM].OUTCOME AS tbl
            INNER JOIN [bronze.NCCM].CLIENT_CASE AS cl
                ON cl.ID = tbl.CLIENT_CASE__C
    ) AS stm;
