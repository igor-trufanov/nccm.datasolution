CREATE VIEW [silver.FNC].VW_DEPARTMENT_DIM
AS
    SELECT 
        UPPER(stm.DEPARTMENT_ID) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(stm.DEPARTMENT_ID)) AS UQ_KEY_HASH,
        '||' AS NK_STRING,
        HASHBYTES('MD5', CONCAT(
            stm.TITLE,
            stm.LEVEL,
            stm.PARENT_ID,
            stm.PARENT_NAME,
            stm.SUPERVISOR_NAME,
            stm.PATH,
            stm.IS_DELETED
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.[Title] AS TITLE,
            NULL AS LEVEL, -- TBD
            tbl.Departmentid AS DEPARTMENT_ID,
            tbl.Parentid AS PARENT_ID,
            tbl.Parentname AS PARENT_NAME,
            tbl.Supervisorname AS SUPERVISOR_NAME,
            NULL AS PATH, -- TBD
            0 AS IS_DELETED -- TBD
        FROM [bronze.FNC].[Department_PROD] AS tbl
    ) AS stm;
