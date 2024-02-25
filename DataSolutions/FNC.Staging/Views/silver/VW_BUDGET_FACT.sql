CREATE VIEW [silver.FNC].VW_BUDGET_FACT
AS
    SELECT 
        UPPER(stm.RECORD_NUMBER) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(stm.RECORD_NUMBER)) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.AMOUNT,
            stm.DEPARTMENT_DIM_ID,
            stm.PERIOD_DATE,
            stm.ACCOUNT_DIM_ID,
            stm.DESCRIPTION,
            stm.BUDGET_KEY
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.Recordno AS RECORD_NUMBER, 
            tbl.Amount AS AMOUNT,
            mpg2.DIM_ID AS DEPARTMENT_DIM_ID,
            tbl.Pstartdate AS PERIOD_DATE,
            mpg.DIM_ID AS ACCOUNT_DIM_ID,
            hdr.Description as DESCRIPTION,
            tbl.Budgetkey AS BUDGET_KEY
        FROM [bronze.FNC].Glbudgetitem AS tbl
            LEFT JOIN [bronze.FNC].Glbudgetheader AS hdr
                ON hdr.Recordno = tbl.Budgetkey
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as mpg
                ON mpg.UQ_KEY = tbl.Acct_no
                AND mpg.DIMENSION = 'ACCOUNT_DIM'
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as mpg2
                ON mpg2.UQ_KEY = tbl.Dept_no
                AND mpg2.DIMENSION = 'DEPARTMENT_DIM'
    ) AS stm;
