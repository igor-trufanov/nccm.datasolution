CREATE VIEW [silver.FNC].VW_TRANSACTION_FACT
AS
    SELECT 
        UPPER(stm.PK) AS UQ_KEY, -- TBD Account_Number part of UQ?
        HASHBYTES('MD5', UPPER(stm.PK)) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.DEPARTMENT_DIM_ID,
            stm.ACCOUNT_DIM_ID,
            stm.BATCH_DATE,
            stm.AMOUNT
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.Recordno AS PK,
            dpt.DEPARTMENT_DIM_ID AS DEPARTMENT_DIM_ID,
            acn.ACCOUNT_DIM_ID AS ACCOUNT_DIM_ID,
            tbl.Batch_date AS BATCH_DATE,
            tbl.Amount AS AMOUNT
        FROM [bronze.FNC].Gldetail_PROD AS tbl -- group
            -- left to one record
            LEFT JOIN [$(SSIReporting)].[gold.FNC].ACCOUNT_DIM AS acn
                ON acn.ACCOUNT_NUMBER = tbl.Accountno
            LEFT JOIN [$(SSIReporting)].[gold.FNC].DEPARTMENT_DIM AS dpt
                ON dpt.DEPARTMENT_ID = tbl.Departmentid
    ) AS stm;
