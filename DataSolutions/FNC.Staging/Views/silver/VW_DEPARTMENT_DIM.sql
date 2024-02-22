CREATE VIEW [silver.FNC].VW_DEPARTMENT_DIM
AS
    WITH RecursiveDeptCTE AS (
        -- Anchor member: Initialize the hierarchy with root level departments
        SELECT 
            recordno, 
            departmentid, 
            Title, 
            Parentkey, 
            Parentid, 
            parentname, 
            1 AS Level,
            Supervisorname,
            CAST(departmentid AS VARCHAR(MAX)) AS HierarchyPath  -- Initialize hierarchy path with the department ID
        FROM [bronze.FNC].[Department_PROD]
         WHERE Parentkey =0 OR Parentid is null or Parentid ='' -- Assuming top-level departments have NULL Parentkey
	     --WHERE Departmentid = 'CPBD'

        UNION ALL

        -- Recursive member: Build up the hierarchy by appending department IDs to the path
        SELECT 
            d.recordno, 
            d.departmentid, 
            d.Title, 
            d.Parentkey, 
            d.Parentid, 
            d.parentname, 
            r.Level + 1 AS Level,
            d.Supervisorname,
            CAST(r.HierarchyPath + ' -> ' + d.departmentid AS VARCHAR(MAX)) AS HierarchyPath  -- Append current department ID to the path
        FROM [bronze.FNC].[Department_PROD] d
        JOIN RecursiveDeptCTE r ON d.Parentkey = r.recordno  -- Join on Parentkey to find sub-departments
    )
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
            tbl.Level AS LEVEL,
            tbl.Departmentid AS DEPARTMENT_ID,
            tbl.Parentid AS PARENT_ID,
            tbl.Parentname AS PARENT_NAME,
            tbl.Supervisorname AS SUPERVISOR_NAME,
            tbl.HierarchyPath AS PATH,
            0 AS IS_DELETED -- TBD
        FROM RecursiveDeptCTE AS tbl
    ) AS stm;
