CREATE PROCEDURE [silver.FNC].SP_DEPARTMENT_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'DEPARTMENT_DIM';
    DECLARE @Timestamp DATETIME = GETDATE();

    ;WITH trg AS (
        SELECT * FROM [$(SSIReporting)].[gold.FNC].DEPARTMENT_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS DEPARTMENT_DIM_ID
        FROM [silver.FNC].VW_DEPARTMENT_DIM AS dm
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.DEPARTMENT_DIM_ID = src.DEPARTMENT_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[DEPARTMENT_ID] = src.[DEPARTMENT_ID],
            trg.[TITLE] = src.[TITLE],
            trg.[LEVEL] = src.[LEVEL],
            trg.[PARENT_ID] = src.[PARENT_ID],
            trg.[PARENT_NAME] = src.[PARENT_NAME],
            trg.[SUPERVISOR_NAME] = src.[SUPERVISOR_NAME],
            trg.[PATH] = src.[PATH],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [DEPARTMENT_DIM_ID],
            [DEPARTMENT_ID],
            [ROW_HASH_SUM],
            [TITLE],
            [LEVEL],
            [PARENT_ID],
            [PARENT_NAME],
            [SUPERVISOR_NAME],
            [PATH],
            [IS_DELETED],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[DEPARTMENT_DIM_ID],
            src.[DEPARTMENT_ID],
            src.[ROW_HASH_SUM],
            src.[TITLE],
            src.[LEVEL],
            src.[PARENT_ID],
            src.[PARENT_NAME],
            src.[SUPERVISOR_NAME],
            src.[PATH],
            src.[IS_DELETED],
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;