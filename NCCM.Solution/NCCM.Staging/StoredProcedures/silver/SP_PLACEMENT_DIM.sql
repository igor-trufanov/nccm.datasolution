CREATE PROCEDURE [silver.NCCM].SP_PLACEMENT_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'PLACEMENT_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    INSERT INTO [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS (UQ_KEY, UQ_KEY_HASH, DIM_ID, DIMENSION)
    SELECT 
        dm.UQ_KEY,
        dm.UQ_KEY_HASH,
        NEXT VALUE FOR [silver.NCCM].SEQ_PLACEMENT_DIM AS DIM_ID, 
        @DimName AS DIMENSION
    FROM [silver.NCCM].VW_PLACEMENT_DIM AS dm
        LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
            ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
            AND ks.DIMENSION = @DimName
    WHERE ks.UQ_KEY_HASH IS NULL;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].PLACEMENT_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS PLACEMENT_DIM_ID,
            ksc.DIM_ID AS CONTACT_DIM_ID
        FROM [silver.NCCM].VW_PLACEMENT_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ksc
                ON ksc.UQ_KEY_HASH = dm.CLIENT_CONTACT_HASH_KEY
                AND ksc.DIMENSION = 'CONTACTS_DIM'
    ) AS src
    ON (
        trg.PLACEMENT_DIM_ID = src.PLACEMENT_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[PLACEMENT_ID] = src.[PLACEMENT_ID],
            trg.[ANCHOR_DATE] = src.[ANCHOR_DATE],
            trg.[CONTACT_DIM_ID] = src.[CONTACT_DIM_ID],
            trg.[END_DATE] = src.[END_DATE],
            trg.[INTERVIEW_DATE] = src.[INTERVIEW_DATE],
            trg.[PLACED_DATE] = src.[PLACED_DATE],
            trg.[PLACEMENT_TYPE] = src.[PLACEMENT_TYPE],
            trg.[REFERRED_TO_EMPLOYER_DATE] = src.[REFERRED_TO_EMPLOYER_DATE],
            trg.[START_DATE] = src.[START_DATE],
            trg.[STATUS] = src.[STATUS],
            trg.[VACANCY_ID] = src.[VACANCY_ID],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [PLACEMENT_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [PLACEMENT_ID],
            [ANCHOR_DATE],
            [CONTACT_DIM_ID],
            [END_DATE],
            [INTERVIEW_DATE],
            [PLACED_DATE],
            [PLACEMENT_TYPE],
            [REFERRED_TO_EMPLOYER_DATE],
            [START_DATE],
            [STATUS],
            [VACANCY_ID],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[PLACEMENT_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[PLACEMENT_ID],
            src.[ANCHOR_DATE],
            src.[CONTACT_DIM_ID],
            src.[END_DATE],
            src.[INTERVIEW_DATE],
            src.[PLACED_DATE],
            src.[PLACEMENT_TYPE],
            src.[REFERRED_TO_EMPLOYER_DATE],
            src.[START_DATE],
            src.[STATUS],
            src.[VACANCY_ID],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;