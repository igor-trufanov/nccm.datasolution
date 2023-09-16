CREATE PROCEDURE [silver.NCCM].SP_PARTICIPANTS_BRIDGE
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRANSACTION TR1;

    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].PARTICIPANTS_BRIDGE
    )
    MERGE trg
    USING (
        SELECT
            ks.DIM_ID AS PROGRAM_ENGAGEMENT_DIM_ID,
            ksc.DIM_ID AS CONTACT_DIM_ID,
            dm.[ROW_HASH_SUM],
            dm.[IS_DELETED]
        FROM [silver.NCCM].VW_PARTICIPANTS_BRIDGE AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.PROGRAM_ENGAGEMENT_HASH_KEY
                AND ks.DIMENSION = 'PROGRAMENGAGEMENT_DIM'
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ksc
                ON ksc.UQ_KEY_HASH = dm.CLIENT_CONTACT_HASH_KEY
                AND ksc.DIMENSION = 'CONTACTS_DIM'
    ) AS src
    ON (
        trg.PROGRAM_ENGAGEMENT_DIM_ID = src.PROGRAM_ENGAGEMENT_DIM_ID
        AND trg.CONTACT_DIM_ID = src.CONTACT_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ROW_HASH_SUM],
            [CONTACT_DIM_ID],
            [PROGRAM_ENGAGEMENT_DIM_ID],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[ROW_HASH_SUM],
            src.[CONTACT_DIM_ID],
            src.[PROGRAM_ENGAGEMENT_DIM_ID],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;