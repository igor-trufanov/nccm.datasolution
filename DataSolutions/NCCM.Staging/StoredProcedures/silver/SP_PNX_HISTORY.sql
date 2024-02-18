CREATE PROCEDURE [silver.NCCM].SP_PNX_HISTORY
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    
    BEGIN TRANSACTION TR1;

        DECLARE @Timestamp DATETIME = GETUTCDATE();
        DECLARE @Date DATE = @Timestamp;
        DECLARE @EndDateTime DATETIME = '9999-12-31 23:59:59.997';

        ;WITH trg AS (
            SELECT * FROM [silver.NCCM].PNX_HISTORY WHERE ROW_IS_CURRENT = 1
        )
        MERGE trg
        USING [silver.NCCM].VW_PNX_HISTORY AS src
        ON (
            trg.UQ_KEY_HASH = src.UQ_KEY_HASH
        )

        WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN 
            UPDATE SET
                trg.ROW_IS_CURRENT = 0,
                trg.ROW_END_DATE_TIME = @Timestamp,
                trg.ROW_REASON_FOR_CHANGE = N'Change detection.',
                trg.ROW_IS_CURRENT_FOR_THE_DAY = IIF(@Date = trg.ROW_CALENDAR_DATE, 0, 1)

        ;WITH trg AS (
            SELECT * FROM [silver.NCCM].PNX_HISTORY WHERE ROW_IS_CURRENT = 1
        )
        MERGE trg
        USING (
            SELECT
                *
            FROM [silver.NCCM].VW_PNX_HISTORY
        ) AS src
        ON (
            trg.UQ_KEY_HASH = src.UQ_KEY_HASH
            AND trg.ROW_HASH_SUM = src.ROW_HASH_SUM
        )

        WHEN NOT MATCHED BY TARGET THEN 
            INSERT (
                [CLIENT],
                [UQ_KEY_HASH],
                [CONTRACT_REFERRAL_STATUS],
                [SERVICING_PLACEMENT_STATUS],
                [SITE_CODE],
                [COMMENCEMENT_OF_SERVICING_DATE],
                [ROW_IS_CURRENT],
                [ROW_IS_CURRENT_FOR_THE_DAY],
                [ROW_EFFECTIVE_DATE_TIME],
                [ROW_CALENDAR_DATE],
                [ROW_END_DATE_TIME],
                [ROW_REASON_FOR_CHANGE],
                [ROW_HASH_SUM]
            ) VALUES (
                src.[CLIENT],
                src.[UQ_KEY_HASH],
                src.[CONTRACT_REFERRAL_STATUS],
                src.[SERVICING_PLACEMENT_STATUS],
                src.[SITE_CODE],
                src.[COMMENCEMENT_OF_SERVICING_DATE],
                1, -- [ROW_IS_CURRENT]
                1, -- [ROW_IS_CURRENT_FOR_THE_DAY],
                @Timestamp, -- [ROW_EFFECTIVE_DATE_TIME],
                @Date, -- [ROW_CALENDAR_DATE],
                @EndDateTime, -- [ROW_END_DATE_TIME],
                NULL, -- [ROW_REASON_FOR_CHANGE]
                src.[ROW_HASH_SUM]
            );

    COMMIT TRANSACTION TR1;

END;