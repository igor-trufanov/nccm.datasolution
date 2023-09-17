CREATE PROCEDURE [bronze.NCCM].SP_PARTICIPANTS
AS
BEGIN

    MERGE [bronze.NCCM].PARTICIPANTS AS trg
    USING [bronze.NCCM].VW_PARTICIPANTS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.CONTACT__C = src.CONTACT__C,
            trg.PROGRAM_ENGAGEMENT__C = src.PROGRAM_ENGAGEMENT__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            CONTACT__C,
            PROGRAM_ENGAGEMENT__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.CONTACT__C,
            src.PROGRAM_ENGAGEMENT__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );

END;