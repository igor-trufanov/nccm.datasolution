CREATE PROCEDURE [bronze.NCCM].SP_CLIENT_CASE
AS
BEGIN

    MERGE [bronze.NCCM].CLIENT_CASE AS trg
    USING [bronze.NCCM].VW_CLIENT_CASE AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.CLIENT_CONTACT__C = src.CLIENT_CONTACT__C,
            trg.FUNDING_PROGRAM__C = src.FUNDING_PROGRAM__C,
            trg.STAGE__C = src.STAGE__C,
            trg.CLIENT_COMMENCEMENT_DATE__C = src.CLIENT_COMMENCEMENT_DATE__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            CLIENT_CONTACT__C,
            FUNDING_PROGRAM__C,
            STAGE__C,
            CLIENT_COMMENCEMENT_DATE__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.CLIENT_CONTACT__C,
            src.FUNDING_PROGRAM__C,
            src.STAGE__C,
            src.CLIENT_COMMENCEMENT_DATE__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );

END;