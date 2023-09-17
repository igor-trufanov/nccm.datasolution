CREATE PROCEDURE [bronze.NCCM].SP_OUTCOME
AS
BEGIN

    MERGE [bronze.NCCM].OUTCOME AS trg
    USING [bronze.NCCM].VW_OUTCOME AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.WRD_PHASE__C = src.WRD_PHASE__C,
            trg.RECORD_TYPE_NAME__C = src.RECORD_TYPE_NAME__C,
            trg.CLIENT_CASE__C = src.CLIENT_CASE__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            WRD_PHASE__C,
            RECORD_TYPE_NAME__C,
            CLIENT_CASE__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.WRD_PHASE__C,
            src.RECORD_TYPE_NAME__C,
            src.CLIENT_CASE__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );

END;