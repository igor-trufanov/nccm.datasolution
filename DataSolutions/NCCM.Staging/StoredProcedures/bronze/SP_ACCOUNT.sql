CREATE PROCEDURE [bronze.NCCM].SP_ACCOUNT
AS
BEGIN

    MERGE [bronze.NCCM].ACCOUNT AS trg
    USING [bronze.NCCM].VW_ACCOUNT AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.EMPLOYER__C = src.EMPLOYER__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            EMPLOYER__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.EMPLOYER__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );


    UPDATE trg
            SET trg.ISDELETED = 1
        FROM [bronze.NCCM].ACCOUNT AS trg
            LEFT JOIN [bronze.NCCM].VW_ACCOUNT AS src
                ON trg.ID = src.ID
        WHERE 
            trg.ISDELETED = 0 
            AND src.ID IS NULL;
END;