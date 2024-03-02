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
            trg.PROGRAM_ENGAGEMENT_ID= src.PROGRAM_ENGAGEMENT_ID,
            trg.CREATED_DATE= src.CREATED_DATE,
            trg.DATE_OF_EXIT= src.DATE_OF_EXIT,
            trg.OWNER_ID= src.OWNER_ID,
            trg.SUBURB__C= src.SUBURB__C,
            trg.POSTAL__CODE= src.POSTAL__CODE,
            trg.CLIENT_COMMENCEMENT_DATE__C = src.CLIENT_COMMENCEMENT_DATE__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            CLIENT_CONTACT__C,
            FUNDING_PROGRAM__C,
            STAGE__C,
            PROGRAM_ENGAGEMENT_ID,
            CREATED_DATE,
            DATE_OF_EXIT,
            OWNER_ID,
            SUBURB__C,
            POSTAL__CODE,
            CLIENT_COMMENCEMENT_DATE__C,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.CLIENT_CONTACT__C,
            src.FUNDING_PROGRAM__C,
            src.STAGE__C,
            src.PROGRAM_ENGAGEMENT_ID,
            src.CREATED_DATE,
            src.DATE_OF_EXIT,
            src.OWNER_ID,
            src.SUBURB__C,
            src.POSTAL__CODE,
            src.CLIENT_COMMENCEMENT_DATE__C,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );

    UPDATE trg
            SET trg.ISDELETED = 1
        FROM [bronze.NCCM].CLIENT_CASE AS trg
            LEFT JOIN [bronze.NCCM].VW_CLIENT_CASE AS src
                ON trg.ID = src.ID
        WHERE 
            trg.ISDELETED = 0 
            AND src.ID IS NULL;

END;