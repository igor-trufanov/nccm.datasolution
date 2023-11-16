CREATE TABLE [raw.NCCM].FFS_AS_CLAIMS (
    [ID] BIGINT IDENTITY(1,1) NOT NULL,
    [RECORD] NVARCHAR(MAX) NULL,
    [FILE_NAME] NVARCHAR(2000) NOT NULL,
    [RUN_ID] UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_FFS_AS_CLAIMS_RUN_ID DEFAULT('00000000-0000-0000-0000-000000000001'),
    [CREATED_AT_UTC] DATETIME CONSTRAINT DF_FFS_AS_CLAIMS_CREATED_AT_UTC DEFAULT(GETUTCDATE())
)