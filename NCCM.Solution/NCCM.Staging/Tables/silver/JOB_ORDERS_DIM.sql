CREATE TABLE [silver.NCCM].JOB_ORDERS_DIM (
    [JOB_ORDER_DIM_ID] INT NOT NULL,
    [NK_STRING] NVARCHAR(MAX) NOT NULL,
    [ROW_HASH_SUM] BINARY(16) NOT NULL,

    [ARCHIVAL] BIT NOT NULL CONSTRAINT DF_JOB_ORDERS_DIM_ARCHIVAL DEFAULT(0),

    -- nullable
    [JOB_ORDER_ID] VARCHAR(18) NULL,
    [CONTACT_DIM_ID] INT NULL,
    [HOST_ORGANISATION] VARCHAR(18) NULL,
    [JOB_ORDER_TYPE] VARCHAR(255) NULL,
    [JOB_TITLE] NVARCHAR(1024) NULL,
    [JOB_TYPE] NVARCHAR(1024) NULL,
    [IS_DELETED] INT NULL,

    [ROW_CALENDAR_DATE] DATE NOT NULL,
    [ROW_CREATED_AT] DATETIME NULL,
    [ROW_UPDATED_AT] DATETIME NULL,
    [ROW_VERSION] ROWVERSION NOT NULL,

    CONSTRAINT [PK_JOB_ORDERS_DIM] PRIMARY KEY (JOB_ORDER_DIM_ID)
)
