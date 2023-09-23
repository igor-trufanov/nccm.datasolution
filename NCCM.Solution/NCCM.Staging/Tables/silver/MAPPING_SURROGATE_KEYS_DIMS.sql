CREATE TABLE [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS (
    [DIM_ID] INT NOT NULL,
    [UQ_KEY] NVARCHAR(MAX) NOT NULL,
    [UQ_KEY_HASH] BINARY(16) NOT NULL,
    [DIMENSION] VARCHAR(255) NOT NULL,

    CONSTRAINT PK_MAPPING_SURROGATE_KEYS_DIMS PRIMARY KEY ([UQ_KEY_HASH], [DIMENSION]),
    CONSTRAINT UQ_MAPPING_SURROGATE_KEYS_DIMS UNIQUE ([DIM_ID], [DIMENSION])
);