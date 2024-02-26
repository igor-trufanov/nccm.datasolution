﻿CREATE TABLE [bronze.NCCM].CLIENT_CASE (
    -- PK
    [ID] VARCHAR(18) NOT NULL,

    -- Attributes
    [CLIENT_CONTACT__C] VARCHAR(18) NULL,
    [FUNDING_PROGRAM__C] VARCHAR(255) NULL,
    [STAGE__C] VARCHAR(255) NULL,
    [PROGRAM_ENGAGEMENT_ID] VARCHAR(255) NULL,
    [CREATED_DATE] DATE NULL,
    [DATE_OF_EXIT] DATE NULL,
    [OWNER_ID] VARCHAR(255) NULL,
    [SUBURB__C] VARCHAR(255) NULL,
    [POSTAL__CODE] VARCHAR(255) NULL,
    [CLIENT_COMMENCEMENT_DATE__C] DATE NULL,
    [ISDELETED] INT NOT NULL,

    -- Hash
    [ROW_HASH_SUM] VARCHAR(32) NOT NULL,

    -- Temporal
    [RowVersion] ROWVERSION NOT NULL,
    [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
    [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),

    CONSTRAINT PK_CLIENT_CASE PRIMARY KEY CLUSTERED ([Id])
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [bronze.NCCM].CLIENT_CASE_HISTORY));
