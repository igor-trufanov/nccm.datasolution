CREATE TABLE [gold.FNC].BUDGET_FACT (
    [BUDGET_FACT_ID] INT IDENTITY(1, 1) NOT NULL,

    -- nullable
    [AMOUNT] DECIMAL(17, 6) NULL,
    [DEPARTMENT_DIM_ID] INT NULL,
    [PERIOD_DATE] DATE NULL,
    [ACCOUNT_DIM_ID] INT NULL,
    [DESCRIPTION] NVARCHAR(255) NULL,

    -- not nullable
    [IS_DELETED] INT NOT NULL,

    [ROW_CREATED_AT] DATETIME NOT NULL,
    [ROW_UPDATED_AT] DATETIME NOT NULL

    CONSTRAINT [PK_BUDGET_FACT] PRIMARY KEY (BUDGET_FACT_ID)
);