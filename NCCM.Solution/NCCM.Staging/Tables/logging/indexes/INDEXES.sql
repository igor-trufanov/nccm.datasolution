CREATE NONCLUSTERED INDEX IX_EXECUTION_LOG_CreatedAtUTC
ON [logging.NCCM].EXECUTION_LOG([CreatedAtUTC] DESC);
