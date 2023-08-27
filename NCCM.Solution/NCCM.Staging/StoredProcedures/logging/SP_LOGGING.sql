/*
 * Logging Stored Procedure to insert execution steps, 
 * if Id is passed the rest values would be updated accordingly to complete an ongoing task.
 * The last logic step within Run Id should be market as @StepNum = -1, the first @StepNum = 0
 *
 * Usage example:
 *
 * DECLARE @Id INT;
 * DECLARE @Dt DATETIME = GETUTCDATE();
 * DECLARE @RunId UNIQUEIDENTIFIER = NEWID();
 * -- to insert a step
 * EXEC @id = [logging.NCCM].SP_LOGGING @RunId = @RunId, @StepName = 'Test', @StepNum = 0, @StartedAtUTC = @Dt
 * -- to complete the step
 * EXEC @id = [logging.NCCM].SP_LOGGING @Id = @Id, @EndedAtUTC = @Dt, @Message = N'Done'
 *
 *
 */

CREATE PROCEDURE [logging.NCCM].SP_LOGGING (
    -- update
    @Id INT NULL = NULL,
    @EndedAtUTC DATETIME NULL = NULL,
    @Message NVARCHAR(MAX) NULL = NULL,
    @ErrorMessage NVARCHAR(MAX) NULL = NULL,
    -- insert
    @RunId UNIQUEIDENTIFIER NULL = NULL,
    @StepName NVARCHAR(1024) NULL = NULL,
    @StepNum INT NULL = NULL,
    @StartedAtUTC DATETIME = NULL
)
AS
BEGIN

    IF (@Id IS NULL)
    BEGIN

        INSERT INTO [logging.NCCM].EXECUTION_LOG (
            [RunId],
            [StepName],
            [StepNum],
            [StartedAtUTC],
            [EndedAtUTC],
            [Message],
            [ErrorMessage]
        ) VALUES (
            @RunId,
            @StepName,
            @StepNum,
            @StartedAtUTC,
            NULL, -- EndedAtUTC
            @Message,
            @ErrorMessage -- ErrorMessage
        );

    END
    ELSE
    BEGIN

        UPDATE [logging.NCCM].EXECUTION_LOG SET 
            [EndedAtUTC] = @EndedAtUTC,
            [Message] = @Message,
            [ErrorMessage] = @ErrorMessage,
            [UpdatedAtUTC] = GETUTCDATE()
        WHERE Id = @Id;

    END;

    RETURN @@IDENTITY;

END;