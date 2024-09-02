CREATE or ALTER PROCEDURE [dbo].[dba_QueryStoreUnforceFailed] 
		@print bit = 0,
		@execute bit = 1
	AS

	/* 
	Usage:
		exec [dbo].[dba_QueryStoreUnforceFailed]  @print=1, @execute=1;

	Description:
		This procedure loops through user databases. In each database, it checks for queries that have is_forced_plan set to 1 in query store where force_failure_count > 0. If any are found, it unforces the plan.
		For an example of why this is important, see https://kendralittle.com/2024/08/12/query-store-failed-forced-plans-general-failure-even-slower-compile-time/
	*/

	BEGIN  

	SET XACT_ABORT, NOCOUNT ON;

	BEGIN TRY

		DECLARE
			@databaseLoopCounter int = 1, 
			@maxDatabaseI int, 
			@databaseName sysname, 
			@dynamicSQL nvarchar(2000),
			@msg nvarchar(2000)

		DECLARE @databases TABLE (i int identity primary key, databaseName sysname);
		CREATE TABLE #forceFailedPlans (forcedFailsId INT IDENTITY, databaseName sysname, queryId INT, planId INT, lastForcedFailureReason nvarchar(128), querySqlText nvarchar(4000));

		/* System databases don't have query store enabled */
		INSERT @databases (databaseName) 
		SELECT [name] 
		FROM sys.databases
		WHERE 
			state_desc = 'ONLINE'
			AND user_access = 0
			AND name NOT IN ('master', 'model', 'msdb', 'tempdb');

		/* Loop through each database */
		SELECT @maxDatabaseI = max(i) FROM @databases;
		WHILE (@databaseLoopCounter <= @maxDatabaseI)
		BEGIN
			SELECT @databaseName = databaseName FROM @databases WHERE i = @databaseLoopCounter;

			/* Check for forced but failed plans. */
			BEGIN
				SET @dynamicSQL = N'
					SELECT ''' + @databaseName + N''', 
						qsqp.query_id, 
						qsqp.plan_id, 
						qsqp.last_force_failure_reason_desc, 
						LEFT(qsqt.query_sql_text, 4000) AS querySqlText
					FROM ' + QUOTENAME(@databaseName) + N'.sys.query_store_plan AS qsqp (NOLOCK) 
					LEFT JOIN ' + QUOTENAME(@databaseName) + N'.sys.query_store_query AS qsq (NOLOCK) ON qsqp.query_id = qsq.query_id
					LEFT JOIN ' + QUOTENAME(@databaseName) + N'.sys.query_store_query_text AS qsqt (NOLOCK) ON qsq.query_text_id = qsqt.query_text_id
					WHERE 
						qsqp.is_forced_plan = 1 AND 
						qsqp.force_failure_count > 0;';

				IF @print = 1
					PRINT @dynamicSQL;

				INSERT #forceFailedPlans (databaseName, queryId, planId, lastForcedFailureReason, querySqlText)
				EXEC sp_executesql @dynamicSQL;

				/* If there is at least one row in #fordeFailedPlans, unforce. Use another loop to handle situations where multiple plans are in this state in a database. */
				/* Start inner loop */
				IF @@ROWCOUNT > 0
				BEGIN
					DECLARE @minForcedFailsId INT = 1, @maxDatabaseIForcedFailsId INT, @queryId INT, @planId INT;

					SELECT @minForcedFailsId = min(forcedFailsId), @maxDatabaseIForcedFailsId = max(forcedFailsId) FROM #forceFailedPlans WHERE databaseName = @databaseName;

					WHILE (@minForcedFailsId <= @maxDatabaseIForcedFailsId)
					BEGIN
						SELECT @queryId = queryId, @planId = planId
						FROM #forceFailedPlans
						WHERE forcedFailsId = @minForcedFailsId;

						PRINT 'Unforcing failed force plan.';
						SET @dynamicSQL = N'EXEC ' + quotename (@databaseName) + N'.sys.sp_query_store_unforce_plan ' + cast (@queryId as nvarchar(100)) + N', ' + cast(@planId as nvarchar(100)) + N';';

						IF @print = 1
							PRINT @dynamicSQL;

						IF @execute = 1
							EXEC sp_executesql @dynamicSQL;

						/* Increment inner loop that fixes force failed plans */
						SET @minForcedFailsId = @minForcedFailsId + 1;
					END
				/* End inner loop */
				END
			END

			/* Increment outer loop that checks each database for forced failed plans */
			SELECT @databaseLoopCounter = @databaseLoopCounter + 1;
			/* End loop through databases */
		END

	END TRY
	BEGIN CATCH
		IF @@trancount > 0 ROLLBACK TRANSACTION
		DECLARE @errorMsg nvarchar(2048) = error_message()  
		RAISERROR (@errorMsg, 16, 1)
		RETURN 55555
	END CATCH
END;
GO
