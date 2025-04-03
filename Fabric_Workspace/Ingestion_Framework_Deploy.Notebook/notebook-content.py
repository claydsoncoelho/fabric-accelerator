# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Follow the steps below to deploy the 
# # Metadata-Driven Framework


# MARKDOWN ********************

# ## 1. Create a Warehouse, for example WH_Control.

# MARKDOWN ********************

# ## 2. Execute the following SQL commands to deploy the database tables:

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE SCHEMA ELT;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC CREATE TABLE [ELT].[IngestDefinition] (
# MAGIC 
# MAGIC 	[IngestID] int NOT NULL, 
# MAGIC 	[SourceSystemName] varchar(50) NOT NULL, 
# MAGIC 	[StreamName] varchar(100) NULL, 
# MAGIC 	[SourceSystemDescription] varchar(200) NULL, 
# MAGIC 	[Backend] varchar(30) NULL, 
# MAGIC 	[DataFormat] varchar(10) NULL, 
# MAGIC 	[EntityName] varchar(100) NULL, 
# MAGIC 	[WatermarkColName] varchar(50) NULL, 
# MAGIC 	[DeltaFormat] varchar(30) NULL, 
# MAGIC 	[LastDeltaDate] datetime2(6) NULL, 
# MAGIC 	[LastDeltaNumber] int NULL, 
# MAGIC 	[LastDeltaString] varchar(50) NULL, 
# MAGIC 	[MaxIntervalMinutes] int NULL, 
# MAGIC 	[MaxIntervalNumber] int NULL, 
# MAGIC 	[DataMapping] varchar(max) NULL, 
# MAGIC 	[SourceFileDropFileSystem] varchar(50) NULL, 
# MAGIC 	[SourceFileDropFolder] varchar(200) NULL, 
# MAGIC 	[SourceFileDropFile] varchar(200) NULL, 
# MAGIC 	[SourceFileDelimiter] char(1) NULL, 
# MAGIC 	[SourceFileHeaderFlag] bit NULL, 
# MAGIC 	[SourceStructure] varchar(max) NULL, 
# MAGIC 	[DestinationRawFileSystem] varchar(50) NULL, 
# MAGIC 	[DestinationRawFolder] varchar(200) NULL, 
# MAGIC 	[DestinationRawFile] varchar(200) NULL, 
# MAGIC 	[RunSequence] int NULL, 
# MAGIC 	[MaxRetries] int NULL, 
# MAGIC 	[ActiveFlag] bit NOT NULL, 
# MAGIC 	[L1TransformationReqdFlag] bit NOT NULL, 
# MAGIC 	[L2TransformationReqdFlag] bit NOT NULL, 
# MAGIC 	[DelayL1TransformationFlag] bit NOT NULL, 
# MAGIC 	[DelayL2TransformationFlag] bit NOT NULL, 
# MAGIC 	[CreatedBy] varchar(128) NOT NULL, 
# MAGIC 	[CreatedTimestamp] datetime2(6) NOT NULL, 
# MAGIC 	[ModifiedBy] varchar(128) NULL, 
# MAGIC 	[ModifiedTimestamp] datetime2(6) NULL, 
# MAGIC 	[BronzeLakehouseID] varchar(128) NULL, 
# MAGIC 	[WH_Control_Conn_String] varchar(255) NULL,
# MAGIC 	[Source_Connection_ID] varchar(255) NULL,
# MAGIC 	[Source_Database_Name] varchar(255) NULL
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE [ELT].[IngestInstance] (
# MAGIC 
# MAGIC 	[IngestInstanceID] varchar(255) NOT NULL, 
# MAGIC 	[IngestID] int NOT NULL, 
# MAGIC 	[SourceFileDropFileSystem] varchar(50) NULL, 
# MAGIC 	[SourceFileDropFolder] varchar(200) NULL, 
# MAGIC 	[SourceFileDropFile] varchar(200) NULL, 
# MAGIC 	[DestinationRawFileSystem] varchar(50) NOT NULL, 
# MAGIC 	[DestinationRawFolder] varchar(200) NOT NULL, 
# MAGIC 	[DestinationRawFile] varchar(200) NOT NULL, 
# MAGIC 	[DataFromTimestamp] datetime2(6) NULL, 
# MAGIC 	[DataToTimestamp] datetime2(6) NULL, 
# MAGIC 	[DataFromNumber] int NULL, 
# MAGIC 	[DataToNumber] int NULL, 
# MAGIC 	[SourceCount] int NULL, 
# MAGIC 	[IngestCount] int NULL, 
# MAGIC 	[IngestStartTimestamp] datetime2(6) NULL, 
# MAGIC 	[IngestEndTimestamp] datetime2(6) NULL, 
# MAGIC 	[IngestStatus] varchar(20) NULL, 
# MAGIC 	[RetryCount] int NULL, 
# MAGIC 	[ReloadFlag] bit NULL, 
# MAGIC 	[CreatedBy] varchar(128) NOT NULL, 
# MAGIC 	[CreatedTimestamp] datetime2(6) NOT NULL, 
# MAGIC 	[ModifiedBy] varchar(128) NULL, 
# MAGIC 	[ModifiedTimestamp] datetime2(6) NULL, 
# MAGIC 	[ADFIngestPipelineRunID] uniqueidentifier NULL
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE [ELT].[L1TransformDefinition] (
# MAGIC 
# MAGIC 	[L1TransformID] int NOT NULL, 
# MAGIC 	[IngestID] int NOT NULL, 
# MAGIC 	[ComputePath] varchar(200) NULL, 
# MAGIC 	[ComputeName] varchar(100) NULL, 
# MAGIC 	[CustomParameters] varchar(max) NULL, 
# MAGIC 	[InputRawFileSystem] varchar(50) NOT NULL, 
# MAGIC 	[InputRawFileFolder] varchar(200) NOT NULL, 
# MAGIC 	[InputRawFile] varchar(200) NOT NULL, 
# MAGIC 	[InputRawFileDelimiter] char(1) NULL, 
# MAGIC 	[InputFileHeaderFlag] bit NULL, 
# MAGIC 	[OutputL1CurateFileSystem] varchar(50) NOT NULL, 
# MAGIC 	[OutputL1CuratedFolder] varchar(200) NOT NULL, 
# MAGIC 	[OutputL1CuratedFile] varchar(200) NOT NULL, 
# MAGIC 	[OutputL1CuratedFileDelimiter] char(1) NULL, 
# MAGIC 	[OutputL1CuratedFileFormat] varchar(10) NULL, 
# MAGIC 	[OutputL1CuratedFileWriteMode] varchar(20) NULL, 
# MAGIC 	[OutputDWStagingTable] varchar(200) NULL, 
# MAGIC 	[LookupColumns] varchar(4000) NULL, 
# MAGIC 	[OutputDWTable] varchar(200) NULL, 
# MAGIC 	[OutputDWTableWriteMode] varchar(20) NULL, 
# MAGIC 	[MaxRetries] int NULL, 
# MAGIC 	[WatermarkColName] varchar(50) NULL, 
# MAGIC 	[ActiveFlag] bit NOT NULL, 
# MAGIC 	[CreatedBy] varchar(128) NOT NULL, 
# MAGIC 	[CreatedTimestamp] datetime2(6) NOT NULL, 
# MAGIC 	[ModifiedBy] varchar(128) NULL, 
# MAGIC 	[ModifiedTimestamp] datetime2(6) NULL
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE [ELT].[L1TransformInstance] (
# MAGIC 
# MAGIC 	[L1TransformInstanceID] varchar(255) NOT NULL, 
# MAGIC 	[L1TransformID] int NOT NULL, 
# MAGIC 	[IngestInstanceID] varchar(255) NULL, 
# MAGIC 	[IngestID] int NOT NULL, 
# MAGIC 	[ComputeName] varchar(100) NULL, 
# MAGIC 	[ComputePath] varchar(200) NULL, 
# MAGIC 	[CustomParameters] varchar(max) NULL, 
# MAGIC 	[InputRawFileSystem] varchar(50) NOT NULL, 
# MAGIC 	[InputRawFileFolder] varchar(200) NOT NULL, 
# MAGIC 	[InputRawFile] varchar(200) NOT NULL, 
# MAGIC 	[InputRawFileDelimiter] char(1) NULL, 
# MAGIC 	[InputFileHeaderFlag] bit NULL, 
# MAGIC 	[OutputL1CurateFileSystem] varchar(50) NOT NULL, 
# MAGIC 	[OutputL1CuratedFolder] varchar(200) NOT NULL, 
# MAGIC 	[OutputL1CuratedFile] varchar(200) NOT NULL, 
# MAGIC 	[OutputL1CuratedFileDelimiter] char(1) NULL, 
# MAGIC 	[OutputL1CuratedFileFormat] varchar(10) NULL, 
# MAGIC 	[OutputL1CuratedFileWriteMode] varchar(20) NULL, 
# MAGIC 	[OutputDWStagingTable] varchar(200) NULL, 
# MAGIC 	[LookupColumns] varchar(4000) NULL, 
# MAGIC 	[OutputDWTable] varchar(200) NULL, 
# MAGIC 	[OutputDWTableWriteMode] varchar(20) NULL, 
# MAGIC 	[IngestCount] int NULL, 
# MAGIC 	[L1TransformInsertCount] int NULL, 
# MAGIC 	[L1TransformUpdateCount] int NULL, 
# MAGIC 	[L1TransformDeleteCount] int NULL, 
# MAGIC 	[L1TransformStartTimestamp] datetime2(6) NULL, 
# MAGIC 	[L1TransformEndTimestamp] datetime2(6) NULL, 
# MAGIC 	[L1TransformStatus] varchar(20) NULL, 
# MAGIC 	[RetryCount] int NULL, 
# MAGIC 	[ActiveFlag] bit NOT NULL, 
# MAGIC 	[ReRunL1TransformFlag] bit NULL, 
# MAGIC 	[IngestADFPipelineRunID] uniqueidentifier NULL, 
# MAGIC 	[L1TransformADFPipelineRunID] uniqueidentifier NULL, 
# MAGIC 	[CreatedBy] varchar(128) NOT NULL, 
# MAGIC 	[CreatedTimestamp] datetime2(6) NOT NULL, 
# MAGIC 	[ModifiedBy] varchar(128) NULL, 
# MAGIC 	[ModifiedTimestamp] datetime2(6) NULL
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Execute the following SQL commands to deploy the stored procedures:

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE PROCEDURE [ELT].[GetIngestDefinition]
# MAGIC 	@SourceSystemName varchar(20),
# MAGIC 	@StreamName VARCHAR(100) = '%', --Default =All Streams
# MAGIC 	@MaxIngestInstance INT = 10
# MAGIC 	
# MAGIC AS
# MAGIC BEGIN
# MAGIC 	
# MAGIC 		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time');
# MAGIC 
# MAGIC with 
# MAGIC 	CTE
# MAGIC as
# MAGIC 
# MAGIC (	--Normal Run
# MAGIC 			SELECT 
# MAGIC 				 [IngestID]
# MAGIC 				,[SourceSystemName]
# MAGIC 				,[StreamName]
# MAGIC 				,[Backend]
# MAGIC 				,[EntityName]
# MAGIC 				,[WatermarkColName]
# MAGIC 				
# MAGIC 				--Delta Dates
# MAGIC 				,[LastDeltaDate]
# MAGIC 				,[DataFromTimestamp] = 
# MAGIC 								CASE 
# MAGIC 									WHEN ([EntityName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL) THEN [LastDeltaDate]
# MAGIC 									ELSE CAST('1900-01-01' AS DateTime)
# MAGIC 								END
# MAGIC 				,[DataToTimestamp] = 
# MAGIC 							CASE 
# MAGIC 								WHEN ([EntityName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL AND [MaxIntervalMinutes] IS NOT NULL AND datediff_big(minute,[LastDeltaDate],@localdate) > [MaxIntervalMinutes]) THEN DateAdd(minute,[MaxIntervalMinutes],[LastDeltaDate])
# MAGIC 								WHEN ([EntityName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL AND [MaxIntervalMinutes] IS NOT NULL AND datediff_big(minute,[LastDeltaDate],@localdate) <= [MaxIntervalMinutes]) THEN CONVERT(VARCHAR(30),@localdate,120)
# MAGIC 								ELSE CONVERT(VARCHAR(30),@localdate,120) 
# MAGIC 							END
# MAGIC 
# MAGIC 				--Delta Numbers
# MAGIC 				,[LastDeltaNumber]
# MAGIC 				,[DataFromNumber] = 
# MAGIC 							CASE 
# MAGIC 								WHEN ([EntityName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL) THEN [LastDeltaNumber]
# MAGIC 					  END
# MAGIC 				,[DataToNumber] = 
# MAGIC 								CASE 
# MAGIC 									WHEN ([EntityName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL) THEN ([LastDeltaNumber] + [MaxIntervalNumber])
# MAGIC 					   END
# MAGIC 
# MAGIC 				,[DataFormat]
# MAGIC 				,[SourceStructure]
# MAGIC 				,[MaxIntervalMinutes]
# MAGIC 				,[MaxIntervalNumber]
# MAGIC 				,[DataMapping]
# MAGIC 				,[RunSequence]
# MAGIC 				,[ActiveFlag]
# MAGIC 				,[L1TransformationReqdFlag]
# MAGIC 				,[L2TransformationReqdFlag]
# MAGIC 				,[DelayL1TransformationFlag]
# MAGIC 				,[DelayL2TransformationFlag]
# MAGIC 				,[DestinationRawFileSystem]
# MAGIC 		
# MAGIC 			--Derived Fields
# MAGIC 				, [DestinationRawFolder] = 
# MAGIC 					REPLACE(REPLACE(REPLACE(REPLACE([DestinationRawFolder] COLLATE SQL_Latin1_General_CP1_CS_AS
# MAGIC 					,'YYYY',CAST(Year(COALESCE([LastDeltaDate],@localdate)) as varchar(4)))
# MAGIC 					,'MM',Right('0'+ CAST(Month(COALESCE([LastDeltaDate],@localdate)) AS varchar(2)),2))
# MAGIC 					,'DD',Right('0'+Cast(Day(COALESCE([LastDeltaDate],@localdate)) as varchar(2)),2))
# MAGIC 					,'HHMISS',
# MAGIC 						RIGHT('0' + CAST(DatePart(hh, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2) +
# MAGIC 						RIGHT('0' + CAST(DatePart(mi, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2) +
# MAGIC 						RIGHT('0' + CAST(DatePart(ss, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2)
# MAGIC 					)
# MAGIC 		
# MAGIC 				, [DestinationRawFile] = 
# MAGIC 					REPLACE(REPLACE(REPLACE(REPLACE([DestinationRawFile] COLLATE SQL_Latin1_General_CP1_CS_AS,
# MAGIC 					'YYYY', CAST(Year(COALESCE([LastDeltaDate], @localdate)) AS varchar(4))),
# MAGIC 					'MM', RIGHT('0' + CAST(Month(COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2)),
# MAGIC 					'DD', RIGHT('0' + CAST(Day(COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2)),
# MAGIC 					'HHMISS',
# MAGIC 						RIGHT('0' + CAST(DatePart(hh, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2) +
# MAGIC 						RIGHT('0' + CAST(DatePart(mi, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2) +
# MAGIC 						RIGHT('0' + CAST(DatePart(ss, COALESCE([LastDeltaDate], @localdate)) AS varchar(2)), 2)
# MAGIC 					)		
# MAGIC 
# MAGIC 
# MAGIC 			--Query
# MAGIC 				,SourceSQL = 
# MAGIC 					CASE
# MAGIC 					-- Customized for simple Purview ATLAS API
# MAGIC 					   WHEN Backend IN ('ATLAS REST API','AZURE REST API') THEN EntityName 
# MAGIC 
# MAGIC 					 --DEFAULT ANSI SQL for Delta Table
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL
# MAGIC 							THEN 
# MAGIC 								CASE 
# MAGIC 									WHEN datediff_big(minute,[LastDeltaDate],@localdate) > [MaxIntervalMinutes]
# MAGIC 										THEN 
# MAGIC 											'SELECT * FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 											+ [WatermarkColName] + ' > ' + ''''+CONVERT(VARCHAR(30),[LastDeltaDate],120) +''''+ ' AND ' + [WatermarkColName] + '<=' +  ''''+ CONVERT(VARCHAR(30), DATEADD(minute,[MaxIntervalMinutes],[LastDeltaDate]),121) +''''
# MAGIC 									ELSE 
# MAGIC 										'SELECT * FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 										+ [WatermarkColName] + ' > ' + ''''+ CONVERT(VARCHAR(30),[LastDeltaDate],120) +''''+ ' AND ' + [WatermarkColName] + '<='  + ''''+ CONVERT(VARCHAR(30), @localdate,120) +''''
# MAGIC 								END
# MAGIC 					 --DEFAULT ANSI SQL for Full Table
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NULL
# MAGIC 							THEN 
# MAGIC 								'SELECT * FROM ' + [EntityName]
# MAGIC 					--Running Number
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL
# MAGIC 							THEN 'SELECT * FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 												+ [WatermarkColName] + ' > ' + ''''+CONVERT(VARCHAR,[LastDeltaNumber]) +'''' + [WatermarkColName] + ' <= ' + ''''+CONVERT(VARCHAR,([LastDeltaNumber] + [MaxIntervalNumber])) +''''
# MAGIC 						ELSE NULL
# MAGIC 					 END
# MAGIC 			
# MAGIC 			--Stats Query
# MAGIC 				,StatSQL = 
# MAGIC 					
# MAGIC 					CASE 
# MAGIC 						-- Customized for simple Purview ATLAS API
# MAGIC 					   WHEN Backend IN ('ATLAS REST API','AZURE REST API') THEN EntityName 
# MAGIC 
# MAGIC 						--DEFAULT ANSI SQL For Delta Table
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL
# MAGIC 								THEN 
# MAGIC 									CASE 
# MAGIC 										WHEN datediff_big(minute,[LastDeltaDate],@localdate) > [MaxIntervalMinutes] 
# MAGIC 											THEN 
# MAGIC 												'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp, MAX('+[WatermarkColName]+') AS DataToTimestamp, count(1) as SourceCount FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 												+ [WatermarkColName] + ' > ' + ''''+CONVERT(varchar(30),LastDeltaDate,120)+''''+ ' AND ' + [WatermarkColName] + ' <= ' + ''''+CONVERT(varchar(30), DATEADD(minute,[MaxIntervalMinutes],[LastDeltaDate]),121)+''''
# MAGIC 										ELSE 
# MAGIC 											'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp, MAX('+[WatermarkColName]+') AS DataToTimestamp, count(1) as SourceCount FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 											+ [WatermarkColName] + ' > ' + ''''+CONVERT(varchar(30),[LastDeltaDate],120) +''''+ ' AND ' + [WatermarkColName] + ' <= ' + ''''+ CONVERT(varchar(30),(@localdate),120)+''''
# MAGIC 										END
# MAGIC 						--Common No Delta
# MAGIC 							WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NULL
# MAGIC 								THEN 'SELECT ''1900-01-01 00:00:00'' AS DataFromTimestamp, ''' + CONVERT(VARCHAR(30),CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time'),120) + ''' AS DataToTimestamp,  COUNT(*) AS SourceCount FROM ' + [EntityName]
# MAGIC 						--Running Number
# MAGIC 							WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL
# MAGIC 									THEN 'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp,' + ' MAX('+[WatermarkColName]+') AS DataToTimestamp,'+ 'COUNT(*) AS SourceCount FROM ' + [EntityName]
# MAGIC 													+ [WatermarkColName] + ' > ' + ''''+CONVERT(VARCHAR,[LastDeltaNumber])+'''' + ' AND ' + [WatermarkColName] + ' <= ' + ''''+CONVERT(VARCHAR,([LastDeltaNumber] + [MaxIntervalNumber]))+''''
# MAGIC 							ELSE NULL
# MAGIC 					 END
# MAGIC 
# MAGIC 				, CAST(0 AS BIT) AS [ReloadFlag]
# MAGIC 				, NULL AS [ADFPipelineRunID]
# MAGIC 				, [BronzeLakehouseID]
# MAGIC 				, [WH_Control_Conn_String]
# MAGIC 				, [Source_Connection_ID] 
# MAGIC 				, [Source_Database_Name] 
# MAGIC 			FROM 
# MAGIC 				[ELT].[IngestDefinition]
# MAGIC 			WHERE 
# MAGIC 				[SourceSystemName]=@SourceSystemName
# MAGIC 				AND [StreamName] LIKE COALESCE(@StreamName, [StreamName])
# MAGIC 				AND [ActiveFlag]=1
# MAGIC 
# MAGIC --ReRun
# MAGIC UNION
# MAGIC 		SELECT 
# MAGIC 				[ID].[IngestID]
# MAGIC 				,[SourceSystemName]
# MAGIC 				,[StreamName]
# MAGIC 				,[Backend]
# MAGIC 				,[EntityName]
# MAGIC 				,[WatermarkColName]
# MAGIC 				,[LastDeltaDate]
# MAGIC 				,II.[DataFromTimestamp]
# MAGIC 				,II.[DataToTimestamp]
# MAGIC 				,ID.[LastDeltaNumber]
# MAGIC 				,II.[DataFromNumber]
# MAGIC 				,II.[DataToNumber]
# MAGIC 				,[DataFormat]
# MAGIC 				,[SourceStructure]
# MAGIC 				,ID.[MaxIntervalMinutes]
# MAGIC 				,ID.[MaxIntervalNumber]
# MAGIC 				,ID.[DataMapping]
# MAGIC 				,ID.[RunSequence]
# MAGIC 				,[ActiveFlag]
# MAGIC 				,[L1TransformationReqdFlag]
# MAGIC 				,[L2TransformationReqdFlag]
# MAGIC 				,[DelayL1TransformationFlag]
# MAGIC 				,[DelayL2TransformationFlag]
# MAGIC 				,II.[DestinationRawFileSystem]
# MAGIC 				,II.[DestinationRawFolder]
# MAGIC 				,II.[DestinationRawFile] 		
# MAGIC 			
# MAGIC 				--Derived Fields
# MAGIC 				,SourceSQL = 
# MAGIC 					CASE
# MAGIC 						-- Customized for simple Purview ATLAS API
# MAGIC 					    WHEN Backend IN ('ATLAS REST API','AZURE REST API') THEN EntityName 
# MAGIC 						--DEFAULT ANSI SQL for Delta Table
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL 
# MAGIC 							THEN 'SELECT * FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 									+ [WatermarkColName] + ' > ' + ''''+ CONVERT(varchar(30),II.[DataFromTimestamp],121)+''''+ ' AND ' + [WatermarkColName] + ' <= ' + ''''+ CONVERT(varchar(30),II.[DataToTimestamp],121)+''''
# MAGIC 						--Common No Delta
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NULL
# MAGIC 							THEN 'SELECT * FROM ' + [EntityName]
# MAGIC 						--Running Number
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL
# MAGIC 								THEN 'SELECT * FROM ' + [EntityName] + ' WHERE ' 
# MAGIC 												+ [WatermarkColName] + ' > ' + ''''+CONVERT(VARCHAR,II.[DataFromNumber])+'''' + ' AND ' + [WatermarkColName] + ' <= ' + ''''+CONVERT(VARCHAR,II.[DataToNumber])+''''
# MAGIC 						
# MAGIC 						ELSE NULL
# MAGIC 					END
# MAGIC 
# MAGIC 				,StatSQL = 
# MAGIC 					CASE 
# MAGIC 					-- Customized for simple Purview ATLAS API
# MAGIC 					   WHEN Backend IN ('ATLAS REST API','AZURE REST API') THEN EntityName 
# MAGIC 
# MAGIC 					--DEFAULT ANSI SQL for Delta Table
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaDate] IS NOT NULL THEN
# MAGIC 									'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp, MAX('+[WatermarkColName]+') AS DataToTimestamp, count(1) as SourceCount FROM ' 
# MAGIC 									+ [EntityName] + ' WHERE ' + [WatermarkColName] + '>' + ''''+CONVERT(varchar(30),II.DataFromTimestamp,121)+''''+ ' AND ' + [WatermarkColName] + '<='+ ''''+CONVERT(varchar(30),II.[DataToTimestamp],121)+''''
# MAGIC 					--Common No Delta
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NULL AND [LastDeltaDate] IS NOT NULL 
# MAGIC 							THEN 'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp,' + ' MAX('+[WatermarkColName]+') AS DataToTimestamp,'+ 'COUNT(*) AS SourceCount FROM ' + [EntityName]
# MAGIC 					--Common No Delta
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NULL
# MAGIC 							THEN 'SELECT SELECT ''1900-01-01 00:00:00'' AS DataFromTimestamp, ''' + CONVERT(VARCHAR(30),CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time'),120) + ''' AS DataToTimestamp, COUNT(*) AS SourceCount FROM ' + [EntityName]
# MAGIC 					--Running Number
# MAGIC 						WHEN [EntityName] IS NOT NULL AND [WatermarkColName] IS NOT NULL AND [LastDeltaNumber] IS NOT NULL
# MAGIC 								THEN 'SELECT MIN('+[WatermarkColName]+') AS DataFromTimestamp,' + ' MAX('+[WatermarkColName]+') AS DataToTimestamp,'+ 'COUNT(*) AS SourceCount FROM ' + [EntityName]
# MAGIC 												+ [WatermarkColName] + ' > ' + ''''+CONVERT(VARCHAR,II.[DataFromNumber]) +'''' + ' AND ' + [WatermarkColName] + ' <= ' + ''''+CONVERT(VARCHAR,II.[DataToNumber])+''''
# MAGIC 						ELSE NULL 	
# MAGIC 					END
# MAGIC 
# MAGIC 				, II.[ReloadFlag]
# MAGIC 				, II.[ADFIngestPipelineRunID]
# MAGIC 				, ID.[BronzeLakehouseID]
# MAGIC 				, ID.[WH_Control_Conn_String]
# MAGIC 				, ID.[Source_Connection_ID] 
# MAGIC 				, ID.[Source_Database_Name] 
# MAGIC 			FROM 
# MAGIC 				[ELT].[IngestDefinition] ID
# MAGIC 					INNER JOIN [ELT].[IngestInstance] AS II
# MAGIC 						ON II.[IngestID]= ID.[IngestID] 
# MAGIC 						AND II.[ReloadFlag]=1
# MAGIC 						AND (II.[IngestStatus] is NULL OR II.[IngestStatus] != 'Running')  --Fetch new instances and ignore instances that are currently running
# MAGIC 			WHERE 
# MAGIC 				ID.[SourceSystemName]=@SourceSystemName
# MAGIC 				AND ID.[StreamName] LIKE COALESCE(@StreamName, [StreamName])
# MAGIC 				AND ID.[ActiveFlag]=1 
# MAGIC 				AND ISNULL(II.RetryCount,0) <= ID.MaxRetries
# MAGIC 				
# MAGIC 	)
# MAGIC 	SELECT 
# MAGIC 		TOP (@MaxIngestInstance) *  
# MAGIC 	FROM CTE
# MAGIC 	ORDER BY 
# MAGIC 		[RunSequence] ASC, [DataFromTimestamp] DESC, [DataToTimestamp] DESC
# MAGIC 
# MAGIC END;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE PROCEDURE [ELT].[GetTransformDefinition_L1] 
# MAGIC 		@IngestID int, 
# MAGIC 		@DeltaDate datetime = null 			
# MAGIC AS
# MAGIC BEGIN
# MAGIC 	--declare @IngestID int 
# MAGIC 	DECLARE @localdate datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) AT TIME ZONE 'AUS Eastern Standard Time')
# MAGIC 	DECLARE @CuratedDate datetime
# MAGIC 	SET @CuratedDate = COALESCE(@DeltaDate,@localdate)
# MAGIC 
# MAGIC 
# MAGIC 		SELECT 
# MAGIC 			--PK/FK
# MAGIC 			TD.[L1TransformID]
# MAGIC 			, TD.[IngestID]
# MAGIC 
# MAGIC 			
# MAGIC 			--Databricks
# MAGIC 			, TD.[ComputeName]
# MAGIC 			, TD.[ComputePath]
# MAGIC 			
# MAGIC 			--Custom
# MAGIC 			, TD.[CustomParameters]
# MAGIC 
# MAGIC 			 --Raw
# MAGIC 			 ,TD.[InputRawFileDelimiter]
# MAGIC 			 ,TD.[InputFileHeaderFlag]
# MAGIC 			
# MAGIC 			--Curated File
# MAGIC 			--Need to test how this performs with a file drop and filedrop reload.
# MAGIC 			--Possibly use date drop file was dropped?
# MAGIC 			,TD.[OutputL1CurateFileSystem]
# MAGIC 			
# MAGIC 			,[OutputL1CuratedFolder] = 
# MAGIC 					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TD.[OutputL1CuratedFolder]  COLLATE SQL_Latin1_General_CP1_CS_AS
# MAGIC 					,'YYYY',CAST(Year(@CuratedDate) as varchar(4)))
# MAGIC 					,'MM',Right('0'+ CAST(Month(@CuratedDate) AS varchar(2)),2))
# MAGIC 					,'DD',Right('0'+Cast(Day(@CuratedDate) as varchar(2)),2))
# MAGIC 					,'HH',Right('0'+ CAST(DatePart(hh,@CuratedDate) as varchar(2)),2))
# MAGIC 					,'MI',Right('0'+ CAST(DatePart(mi,@CuratedDate) as varchar(2)),2))
# MAGIC 					,'SS',Right('0'+ CAST(DatePart(ss,@CuratedDate) as varchar(2)),2))
# MAGIC 
# MAGIC 			,[OutputL1CuratedFile] = 
# MAGIC 					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TD.[OutputL1CuratedFile] COLLATE SQL_Latin1_General_CP1_CS_AS
# MAGIC 					,'YYYY',CAST(Year(@CuratedDate) as varchar(4)))
# MAGIC 					,'MM',Right('0'+ CAST(Month(@CuratedDate) AS varchar(2)),2))
# MAGIC 					,'DD',Right('0'+Cast(Day(@CuratedDate) as varchar(2)),2))
# MAGIC 					,'HH',Right('0'+ CAST(DatePart(hh,@CuratedDate) as varchar(2)),2))
# MAGIC 					,'MI',Right('0'+ CAST(DatePart(mi,@CuratedDate) as varchar(2)),2))
# MAGIC 					,'SS',Right('0'+ CAST(DatePart(ss,@CuratedDate) as varchar(2)),2))
# MAGIC 
# MAGIC 			, TD.[OutputL1CuratedFileDelimiter]
# MAGIC 			, TD.[OutputL1CuratedFileFormat]
# MAGIC 			, TD.[OutputL1CuratedFileWriteMode]
# MAGIC 
# MAGIC 			--SQL
# MAGIC 			, [LookupColumns]
# MAGIC 			, TD.[OutputDWStagingTable]
# MAGIC 			, TD.[OutputDWTable]
# MAGIC 			, TD.[OutputDWTableWriteMode]
# MAGIC 
# MAGIC 			--Max Retries
# MAGIC 			, TD.[MaxRetries]
# MAGIC 			
# MAGIC 
# MAGIC 		FROM
# MAGIC 			[ELT].[L1TransformDefinition] TD
# MAGIC 				LEFT JOIN [ELT].[IngestDefinition] ID
# MAGIC 					ON TD.[IngestID] = ID.[IngestID]
# MAGIC 		WHERE 
# MAGIC 			TD.[IngestID] = @IngestID and 
# MAGIC 			TD.[ActiveFlag] = 1 
# MAGIC 			and ID.[ActiveFlag] = 1 
# MAGIC 			and ID.[L1TransformationReqdFlag] =1
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- GetTransformInstance_L1
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[GetTransformInstance_L1]
# MAGIC 	@SourceSystemName varchar(20),
# MAGIC 	@StreamName varchar(100) = '%',
# MAGIC 	@MaxTransformInstance int = 10,
# MAGIC 	@L1TransformInstanceId varchar(255) = NULL, --To fetch all transform instances set Parameter as NULL otherwise provide a specific instance id
# MAGIC 	@DelayL1TransformationFlag INT=NULL --Pass @DelayL1TransformationFlag=0 to fetch all instances that needs to be transformed in the current pipeline (usually the ingestion pipeline). Pass @DelayL1TransformationFlag=1 to fetch all transformations that are scheduled for a later time.
# MAGIC AS
# MAGIC begin
# MAGIC 	--Limit Number of Transform Instances to prevent queuing at DWH
# MAGIC     SELECT top (@MaxTransformInstance) 
# MAGIC         L1TI.[L1TransformInstanceID]
# MAGIC         , L1TI.[L1TransformID]
# MAGIC         , L1TI.[IngestID]	
# MAGIC         , L1TI.[ComputeName]
# MAGIC         , L1TI.[ComputePath]
# MAGIC         , L1TI.[CustomParameters]
# MAGIC         , L1TI.[InputRawFileSystem]
# MAGIC         , L1TI.[InputRawFileFolder]
# MAGIC         , L1TI.[InputRawFile]
# MAGIC         , L1TI.[InputRawFileDelimiter]
# MAGIC         , L1TI.[InputFileHeaderFlag]
# MAGIC         , L1TI.[OutputL1CurateFileSystem]
# MAGIC         , L1TI.[OutputL1CuratedFolder]
# MAGIC         , L1TI.[OutputL1CuratedFile]
# MAGIC         , L1TI.[OutputL1CuratedFileDelimiter]
# MAGIC         , L1TI.[OutputL1CuratedFileFormat]
# MAGIC         , L1TI.[OutputL1CuratedFileWriteMode]
# MAGIC         , L1TI.[OutputDWStagingTable]
# MAGIC         , L1TI.[LookupColumns]
# MAGIC         , L1TI.[OutputDWTable]
# MAGIC         , L1TI.[OutputDWTableWriteMode]
# MAGIC         , L1TI.[ReRunL1TransformFlag]
# MAGIC         , L1TD.[MaxRetries]
# MAGIC         , L1TD.[WatermarkColName]
# MAGIC         
# MAGIC     FROM 
# MAGIC         [ELT].[L1TransformInstance] as L1TI
# MAGIC             LEFT JOIN [ELT].[L1TransformDefinition] as L1TD
# MAGIC                 ON L1TI.[L1TransformID] = L1TD.[L1TransformID]
# MAGIC             LEFT JOIN [ELT].[IngestDefinition] as ID
# MAGIC                 ON ID.[IngestID]= L1TD.[IngestID]		
# MAGIC     WHERE 
# MAGIC         ID.[SourceSystemName] =@SourceSystemName
# MAGIC         AND ID.[StreamName] like COALESCE(@StreamName,ID.[StreamName])
# MAGIC         AND (ID.ActiveFlag = 1 AND L1TD.ActiveFlag = 1)
# MAGIC         AND (L1TI.[ActiveFlag]=1 OR L1TI.[ReRunL1TransformFlag]=1)
# MAGIC         AND L1TI.[L1TransformInstanceID] = COALESCE(@L1TransformInstanceId, L1TI.[L1TransformInstanceID])
# MAGIC         AND (L1TI.[L1TransformStatus] IS NULL OR L1TI.[L1TransformStatus] NOT IN ('Running','DWUpload'))  --Fetch new instances and ignore instances that are currently running
# MAGIC         AND ID.[DelayL1TransformationFlag] = COALESCE(@DelayL1TransformationFlag,ID.[DelayL1TransformationFlag])
# MAGIC         AND ISNULL(L1TI.RetryCount,0) <= L1TD.MaxRetries
# MAGIC     ORDER BY L1TI.[L1TransformInstanceID] ASC
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- InsertIngestInstance
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[InsertIngestInstance]
# MAGIC 	@IngestID INT 
# MAGIC 	,@SourceFileDropFileSystem varchar(50)=null 
# MAGIC 	,@SourceFileDropFolder varchar(200)=null
# MAGIC 	,@SourceFileDropFile varchar(200)=null
# MAGIC 	,@DestinationRawFileSystem varchar(50)=null 
# MAGIC 	,@DestinationRawFolder varchar(200)=null
# MAGIC 	,@DestinationRawFile varchar(200)=null
# MAGIC 	,@ReloadFlag bit =0
# MAGIC 	,@ADFPipelineRunID UniqueIdentifier=null
# MAGIC AS
# MAGIC 
# MAGIC BEGIN
# MAGIC 
# MAGIC DECLARE @localdate as datetime = CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time');
# MAGIC 
# MAGIC DECLARE @localdateStr as varchar(255) = CONVERT(VARCHAR(30), GETDATE(), 121);
# MAGIC 
# MAGIC DECLARE @seed as varchar(255)  = CAST(@IngestID AS VARCHAR) + @localdateStr;
# MAGIC 	
# MAGIC DECLARE @primary_key AS varchar(255) = CONVERT(VARCHAR(255), HASHBYTES('SHA2_256', @seed), 2);
# MAGIC 
# MAGIC DECLARE @MaxID AS BIGINT
# MAGIC 
# MAGIC --	IF EXISTS(SELECT * FROM [ELT].[IngestInstance])
# MAGIC --		SET @MaxID = (SELECT MAX([IngestInstanceID]) FROM [ELT].[IngestInstance]);
# MAGIC --	ELSE
# MAGIC --		SET @MaxID = 0;
# MAGIC 
# MAGIC 
# MAGIC 	--NOTE:Potential enhancements for lookup.
# MAGIC 	-- 1. Job to Purge the Instance Tables to Blob after 3 months
# MAGIC 	-- 2. Use the AdfPipeline Instance Run ID for the lookup, though this needs to be the individual. Lookup Including Index on the lookup columns.
# MAGIC 	-- 3. Create a Hash Column on DestinationFileSystem/Folder/File and use as the lookup. Including Index on the lookup columns.
# MAGIC 
# MAGIC 	--Normal Run
# MAGIC 	IF (@ReloadFlag=0 AND NOT EXISTS (
# MAGIC 										SELECT 1 
# MAGIC 										FROM [ELT].[IngestInstance] 
# MAGIC 										WHERE [DestinationRawFileSystem] = @DestinationRawFileSystem
# MAGIC 										AND [DestinationRawFolder] = @DestinationRawFolder
# MAGIC 										AND [DestinationRawFile] = @DestinationRawFile
# MAGIC 									)
# MAGIC 		)
# MAGIC 	BEGIN
# MAGIC 
# MAGIC 		INSERT INTO [ELT].[IngestInstance]
# MAGIC 				   (IngestInstanceID
# MAGIC 				   ,[IngestID]
# MAGIC 				   ,[SourceFileDropFileSystem]
# MAGIC 				   ,[SourceFileDropFolder]
# MAGIC 				   ,[SourceFileDropFile]
# MAGIC 				   ,[DestinationRawFileSystem]
# MAGIC 				   ,[DestinationRawFolder]
# MAGIC 				   ,[DestinationRawFile]
# MAGIC 				   ,[IngestStartTimestamp]
# MAGIC 				   ,[IngestEndTimestamp]
# MAGIC 				   ,[IngestStatus]
# MAGIC 				   ,[RetryCount]
# MAGIC 				   ,[ReloadFlag]
# MAGIC 				   ,[CreatedBy]
# MAGIC 				   ,[CreatedTimestamp]
# MAGIC 				   ,[ModifiedBy]
# MAGIC 				   ,[ModifiedTimestamp]
# MAGIC 				   ,ADFIngestPipelineRunID)
# MAGIC 			 VALUES
# MAGIC 				   (@primary_key
# MAGIC 				   ,@IngestID
# MAGIC 				   ,@SourceFileDropFileSystem
# MAGIC 				   ,@SourceFileDropFolder
# MAGIC 				   ,@SourceFileDropFile
# MAGIC 				   ,@DestinationRawFileSystem
# MAGIC 				   ,@DestinationRawFolder
# MAGIC 				   ,@DestinationRawFile
# MAGIC 				   ,@localdate
# MAGIC 				   ,NULL
# MAGIC 				   ,'Running'
# MAGIC 				   ,0
# MAGIC                    ,0
# MAGIC 				   ,suser_sname()
# MAGIC 				   ,@localdate
# MAGIC 				   ,NULL
# MAGIC 				   ,NULL
# MAGIC 				   ,@ADFPipelineRunID)
# MAGIC 	END
# MAGIC 
# MAGIC 	--Re-load
# MAGIC 	IF (@ReloadFlag=1 
# MAGIC 		OR EXISTS (SELECT 1 FROM [ELT].[IngestInstance] 
# MAGIC 					WHERE [DestinationRawFileSystem] = @DestinationRawFileSystem
# MAGIC 						AND [DestinationRawFolder] = @DestinationRawFolder
# MAGIC 						AND [DestinationRawFile] = @DestinationRawFile)
# MAGIC 		)
# MAGIC 	BEGIN
# MAGIC 		Update [ELT].[IngestInstance]
# MAGIC 		SET [IngestStartTimestamp] = @localdate
# MAGIC 			,[IngestEndTimestamp] = NULL
# MAGIC 			,[SourceCount]=NULL
# MAGIC 			,[IngestCount]=NULL
# MAGIC 			,[IngestStatus]='Running'
# MAGIC 			,[ModifiedBy]=suser_sname()
# MAGIC 			,[ModifiedTimestamp] = @localdate
# MAGIC 			,ADFIngestPipelineRunID = @ADFPipelineRunID
# MAGIC 		--Unique Keys
# MAGIC 		WHERE [DestinationRawFileSystem] = @DestinationRawFileSystem
# MAGIC 		AND [DestinationRawFolder] = @DestinationRawFolder
# MAGIC 		AND [DestinationRawFile] = @DestinationRawFile
# MAGIC 	END
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- InsertTransformInstance_L1
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[InsertTransformInstance_L1]
# MAGIC 	--PK/FK
# MAGIC 	@L1TransformID int = null,
# MAGIC 	@IngestInstanceID varchar(255)=null,
# MAGIC 	@IngestID int,
# MAGIC 
# MAGIC 	--Databricks Notebook
# MAGIC 	@ComputeName varchar(100) = null,
# MAGIC 	@ComputePath varchar(200) = null,
# MAGIC 	
# MAGIC 	--Custom
# MAGIC 	@CustomParameters varchar(max) = null,
# MAGIC 
# MAGIC 	--Input File
# MAGIC 	@InputRawFileSystem varchar(50) = null,
# MAGIC     @InputRawFileFolder varchar(200) = null,
# MAGIC     @InputRawFile varchar(200) = null,
# MAGIC     @InputRawFileDelimiter char(1) = null,
# MAGIC 	@InputFileHeaderFlag bit = null,
# MAGIC 	
# MAGIC 	--Curated File 
# MAGIC 	@OutputL1CurateFileSystem varchar(50) = null,
# MAGIC     @OutputL1CuratedFolder varchar(200) = null,
# MAGIC     @OutputL1CuratedFile varchar(200) = null,
# MAGIC     @OutputL1CuratedFileDelimiter char(1) = null,
# MAGIC     @OutputL1CuratedFileFormat varchar(10) = null,
# MAGIC     @OutputL1CuratedFileWriteMode varchar(20) = null,
# MAGIC     
# MAGIC 	--SQL
# MAGIC 	@OutputDWStagingTable varchar(200) = null,
# MAGIC 	@LookupColumns varchar(4000) = null,
# MAGIC     @OutputDWTable varchar(200) = null,
# MAGIC     @OutputDWTableWriteMode varchar(20) = null,
# MAGIC     @IngestCount int = null,
# MAGIC 
# MAGIC 	--ADF Pipeline IDs
# MAGIC 	@IngestADFPipelineRunID  uniqueidentifier = null
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC AS
# MAGIC BEGIN
# MAGIC 
# MAGIC 
# MAGIC DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time');
# MAGIC 
# MAGIC DECLARE @localdateStr as varchar(255) = CONVERT(VARCHAR(30), GETDATE(), 121);
# MAGIC 
# MAGIC DECLARE @seed as varchar(255)  = CAST(@IngestID AS VARCHAR) + @localdateStr;
# MAGIC 	
# MAGIC DECLARE @primary_key AS varchar(255) = CONVERT(VARCHAR(255), HASHBYTES('SHA2_256', @seed), 2);
# MAGIC 
# MAGIC 	--Check if Transformation records already exists for the input file for same transformation e.g it's a reload
# MAGIC 		IF NOT EXISTS 
# MAGIC 			(
# MAGIC 				SELECT 1 
# MAGIC 				FROM 
# MAGIC 					[ELT].[L1TransformInstance]
# MAGIC 				WHERE 
# MAGIC 					[IngestID] = @IngestID
# MAGIC 					AND L1TransformID = @L1TransformID
# MAGIC 					AND InputRawFileSystem = @InputRawFileSystem
# MAGIC 					AND InputRawFileFolder = @InputRawFileFolder
# MAGIC 					AND InputRawFile = @InputRawFile
# MAGIC 	
# MAGIC 			)
# MAGIC 
# MAGIC 
# MAGIC 	BEGIN
# MAGIC 	--If this is a new transformation
# MAGIC 		INSERT INTO [ELT].[L1TransformInstance]
# MAGIC 			(
# MAGIC 				[L1TransformInstanceID]
# MAGIC 				,[L1TransformID]
# MAGIC 				,[IngestInstanceID]
# MAGIC 				,[IngestID]
# MAGIC 				,[ComputeName]
# MAGIC 				,[ComputePath]
# MAGIC 				,[CustomParameters]
# MAGIC 				,[InputRawFileSystem]
# MAGIC 				,[InputRawFileFolder]
# MAGIC 				,[InputRawFile]
# MAGIC 				,[InputRawFileDelimiter]
# MAGIC 				,[InputFileHeaderFlag]
# MAGIC 				,[OutputL1CurateFileSystem]
# MAGIC 				,[OutputL1CuratedFolder]
# MAGIC 				,[OutputL1CuratedFile]
# MAGIC 				,[OutputL1CuratedFileDelimiter]
# MAGIC 				,[OutputL1CuratedFileFormat]
# MAGIC 				,[OutputL1CuratedFileWriteMode]
# MAGIC 				,[OutputDWStagingTable]
# MAGIC 				,[LookupColumns]
# MAGIC 				,[OutputDWTable]
# MAGIC 				,[OutputDWTableWriteMode]
# MAGIC 				,[RetryCount]
# MAGIC 				,[ActiveFlag]
# MAGIC 				,[IngestCount]
# MAGIC 				,[IngestADFPipelineRunID]
# MAGIC 				,[CreatedBy]
# MAGIC 				,[CreatedTimestamp]
# MAGIC 			   
# MAGIC 			)
# MAGIC 		VALUES
# MAGIC 			(
# MAGIC 				@primary_key
# MAGIC 				,@L1TransformID
# MAGIC 				,@IngestInstanceID
# MAGIC 				,@IngestID
# MAGIC 				,@ComputeName
# MAGIC 				,@ComputePath
# MAGIC 				,@CustomParameters
# MAGIC 				,@InputRawFileSystem
# MAGIC 				,@InputRawFileFolder
# MAGIC 				,@InputRawFile
# MAGIC 				,@InputRawFileDelimiter
# MAGIC 				,@InputFileHeaderFlag
# MAGIC 				,@OutputL1CurateFileSystem
# MAGIC 				,@OutputL1CuratedFolder
# MAGIC 				,@OutputL1CuratedFile
# MAGIC 				,@OutputL1CuratedFileDelimiter
# MAGIC 				,@OutputL1CuratedFileFormat
# MAGIC 				,@OutputL1CuratedFileWriteMode
# MAGIC 				,@OutputDWStagingTable
# MAGIC 				,@LookupColumns
# MAGIC 				,@OutputDWTable
# MAGIC 				,@OutputDWTableWriteMode
# MAGIC 				,0
# MAGIC 				,1
# MAGIC 				,@IngestCount
# MAGIC 				,@IngestADFPipelineRunID
# MAGIC 				,SUSER_SNAME()
# MAGIC 				,@localdate
# MAGIC 		)
# MAGIC 		END
# MAGIC 	ELSE
# MAGIC 		--If this is an existing Transformation
# MAGIC 		BEGIN
# MAGIC 			--Just update one record in case if there are duplicates
# MAGIC 			UPDATE [ELT].[L1TransformInstance]
# MAGIC 			SET 
# MAGIC 				
# MAGIC 				[IngestCount] = null
# MAGIC 				,[L1TransformInsertCount] = null
# MAGIC 				,[L1TransformUpdateCount] = null
# MAGIC 				,[L1TransformDeleteCount] = null
# MAGIC 				,L1TransformStartTimestamp = null
# MAGIC 				,[L1TransformEndTimestamp] = null
# MAGIC 				,[L1TransformStatus] = null
# MAGIC 				,[RetryCount] = 0
# MAGIC 				,[ActiveFlag] = 1
# MAGIC 				,[ReRunL1TransformFlag] = 1
# MAGIC 				,[IngestADFPipelineRunID] = @IngestADFPipelineRunID
# MAGIC 				,[L1TransformADFPipelineRunID] = null
# MAGIC 				,[ModifiedBy] = suser_sname()
# MAGIC 				,[ModifiedTimestamp] = @localdate
# MAGIC 				
# MAGIC 		WHERE 
# MAGIC 			[IngestID] = @IngestID
# MAGIC 			AND L1TransformID = @L1TransformID
# MAGIC 			AND InputRawFileSystem = @InputRawFileSystem
# MAGIC 			AND InputRawFileFolder = @InputRawFileFolder
# MAGIC 			AND InputRawFile = @InputRawFile
# MAGIC 		END
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- UpdateIngestDefinition
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[UpdateIngestDefinition]
# MAGIC 	@IngestID INT,
# MAGIC 	@LastDeltaDate Datetime2=null,
# MAGIC 	@LastDeltaNumer int=null,
# MAGIC 	@IngestStatus varchar(20),
# MAGIC 	@ReloadFlag bit=0
# MAGIC AS
# MAGIC BEGIN
# MAGIC 
# MAGIC 		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
# MAGIC 
# MAGIC 		Update 
# MAGIC 			[ELT].[IngestDefinition]
# MAGIC 		SET 
# MAGIC 			[LastDeltaDate] =
# MAGIC 							CASE
# MAGIC 								--When Successful and the DataToDate does not move forward since LastDeltaDate, Increase LastDeltaDate by the Interval
# MAGIC 								WHEN @LastDeltaDate IS NOT NULL AND @ReloadFlag <> 1 AND @IngestStatus IN ('Success','ReRunSuccess') AND @LastDeltaDate = [LastDeltaDate] 
# MAGIC 									and [MaxIntervalMinutes] is NOT NULL
# MAGIC 									THEN 
# MAGIC 										CASE 
# MAGIC 											WHEN 
# MAGIC 												DateAdd(minute,[MaxIntervalMinutes],@LastDeltaDate) > CONVERT(datetime2,CONVERT(datetimeoffset, getdate()) AT TIME ZONE 'AUS Eastern Standard Time')
# MAGIC 													THEN CONVERT(VARCHAR(30),CONVERT(datetime2,CONVERT(datetimeoffset, getdate()) AT TIME ZONE 'AUS Eastern Standard Time'),120)
# MAGIC 											ELSE
# MAGIC 												DateAdd(minute,[MaxIntervalMinutes],[LastDeltaDate])
# MAGIC 										END
# MAGIC 								--Re-run delta date is later than existing delta date
# MAGIC 								WHEN @LastDeltaDate IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess') AND datediff_big(ss,[LastDeltaDate],@LastDeltaDate) >= 0 
# MAGIC 									THEN @LastDeltaDate
# MAGIC 								--Re-run delta date is earlier than existing delta date
# MAGIC 								WHEN @LastDeltaDate IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess')  AND datediff_big(ss,@LastDeltaDate,[LastDeltaDate]) >=0 
# MAGIC 									THEN [LastDeltaDate]
# MAGIC 								ELSE [LastDeltaDate]
# MAGIC 							END
# MAGIC 			, [LastDeltaNumber] = 
# MAGIC 							CASE
# MAGIC 								WHEN @LastDeltaNumer IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess') 
# MAGIC 									THEN @LastDeltaNumer
# MAGIC 								WHEN @LastDeltaNumer IS NOT NULL AND @IngestStatus IN ('Failure','ReRunFailure') 
# MAGIC 									THEN [LastDeltaNumber]
# MAGIC 								WHEN @LastDeltaNumer IS NULL AND @ReloadFlag <> 1 AND @IngestStatus IN ('Success','ReRunSuccess')  
# MAGIC 									THEN ([LastDeltaNumber] + [MaxIntervalNumber])
# MAGIC 								ELSE [LastDeltaNumber]
# MAGIC 							END
# MAGIC 			,[ModifiedBy] =suser_sname()
# MAGIC 			, [ModifiedTimestamp]=@localdate
# MAGIC 	WHERE [IngestID]=@IngestID
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- UpdateIngestInstance
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[UpdateIngestInstance]
# MAGIC 	@ADFIngestPipelineRunID Uniqueidentifier,
# MAGIC 	@IngestStatus varchar(20) =null,
# MAGIC 	@DataFromTimestamp Datetime2 =null,
# MAGIC 	@DataToTimestamp Datetime2 =null,
# MAGIC 	@DataFromNumber int =null,
# MAGIC 	@DataToNumber int =null,
# MAGIC 	@SourceCount int=null,
# MAGIC 	@IngestCount int=null,
# MAGIC 	@ReloadFlag bit
# MAGIC AS
# MAGIC BEGIN
# MAGIC 
# MAGIC 		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
# MAGIC 
# MAGIC 	Update 
# MAGIC 		[ELT].[IngestInstance]
# MAGIC 	SET 
# MAGIC 		[DataFromTimestamp] = @DataFromTimestamp
# MAGIC 		,[DataToTimestamp]=@DataToTimestamp
# MAGIC 		,[DataFromNumber] = @DataFromNumber
# MAGIC 		,[DataToNumber]=@DataToNumber
# MAGIC 		,[SourceCount] =@SourceCount
# MAGIC 		,[IngestCount]=@IngestCount
# MAGIC 		,[IngestEndTimestamp] =@localdate
# MAGIC 		,[IngestStatus] =(CASE WHEN @ReloadFlag=1 AND @IngestStatus='Success' THEN 'ReRunSuccess'
# MAGIC 							WHEN   @ReloadFlag=1 AND @IngestStatus='Failure' THEN 'ReRunFailure'
# MAGIC 							ELSE @IngestStatus
# MAGIC 						END)
# MAGIC 		,[RetryCount] = (CASE WHEN @IngestStatus  IN ('Success','ReRunSuccess') THEN 0
# MAGIC 							WHEN @IngestStatus IN ('Failure','ReRunFailure')  THEN ISNULL([RetryCount],0) +1
# MAGIC 						END)
# MAGIC 		,[ReloadFlag] =(CASE WHEN @ReloadFlag=1 AND @IngestStatus IN ('Success','ReRunSuccess') THEN 0
# MAGIC 							WHEN @ReloadFlag=1 AND @IngestStatus IN ('Failure','ReRunFailure') THEN 1
# MAGIC 							ELSE 0
# MAGIC 						 END)
# MAGIC 		,[ModifiedBy]=suser_sname()
# MAGIC 		,[ModifiedTimestamp] = @localdate
# MAGIC 	WHERE
# MAGIC 		ADFIngestPipelineRunID =@ADFIngestPipelineRunID
# MAGIC END;
# MAGIC 
# MAGIC GO;
# MAGIC 
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC -- UpdateIngestInstance
# MAGIC ----------------------------------------------------------------------------------------------------------------
# MAGIC CREATE PROCEDURE [ELT].[UpdateTransformInstance_L1]
# MAGIC 	@L1TransformInstanceId VARCHAR(255)
# MAGIC    , @L1TransformStatus VARCHAR(20)
# MAGIC    , @L1TransformADFPipelineRunID UNIQUEIDENTIFIER
# MAGIC    , @IngestCount INT = NULL
# MAGIC    , @L1TransformInsertCount INT = NULL
# MAGIC    , @L1TransformUpdateCount INT = NULL
# MAGIC    , @L1TransformDeleteCount INT = NULL
# MAGIC    , @MaxRetries int = null
# MAGIC AS
# MAGIC BEGIN
# MAGIC 	
# MAGIC 	DECLARE @localdate datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
# MAGIC 
# MAGIC 		Update 
# MAGIC 			[ELT].[L1TransformInstance]
# MAGIC 		SET 
# MAGIC 			[L1TransformStartTimestamp] = CASE 
# MAGIC 											WHEN @L1TransformStatus IN ('Running') 
# MAGIC 												THEN @localdate 
# MAGIC 											ELSE [L1TransformStartTimestamp] 
# MAGIC 										END
# MAGIC 
# MAGIC 			, [L1TransformEndTimestamp] = CASE
# MAGIC 												WHEN @L1TransformStatus IN ('Success','Failure','ReRunSuccess','ReRunFailure') 
# MAGIC 													THEN @localdate 
# MAGIC 												ELSE NULL 
# MAGIC 											END
# MAGIC 										
# MAGIC 			, [L1TransformStatus] = CASE 
# MAGIC 										WHEN ([ReRunL1TransformFlag] = 1 OR [RetryCount] >0) AND @L1TransformStatus='Success' 
# MAGIC 											THEN 'ReRunSuccess'
# MAGIC 										WHEN [ReRunL1TransformFlag] = 1 AND @L1TransformStatus='Failure' 
# MAGIC 											THEN 'ReRunFailure'
# MAGIC 									ELSE @L1TransformStatus 
# MAGIC 								 END
# MAGIC 			, [ActiveFlag] = CASE
# MAGIC 								WHEN @L1TransformStatus IN ('Success','ReRunSuccess') 
# MAGIC 									THEN 0
# MAGIC 								WHEN @L1TransformStatus = 'Failure' and ISNULL(RetryCount,0) +1 >= @MaxRetries
# MAGIC 									THEN 0
# MAGIC 								ELSE 1 
# MAGIC 							END
# MAGIC 			, [ReRunL1TransformFlag] =	CASE 
# MAGIC 											WHEN [ReRunL1TransformFlag] = 1 AND @L1TransformStatus IN ('Success','ReRunSuccess') 
# MAGIC 												THEN 0
# MAGIC 											WHEN @L1TransformStatus in ('Failure', 'ReRunFailure') and ISNULL(RetryCount,0) +1 >= @MaxRetries
# MAGIC 													THEN 0
# MAGIC 											ELSE [ReRunL1TransformFlag] 
# MAGIC 										END
# MAGIC 			, [RetryCount] = CASE
# MAGIC 								WHEN
# MAGIC 									@L1TransformStatus in ('Success', 'ReRunSuccess')
# MAGIC 										THEN 0
# MAGIC 								WHEN	
# MAGIC 									@L1TransformStatus in ('Failure', 'ReRunFailure')
# MAGIC 										THEN ISNULL([RetryCount],0) + 1
# MAGIC 								Else [RetryCount]
# MAGIC 								END
# MAGIC 							
# MAGIC 			, [ModifiedBy] =suser_sname()
# MAGIC 			, [ModifiedTimestamp]=@localdate
# MAGIC 			, [L1TransformADFPipelineRunID] = @L1TransformADFPipelineRunID
# MAGIC 			, [IngestCount] = ISNULL(@IngestCount,[IngestCount])
# MAGIC 			, [L1TransformInsertCount] = ISNULL(@L1TransformInsertCount,[L1TransformInsertCount])
# MAGIC 			, [L1TransformUpdateCount] = ISNULL(@L1TransformUpdateCount,[L1TransformUpdateCount])
# MAGIC 			, [L1TransformDeleteCount] = ISNULL(@L1TransformDeleteCount,[L1TransformDeleteCount])
# MAGIC 	WHERE 
# MAGIC 		[L1TransformInstanceID] = @L1TransformInstanceId
# MAGIC END;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Populate the two main tables of the framework: 
# ## <strong>IngestDefintition</strong> and <strong>L1TransformDefinition</strong>)
# ## Use the script below or any other process to populate those two tables.

# CELL ********************

# MAGIC %%sql
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- REPLACE YOUR SOURCE TABLES AND FABRIC OBJECTS HERE
# MAGIC --------------------------------------------------------------------------------------------------------------------------------
# MAGIC 
# MAGIC SELECT *
# MAGIC INTO ELT.Z_Source_Metadata
# MAGIC FROM (
# MAGIC     SELECT 'dbo' AS Schema_Name, 'Customers'	AS Table_Name, 'CustomerID'		                    AS primary_key, NULL	AS WatermarkColName UNION
# MAGIC 	SELECT 'dbo' AS Schema_Name, 'Orders'		AS Table_Name, 'CustomerID|OrderDate|TotalAmount'	AS primary_key, NULL	AS WatermarkColName UNION
# MAGIC 	SELECT 'dbo' AS Schema_Name, 'Inventory'	AS Table_Name, 'ProductID'		                    AS primary_key, NULL	AS WatermarkColName 
# MAGIC ) A;
# MAGIC 
# MAGIC SELECT *
# MAGIC INTO ELT.Z_Fabric_Metadata
# MAGIC FROM (SELECT 
# MAGIC     '091ac07b-CHANGE-ME-a8f7-12c4de6e85d9' as L1NotebookID, 
# MAGIC     'eb012223-CHANGE-ME-bae2-00269b7ed3e9' as BronzeLakehouseID,
# MAGIC     '32ebfCHANGE-MEaoulq.datawarehouse.fabric.microsoft.com' AS WH_Control_Conn_String,
# MAGIC     'System01' as SourceSystemName,
# MAGIC     'System01 data comming from db_test database.' as SourceSystemDescription,
# MAGIC     'SQL Server' as Backend, 
# MAGIC     '8e5b0672-CHANGE-ME-b2d4-154b88d9dd9b' as Source_Connection_ID, 
# MAGIC     'db_test' as Source_Database_Name 
# MAGIC ) B;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- NO CHANGES AFTER THIS POINT
# MAGIC --------------------------------------------------------------------------------------------------------------------------------
# MAGIC 
# MAGIC SELECT *
# MAGIC INTO ELT.Z_Meta
# MAGIC FROM (
# MAGIC 	SELECT *
# MAGIC 	FROM ELT.Z_Source_Metadata
# MAGIC 	INNER JOIN ELT.Z_Fabric_Metadata fabric ON 1=1
# MAGIC ) C;
# MAGIC 
# MAGIC --------------------------------------- IngestDefinition -------------------------------------------------
# MAGIC 
# MAGIC INSERT INTO [ELT].[IngestDefinition] 
# MAGIC SELECT 
# MAGIC 	source.[IngestID],
# MAGIC 	source.[SourceSystemName],
# MAGIC 	source.[StreamName],
# MAGIC 	source.[SourceSystemDescription],
# MAGIC 	source.[Backend],
# MAGIC 	source.[DataFormat],
# MAGIC 	source.[EntityName],
# MAGIC 	source.[WatermarkColName],
# MAGIC 	source.[DeltaFormat],
# MAGIC 	source.[LastDeltaDate],
# MAGIC 	source.[LastDeltaNumber],
# MAGIC 	source.[LastDeltaString],
# MAGIC 	source.[MaxIntervalMinutes],
# MAGIC 	source.[MaxIntervalNumber],
# MAGIC 	source.[DataMapping],
# MAGIC 	source.[SourceFileDropFileSystem],
# MAGIC 	source.[SourceFileDropFolder],
# MAGIC 	source.[SourceFileDropFile],
# MAGIC 	source.[SourceFileDelimiter],
# MAGIC 	source.[SourceFileHeaderFlag],
# MAGIC 	source.[SourceStructure],
# MAGIC 	source.[DestinationRawFileSystem],
# MAGIC 	source.[DestinationRawFolder],
# MAGIC 	source.[DestinationRawFile],
# MAGIC 	source.[RunSequence],
# MAGIC 	source.[MaxRetries],
# MAGIC 	source.[ActiveFlag],
# MAGIC 	source.[L1TransformationReqdFlag],
# MAGIC 	source.[L2TransformationReqdFlag],
# MAGIC 	source.[DelayL1TransformationFlag],
# MAGIC 	source.[DelayL2TransformationFlag],
# MAGIC 	source.[CreatedBy],
# MAGIC 	source.[CreatedTimestamp],
# MAGIC 	source.[ModifiedBy],
# MAGIC 	source.[ModifiedTimestamp],
# MAGIC 	source.[BronzeLakehouseID],
# MAGIC 	source.[WH_Control_Conn_String],
# MAGIC     source.[Source_Connection_ID],
# MAGIC     source.[Source_Database_Name]
# MAGIC FROM (
# MAGIC 	SELECT
# MAGIC 		(select coalesce(max(IngestID),0) from [ELT].[IngestDefinition]) + ROW_NUMBER() OVER (ORDER BY Table_Name) as IngestID,
# MAGIC 		Z_Meta.SourceSystemName,
# MAGIC 		Z_Meta.Table_Name AS [StreamName],
# MAGIC 		Z_Meta.SourceSystemDescription,
# MAGIC 		Z_Meta.Backend,
# MAGIC 		NULL AS [DataFormat],  
# MAGIC 		(Schema_Name + '.' + Table_Name) as [EntityName],   
# MAGIC 		Z_Meta.WatermarkColName AS [WatermarkColName], 
# MAGIC 		NULL AS [DeltaFormat],  
# MAGIC 		CAST('1900-01-01' AS datetime2(6)) AS [LastDeltaDate],
# MAGIC 		NULL AS [LastDeltaNumber], 
# MAGIC 		NULL AS [LastDeltaString], 
# MAGIC 		NULL AS [MaxIntervalMinutes], 
# MAGIC 		NULL AS [MaxIntervalNumber], 
# MAGIC 		NULL AS [DataMapping],  
# MAGIC 		NULL AS [SourceFileDropFileSystem], 
# MAGIC 		NULL AS [SourceFileDropFolder], 
# MAGIC 		NULL AS [SourceFileDropFile], 
# MAGIC 		NULL AS [SourceFileDelimiter], 
# MAGIC 		NULL AS [SourceFileHeaderFlag], 
# MAGIC 		NULL AS [SourceStructure], 
# MAGIC 		'Files' AS [DestinationRawFileSystem],
# MAGIC 		'raw_bronze/'+Z_Meta.SourceSystemName+'/'+ Schema_Name +'/'+  Z_Meta.Table_Name +'/YYYY-MM' as [DestinationRawFolder], 
# MAGIC 		Schema_Name +'_'+ Z_Meta.Table_Name + '_'+ 'YYYY-MM-DD_HHMISS.parquet' as [DestinationRawFile], 
# MAGIC 		100 AS [RunSequence], 
# MAGIC 		3 AS [MaxRetries],
# MAGIC 		CAST(1 AS bit) AS [ActiveFlag],
# MAGIC 		CAST(1 AS bit) AS [L1TransformationReqdFlag],
# MAGIC 		CAST(0 AS bit) AS [L2TransformationReqdFlag],
# MAGIC 		CAST(0 AS bit) AS [DelayL1TransformationFlag],
# MAGIC 		CAST(1 AS bit) AS [DelayL2TransformationFlag], 
# MAGIC 		'initial_load' AS [CreatedBy],
# MAGIC 		CAST(GETDATE() AS datetime2(6)) AS [CreatedTimestamp],
# MAGIC 		'initial_load' AS [ModifiedBy], 
# MAGIC 		CAST(GETDATE() AS datetime2(6)) AS [ModifiedTimestamp],
# MAGIC 		Z_Meta.BronzeLakehouseID, 
# MAGIC 		Z_Meta.WH_Control_Conn_String,
# MAGIC         Z_Meta.Source_Connection_ID,
# MAGIC         Z_Meta.Source_Database_Name
# MAGIC 
# MAGIC 	FROM ELT.Z_Meta
# MAGIC 
# MAGIC 	LEFT JOIN [ELT].[IngestDefinition] AS target
# MAGIC 	    ON Z_Meta.SourceSystemName = target.SourceSystemName
# MAGIC 	    AND Z_Meta.Table_Name = target.StreamName
# MAGIC         
# MAGIC 	WHERE target.SourceSystemName IS NULL
# MAGIC ) AS source
# MAGIC   -- This ensures only new records are inserted
# MAGIC 
# MAGIC 
# MAGIC --------------------------------------- L1TransformDefinition -------------------------------------------------
# MAGIC 
# MAGIC INSERT INTO [ELT].[L1TransformDefinition] (
# MAGIC     [L1TransformID],
# MAGIC     [IngestID],
# MAGIC     [ComputePath],
# MAGIC     [ComputeName],
# MAGIC     [CustomParameters],
# MAGIC     [InputRawFileSystem],
# MAGIC     [InputRawFileFolder],
# MAGIC     [InputRawFile],
# MAGIC     [InputRawFileDelimiter],
# MAGIC     [InputFileHeaderFlag],
# MAGIC     [OutputL1CurateFileSystem],
# MAGIC     [OutputL1CuratedFolder],
# MAGIC     [OutputL1CuratedFile],
# MAGIC     [OutputL1CuratedFileDelimiter],
# MAGIC     [OutputL1CuratedFileFormat],
# MAGIC     [OutputL1CuratedFileWriteMode],
# MAGIC     [OutputDWStagingTable],
# MAGIC     [LookupColumns],
# MAGIC     [OutputDWTable],
# MAGIC     [OutputDWTableWriteMode],
# MAGIC     [MaxRetries],
# MAGIC     [WatermarkColName],
# MAGIC     [ActiveFlag],
# MAGIC     [CreatedBy],
# MAGIC     [CreatedTimestamp],
# MAGIC     [ModifiedBy],
# MAGIC     [ModifiedTimestamp]
# MAGIC )
# MAGIC SELECT 
# MAGIC 	(select COALESCE(max(L1TransformID), 0) from [ELT].[L1TransformDefinition]) + ROW_NUMBER() OVER (ORDER BY source.IngestID) as L1TransformID,
# MAGIC 	source.IngestID, 
# MAGIC 	NULL AS [ComputePath],
# MAGIC 	Z_Meta.L1NotebookID AS [ComputeName],
# MAGIC 	NULL AS [CustomParameters],
# MAGIC 	[DestinationRawFileSystem] as InputRawFileSystem,
# MAGIC 	[DestinationRawFolder] as InputRawFileFolder,
# MAGIC 	[DestinationRawFile] as InputRawFile,
# MAGIC 	NULL AS [InputRawFileDelimiter],
# MAGIC 	NULL AS [InputFileHeaderFlag],
# MAGIC 	'Not Applicable' AS [OutputL1CurateFileSystem],
# MAGIC 	'Not Applicable' as [OutputL1CuratedFolder],
# MAGIC 	'Not Applicable' as [OutputL1CuratedFile],
# MAGIC 	NULL AS [OutputL1CuratedFileDelimiter],
# MAGIC 	NULL AS [OutputL1CuratedFileFormat],
# MAGIC 	NULL AS [OutputL1CuratedFileWriteMode],
# MAGIC 	NULL AS [OutputDWStagingTable],
# MAGIC 	Z_Meta.primary_key AS [LookupColumns],
# MAGIC 	StreamName AS [OutputDWTable],
# MAGIC 	'append' AS [OutputDWTableWriteMode],
# MAGIC 	3 AS [MaxRetries],
# MAGIC 	source.WatermarkColName AS [WatermarkColName],
# MAGIC 	CAST(1 AS bit) AS [ActiveFlag],
# MAGIC 	'initial_load' AS [CreatedBy],
# MAGIC 	GETDATE() AS [CreatedTimestamp],
# MAGIC 	'initial_load' AS [ModifiedBy],
# MAGIC 	GETDATE() AS [ModifiedTimestamp]
# MAGIC FROM [ELT].[IngestDefinition] source
# MAGIC INNER JOIN [ELT].Z_Meta
# MAGIC 	ON source.SourceSystemName = Z_Meta.SourceSystemName
# MAGIC 	AND source.StreamName = Z_Meta.Table_Name
# MAGIC LEFT JOIN [ELT].[L1TransformDefinition] target 
# MAGIC 	ON source.IngestID = target.IngestID
# MAGIC WHERE target.IngestID IS NULL;
# MAGIC 
# MAGIC --------------------------- Drop temp tables--------------------------------------------------
# MAGIC DROP TABLE ELT.Z_Source_Metadata;
# MAGIC DROP TABLE ELT.Z_Fabric_Metadata;
# MAGIC DROP TABLE ELT.Z_Meta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
