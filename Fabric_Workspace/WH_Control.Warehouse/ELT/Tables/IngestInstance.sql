CREATE TABLE [ELT].[IngestInstance] (

	[IngestInstanceID] varchar(255) NOT NULL, 
	[IngestID] int NOT NULL, 
	[SourceFileDropFileSystem] varchar(50) NULL, 
	[SourceFileDropFolder] varchar(200) NULL, 
	[SourceFileDropFile] varchar(200) NULL, 
	[DestinationRawFileSystem] varchar(50) NOT NULL, 
	[DestinationRawFolder] varchar(200) NOT NULL, 
	[DestinationRawFile] varchar(200) NOT NULL, 
	[DataFromTimestamp] datetime2(6) NULL, 
	[DataToTimestamp] datetime2(6) NULL, 
	[DataFromNumber] int NULL, 
	[DataToNumber] int NULL, 
	[SourceCount] int NULL, 
	[IngestCount] int NULL, 
	[IngestStartTimestamp] datetime2(6) NULL, 
	[IngestEndTimestamp] datetime2(6) NULL, 
	[IngestStatus] varchar(20) NULL, 
	[RetryCount] int NULL, 
	[ReloadFlag] bit NULL, 
	[CreatedBy] varchar(128) NOT NULL, 
	[CreatedTimestamp] datetime2(6) NOT NULL, 
	[ModifiedBy] varchar(128) NULL, 
	[ModifiedTimestamp] datetime2(6) NULL, 
	[ADFIngestPipelineRunID] uniqueidentifier NULL
);

