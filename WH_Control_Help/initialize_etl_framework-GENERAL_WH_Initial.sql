--------------------------------------- Create temp tables -------------------------------------------------

SELECT *
INTO ELT.Z_Source_Metadata
FROM (
    SELECT 'dbo' AS Schema_Name, 'F1ASR_ASSET_BOOK' AS Table_Name, 'ASSNBRI|BKNBR|REG_NAME' AS primary_key UNION 
    SELECT 'dbo' AS Schema_Name, 'F1ASR_ASSET_BOOK_ACCT' AS Table_Name, 'ASSNBRI|BKNBR|REG_NAME|TRANS_LEG_CODE' AS primary_key UNION 
    SELECT 'dbo' AS Schema_Name, 'F1ASR_REG_ASSET' AS Table_Name, 'ASSNBRI|REG_NAME' AS primary_key
) A;

SELECT *
INTO ELT.Z_Fabric_Metadata
FROM (SELECT 
    '1ca0ad73-1aae-45f4-b5ff-d3fb9e718464' as L1NotebookID, 
    '6c764493-7e36-4433-961e-fe14afd70ea9' as BronzeLakehouseID,
    '7cvmx6lyyj5edglttzwapovooe-tur52lnkkukerowa7ny6z762zm.datawarehouse.fabric.microsoft.com' AS WH_Control_Conn_String,
    'Tech_One' as SourceSystemName,
    'Tech One data comming from finprod database.' as SourceSystemDescription,
    'SQL Server' AS Backend
) B;

--------------------------------------- IngestDefinition -------------------------------------------------

INSERT INTO [ELT].[IngestDefinition] (
    [IngestID],
    [SourceSystemName],
    [StreamName],
    [SourceSystemDescription],
    [Backend],
    [DataFormat],
    [EntityName],
    [WatermarkColName],
    [DeltaFormat],
    [LastDeltaDate],
    [LastDeltaNumber],
    [LastDeltaString],
    [MaxIntervalMinutes],
    [MaxIntervalNumber],
    [DataMapping],
    [SourceFileDropFileSystem],
    [SourceFileDropFolder],
    [SourceFileDropFile],
    [SourceFileDelimiter],
    [SourceFileHeaderFlag],
    [SourceStructure],
    [DestinationRawFileSystem],
    [DestinationRawFolder],
    [DestinationRawFile],
    [RunSequence],
    [MaxRetries],
    [ActiveFlag],
    [L1TransformationReqdFlag],
    [L2TransformationReqdFlag],
    [DelayL1TransformationFlag],
    [DelayL2TransformationFlag],
    [CreatedBy],
    [CreatedTimestamp],
    [ModifiedBy],
    [ModifiedTimestamp],
    [BronzeLakehouseID],
    [WH_Control_Conn_String]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY Table_Name) as IngestID,
    t.SourceSystemName,
    Table_Name AS [StreamName],
    t.SourceSystemDescription,
    t.Backend,
    NULL AS [DataFormat],  
    (Schema_Name + '.' + Table_Name) as [EntityName],   
    NULL AS [WatermarkColName], 
    NULL AS [DeltaFormat],  
    CAST('1900-01-01' AS datetime) AS [LastDeltaDate],
    NULL AS [LastDeltaNumber], 
    NULL AS [LastDeltaString], 
    NULL AS [MaxIntervalMinutes], 
    NULL AS [MaxIntervalNumber], 
    NULL AS [DataMapping],  
    NULL AS [SourceFileDropFileSystem], 
    NULL AS [SourceFileDropFolder], 
    NULL AS [SourceFileDropFile], 
    NULL AS [SourceFileDelimiter], 
    NULL AS [SourceFileHeaderFlag], 
    NULL AS [SourceStructure], 
    'Files' AS [DestinationRawFileSystem],
    'raw_bronze/'+t.SourceSystemName+'/'+ Schema_Name +'/'+  Table_Name +'/YYYY-MM' as [DestinationRawFolder], 
    Schema_Name +'_'+ Table_Name + '_'+ 'YYYY-MM-DD_HHMISS.parquet' as [DestinationRawFile], 
    100 AS [RunSequence], 
    3 AS [MaxRetries],
    CAST(1 AS bit) AS [ActiveFlag],
    CAST(1 AS bit) AS [L1TransformationReqdFlag],
    CAST(1 AS bit) AS [L2TransformationReqdFlag],
    CAST(0 AS bit) AS [DelayL1TransformationFlag],
    CAST(1 AS bit) AS [DelayL2TransformationFlag], 
    'initial_load' AS [CreatedBy],
    GETDATE() AS [CreatedTimestamp],
    'initial_load' AS [ModifiedBy], 
    GETDATE() AS [ModifiedTimestamp],
    t.BronzeLakehouseID, 
    t.WH_Control_Conn_String
from ELT.Z_Source_Metadata
INNER JOIN ELT.Z_Fabric_Metadata t ON 1=1;


--------------------------------------- L1TransformDefinition -------------------------------------------------

INSERT INTO [ELT].[L1TransformDefinition] (
    [L1TransformID],
    [IngestID],
    [ComputePath],
    [ComputeName],
    [CustomParameters],
    [InputRawFileSystem],
    [InputRawFileFolder],
    [InputRawFile],
    [InputRawFileDelimiter],
    [InputFileHeaderFlag],
    [OutputL1CurateFileSystem],
    [OutputL1CuratedFolder],
    [OutputL1CuratedFile],
    [OutputL1CuratedFileDelimiter],
    [OutputL1CuratedFileFormat],
    [OutputL1CuratedFileWriteMode],
    [OutputDWStagingTable],
    [LookupColumns],
    [OutputDWTable],
    [OutputDWTableWriteMode],
    [MaxRetries],
    [WatermarkColName],
    [ActiveFlag],
    [CreatedBy],
    [CreatedTimestamp],
    [ModifiedBy],
    [ModifiedTimestamp]
)
SELECT ROW_NUMBER() OVER (ORDER BY IngestID) as L1TransformID,
IngestID, 
NULL AS [ComputePath],
t.L1NotebookID AS [ComputeName],
NULL AS [CustomParameters],
[DestinationRawFileSystem] as InputRawFileSystem,
[DestinationRawFolder] as InputRawFileFolder,
[DestinationRawFile] as InputRawFile,
NULL AS [InputRawFileDelimiter],
NULL AS [InputFileHeaderFlag],
'Not Applicable' AS [OutputL1CurateFileSystem],
'Not Applicable' as [OutputL1CuratedFolder],
'Not Applicable' as [OutputL1CuratedFile],
NULL AS [OutputL1CuratedFileDelimiter],
NULL AS [OutputL1CuratedFileFormat],
NULL AS [OutputL1CuratedFileWriteMode],
NULL AS [OutputDWStagingTable],
NULL AS [LookupColumns],
StreamName AS [OutputDWTable],
'append' AS [OutputDWTableWriteMode],
3 AS [MaxRetries],
NULL AS [WatermarkColName],
CAST(1 AS bit) AS [ActiveFlag],
'initial_load' AS [CreatedBy],
GETDATE() AS [CreatedTimestamp],
'initial_load' AS [ModifiedBy],
GETDATE() AS [ModifiedTimestamp]
FROM [ELT].[IngestDefinition]
INNER JOIN ELT.Z_Fabric_Metadata t ON 1=1;


UPDATE [ELT].[L1TransformDefinition]
SET LookupColumns = t.primary_key
FROM [ELT].[L1TransformDefinition] TargetTable
INNER JOIN [ELT].[IngestDefinition] SourceTable ON TargetTable.IngestID = SourceTable.IngestID
INNER JOIN ELT.Z_Source_Metadata t ON t.Table_Name = SourceTable.StreamName;


--------------------------- Drop temp tables--------------------------------------------------
DROP TABLE ELT.Z_Source_Metadata;
DROP TABLE ELT.Z_Fabric_Metadata;