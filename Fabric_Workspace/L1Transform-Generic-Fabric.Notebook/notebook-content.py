# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c5989c50-bf8f-4cce-8f43-84174316cdcf",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "c5a446d5-a746-47ea-94a1-d74b3faa71b0"
# META     }
# META   }
# META }

# CELL ********************

%run /Utils_CommonTransforms

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /Utils_DeltaLakeFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook Parameters

# PARAMETERS CELL ********************

L1TransformInstanceID = None
L1TransformID = None
IngestID = None
CustomParameters = None
InputRawFileSystem = None
InputRawFileFolder = None
InputRawFile = None
InputRawFileDelimiter = None
InputFileHeaderFlag = None
OutputL1CurateFileSystem = None
OutputL1CuratedFolder = None
OutputL1CuratedFile = None
OutputL1CuratedFileDelimiter = None
OutputL1CuratedFileFormat = None
OutputL1CuratedFileWriteMode = None
OutputDWStagingTable = None
LookupColumns = None
OutputDWTable = None
OutputDWTableWriteMode = None
ReRunL1TransformFlag = None
WatermarkColName = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Parameters for Testing only, should be commented off
# L1TransformInstanceID = 2
# L1TransformID = 21
# IngestID = 21
# CustomParameters = None
# InputRawFileSystem = 'Files'
# InputRawFileFolder = 'raw-bronze/wwi/Sales/Orders/2013-01'
# InputRawFile = 'Sales_Orders_2013-01-01_000000.parquet'
# InputRawFileDelimiter = None
# InputFileHeaderFlag = None
# OutputL1CurateFileSystem = None
# OutputL1CuratedFolder = None
# OutputL1CuratedFile = None
# OutputL1CuratedFileDelimiter = None
# OutputL1CuratedFileFormat = None
# OutputL1CuratedFileWriteMode = None
# OutputDWStagingTable = None
# LookupColumns = 'OrderID'
# OutputDWTable = 'silver.sales_orders'
# OutputDWTableWriteMode = 'append'
# ReRunL1TransformFlag = None
# WatermarkColName = 'LastEditedWhen'


# L1TransformInstanceID = 1
# L1TransformID = 1
# IngestID = 1
# CustomParameters = None
# InputRawFileSystem = "Files"
# InputRawFileFolder = "raw-bronze/wwi/Customers/1900-01"
# InputRawFile = "Customers_1900-01-01_000000.parquet"
# InputRawFileDelimiter = None
# InputFileHeaderFlag = False
# OutputL1CurateFileSystem = None
# OutputL1CuratedFolder = None
# OutputL1CuratedFile = None
# OutputL1CuratedFileDelimiter = None
# OutputL1CuratedFileFormat = None
# OutputL1CuratedFileWriteMode = None
# OutputDWStagingTable = None
# LookupColumns = "CustomerID"
# OutputDWTable = "silver.Customers"
# OutputDWTableWriteMode = "append"
# ReRunL1TransformFlag = False
# DeltaName = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read, cleanse and enrich raw/bronze data

# CELL ********************

df = readFile('bronze',InputRawFileSystem,InputRawFileFolder,InputRawFile)
ingestCount = df.count()

ct=CommonTransforms(df)

# Remove duplicates
if LookupColumns is not None:
    df=ct.deDuplicate(LookupColumns.split('|'))
else:
    df=ct.deDuplicate()

# Remove leading and trailing spaces from all string columns
df=ct.trim()

# # Replace Null Value with generic values
df = ct.replaceNull(0)
df = ct.replaceNull("NA")
df = ct.replaceNull("1900-01-01")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load standardized/silver data

# CELL ********************

if OutputDWTableWriteMode == 'append' and LookupColumns is not None and ingestCount>0:
    output = upsertDelta(df,OutputDWTable,LookupColumns,WatermarkColName)
    numSourceRows = output["numSourceRows"]
    numTargetRowsInserted = output["numTargetRowsInserted"]
    numTargetRowsUpdated = output["numTargetRowsUpdated"]
    numTargetRowsDeleted = output["numTargetRowsDeleted"]
elif OutputDWTableWriteMode == 'overwrite' and ingestCount>0:
    output = insertDelta (df, OutputDWTable, OutputDWTableWriteMode)
    numSourceRows = ingestCount
    numTargetRowsInserted = output["numOutputRows"]
    numTargetRowsUpdated ="0"
    numTargetRowsDeleted ="0"
else:
    numSourceRows = ingestCount
    numTargetRowsInserted = "0"
    numTargetRowsUpdated ="0"
    numTargetRowsDeleted ="0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Return Values (to pipeline)

# CELL ********************

import json
notebookutils.notebook.exit(json.dumps({
  "numSourceRows": numSourceRows,
  "numTargetRowsInserted": numTargetRowsInserted,
  "numTargetRowsUpdated": numTargetRowsUpdated,
  "numTargetRowsDeleted": numTargetRowsDeleted
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
