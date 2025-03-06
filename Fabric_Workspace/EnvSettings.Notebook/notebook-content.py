# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Environment Variables
# Use this notebook to set environment specific variables that can be referenced in other spark notebooks. These variables need to set once in each environment

# CELL ********************

# Fabric Workspace ID for the Bronze medallion layer
bronzeWorkspaceId = "eae69c09-0dc4-4ec5-8170-2de7483fcb7d"

# Bronze Lakehouse name. Set to None if not applicable.
bronzeLakehouseName = "LH_Bronze"

# Bronze data warehouse name. Set to None if not applicable.
bronzeDatawarehouseName = None

# Fabric Workspace ID for the Silver medallion layer. Use the same ID if all medallion layers are in the same workspace.
silverWorkspaceId = None

# Silver Lakehouse name. Set to None if not applicable.
silverLakehouseName = None

# Silver data warehouse name. Set to None if not applicable.
silverDatawarehouseName = None

# Fabric Workspace ID for the Gold medallion layer. Use the same ID if all medallion layers are in the same workspace.
goldWorkspaceId = None

# Gold Lakehouse name. Set to None if not applicable.
goldLakehouseName = None

# Gold data warehouse name. Set to None if not applicable.
goldDatawarehouseName = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
