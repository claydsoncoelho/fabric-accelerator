# Microsoft Fabric Accelerator

## Disclamer
This framework is based on **[Fabric Accelerator](https://bennyaustin.com/2024/11/17/fabric-accelerator/)** 

The Microsoft Fabric Accelerator is a collection of reusable code artifacts integrated with an orchestration framework for Microsoft Fabric. This accelerator helps you to automate the pipelines.

## Content

- ### Fabric_Workspace
A fabric Workspace repository, that you must upload to your Azure DevOps repo and connect to your Fabric Workspace. This workspace has an metadata orchestration framework that will help you to ingest data from source systems faster.

- ### WH_Control_Help
SQL scripts to help you to populate the orchestration framework tables (**WH_Control.ELT.IngestDefinition** and **WH_Control.ELT.L1TransformDefinition**).

## Setup

Follow these steps to setup Microsoft Fabric Accelerator:

1. In Azure DevOps, create a repo and upload the content of **Fabric_Workspace** folder to it.

2. In Fabric, create a Workspace and link it to the Azure DevOps repo.

3. Create a Fabric Lakehouse to be be the landing zone (bronze layer) of the Data Platform. Take note of the Connetion ID. You will need it later.

4. Open **L1Transform-Generic-Fabric** Notebook and attach it to the bronze Lakehouse that you've just created.

5. In WH_Control, populate the **WH_Control.ELT.IngestDefinition** and **WH_Control.ELT.L1TransformDefinition** tables with metadata about the source tables that want to ingest into the landing zone. You can use the scripts in **WH_Control_Help** to help you. **Atention**: ELT.L1TransformDefinition.ComputeName must be the L1Transform-Generic-Fabric Notebook ID.

6. Create a connection to the source system.

7. Open **Ingest Tables** Pipeline, go to **Copy Source to Lakehouse** Activity, then to **Source** tab and replace the Connection by the connection to the source system that you've just created.

8. Open **Master ELT Orchestration** Pipeline, got to Parameters tab, and change the follwoing parameters:
 - SourceSystemName: Source system name (The same that you used to populate **WH_Control.ELT.IngestDefinition** on step 4.).
 - StreamName: Leave it blank to process all tables defined in **WH_Control.ELT.IngestDefinition**.
 - DelayTransformation: 0
 - BronzeObjectID: The landing zone (bronze) Lakehouse ID.
 - BronzeWorkspaceID: The Workspace ID.

9. Open **EnvSettings** Notebook and change the folowing variables:
 - bronzeWorkspaceId: The Workspace ID.
 - bronzeLakehouseName: The name of the landing zone (bronze) Lakehouse.

10. Execute **Master ELT Orchestration** Pipeline to test it.
