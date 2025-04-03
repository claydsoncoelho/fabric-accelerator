# Microsoft Fabric Ingestion Framework

## Disclamer
This framework is based on:
 - **[Fabric Accelerator](https://bennyaustin.com/2024/11/17/fabric-accelerator/)**
 - **[ELT Framework](https://github.com/bennyaustin/elt-framework/wiki)**

The Microsoft Fabric Accelerator is a collection of reusable code artifacts integrated with an orchestration framework for Microsoft Fabric. 
This Ingestion Framework, based on Microsoft Fabric Accelerator, helps you to automate the data ingestion from a source systems into Fabric Lakehouses.

## Content

- ### Fabric_Workspace
A fabric Workspace repository that you must upload to your Azure DevOps repo and connect to your Fabric Workspace. This workspace has an metadata orchestration framework that will help you to ingest data from source systems faster.

- ### WH_Control_Help
SQL scripts to help you to populate the two main orchestration framework tables:
 - **WH_Control.ELT.IngestDefinition**
 - **WH_Control.ELT.L1TransformDefinition**

## Overview

During the setup of this framework, you will perform 3 main steps:

1. On Azure DevOps, import the Fabric Workspace repository to a branch in Azure DevOps repo.
2. On Fabric, create the WH_Contrl Warehouse, which is the brain of the framework.
3. On Fabric, populate the two main tables of the framework with information about the source system and tables that you want to ingest.

## Setup

Follow these steps to setup Microsoft Fabric Accelerator:

1. In Azure DevOps, create a repo and upload the content of **Fabric_Workspace** folder to it.
2. In Fabric, create a **Workspace** and link it to the Azure DevOps repo.
3. Create a Fabric **Lakehouse** to be be the landing zone (bronze layer) of the Data Platform. Take note of the Lakehouse ID. You will need it later.
4. Open **L1Transform-Generic-Fabric** Notebook and adjust the **defaultLakehouse** parameter with the name of the bronze Lakehouse that you've just created. Also, take note of the this Notebook ID. You will need it later.
5. Open **Utils_DeltaLakeFunctions** Notebook and adjust the **BronzeLakehouseName** parameter with the name of the bronze Lakehouse that you've just created.
6. Create a connection to the source system. Take note of the Connetion ID. You will need it later.
7. Create a Warehouse, usually WH_Control, to be the configuration database of the Ingestion Framework. 
8. Follow the steps in **Ingestion_Framework_Deploy** Notebook to deploy the tables and stored procedures of WH_Control.
The information that you will need to populate those tables is:
    - **L1NotebookID:** L1Transform-Generic-Fabric Notebook ID.
    - **BronzeLakehouseID:** Lakehouse ID of the bronze Lakehouse that you created.
    - **WH_Control_Conn_String:** Connection string of your WH_Control Lakehouse. 
    - **SourceSystemName:** Source system name.
    - **SourceSystemDescription:** Source system description.
    - **Backend:** Source system technology (SQL Server, for example).
    - **Source_Connection_ID:** The ID of the connection to the source system
    - **Source_Database_Name:** The name o the source database.
    - General information about the **source tables** to be ingested (Schema, Table Name, Primery Key and Watermark column(optional)).
9. Open **Master ELT Orchestration** Pipeline, change its parameters accordingly and run it to test the ingestion process.
