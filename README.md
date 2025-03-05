# Microsoft Fabric Accelerator

## Disclamer
This framework is based on **[Fabric Accelerator](https://bennyaustin.com/2024/11/17/fabric-accelerator/)** 

The Microsoft Fabric Accelerator is a collection of reusable code artifacts integrated with an orchestration framework for Microsoft Fabric. This accelerator helps you to automate the pipelines.

## Content

- ### Fabric_Workspace
A fabric Workspace repository, that you must upload to your Azure DevOps repo and connect to your Fabric Workspace. This workspace has an metadata orchestration framework that will help you to ingest data from source systems faster.

- ### WH_Control_Deployment
A group of SQL Scrips that you must run in WH_Control Warehouse to deploy WH_Control objects (tables and stored procedures).

## Setup

Follow these steps to setup Microsoft Fabric Accelerator:

1. Create an Azure DevOps repo and upload the content of Fabric_Workspace to it.