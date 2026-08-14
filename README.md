🚀 Azure Data Factory End-to-End Project

An end-to-end Azure Data Factory (ADF) project demonstrating real-world data engineering patterns including API ingestion, on-premises file ingestion, incremental loading, watermark management, orchestration, alerts, auditing, and REST API pagination.

📚 Medium Blog Series — Incremental Data Loading

I have documented the incremental loading concepts and implementation in a 4-part Medium series.

💡 Click any button below to open the detailed article.









🔗 Incremental Loading — Complete Series

Part 1 → Part 2 → Part 3 → Part 4

The series covers the concepts and implementation behind the incremental loading pipelines used in this project.

🏗️ Project Overview

This project contains 7 Azure Data Factory pipelines covering common real-world Data Engineering scenarios.

#

Pipeline

Key Concept

1

pl_API_Ingestion

REST API → ADLS Gen2

2

pl_OnPremFiles_To_AzureBlobBronze

On-Prem Files → ADLS Gen2

3

pl_SQLtoAzure_Incremental_Json

Incremental Load + JSON Watermark

4

pl_dailydata_load_with_alerts

Orchestration + Logic App Alerts

5

pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable

Multi-Table Incremental Load

6

pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable_ScriptActivity

Incremental Load + Audit Logging

7

pl_paginationExample

REST API Pagination

🔹 Pipeline 1 — API Ingestion

pl_API_Ingestion

What it does

Pulls DimAirport.json from GitHub Raw content into the ADLS Gen2 Bronze layer.

Flow

GitHub Raw
    ↓
Copy API Data
    ↓
ADLS Gen2 Bronze

Source

ds_APISource
    ↓
ls_githubAPI_source
    ↓
HttpServer / Anonymous
    ↓
DimAirport.json

Sink

ds_github_sink
    ↓
ls_bronze
    ↓
ADLS Gen2
    ↓
bronze/github/DimAirport.json

Key Settings

JsonSource with HttpReadSettings

HTTP GET method

JsonSink with AzureBlobFSWriteSettings

retry = 2

retryIntervalInSeconds = 30

enableStaging = false

🔹 Pipeline 2 — On-Premises Files to ADLS Bronze

pl_OnPremFiles_To_AzureBlobBronze

What it does

Migrates 3 CSV files in parallel from an on-premises Windows file server to ADLS Gen2 Bronze.

Files

DimAirline.csv
DimFlight.csv
DimPassenger.csv

Flow

On-Prem Windows File Server
            ↓
        ForEach
       (Parallel)
            ↓
       Copy Activity
            ↓
         ADLS Gen2
          Bronze

Pipeline Parameter

file_name

Example:

[
  {"fileName": "DimAirline.csv"},
  {"fileName": "DimFlight.csv"},
  {"fileName": "DimPassenger.csv"}
]

Key Expressions

@pipeline().parameters.file_name

Used as the ForEach items source.

@item().fileName

Gets the current iteration's file name.

@dataset().p_fileName_source

Passes the file name to the source dataset at runtime.

@dataset().p_fileName_sink

Passes the file name to the sink dataset at runtime.

Source

ds_OnPremSource
    ↓
ls_onPrem_Files
    ↓
FileServer / IROnPremise
    ↓
E:\OnPremiseFiles\{fileName}

Sink

ds_onPremSink
    ↓
ls_bronze
    ↓
ADLS Gen2
    ↓
bronze/OnPremDataFiles/{fileName}

🔹 Pipeline 3 — Incremental Load Using JSON Watermark

pl_SQLtoAzure_Incremental_Json

What it does

Performs an incremental load from Azure SQL FactBookings to ADLS Gen2 in Parquet format.

A JSON file is used as the watermark store.

Flow

LastLoad Lookup ─────┐
                     ├──→ Copy SQL Data ──→ Parquet
LatestLoad Lookup ───┘
                              ↓
                       Update Watermark

Last Load

Reads lastload.json from Azure Blob and gets the last processed booking_date.

Latest Load

SELECT MAX(booking_date) AS LatestLoad
FROM dbo.FactBookings;

Incremental Query

SELECT *
FROM dbo.FactBookings
WHERE booking_date > '@{activity('LastLoad').output.firstRow.lastload}'
  AND booking_date <= '@{activity('LatestLoad').output.firstRow.LatestLoad}';

Sink

Azure SQL
    ↓
Copy Activity
    ↓
ADLS Gen2
    ↓
Bronze/sql/
    ↓
Parquet + Snappy Compression

Watermark Update

The latest watermark is written back to lastload.json.

LatestLoad
    ↓
additionalColumns
    ↓
lastload = LatestLoad
    ↓
Overwrite lastload.json

📖 Detailed Explanation

This pipeline is part of the Incremental Data Loading Medium series at the top of this README.

🔹 Pipeline 4 — Daily Orchestration & Alerts

pl_dailydata_load_with_alerts

What it does

Acts as an orchestrator pipeline that calls the incremental pipeline and sends the execution status to an Azure Logic App.

Flow

DailyDataLoad
     ↓
Execute Pipeline
     ↓
pl_SQLtoAzure_Incremental_Json
     ↓
Call Logic App

Execute Pipeline

Calls:

pl_SQLtoAzure_Incremental_Json

with:

waitOnCompletion = true

Logic App Trigger

The Logic App is called when the child pipeline:

✅ Succeeds

❌ Fails

Payload

{
  "pipeline_name": "@{pipeline().Pipeline}",
  "run_id": "@{pipeline().RunId}",
  "status": "@{activity('DailyDataLoad').Status}",
  "error": "@{if(equals(activity('DailyDataLoad').Status,'Failed'), string(activity('DailyDataLoad').error), 'No Error')}"
}

🔹 Pipeline 5 — Multi-Table Incremental Load

pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable

What it does

Performs multi-table incremental loading from On-Premises SQL Server to Azure SQL using a centralized watermark table.

Source Tables

dbo.iplteams
dbo.salesitems

Both tables use:

last_updated

as the watermark column.

Flow

             ForEach Table
                  ↓
       ┌──────────┴──────────┐
       ↓                     ↓
 Old Watermark          New Watermark
       ↓                     ↓
       └──────────┬──────────┘
                  ↓
             If Condition
             /          \
          TRUE          FALSE
           ↓              ↓
         Copy            Skip
           ↓
    Update Watermark

Old Watermark

SELECT WatermarkValue
FROM watermarktable
WHERE TableName = '@{item().TableName}';

New Watermark

SELECT MAX(last_updated) AS NewWatermarkValue
FROM @{item().TableName};

If Condition

@greater(
    activity('New_LookUp_Col').output.firstRow.NewWatermarkValue,
    activity('Old_LookUp_Col').output.firstRow.WatermarkValue
)

Dynamic SQL

The pipeline dynamically builds the incremental query using:

Table schema

Table name

Watermark column

Old watermark

New watermark

Sink

AzureSqlSink
    ↓
writeBehavior = upsert
    ↓
useTempDB = true
    ↓
keys = [id]

Watermark Update

Stored procedure:

[dbo].[usp_write_watermark]

with parameters:

last_updated
tableName

📖 Detailed Explanation

This pipeline is part of the Incremental Data Loading Medium series at the top of this README.

🔹 Pipeline 6 — Incremental Load + Audit Logging

pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable_ScriptActivity

What it does

An enhanced version of Pipeline 5 with:

Pipeline execution timing

Script Activity

Success audit logging

Failure audit logging

Rows copied tracking

Error tracking

Flow

Set Start Time
      ↓
Old Watermark Lookup
      ↓
New Watermark Lookup
      ↓
If Condition
      ↓
     Copy
    /    \
Success  Failed
   ↓        ↓
Audit     Audit
Success   Failure

Set Start Time

PipelineStartTime = @utcNow()

Script Activity — Update Watermark

UPDATE dbo.watermarktable
SET WatermarkValue = @last_updated
WHERE TableName = @tableName;

Success Audit

INSERT INTO dbo.PipelineAudit
(
    PipelineName,
    TableName,
    SourceName,
    TargetName,
    WatermarkOld,
    WatermarkNew,
    RowsCopied,
    Status,
    ErrorMessage,
    StartTime,
    EndTime
)
VALUES
(
    @PipelineName,
    @TableName,
    @SourceName,
    @TargetName,
    @OldWatermark,
    @NewWatermark,
    @RowsCopied,
    @Status,
    @ErrorMessage,
    @StartTime,
    @EndTime
);

Failure Audit

The failure branch records:

Status = 'Failed'
RowsCopied = 0
ErrorMessage = Copy Activity error message

Important Expressions

@pipeline().Pipeline

Pipeline name.

@item().TableName

Current table name.

@activity('Copy_FromOnPremSQL_ToAzureSQL').output.rowsCopied

Rows copied.

@activity('Copy_FromOnPremSQL_ToAzureSQL').error.message

Error message.

@variables('PipelineStartTime')

Pipeline start time.

@utcNow()

End time.

Evolution from Pipeline 5

The previous:

SP_Update_WaterMark

activity is now inactive because the watermark update is handled by Script Activity.

📖 Detailed Explanation

This pipeline represents the advanced incremental loading and auditing approach documented in the Medium series at the top of this README.

🔹 Pipeline 7 — REST API Pagination

pl_paginationExample

What it does

Ingests Pokémon data from PokéAPI across multiple pages using ADF's native RANGE pagination.

Flow

PokéAPI
   ↓
Web Activity
   ↓
Get Total Count
   ↓
Copy Activity
   ↓
RANGE Pagination
   ↓
ADLS Gen2 Bronze

Initial API Request

https://pokeapi.co/api/v2/pokemon?limit=20&offset=0

The response contains the total Pokémon count.

Pagination Rule

QueryParameters.offset =
RANGE:0:@{activity('Get API Data').output.count}:20

This generates requests such as:

offset=0
offset=20
offset=40
offset=60
...
offset=1300

Sink

ds_API_Target_JSON
    ↓
ADLS Gen2
    ↓
bronze/PaginationExample/

🎯 Key ADF Concepts Demonstrated

REST API ingestion

ADLS Gen2

On-Premises File Server

Self-hosted Integration Runtime

ForEach activity

Parallel processing

Pipeline parameters

Dataset parameters

Dynamic expressions

Incremental loading

JSON watermark

Watermark table

Azure SQL Upsert

Execute Pipeline

Web Activity

Logic App integration

Script Activity

Audit logging

Error handling

REST API pagination

RANGE pagination

📚 Medium Articles

🔵 Incremental Data Loading Series

Article

Link

Part 1 — Incremental Loading

📖 Read on Medium

Part 2 — Incremental Loading

📖 Read on Medium

Part 3 — Incremental Loading

📖 Read on Medium

Part 4 — Incremental Loading

📖 Read on Medium

💡 For the incremental pipelines in this repository, start with Part 1 and follow the series sequentially.

💼 Interview Summary

I built multiple Azure Data Factory pipelines covering real-world data engineering scenarios. The project includes REST API ingestion, parallel on-premises file migration, watermark-based incremental loading, multi-table processing using ForEach, Azure SQL upserts, Logic App alerts, Script Activity-based watermark management, audit logging, and REST API pagination.

🔗 GitHub Repository



👨‍💻 Author

Chethan

Azure Data Factory • SQL • Azure • Python • Data Engineering
