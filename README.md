# 🚀 Azure Data Factory End-to-End Project

An end-to-end **Azure Data Factory (ADF)** project demonstrating real-world data engineering patterns — REST API ingestion, on-premises file ingestion, incremental loading, watermark management, orchestration, alerting, audit logging, and REST API pagination.

<p align="center">
  <img src="https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=microsoftsqlserver&logoColor=white" />
  <img src="https://img.shields.io/badge/ADLS%20Gen2-00758F?style=for-the-badge&logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/Data%20Engineering-2E8B57?style=for-the-badge" />
</p>

---

## 📚 Medium Blog Series — Incremental Data Loading

I've documented the incremental loading concepts and implementation behind this project in a **4-part Medium series**. Click a button below to jump straight to the article.

<p align="center">
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-d23921dfff28">
    <img src="https://img.shields.io/badge/Part%201-Incremental%20Loading-black?style=for-the-badge&logo=medium&logoColor=white" />
  </a>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-98fa8399da19">
    <img src="https://img.shields.io/badge/Part%202-Incremental%20Loading-black?style=for-the-badge&logo=medium&logoColor=white" />
  </a>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-7b739c6b9c45">
    <img src="https://img.shields.io/badge/Part%203-Incremental%20Loading-black?style=for-the-badge&logo=medium&logoColor=white" />
  </a>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-a2f41100ea6c">
    <img src="https://img.shields.io/badge/Part%204-Incremental%20Loading-black?style=for-the-badge&logo=medium&logoColor=white" />
  </a>
</p>

> 💡 For the incremental pipelines in this repository, start with **Part 1** and follow the series sequentially.

---

## 🏗️ Project Overview

This project contains **7 Azure Data Factory pipelines**, each covering a common real-world data engineering scenario.

| # | Pipeline | Key Concept |
|---|----------|-------------|
| 1 | `pl_API_Ingestion` | REST API → ADLS Gen2 |
| 2 | `pl_OnPremFiles_To_AzureBlobBronze` | On-Prem Files → ADLS Gen2 |
| 3 | `pl_SQLtoAzure_Incremental_Json` | Incremental Load + JSON Watermark |
| 4 | `pl_dailydata_load_with_alerts` | Orchestration + Logic App Alerts |
| 5 | `pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable` | Multi-Table Incremental Load |
| 6 | `pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable_ScriptActivity` | Incremental Load + Audit Logging |
| 7 | `pl_paginationExample` | REST API Pagination |

---

### 🔹 Pipeline 1 — API Ingestion
**`pl_API_Ingestion`**

Pulls `DimAirport.json` from GitHub Raw content into the ADLS Gen2 Bronze layer.

```
GitHub Raw → Copy API Data → ADLS Gen2 Bronze
```

- **Source:** `ds_APISource` → `ls_githubAPI_source` → HttpServer / Anonymous → `DimAirport.json`
- **Sink:** `ds_github_sink` → `ls_bronze` → ADLS Gen2 → `bronze/github/DimAirport.json`
- **Key Settings:** `JsonSource` with `HttpReadSettings`, HTTP GET, `JsonSink` with `AzureBlobFSWriteSettings`, `retry = 2`, `retryIntervalInSeconds = 30`, `enableStaging = false`

---

### 🔹 Pipeline 2 — On-Premises Files to ADLS Bronze
**`pl_OnPremFiles_To_AzureBlobBronze`**

Migrates 3 CSV files in parallel from an on-premises Windows file server to ADLS Gen2 Bronze: `DimAirline.csv`, `DimFlight.csv`, `DimPassenger.csv`.

```
On-Prem Windows File Server → ForEach (Parallel) → Copy Activity → ADLS Gen2 Bronze
```

- **Pipeline Parameter:** `file_name` — e.g. `[{"fileName": "DimAirline.csv"}, {"fileName": "DimFlight.csv"}, {"fileName": "DimPassenger.csv"}]`
- **Key Expressions:** `@pipeline().parameters.file_name`, `@item().fileName`, `@dataset().p_fileName_source`, `@dataset().p_fileName_sink`
- **Source:** `ds_OnPremSource` → `ls_onPrem_Files` → FileServer / IROnPremise → `E:\OnPremiseFiles\{fileName}`
- **Sink:** `ds_onPremSink` → `ls_bronze` → ADLS Gen2 → `bronze/OnPremDataFiles/{fileName}`

---

### 🔹 Pipeline 3 — Incremental Load Using JSON Watermark
**`pl_SQLtoAzure_Incremental_Json`**

Performs an incremental load from Azure SQL `FactBookings` to ADLS Gen2 in Parquet format, using a JSON file as the watermark store.

```
LastLoad Lookup ─┐
                  ├─→ Copy SQL Data → Parquet
LatestLoad Lookup ┘
                        ↓
                 Update Watermark
```

- **Last Load:** reads `lastload.json` from Azure Blob to get the last processed `booking_date`
- **Latest Load:** `SELECT MAX(booking_date) AS LatestLoad FROM dbo.FactBookings;`
- **Incremental Query:**
  ```sql
  SELECT * FROM dbo.FactBookings
  WHERE booking_date > '@{activity('LastLoad').output.firstRow.lastload}'
  AND booking_date <= '@{activity('LatestLoad').output.firstRow.LatestLoad}';
  ```
- **Sink:** Azure SQL → Copy Activity → ADLS Gen2 → `Bronze/sql/` → Parquet + Snappy compression
- **Watermark Update:** the latest watermark is written back to `lastload.json`

📖 *Detailed explanation in Part 1 of the Medium series above.*

---

### 🔹 Pipeline 4 — Daily Orchestration & Alerts
**`pl_dailydata_load_with_alerts`**

Orchestrator pipeline that calls the incremental pipeline and reports execution status to an Azure Logic App.

```
DailyDataLoad → Execute Pipeline → pl_SQLtoAzure_Incremental_Json → Call Logic App
```

- **Execute Pipeline:** calls `pl_SQLtoAzure_Incremental_Json` with `waitOnCompletion = true`
- **Logic App Trigger:** fires on both ✅ Success and ❌ Failure
- **Payload:**
  ```json
  {
    "pipeline_name": "@{pipeline().Pipeline}",
    "run_id": "@{pipeline().RunId}",
    "status": "@{activity('DailyDataLoad').Status}",
    "error": "@{if(equals(activity('DailyDataLoad').Status,'Failed'), string(activity('DailyDataLoad').error), 'No Error')}"
  }
  ```

---

### 🔹 Pipeline 5 — Multi-Table Incremental Load
**`pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable`**

Multi-table incremental loading from On-Premises SQL Server to Azure SQL using a centralized watermark table, for `dbo.iplteams` and `dbo.salesitems`, both keyed on a `last_updated` watermark column.

```
ForEach Table
     ↓
Old Watermark ─┐
               ├→ If Condition ─→ TRUE → Copy → Update Watermark
New Watermark ─┘                → FALSE → Skip
```

- **Old Watermark:** `SELECT WatermarkValue FROM watermarktable WHERE TableName = '@{item().TableName}';`
- **New Watermark:** `SELECT MAX(last_updated) AS NewWatermarkValue FROM @{item().TableName};`
- **If Condition:** `@greater(activity('New_LookUp_Col').output.firstRow.NewWatermarkValue, activity('Old_LookUp_Col').output.firstRow.WatermarkValue)`
- **Dynamic SQL** built from table schema, table name, watermark column, old and new watermark values
- **Sink:** `AzureSqlSink`, `writeBehavior = upsert`, `useTempDB = true`, `keys = [id]`
- **Watermark Update:** stored procedure `[dbo].[usp_write_watermark]` with parameters `last_updated`, `tableName`

📖 *Detailed explanation in the Medium series above.*

---

### 🔹 Pipeline 6 — Incremental Load + Audit Logging
**`pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable_ScriptActivity`**

An enhanced version of Pipeline 5 adding execution timing, a Script Activity for watermark updates, success/failure audit logging, rows-copied tracking, and error tracking.

```
Set Start Time → Old Watermark Lookup → New Watermark Lookup → If Condition
                                                                     ↓
                                                          Copy → Success/Failed → Audit
```

- **Set Start Time:** `PipelineStartTime = @utcNow()`
- **Script Activity — Update Watermark:**
  ```sql
  UPDATE dbo.watermarktable
  SET WatermarkValue = @last_updated
  WHERE TableName = @tableName;
  ```
- **Success/Failure Audit** writes to `dbo.PipelineAudit`: `PipelineName`, `TableName`, `SourceName`, `TargetName`, `WatermarkOld`, `WatermarkNew`, `RowsCopied`, `Status`, `ErrorMessage`, `StartTime`, `EndTime`
- **Key Expressions:** `@pipeline().Pipeline`, `@item().TableName`, `@activity('Copy_FromOnPremSQL_ToAzureSQL').output.rowsCopied`, `@activity('Copy_FromOnPremSQL_ToAzureSQL').error.message`, `@variables('PipelineStartTime')`, `@utcNow()`
- **Evolution from Pipeline 5:** the old `SP_Update_WaterMark` activity is now inactive — watermark updates are handled by the Script Activity instead

📖 *Detailed explanation in the Medium series above.*

---

### 🔹 Pipeline 7 — REST API Pagination
**`pl_paginationExample`**

Ingests Pokémon data from the PokéAPI across multiple pages using ADF's native `RANGE` pagination.

```
PokéAPI → Web Activity → Get Total Count → Copy Activity → RANGE Pagination → ADLS Gen2 Bronze
```

- **Initial Request:** `https://pokeapi.co/api/v2/pokemon?limit=20&offset=0`
- **Pagination Rule:** `QueryParameters.offset = RANGE:0:@{activity('Get API Data').output.count}:20` → generates `offset=0, 20, 40, ... 1300`
- **Sink:** `ds_API_Target_JSON` → ADLS Gen2 → `bronze/PaginationExample/`

---

## 🎯 Key ADF Concepts Demonstrated

`REST API ingestion` · `ADLS Gen2` · `On-Premises File Server` · `Self-hosted Integration Runtime` · `ForEach activity` · `Parallel processing` · `Pipeline parameters` · `Dataset parameters` · `Dynamic expressions` · `Incremental loading` · `JSON watermark` · `Watermark table` · `Azure SQL Upsert` · `Execute Pipeline` · `Web Activity` · `Logic App integration` · `Script Activity` · `Audit logging` · `Error handling` · `REST API pagination` · `RANGE pagination`

---

## 📚 Medium Articles

<p align="center">
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-d23921dfff28">
    <img src="https://img.shields.io/badge/📖%20Part%201-Read%20on%20Medium-02b875?style=for-the-badge" />
  </a><br/>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-98fa8399da19">
    <img src="https://img.shields.io/badge/📖%20Part%202-Read%20on%20Medium-02b875?style=for-the-badge" />
  </a><br/>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-7b739c6b9c45">
    <img src="https://img.shields.io/badge/📖%20Part%203-Read%20on%20Medium-02b875?style=for-the-badge" />
  </a><br/>
  <a href="https://medium.com/@jc.prabhaswork/azure-data-factory-incremental-data-loading-series-a2f41100ea6c">
    <img src="https://img.shields.io/badge/📖%20Part%204-Read%20on%20Medium-02b875?style=for-the-badge" />
  </a>
</p>

---

## 💼 Interview Summary

I built multiple Azure Data Factory pipelines covering real-world data engineering scenarios — REST API ingestion, parallel on-premises file migration, watermark-based incremental loading, multi-table processing using ForEach, Azure SQL upserts, Logic App alerts, Script Activity-based watermark management, audit logging, and REST API pagination.

---

## 🔗 Repository

[github.com/Prabhas92/ADFProject](https://github.com/Prabhas92/ADFProject)

## 👨‍💻 Author

**Chethan Prabhas**

Azure Data Factory • SQL • Azure • Python • Data Engineering
