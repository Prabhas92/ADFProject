# ADFProject
# Pipeline 1: pl_API_Ingestion
What it does: Pulls DimAirport.json from GitHub Raw content into ADLS Gen2 Bronze layer.
Flow: Copy API Data (single Copy Activity)
Source: ds_APISource → ls_githubAPI_source (HttpServer, Anonymous) → raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/.../DimAirport.json
Sink: ds_github_sink → ls_bronze (ADLS Gen2) → bronze/github/DimAirport.json
Key settings:

JsonSource with HttpReadSettings, GET method
JsonSink with AzureBlobFSWriteSettings
retry=2, retryIntervalInSeconds=30
enableStaging=false

# Pipeline 2: pl_OnPremFiles_To_AzureBlobBronze
What it does: Migrates 3 CSV files in parallel from on-premise Windows file server to ADLS Gen2.
Flow: ForEachOnPremFiles (parallel) → Copy data1 (inside each iteration)
Parameter: file_name array = [{fileName: DimAirline.csv}, {fileName: DimFlight.csv}, {fileName: DimPassenger.csv}]
Key expressions:
@pipeline().parameters.file_name    → ForEach items source
@item().fileName                    → Current iteration's file name
@dataset().p_fileName_source        → Passed to source dataset at runtime
@dataset().p_fileName_sink          → Passed to sink dataset at runtime
Source: ds_OnPremSource → ls_onPrem_Files (FileServer, IROnPremise) → E:\OnPremiseFiles{fileName}
Sink: ds_onPremSink → ls_bronze (ADLS Gen2) → bronze/OnPremDataFiles/{fileName}

# Pipeline 3: pl_SQLtoAzure_Incremental_Json
What it does: Incremental load from Azure SQL FactBookings table to ADLS Gen2 Parquet using a JSON file as the watermark store.
Flow:
LastLoad (Lookup)  ──┐
                      ├─ both Succeeded → Copy SQL Data → Watermark (Copy)
LatestLoad (Lookup) ──┘
LastLoad: Reads lastload.json from Azure Blob → gets last processed booking_date
LatestLoad: SELECT MAX(booking_date) AS LatestLoad FROM dbo.FactBookings
Copy SQL Data query:
sqlSELECT * FROM dbo.FactBookings
WHERE booking_date > '@{activity('LastLoad').output.firstRow.lastload}'
AND booking_date <= '@{activity('LatestLoad').output.firstRow.LatestLoad}'
Sink: ParquetSink → ADLS Bronze/sql/ (Snappy compression)
Watermark Update (the clever bit):

Source: ds_emptyjson (reads empty.json — no real data)
additionalColumns: injects lastload = @activity('LatestLoad').output.firstRow.LatestLoad
Sink: overwrites lastload.json with the new watermark value

# Pipeline 4: pl_dailydata_load_with_alerts
What it does: Orchestrator that calls the incremental pipeline and always fires a Logic App alert.
Flow:
DailyDataLoad (ExecutePipeline) → Call Logic App (WebActivity)
DailyDataLoad: Calls pl_SQLtoAzure_Incremental_Json, waitOnCompletion=true
Call Logic App: HTTP POST to Azure Logic App — dependency: ['Succeeded', 'Failed']
Payload sent to Logic App:
json{
  "pipeline_name": "@{pipeline().Pipeline}",
  "run_id": "@{pipeline().RunId}",
  "status": "@{activity('DailyDataLoad').Status}",
  "error": "@{if(equals(activity('DailyDataLoad').Status,'Failed'), string(activity('DailyDataLoad').error), 'No Error')}"
}

# Pipeline 5: pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable
What it does: Multi-table CDC from On-Prem SQL to Azure SQL with watermark table, IfCondition guard, and stored procedure update.
Tables: dbo.iplteams, dbo.salesitems (both have last_updated as watermark column)
Flow inside ForEach:
Old_LookUp_Col ──┐
                  ├─ both Succeeded → IfCondition → [if TRUE] → Copy → SP_Update_WaterMark
New_LookUp_Col ──┘                                 [if FALSE] → skip
Old_LookUp_Col:
sqlSELECT WatermarkValue FROM watermarktable WHERE TableName = '@{item().TableName}'
New_LookUp_Col:
sqlSELECT MAX(last_updated) AS NewWatermarkValue FROM @{item().TableName}
IfCondition expression:
@greater(
  activity('New_LookUp_Col').output.firstRow.NewWatermarkValue,
  activity('Old_LookUp_Col').output.firstRow.WatermarkValue
)
Copy dynamic SQL:
@concat(
  'SELECT * FROM ', item().TableSchemaName, '.', item().TableName,
  ' WHERE ', item().WaterMark_Column,
  ' > ''', formatDateTime(Old.WatermarkValue,'yyyy-MM-dd HH:mm:ss'),
  ''' AND ', item().WaterMark_Column,
  ' <= ''', formatDateTime(New.NewWatermarkValue,'yyyy-MM-dd HH:mm:ss'), ''''
)
Sink: AzureSqlSink — writeBehavior=upsert, useTempDB=true, keys=[id]
SP_Update_WaterMark: [dbo].[usp_write_watermark] with params last_updated, tableName

# Pipeline 6: pl_onPremSQl_To_AzureSQL_Incremental_WatermarkTable_ScriptActivity
What it does: Evolution of Pipeline 5. Adds SetVariable for timing, Script activity replacing SP, and dual audit logging (success + failure).
Additional elements:
SetVariable — Set Start Time (runs first):
PipelineStartTime = @utcNow()
Script Activity — UpdateWatermark Using SQL:
sqlUPDATE dbo.watermarktable
SET WatermarkValue = @last_updated
WHERE TableName = @tableName;
AuditLog Script (Success branch):
sqlINSERT INTO dbo.PipelineAudit (PipelineName, TableName, SourceName, TargetName,
WatermarkOld, WatermarkNew, RowsCopied, Status, ErrorMessage, StartTime, EndTime)
VALUES (@PipelineName, @TableName, @SourceName, @TargetName, @OldWatermark, 
@NewWatermark, @RowsCopied, @Status, @ErrorMessage, @StartTime, @EndTime);
AuditLog_copy Script (Failure branch — runs when Copy fails):

Same INSERT but Status='Failed', ErrorMessage=@activity('Copy_...').error.message, RowsCopied=0

SP_Update_WaterMark: state=Inactive, onInactiveMarkAs=Succeeded — superseded by Script Activity
Key expressions in audit:
@pipeline().Pipeline                                              → Pipeline name
@item().TableName                                                 → Current table
@activity('Copy_FromOnPremSQL_ToAzureSQL').output.rowsCopied     → Rows copied
@activity('Copy_FromOnPremSQL_ToAzureSQL').error.message         → Error text
@variables('PipelineStartTime')                                   → Run start time
@utcNow()                                                         → End time

# Pipeline 7: pl_paginationExample
What it does: Ingests all Pokémon from PokéAPI across multiple pages using ADF's native RANGE pagination.
Flow:
Get API Data (WebActivity GET) → Copy data1 (Copy with REST pagination)
Get API Data: GET https://pokeapi.co/api/v2/pokemon?limit=20&offset=0 → returns JSON with count field (total Pokémon, e.g. 1302)
Copy data1 pagination rule:
QueryParameters.offset = RANGE:0:@{activity('Get API Data').output.count}:20
This generates: offset=0, offset=20, offset=40, ..., offset=1300 — one HTTP request per step
Sink: ds_API_Target_JSON → Azure Blob bronze/PaginationExample/
