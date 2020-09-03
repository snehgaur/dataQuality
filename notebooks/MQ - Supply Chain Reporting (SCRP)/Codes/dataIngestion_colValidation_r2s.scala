// Databricks notebook source
dbutils.widgets.text("source_layer", "")
val source_layer = dbutils.widgets.get("source_layer")
dbutils.widgets.text("business_name", "")
val business_name = dbutils.widgets.get("business_name")
dbutils.widgets.text("loaded_timeStamp", "")
val loaded_timeStamp = dbutils.widgets.get("loaded_timeStamp")
dbutils.widgets.text("source_name", "")
val source_name = dbutils.widgets.get("source_name")
dbutils.widgets.text("object_name","")
val object_name = dbutils.widgets.get("object_name")
dbutils.widgets.text("stage_path","")
val stage_path = dbutils.widgets.get("stage_path")
dbutils.widgets.text("stage_object_name","")
val stage_object_name = dbutils.widgets.get("stage_object_name")
dbutils.widgets.text("rejected_path","")
val rejected_path = dbutils.widgets.get("rejected_path")
dbutils.widgets.text("rejected_object_name","")
val rejected_object_name = dbutils.widgets.get("rejected_object_name")
dbutils.widgets.text("raw_object_name","")
val raw_object_name = dbutils.widgets.get("raw_object_name")
dbutils.widgets.text("landingrawpath", "")
val landingrawpath = dbutils.widgets.get("landingrawpath")
dbutils.widgets.text("srvc_control_table","")
val srvc_control_table = dbutils.widgets.get("srvc_control_table")
dbutils.widgets.text("src_control_table","")
val src_control_table = dbutils.widgets.get("src_control_table")
dbutils.widgets.text("adls_storage_account_name","")
val adls_storage_account_name = dbutils.widgets.get("adls_storage_account_name")
dbutils.widgets.text("adls_container_name","")
val adls_container_name = dbutils.widgets.get("adls_container_name")
dbutils.widgets.text("file_format","")
val file_format = dbutils.widgets.get("file_format") 
dbutils.widgets.text("filename","")
val filename = dbutils.widgets.get("filename")
dbutils.widgets.text("filelog_tablename","")
val filelog_tablename = dbutils.widgets.get("filelog_tablename")

dbutils.widgets.text("pipeline_name","")
val pipeline_name = dbutils.widgets.get("pipeline_name")
dbutils.widgets.text("pipeline_runid","")
val pipeline_runid = dbutils.widgets.get("pipeline_runid")
dbutils.widgets.text("pipeline_starttime","")
val pipeline_starttime = dbutils.widgets.get("pipeline_starttime")

dbutils.widgets.text("column_count","")
val column_count = dbutils.widgets.get("column_count")
dbutils.widgets.text("process_runlog","")
val process_runlog = dbutils.widgets.get("process_runlog")
dbutils.widgets.text("audit_log","")
val audit_log = dbutils.widgets.get("audit_log")

// COMMAND ----------

val rawtablename = s"supplychain_raw.${raw_object_name}"
val stagetablename = s"supplychain_stage.${stage_object_name}"
//val rawtablename = "supplychain_raw.5d_past_due_preq"
//val stagetablename = "supplychain_stage.5d_past_due_preq"
//val audittablename = "supplychain_cntrl_mdt.audit_log"
//val filelog_deltatablename = "supplychain_cntrl_mdt.file_cntrl_mdt"

// COMMAND ----------

import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.Row
import scala.collection.mutable.MultiMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.lit
import scala.collection.Seq
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf;
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import java.lang.String
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.sql.DataFrameReader
import scala.reflect.io.Path
import java.io.File
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.apache.spark.sql.types.IntegerType
import scala.util.matching.Regex
import java.util.regex.Pattern
import scala.collection.immutable.Map
import collection.mutable.{ HashMap, MultiMap, Set }
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,ADLS Configuration
val source_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-elancoread")
val key=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "ADLS-MQ-SupplyChain")
val secret=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "Elanco-MQ-NonProd-SCRP-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-ClientID")
val tenantid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-TenantID")

spark.conf.set("fs.azure.account.key.bdaze1imqdl01.blob.core.windows.net",key)
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

val source_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"
val pswd=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "ADLS-MQ-SupplyChain")
val secret=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "Elanco-MQ-NonProd-SCRP-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-ClientID")
val tenantid=dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsServicePrincipal01-TenantID")
val emp_raw = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/"
val cntrl_metadata = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"control_metadata/"
val path_cntrl_table = "cntrl_metadata/file_cntrl_mdt"

// COMMAND ----------

// DBTITLE 1,JDBC Connection
val jdbcUsername = dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-username")
val jdbcPassword = dbutils.secrets.get(scope = "AKV_SUPPLYCHAIN", key = "DataOpsDB-elancoread")
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
val jdbcHostname = "dataops-dev.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "diqframework"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)
val df_filelog_tablename = spark.read.jdbc(jdbcUrl, s"$filelog_tablename", connectionProperties)
val df_src_control_table = spark.read.jdbc(jdbcUrl, s"$src_control_table", connectionProperties)
val df_process_runlog = spark.read.jdbc(jdbcUrl, s"$process_runlog", connectionProperties)
val df_audit_log = spark.read.jdbc(jdbcUrl, s"$audit_log", connectionProperties)

// COMMAND ----------

val filelog_tablename_check= df_filelog_tablename.select(
   col("BusinessName").as("business_name")
  ,col("ObjectName").as("object_name")
  ,col("FileName").as("file_name")
  ,col("ColumnCount").as("column_count")
  ,col("LoadedTimeStamp").as("load_timestamp")).filter(col("business_name")===(s"${business_name}") && col("object_name")===(s"${object_name}"))

// COMMAND ----------

val process_runlog_check= df_process_runlog.select(
   col("ServiceCategory").as("service_category")
  ,col("BusinessName").as("business_name")
  ,col("ObjectName").as("object_name")
  ,col("FileName").as("file_name")
  ,col("ColumnCount").as("column_count")
  ,col("LoadFormat").as("load_format")
  ,col("Status").as("status")
  ,col("RawFileLoadedTimeStamp").as("raw_load_datetime")
  ,col("ProcessRunLoadTimestamp").as("load_timestamp")).filter(col("business_name")===(s"${business_name}") && col("object_name")===(s"${object_name}") && col("status")==="Success").createOrReplaceTempView("tb_process_runlog_check")

// COMMAND ----------

val filelog_process_runlog_check= spark.sql(s"select service_category, business_name, object_name,file_name, column_count, max(raw_load_datetime) as max_raw_load_datetime, max(load_timestamp)as max_load_datetime from tb_process_runlog_check where service_category like 'Data_Ingestion' group by service_category, business_name, object_name,file_name, column_count")

// COMMAND ----------

filelog_process_runlog_check.show(false)

// COMMAND ----------

val val_filter = filelog_process_runlog_check.select(col("max_raw_load_datetime"))
val final_val_filter = if(!val_filter.rdd.isEmpty()) (val_filter.head(1)(0)(0))
val final_frame= (if (!val_filter.rdd.isEmpty()) filelog_tablename_check.where(col("load_timestamp") > final_val_filter) else filelog_tablename_check)

// COMMAND ----------

//final_frame.show(false)
final_frame.createOrReplaceTempView("df_final_frame")
val temp_filename = spark.sql("select file_name from df_final_frame group by file_name")
//temp_filename.show(false)
//filename Name
val filename = if (!final_frame.rdd.isEmpty()) temp_filename.head(1)(0)(0) else dbutils.notebook.exit("No New File -> Job Exit")

// COMMAND ----------

// DBTITLE 1,Column Validation Service Call
val result = dbutils.notebook.run("/MQ - Supply Chain Reporting (SCRP)/Services/dataIngestion_columnValidation/", 60, Map("business_name" -> s"$business_name", "object_name" -> s"$object_name", "control_table" -> s"$srvc_control_table", "adls_storage_account_name" -> s"$adls_storage_account_name", "adls_container_name" -> s"$adls_container_name", "landingrawpath" -> s"$landingrawpath", "file_format" -> s"$file_format", "rawtablename" -> s"$rawtablename", "filename" -> s"$filename", "source_col_count" -> s"$column_count","audit_log" -> s"$audit_log"))

// COMMAND ----------

val df_auditlog_check= df_audit_log.select(
   col("Status").as("status")
  ,col("ServiceName").as("service_name")
  ,col("BusinessName").as("business_name")
  ,col("ObjectName").as("object_name")
  ,col("FileName").as("filename")
  ,col("LoadTimestamp").as("load_datetime")
).createOrReplaceTempView("tb_auditlog_check")

// COMMAND ----------

//Extract Latest Column Validation Values
val validate_colVal_status = spark.sql(s"""select trim(status) from tb_auditlog_check where service_name = "ColumnNameValidation" and business_name = "$business_name" and object_name = "$object_name" and filename = "$filename" ORDER BY load_datetime DESC LIMIT 1""")

val validate_colSizeVal_status = spark.sql(s"""select trim(status) from tb_auditlog_check where service_name = "NoOfColumnValidation" and business_name = "$business_name" and object_name = "$object_name" and filename = "$filename" ORDER BY load_datetime DESC LIMIT 1""")

val colVal_status = if (!validate_colVal_status.rdd.isEmpty()) validate_colVal_status.head(1)(0)(0) else null
val colSizeVal_status = if (!validate_colSizeVal_status.rdd.isEmpty()) validate_colSizeVal_status.head(1)(0)(0) else null

// COMMAND ----------

val df_src_control_table_valid = df_src_control_table.select(
  col("BusinessName")
  ,col("ObjectName")
).filter(col("BusinessName")===(s"${business_name}") && col("ObjectName")===(s"${object_name}"))

val df_filelog_tablename_valid = df_filelog_tablename.select(
  col("BusinessName"),
  col("ObjectName"),
  col("LoadFormat"),
  col("FileName"),
  col("ColumnCount"),
  col("LoadedTimeStamp")
  ).filter(col("BusinessName")===(s"${business_name}") && col("ObjectName")===(s"${object_name}") && col("FileName")===(s"${filename}"))

// COMMAND ----------

// DBTITLE 1,Process Table Entry
val df_join = df_src_control_table_valid.join(df_filelog_tablename_valid,df_src_control_table_valid("ObjectName") ===  df_filelog_tablename_valid("ObjectName")
                                                                    && df_src_control_table_valid("BusinessName") ===  df_filelog_tablename_valid("BusinessName"),"inner").
drop(df_src_control_table_valid("ObjectName")).
drop(df_src_control_table_valid("BusinessName"))

// COMMAND ----------

val val_final_failed = df_join.select(
  lit("Source").as("SourceLayer")
  ,lit("Raw").as("TargetLayer")
  ,lit("Data_Ingestion").as("ServiceCategory")
  ,lit("Column_Validation").as("ServiceName")
  ,lit("Failed").as("Status")
  ,col("BusinessName")
  ,col("ObjectName")
  ,col("FileName")
  ,col("ColumnCount")
  ,lit(s"Failure in Column Validation : Check the Audit Log Table: $audit_log").as("Comment")
  ,col("LoadFormat")
  ,col("LoadedTimeStamp").as("RawFileLoadedTimeStamp")
).withColumn("ProcessRunLoadTimestamp",lit(current_timestamp())).
withColumn("PipelineName", lit(s"$pipeline_name")).
withColumn("PipelineRunId",lit(s"$pipeline_runid")).
withColumn("PipelineStartTime", lit(s"$pipeline_starttime").cast(TimestampType)).
withColumn("PipelineEndTime",lit(current_timestamp())).distinct().createOrReplaceTempView("tb_val_final_failed")

// COMMAND ----------

val val_final = df_join.select(
  lit("Source").as("SourceLayer")
  ,lit("Raw").as("TargetLayer")
  ,lit("Data_Ingestion").as("ServiceCategory")
  ,lit("Column_Validation").as("ServiceName")
  ,lit("Success").as("Status")
  ,col("BusinessName")
  ,col("ObjectName")
  ,col("FileName")
  ,col("ColumnCount")
  ,lit("Column Validation : Completed Successfully").as("Comment")
  ,col("LoadFormat")
  ,col("LoadedTimeStamp").as("RawFileLoadedTimeStamp")
).withColumn("ProcessRunLoadTimestamp",lit(current_timestamp())).
withColumn("PipelineName", lit(s"$pipeline_name")).
withColumn("PipelineRunId",lit(s"$pipeline_runid")).
withColumn("PipelineStartTime", lit(s"$pipeline_starttime").cast(TimestampType)).
withColumn("PipelineEndTime",lit(current_timestamp())).distinct().createOrReplaceTempView("tb_val_final")

// COMMAND ----------

//Validation Check Entry on ProcessRunlog
if (colVal_status =="Pass" && colSizeVal_status =="Pass") spark.table("tb_val_final").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$process_runlog", connectionProperties) else spark.table("tb_val_final_failed").write.mode(SaveMode.Append).jdbc(jdbcUrl, s"$process_runlog", connectionProperties)

// COMMAND ----------

//Validation Check
if (colVal_status =="Pass" && colSizeVal_status =="Pass") "Validation Pass" else dbutils.notebook.exit("Column Mismatch -> Validation Failed- Job Exit")

// COMMAND ----------

val rawdataframe= 
if (colVal_status =="Pass" && colSizeVal_status =="Pass")
if (s"$file_format" == "csv")
spark.read.format("csv").option("header","true").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$landingrawpath/$filename")
else
spark.read.format("avro").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/$landingrawpath/$filename")
else
null
//rawdataframe.printSchema()

// COMMAND ----------

val final_rawdataframe= if (colVal_status =="Pass" && colSizeVal_status =="Pass") rawdataframe.withColumn("file_name",lit(s"${filename}")).withColumn("fileloaded_timestamp",lit(s"${loaded_timeStamp}").cast(TimestampType)).withColumn("load_datetime",lit(current_timestamp())).withColumn("datepart",lit(current_date())) else null

// COMMAND ----------

if (colVal_status =="Pass" && colSizeVal_status =="Pass") 
final_rawdataframe.write.mode(SaveMode.Append).insertInto(s"${rawtablename}") else null