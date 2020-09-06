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

dbutils.widgets.text("process_runlogId","")
val process_runlogId = dbutils.widgets.get("process_runlogId")

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

val scratchpad_dqdt_obj_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/Sratchpad/"+s"$object_name"+s"/dqdt_$object_name"
val scratchpad_dqpk_obj_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/Sratchpad/"+s"$object_name"+s"/dqpk_$object_name"

val scratchpad_dctm_obj_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/Sratchpad/"+s"$object_name"+s"/dctm_$object_name"
val scratchpad_dcdd_obj_path = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/Sratchpad/"+s"$object_name"+s"/dcdd_$object_name"

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

val stagetablename = s"supplychain_stage_db.${stage_object_name}"

// COMMAND ----------

// DBTITLE 1,raw2stage_dataCleansing_trim
dbutils.notebook.run("/MQ - Supply Chain Reporting (SCRP)/Codes/raw2stage_dataCleansing_trim/", 1200, Map("process_runlogId" -> s"$process_runlogId","source_layer" -> s"$source_layer", "business_name" -> s"$business_name", "loaded_timeStamp" -> s"$loaded_timeStamp", "source_name" -> s"$source_name", "object_name" -> s"$object_name", "stage_path" -> s"$stage_path", "stage_object_name" -> s"$stage_object_name", "rejected_path" -> s"$rejected_path","rejected_object_name" -> s"$rejected_object_name", "raw_object_name" -> s"$raw_object_name","landingrawpath" -> s"$landingrawpath", "srvc_control_table" -> s"$srvc_control_table","src_control_table" -> s"$src_control_table", "adls_storage_account_name" -> s"$adls_storage_account_name", "adls_container_name" -> s"$adls_container_name","file_format" -> s"$file_format", "filename" -> s"$filename", "filelog_tablename" -> s"$filelog_tablename", "pipeline_name" -> s"$pipeline_name","pipeline_runid" -> s"$pipeline_runid","pipeline_starttime" -> s"$pipeline_starttime", "column_count" -> s"$column_count", "process_runlog" -> s"$process_runlog", "audit_log" -> s"$audit_log","scratchpad_dqpk_obj_path" -> s"$scratchpad_dqpk_obj_path","scratchpad_dctm_obj_path" -> s"$scratchpad_dctm_obj_path"))

// COMMAND ----------

// DBTITLE 1,raw2stage_dataCleansing_deDup
dbutils.notebook.run("/MQ - Supply Chain Reporting (SCRP)/Codes/raw2stage_dataCleansing_deDup/", 1200, Map("process_runlogId" -> s"$process_runlogId","source_layer" -> s"$source_layer", "business_name" -> s"$business_name", "loaded_timeStamp" -> s"$loaded_timeStamp", "source_name" -> s"$source_name", "object_name" -> s"$object_name", "stage_path" -> s"$stage_path", "stage_object_name" -> s"$stage_object_name", "rejected_path" -> s"$rejected_path","rejected_object_name" -> s"$rejected_object_name", "raw_object_name" -> s"$raw_object_name","landingrawpath" -> s"$landingrawpath", "srvc_control_table" -> s"$srvc_control_table","src_control_table" -> s"$src_control_table", "adls_storage_account_name" -> s"$adls_storage_account_name", "adls_container_name" -> s"$adls_container_name","file_format" -> s"$file_format", "filename" -> s"$filename", "filelog_tablename" -> s"$filelog_tablename", "pipeline_name" -> s"$pipeline_name","pipeline_runid" -> s"$pipeline_runid","pipeline_starttime" -> s"$pipeline_starttime", "column_count" -> s"$column_count", "process_runlog" -> s"$process_runlog", "audit_log" -> s"$audit_log","scratchpad_dctm_obj_path" -> s"$scratchpad_dctm_obj_path","scratchpad_dcdd_obj_path" -> s"$scratchpad_dcdd_obj_path"))

// COMMAND ----------

// DBTITLE 1,Writing to Stage Table
val finaldf = sqlContext.read.parquet(scratchpad_dcdd_obj_path)

// COMMAND ----------

finaldf.write.format("avro").mode("append").insertInto(s"${stagetablename}")